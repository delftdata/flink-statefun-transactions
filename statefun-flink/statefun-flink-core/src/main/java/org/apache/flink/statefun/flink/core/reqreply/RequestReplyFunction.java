/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.flink.core.reqreply;

import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.polyglotAddressToSdkAddress;
import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.sdkAddressToPolyglotAddress;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.generated.ResponseToTransactionFunction;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.EgressMessage;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.BatchDetails;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.BatchDetailsList;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.TransactionDetails;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.Invocation;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.InvocationBatchRequest;
import org.apache.flink.statefun.sdk.*;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RequestReplyFunction implements StatefulFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestReplyFunction.class);

  private final RequestReplyClient client;
  private final int maxNumBatchRequests;

  /**
   * A request state keeps tracks of the number of inflight & batched requests.
   *
   * <p>A tracking state can have one of the following values:
   *
   * <ul>
   *   <li>NULL - there is no inflight request, and there is nothing in the backlog.
   *   <li>0 - there's an inflight request, but nothing in the backlog.
   *   <li>{@code > 0} There is an in flight request, and @requestState items in the backlog.
   * </ul>
   */
  @Persisted
  private final PersistedValue<Integer> requestState =
      PersistedValue.of("request-state", Integer.class);
  @Persisted
  private final PersistedValue<Boolean> tpcInFlight =
          PersistedValue.of("tpc-in-flight", Boolean.class);
  @Persisted
  private final PersistedValue<Boolean> sagasInFlight =
          PersistedValue.of("sagas-in-flight", Boolean.class);
  @Persisted
  private final PersistedAppendingBuffer<TransactionDetails> transactionDetailsInFlight =
          PersistedAppendingBuffer.of("transaction-details-in-flight", TransactionDetails.class);
  @Persisted
  private final PersistedValue<BatchDetailsList> batches =
      PersistedValue.of("batches", BatchDetailsList.class);
  @Persisted
  private final PersistedRemoteFunctionValues managedStates;
  @Persisted
  private final PersistedValue<Boolean> locked =
          PersistedValue.of("locked", Boolean.class);
  @Persisted
  private final PersistedValue<FromFunction.InvocationResponse> tpcTransactionResult =
          PersistedValue.of("transaction-result", FromFunction.InvocationResponse.class);

  public RequestReplyFunction(
      PersistedRemoteFunctionValues managedStates,
      int maxNumBatchRequests,
      RequestReplyClient client) {
    this.managedStates = Objects.requireNonNull(managedStates);
    this.client = Objects.requireNonNull(client);
    this.maxNumBatchRequests = maxNumBatchRequests;
  }

  @Override
  public void invoke(Context context, Object input) {
    InternalContext castedContext = (InternalContext) context;
    if (!(input instanceof AsyncOperationResult)) {
      if (context.isTransaction()) {
        onTransactionRequest(castedContext, (Any) input);
      } else {
        onRegularRequest(castedContext, (Any) input);
      }
      return;
    }
    @SuppressWarnings("unchecked")
    AsyncOperationResult<ToFunction, FromFunction> result =
        (AsyncOperationResult<ToFunction, FromFunction>) input;
    onAsyncResult(castedContext, result);
  }

  private void onRegularRequest(InternalContext context, Any message) {
    Invocation.Builder invocationBuilder = singeInvocationBuilder(context, message);
    if (requestState.getOrDefault(-1) < 0 && !locked.getOrDefault(false)) {
      // no inflight requests, and nothing in the batch and no lock.
      // so we let this request to go through, and change state to indicate that:
      // a) there is a request in flight.
      // b) there is nothing in the batch.
      requestState.set(0);
      // LOGGER.info("Sending out regular invocation to function: " + context.self().type().toString());
      sendToFunction(context, invocationBuilder);
      return;
    }
    // there is at least one request in flight (inflightOrBatched >= 0),
    // so we add that request to the batch.
    addRegularInvocationToBatch(context, invocationBuilder);
  }

  private void onTransactionRequest(InternalContext context, Any message) {
    Invocation.Builder invocationBuilder = singeInvocationBuilder(context, message);

    String currentTransactionId = null;
    if (transactionDetailsInFlight.view().iterator().hasNext()) {
      TransactionDetails currentTransactionDetails = transactionDetailsInFlight.view().iterator().next();
      currentTransactionId = currentTransactionDetails.getTransactionId();
    }

    // Handle active transaction
    if (currentTransactionId != null &&
            context.getTransactionId().equals(currentTransactionId) &&
            locked.getOrDefault(false)) {
      if (context.getTransactionMessage().equals(Context.TransactionMessage.ABORT)) {
        cleanUpAfterTransaction();
        if (!tpcInFlight.getOrDefault(false)) {
          continueProcessingBatchedRequests(context);
        }
      } else if (context.getTransactionMessage().equals(Context.TransactionMessage.COMMIT)) {
        InvocationResponse response = tpcTransactionResult.get();
        cleanUpAfterTransaction();
        if (response != null) {
          handleInvocationResponse(context, response);
        }
        continueProcessingBatchedRequests(context);
      }
      return;
    }

    // Handled aborted transaction still in queue
    if (context.getTransactionMessage().equals(Context.TransactionMessage.ABORT)) {
      removeTpcInvocationFromQueueIfQueued(context);
      return;
    }

    // Handle new SAGAs invocation
    if (context.getTransactionMessage().equals(Context.TransactionMessage.SAGAS)) {
      // LOGGER.info("Received SAGAs invocation for remote function: " + context.self().type().toString());
      if (requestState.getOrDefault(-1) < 0 && !locked.getOrDefault(false)) {
        TransactionDetails transactionDetails = getTransactionDetailsBuilder(context).setIndex(0).build();
        List<TransactionDetails> l = new ArrayList<>();
        l.add(transactionDetails);
        InvocationBatchRequest.Builder batchBuilder = InvocationBatchRequest.newBuilder();
        batchBuilder.addInvocations(invocationBuilder);
        startSagasBatch(context, batchBuilder, l);
        requestState.set(0);
      } else {
        addSagasInvocationToBatch(context, invocationBuilder);
      }
      return;
    }

    // Handle new transaction prepare
    if (context.getTransactionMessage().equals(Context.TransactionMessage.PREPARE)) {
      // LOGGER.info("Received transaction prepare invocation for remote function: " + context.self().type().toString());
      if (requestState.getOrDefault(-1) < 0 && !locked.getOrDefault(false)) {
        startTpcTransaction(context, invocationBuilder.build(), getTransactionDetailsBuilder(context).build());
        requestState.set(0);
      } else {
        addTpcInvocationToBatch(context, invocationBuilder);
      }
      return;
    }

    LOGGER.info("Received UNEXPECTED transaction invocation for remote function: " + context.self().type().toString());
  }

  private void removeTpcInvocationFromQueueIfQueued(InternalContext context) {
    BatchDetailsList batchesList =
            batches.getOrDefault(BatchDetailsList.getDefaultInstance());
    for (int i = 0; i < batchesList.getBatchesCount(); i++) {
      BatchDetails batchDetails = batchesList.getBatches(i);
      if (batchDetails.getBatchType().equals(BatchDetails.BatchType.TPC)) {
        if (batchDetails.getTransactionDetails(0).getTransactionId().equals(context.getTransactionId())) {
          BatchDetailsList.Builder builder = batchesList.toBuilder();
          builder.removeBatches(i);
          batches.set(builder.build());
          context.functionTypeMetrics().consumeBacklogMessages(1);
          requestState.set(requestState.get() - 1);
          // LOGGER.info("Removed transaction from the queue: " + context.self().type().toString());
        }
      }
    }
  }

  private void addRegularInvocationToBatch(InternalContext context, Invocation.Builder invocationBuilder) {
    BatchDetailsList batchesList =
            batches.getOrDefault(BatchDetailsList.getDefaultInstance());
    BatchDetailsList.Builder builder = batchesList.toBuilder();
    if (batchesList.getBatchesCount() == 0 ||
            batchesList.getBatches(batchesList.getBatchesCount() - 1).getBatchType().equals(BatchDetails.BatchType.TPC)) {
      BatchDetails batchDetails = BatchDetails.newBuilder()
              .setBatchType(BatchDetails.BatchType.REGULAR)
              .addInvocations(invocationBuilder)
              .build();
      builder.addBatches(batchDetails);
    } else {
      int lastBatchIndex = batchesList.getBatchesCount() - 1;
      BatchDetails.Builder batchDetails = batchesList.getBatches(lastBatchIndex).toBuilder();
      batchDetails.addInvocations(invocationBuilder);
      builder.setBatches(lastBatchIndex, batchDetails.build());
    }
    batches.set(builder.build());
    increaseNumBatched(context);
  }

  private void addSagasInvocationToBatch(InternalContext context, Invocation.Builder invocationBuilder) {
    BatchDetailsList batchesList =
            batches.getOrDefault(BatchDetailsList.getDefaultInstance());
    BatchDetailsList.Builder builder = batchesList.toBuilder();
    TransactionDetails.Builder transactionDetailsBuilder = getTransactionDetailsBuilder(context);
    if (batchesList.getBatchesCount() == 0 ||
            batchesList.getBatches(batchesList.getBatchesCount() - 1).getBatchType().equals(BatchDetails.BatchType.TPC)) {
      transactionDetailsBuilder.setIndex(0);
      BatchDetails batchDetails = BatchDetails.newBuilder()
              .setBatchType(BatchDetails.BatchType.REGULAR)
              .addInvocations(invocationBuilder)
              .addTransactionDetails(transactionDetailsBuilder)
              .build();
      builder.addBatches(batchDetails);
    } else {
      int lastBatchIndex = batchesList.getBatchesCount() - 1;
      BatchDetails.Builder batchDetails = batchesList.getBatches(lastBatchIndex).toBuilder();
      int newInvocationIndex = batchDetails.getInvocationsCount();
      transactionDetailsBuilder.setIndex(newInvocationIndex);
      batchDetails
              .addInvocations(invocationBuilder)
              .addTransactionDetails(transactionDetailsBuilder);
      builder.setBatches(lastBatchIndex, batchDetails.build());
    }
    batches.set(builder.build());
    increaseNumBatched(context);
  }

  private void addTpcInvocationToBatch(InternalContext context, Invocation.Builder invocationBuilder) {
    BatchDetailsList batchesList =
            batches.getOrDefault(BatchDetailsList.getDefaultInstance());
    BatchDetailsList.Builder builder = batchesList.toBuilder();
    sendBlockingFunctions(context, invocationBuilder, batchesList);
    TransactionDetails.Builder transactionDetailsBuilder = getTransactionDetailsBuilder(context);
    BatchDetails batchDetails = BatchDetails.newBuilder()
              .addInvocations(invocationBuilder)
              .addTransactionDetails(transactionDetailsBuilder)
              .build();
    builder.addBatches(batchDetails);
    batches.set(builder.build());
    increaseNumBatched(context);
  }

  private void increaseNumBatched(InternalContext context) {
    int inflightOrBatched = requestState.getOrDefault(-1);
    inflightOrBatched++;
    requestState.set(inflightOrBatched);
    context.functionTypeMetrics().appendBacklogMessages(1);
    if (isMaxNumBatchRequestsExceeded(inflightOrBatched)) {
      // LOGGER.info("Function is locked due to maxNumBatchRequests: " + context.self().type().toString());
      context.awaitAsyncOperationComplete();
    }
  }

  private void sendBlockingFunctions(InternalContext context, Invocation.Builder invocationBuilder,
                                     BatchDetailsList batchDetailsList) {
    List<Address> blockingAddresses = new ArrayList<>();
    if (locked.getOrDefault(false)) {
      Address address = polyglotAddressToSdkAddress(
              transactionDetailsInFlight.view().iterator().next().getCoordinatorAddress());
      blockingAddresses.add(address);
    }
    for (BatchDetails currBatchDetails : batchDetailsList.getBatchesList()) {
      if (currBatchDetails.getBatchType().equals(BatchDetails.BatchType.TPC)) {
        Address address = polyglotAddressToSdkAddress(
                currBatchDetails.getTransactionDetails(0).getCoordinatorAddress());
        blockingAddresses.add(address);
      }
    }
    context.sendBlockingFunctions(
            polyglotAddressToSdkAddress(invocationBuilder.getCaller()),
            context.getTransactionId(),
            blockingAddresses);
  }

  private void startSagasBatch(InternalContext context, InvocationBatchRequest.Builder batchBuilder,
                               List<TransactionDetails> transactionDetails) {
//    LOGGER.info("Sending out SAGAS - transaction invocation to function: " + context.self().type().toString() + " " + context.self().id());
    sagasInFlight.set(true);
    transactionDetailsInFlight.appendAll(transactionDetails);
    sendToFunction(context, batchBuilder);
  }

  private void startTpcTransaction(InternalContext context, Invocation invocation,
                                   TransactionDetails transactionDetails) {
    // LOGGER.info("Sending out TPC - transaction invocation to function: " + context.self().type().toString());
    tpcInFlight.set(true);
    locked.set(true);
    transactionDetailsInFlight.append(transactionDetails);
    sendToFunction(context, invocation.toBuilder());
  }

  private void cleanUpAfterTransaction() {
    locked.clear();
    transactionDetailsInFlight.clear();
    tpcTransactionResult.clear();
  }

  private void onAsyncResult(
      InternalContext context, AsyncOperationResult<ToFunction, FromFunction> asyncResult) {
    if (asyncResult.unknown()) {
      ToFunction batch = asyncResult.metadata();
      sendToFunction(context, batch);
      return;
    }

    InvocationResponse invocationResult = unpackInvocationOrThrow(context.self(), asyncResult);

    // Handle response of outgoing TPC invocation
    if (tpcInFlight.getOrDefault(false)) {
      tpcInFlight.clear();
      if (locked.getOrDefault(false)) {
        // LOGGER.info("Received async result invocation for LOCKED (CURRENT) TRANSACTION: " +
        //         context.self().type().toString());
        tpcTransactionResult.set(invocationResult);
        TransactionDetails transactionDetails = transactionDetailsInFlight.view().iterator().next();
        replyToTransactionFunction(context, !invocationResult.getFailed(0), transactionDetails.getTransactionId(),
                polyglotAddressToSdkAddress(transactionDetails.getCoordinatorAddress()));
      } else {
        // LOGGER.info("Received async result invocation for UNLOCKED (/OLD) TRANSACTION: " +
        //         context.self().type().toString());
        continueProcessingBatchedRequests(context);
      }
      return;
    }

    // Handle response of outgoing SAGAS (batch) invocation
    if (sagasInFlight.getOrDefault(false)) {
      sagasInFlight.clear();
      Iterable<TransactionDetails> transactionDetailsIterable = transactionDetailsInFlight.view();
      for (TransactionDetails currentTransactionDetails : transactionDetailsIterable) {
        replyToTransactionFunction(context,
                !invocationResult.getFailed(currentTransactionDetails.getIndex()),
                currentTransactionDetails.getTransactionId(),
                polyglotAddressToSdkAddress(currentTransactionDetails.getCoordinatorAddress()));
      }
      cleanUpAfterTransaction();
      handleInvocationResponse(context, invocationResult);
      continueProcessingBatchedRequests(context);
      return;
    }

    // Handle unexpected response when function is locked
    if (locked.getOrDefault(false)) {
      // LOGGER.info("Received UNEXPECTED regular async result invocation: " + context.self().type().toString());
      return;
    }

    // Handle regular async response
    // LOGGER.info("Received regular async result invocation: " + context.self().type().toString());
    handleInvocationResponse(context, invocationResult);
    continueProcessingBatchedRequests(context);
  }

  private void replyToTransactionFunction(InternalContext context, boolean success, String transactionId, Address address) {
    ResponseToTransactionFunction response = ResponseToTransactionFunction.newBuilder()
            .setSuccess(success)
            .setTransactionId(transactionId)
            .build();
    context.send(address, response);
  }

  private TransactionDetails.Builder getTransactionDetailsBuilder(InternalContext context) {
    TransactionDetails.Builder transactionDetailsBuilder = TransactionDetails.newBuilder();
    transactionDetailsBuilder
            .setCoordinatorAddress(sdkAddressToPolyglotAddress(context.caller()))
            .setTransactionId(context.getTransactionId());
    return transactionDetailsBuilder;
  }

  private void continueProcessingBatchedRequests(InternalContext context) {
    BatchDetailsList batchesList =
            batches.getOrDefault(BatchDetailsList.getDefaultInstance());

    if (batchesList.getBatchesCount() == 0) {
      requestState.clear();
      return;
    }

    if (locked.getOrDefault(false) || tpcInFlight.getOrDefault(false)
            || sagasInFlight.getOrDefault(false)) {
      return;
    }

    BatchDetails currentBatchDetails = batchesList.getBatches(0);

    if (currentBatchDetails.getBatchType().equals(BatchDetails.BatchType.TPC)) {
      startTpcTransaction(context, currentBatchDetails.getInvocations(0),
              currentBatchDetails.getTransactionDetails(0));
    } else {
      InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
      builder.addAllInvocations(currentBatchDetails.getInvocationsList());
      if (currentBatchDetails.getTransactionDetailsCount() > 0) {
        startSagasBatch(context, builder, currentBatchDetails.getTransactionDetailsList());
      } else {
        sendToFunction(context, builder);
      }
    }
    batches.set(batchesList.toBuilder().removeBatches(0).build());
    context.functionTypeMetrics().consumeBacklogMessages(currentBatchDetails.getInvocationsCount());
    requestState.set(requestState.get() - currentBatchDetails.getInvocationsCount());
    if (requestState.get() < 0) {
      requestState.set(0);
      LOGGER.info("Some counting mistake happened");
    }
  }

  private InvocationResponse unpackInvocationOrThrow(
      Address self, AsyncOperationResult<ToFunction, FromFunction> result) {
    if (result.failure()) {
      throw new IllegalStateException(
          "Failure forwarding a message to a remote function " + self, result.throwable());
    }
    FromFunction fromFunction = result.value();
    if (fromFunction.hasInvocationResult()) {
      return fromFunction.getInvocationResult();
    }
    return InvocationResponse.getDefaultInstance();
  }

  private void handleInvocationResponse(Context context, InvocationResponse invocationResult) {
    handleOutgoingMessages(context, invocationResult);
    handleOutgoingDelayedMessages(context, invocationResult);
    handleEgressMessages(context, invocationResult);
    handleStateMutations(invocationResult);
  }

  private void handleEgressMessages(Context context, InvocationResponse invocationResult) {
    for (EgressMessage egressMessage : invocationResult.getOutgoingEgressesList()) {
      EgressIdentifier<Any> id =
          new EgressIdentifier<>(
              egressMessage.getEgressNamespace(), egressMessage.getEgressType(), Any.class);
      context.send(id, egressMessage.getArgument());
    }
  }

  private void handleOutgoingMessages(Context context, InvocationResponse invocationResult) {
    for (FromFunction.Invocation invokeCommand : invocationResult.getOutgoingMessagesList()) {
      final Address to = polyglotAddressToSdkAddress(invokeCommand.getTarget());
      final Any message = invokeCommand.getArgument();

      context.send(to, message);
    }
  }

  private void handleOutgoingDelayedMessages(Context context, InvocationResponse invocationResult) {
    for (FromFunction.DelayedInvocation delayedInvokeCommand :
        invocationResult.getDelayedInvocationsList()) {
      final Address to = polyglotAddressToSdkAddress(delayedInvokeCommand.getTarget());
      final Any message = delayedInvokeCommand.getArgument();
      final long delay = delayedInvokeCommand.getDelayInMs();

      context.sendAfter(Duration.ofMillis(delay), to, message);
    }
  }

  // --------------------------------------------------------------------------------
  // State Management
  // --------------------------------------------------------------------------------

  private void addStates(ToFunction.InvocationBatchRequest.Builder batchBuilder) {
    managedStates.forEach(
        (stateName, stateValue) -> {
          ToFunction.PersistedValue.Builder valueBuilder =
              ToFunction.PersistedValue.newBuilder().setStateName(stateName);

          if (stateValue != null) {
            valueBuilder.setStateValue(ByteString.copyFrom(stateValue));
          }
          batchBuilder.addState(valueBuilder);
        });
  }

  private void handleStateMutations(InvocationResponse invocationResult) {
    for (FromFunction.PersistedValueMutation mutate : invocationResult.getStateMutationsList()) {
      final String stateName = mutate.getStateName();
      switch (mutate.getMutationType()) {
        case DELETE:
          managedStates.clearValue(stateName);
          break;
        case MODIFY:
          managedStates.setValue(stateName, mutate.getStateValue().toByteArray());
          break;
        case UNRECOGNIZED:
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + mutate.getMutationType());
      }
    }
  }

  // --------------------------------------------------------------------------------
  // Send Message to Remote Function
  // --------------------------------------------------------------------------------
  /**
   * Returns an {@link Invocation.Builder} set with the input {@code message} and the caller
   * information (is present).
   */
  private static Invocation.Builder singeInvocationBuilder(Context context, Any message) {
    Invocation.Builder invocationBuilder = Invocation.newBuilder();
    if (context.caller() != null) {
      invocationBuilder.setCaller(sdkAddressToPolyglotAddress(context.caller()));
    }
    invocationBuilder.setArgument(message);
    return invocationBuilder;
  }

  /**
   * Sends a {@link InvocationBatchRequest} to the remote function consisting out of a single
   * invocation represented by {@code invocationBuilder}.
   */
  private void sendToFunction(Context context, Invocation.Builder invocationBuilder) {
    InvocationBatchRequest.Builder batchBuilder = InvocationBatchRequest.newBuilder();
    batchBuilder.addInvocations(invocationBuilder);
    sendToFunction(context, batchBuilder);
  }

  /** Sends a {@link InvocationBatchRequest} to the remote function. */
  private void sendToFunction(Context context, InvocationBatchRequest.Builder batchBuilder) {
    batchBuilder.setTarget(sdkAddressToPolyglotAddress(context.self()));
    addStates(batchBuilder);
    ToFunction toFunction = ToFunction.newBuilder().setInvocation(batchBuilder).build();
    sendToFunction(context, toFunction);
  }

  private void sendToFunction(Context context, ToFunction toFunction) {
    ToFunctionRequestSummary requestSummary =
        new ToFunctionRequestSummary(
            context.self(),
            toFunction.getSerializedSize(),
            toFunction.getInvocation().getStateCount(),
            toFunction.getInvocation().getInvocationsCount());
    RemoteInvocationMetrics metrics = ((InternalContext) context).functionTypeMetrics();
    CompletableFuture<FromFunction> responseFuture =
        client.call(requestSummary, metrics, toFunction);

    context.registerAsyncOperation(toFunction, responseFuture);
  }

  private boolean isMaxNumBatchRequestsExceeded(final int currentNumBatchRequests) {
    return maxNumBatchRequests > 0 && currentNumBatchRequests >= maxNumBatchRequests;
  }
}
