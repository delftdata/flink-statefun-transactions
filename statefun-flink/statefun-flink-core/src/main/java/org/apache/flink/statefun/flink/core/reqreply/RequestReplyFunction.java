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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.EgressMessage;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.Invocation;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.InvocationBatchRequest;
import org.apache.flink.statefun.sdk.*;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
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
          PersistedValue.of("sagas-in-flinght", Boolean.class);
  @Persisted
  private final PersistedValue<List> batch =
      PersistedValue.of("batch", List.class);
  @Persisted
  private final PersistedRemoteFunctionValues managedStates;
  @Persisted
  private final PersistedValue<Boolean> locked =
          PersistedValue.of("locked", Boolean.class);
  @Persisted
  private final PersistedValue<Address> transactionResponseAddress =
          PersistedValue.of("transaction-response-address", Address.class);
  @Persisted
  private final PersistedValue<String> transactionId =
          PersistedValue.of("transaction-id", String.class);
  @Persisted
  private final PersistedValue<FromFunction.InvocationResponse> transactionResult =
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
    LOGGER.info("Received regular invocation to function: " + context.self().type().toString());
    Invocation.Builder invocationBuilder = singeInvocationBuilder(context, message);
    if (requestState.getOrDefault(-1) < 0 && !locked.getOrDefault(false)) {
      // no inflight requests, and nothing in the batch.
      // so we let this request to go through, and change state to indicate that:
      // a) there is a request in flight.
      // b) there is nothing in the batch.
      requestState.set(0);
      LOGGER.info("Sending out regular invocation to function: " + context.self().type().toString());
      sendToFunction(context, invocationBuilder);
      return;
    }
    // there is at least one request in flight (inflightOrBatched >= 0),
    // so we add that request to the batch.
    addToBatch(context, invocationBuilder);
  }

  private void onTransactionRequest(InternalContext context, Any message) {
    Invocation.Builder invocationBuilder = singeInvocationBuilder(context, message);

    // Handle active transaction
    if (transactionId.getOrDefault("-").equals(context.getTransactionId())
            && locked.getOrDefault(false)) {
      if (context.getTransactionMessage().equals(Context.TransactionMessage.ABORT)) {
        LOGGER.info("Received transaction abort invocation for remote function: " + context.self().type().toString());
        cleanUpAfterTransaction(context);
      } else if (context.getTransactionMessage().equals(Context.TransactionMessage.COMMIT)) {
        LOGGER.info("Received transaction commit invocation for remote function: " + context.self().type().toString());
        handleInvocationResponse(context, transactionResult.get());
        cleanUpAfterTransaction(context);
      } else {
        LOGGER.info("Received unexpected message for current transaction ID for remote function: " + context.self().type().toString());
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
      LOGGER.info("Received SAGAs invocation for remote function: " + context.self().type().toString());
      if (requestState.getOrDefault(-1) < 0 && !locked.getOrDefault(false)) {
        startSagasTransaction(context, invocationBuilder, context.getTransactionId());
      } else {
        addToBatch(context, invocationBuilder);
      }
    }

    // Handle new transaction prepare
    if (context.getTransactionMessage().equals(Context.TransactionMessage.PREPARE)) {
      LOGGER.info("Received transaction prepare invocation for remote function: " + context.self().type().toString());
      if (requestState.getOrDefault(-1) < 0 && !locked.getOrDefault(false)) {
        startTpcTransaction(context, invocationBuilder, context.getTransactionId());
      } else {
        addToBatch(context, invocationBuilder);
      }
      return;
    }

    LOGGER.info("Received UNEXPECTED transaction invocation for remote function: " + context.self().type().toString());
  }

  private void removeTpcInvocationFromQueueIfQueued(InternalContext context) {
    List<ImmutablePair<ImmutablePair<Context.TransactionMessage, String>,ToFunction.Invocation>> batchList =
            batch.getOrDefault(new ArrayList());
    for (int i = 0; i < batchList.size(); i++) {
      ImmutablePair<ImmutablePair<Context.TransactionMessage, String>,ToFunction.Invocation> current = batchList.get(i);
      if (current.getKey().getValue() != null && current.getKey().getValue().equals(context.getTransactionId())) {
        batchList.remove(i);
        batch.set(batchList);
        context.functionTypeMetrics().consumeBacklogMessages(1);
        requestState.set(requestState.get() - 1);
        LOGGER.info("Removed transaction from the queue: " + context.self().type().toString());
        break;
      }
    }
  }

  private void addToBatch(InternalContext context, Invocation.Builder invocationBuilder) {
    int inflightOrBatched = requestState.getOrDefault(-1);
    List batchList = batch.getOrDefault(new ArrayList());
    batchList.add(new ImmutablePair<>(
            new ImmutablePair<>(context.getTransactionMessage(), context.getTransactionId()),
            invocationBuilder.build()));
    batch.set(batchList);
    inflightOrBatched++;
    requestState.set(inflightOrBatched);
    context.functionTypeMetrics().appendBacklogMessages(1);
    if (isMaxNumBatchRequestsExceeded(inflightOrBatched)) {
      LOGGER.info("Function is locked due to maxNumBatchRequests: " + context.self().type().toString());
      context.awaitAsyncOperationComplete();
    }
  }

  private void startSagasTransaction(InternalContext context, Invocation.Builder invocationBuilder, String id) {
    LOGGER.info("Sending out transaction invocation to function: " + context.self().type().toString());
    requestState.set(0);
    sagasInFlight.set(true);
    transactionId.set(id);
    transactionResponseAddress.set(polyglotAddressToSdkAddress(invocationBuilder.getCaller()));
    sendToFunction(context, invocationBuilder);
  }

  private void startTpcTransaction(InternalContext context, Invocation.Builder invocationBuilder, String id) {
    LOGGER.info("Sending out transaction invocation to function: " + context.self().type().toString());
    requestState.set(0);
    tpcInFlight.set(true);
    locked.set(true);
    transactionId.set(id);
    transactionResponseAddress.set(polyglotAddressToSdkAddress(invocationBuilder.getCaller()));
    sendToFunction(context, invocationBuilder);
  }

  private void cleanUpAfterTransaction(InternalContext context) {
    locked.clear();
    transactionId.clear();
    transactionResponseAddress.clear();
    transactionResult.clear();
    if (!tpcInFlight.getOrDefault(false)) {
      continueProcessingBatchedRequests(context);
    }
  }

  private void onAsyncResult(
      InternalContext context, AsyncOperationResult<ToFunction, FromFunction> asyncResult) {
    if (asyncResult.unknown()) {
      ToFunction batch = asyncResult.metadata();
      sendToFunction(context, batch);
      return;
    }

    InvocationResponse invocationResult = unpackInvocationOrThrow(context.self(), asyncResult);

    if (tpcInFlight.getOrDefault(false)) {
      tpcInFlight.clear();
      if (locked.getOrDefault(false)) {
        LOGGER.info("Received async result invocation for LOCKED (CURRENT) TRANSACTION: " + context.self().type().toString());
        transactionResult.set(invocationResult);
        replyToTransactionFunction(context, invocationResult);
      } else {
        LOGGER.info("Received async result invocation for UNLOCKED (/OLD) TRANSACTION: " + context.self().type().toString());
        continueProcessingBatchedRequests(context);
      }
      return;
    }

    if (sagasInFlight.getOrDefault(false)) {
      sagasInFlight.clear();
      LOGGER.info("Received SAGAS result for function: " + context.self().type().toString());
      replyToTransactionFunction(context, invocationResult);
      // If successful
      if (!invocationResult.getFailed()) {
        handleInvocationResponse(context, invocationResult);
      }
      cleanUpAfterTransaction(context);
    }

    if (locked.getOrDefault(false)) {
      LOGGER.info("Received UNEXPECTED regular async result invocation: " + context.self().type().toString());
    } else {
      LOGGER.info("Received regular async result invocation: " + context.self().type().toString());
      handleInvocationResponse(context, invocationResult);
      continueProcessingBatchedRequests(context);
    }
  }

  private void replyToTransactionFunction(InternalContext context, InvocationResponse invocationResult) {
    FromFunction.ResponseToTransactionFunction response = FromFunction.ResponseToTransactionFunction.newBuilder()
            .setSuccess(!invocationResult.getFailed())
            .setTransactionId(transactionId.getOrDefault("-"))
            .build();
    Address to = transactionResponseAddress.get();
    context.send(to, response);
  }

  private void continueProcessingBatchedRequests(InternalContext context) {
    List<ImmutablePair<ImmutablePair<Context.TransactionMessage, String>,ToFunction.Invocation>> batchList =
            batch.getOrDefault(new ArrayList());
    if (batchList.size() == 0) {
      requestState.clear();
      return;
    }

    // Finding first transaction
    InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
    int size = batchList.size();
    for (int i = 0; i < size; i++) {
      ImmutablePair<ImmutablePair<Context.TransactionMessage, String>,ToFunction.Invocation> current = batchList.get(0);
      if (current.getKey().getValue() != null) {
        if (i == 0) {
          if (current.getKey().getKey().equals(Context.TransactionMessage.SAGAS)) {
            startSagasTransaction(context, current.getValue().toBuilder(), current.getKey().getValue());
          } else if (current.getKey().getKey().equals(Context.TransactionMessage.PREPARE)) {
            startTpcTransaction(context, current.getValue().toBuilder(), current.getKey().getValue());
          }
          batchList.remove(0);
        } else {
          sendToFunction(context, builder);
        }
        batch.set(batchList);
        requestState.set(batchList.size());
        context.functionTypeMetrics().consumeBacklogMessages(builder.getInvocationsCount());
        return;
      }
      builder.addInvocations(current.getValue());
      batchList.remove(0);
    }

    // No transaction found
    if (builder.getInvocationsCount() > 0) {
      sendToFunction(context, builder);
      batch.set(batchList);
      requestState.set(batchList.size());
      context.functionTypeMetrics().consumeBacklogMessages(builder.getInvocationsCount());
    } else {
      requestState.clear();
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
