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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.EgressMessage;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
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
  private final PersistedValue<Boolean> transactionInFlight =
          PersistedValue.of("transaction-in-flight", Boolean.class);

  @Persisted
  private final PersistedAppendingBuffer<ToFunction.Invocation> batch =
      PersistedAppendingBuffer.of("batch", ToFunction.Invocation.class);
  @Persisted
  private final PersistedRemoteFunctionValues managedStates;
  @Persisted
  private final PersistedValue<ToFunction.Invocation> nextTransaction =
          PersistedValue.of("next-transaction", ToFunction.Invocation.class);
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
        LOGGER.info("Received regular invocation for remote function: " + context.self().type().toString());
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
    int inflightOrBatched = requestState.getOrDefault(-1);
    if (inflightOrBatched < 0 && !locked.getOrDefault(false)) {
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
    batch.append(invocationBuilder.build());
    inflightOrBatched++;
    requestState.set(inflightOrBatched);
    context.functionTypeMetrics().appendBacklogMessages(1);
    if (isMaxNumBatchRequestsExceeded(inflightOrBatched)) {
      // we are at capacity, can't add anything to the batch.
      // we need to signal to the runtime that we are unable to process any new input
      // and we must wait for our in flight asynchronous operation to complete before
      // we are able to process more input.
      context.awaitAsyncOperationComplete();
    }
  }

  private void onTransactionRequest(InternalContext context, Any message) {
    Invocation.Builder invocationBuilder = singeInvocationBuilder(context, message);
    if (context.getTransactionMessage().equals(Context.TransactionMessage.PREPARE)) {
      LOGGER.info("Received transaction prepare invocation for remote function: " + context.self().type().toString());
      nextTransaction.set(invocationBuilder.build());
      transactionId.set(context.getTransactionId());
      transactionResponseAddress.set(context.caller());
      // Lock new requests to avoid extra waiting transactions
      context.awaitAsyncOperationComplete();
      if (requestState.getOrDefault(-1) < 0 && !locked.getOrDefault(false)) {
        startNextTransaction(context);
      }
    } else if (context.getTransactionMessage().equals(Context.TransactionMessage.ABORT)
            && isCurrentTransaction(context.getTransactionId())) {
      LOGGER.info("Received transaction abort invocation for remote function: " + context.self().type().toString());
      cleanUpAfterTxn(context);
    } else if (context.getTransactionMessage().equals(Context.TransactionMessage.COMMIT)
            && isCurrentTransaction(context.getTransactionId())) {
      LOGGER.info("Received transaction commit invocation for remote function: " + context.self().type().toString());
      handleInvocationResponse(context, transactionResult.get());
      cleanUpAfterTxn(context);
    } else {
      LOGGER.info("Received UNEXPECTED transaction invocation for remote function: " + context.self().type().toString());
    }
  }

  private void startNextTransaction(InternalContext context) {
    LOGGER.info("Sending out transaction invocation to function: " + context.self().type().toString());
    requestState.set(0);
    locked.set(true);
    transactionInFlight.set(true);
    sendToFunction(context, nextTransaction.get().toBuilder());
    nextTransaction.clear();
  }

  private void cleanUpAfterTxn(InternalContext context) {
    locked.clear();
    transactionResponseAddress.clear();
    transactionResult.clear();
    transactionId.clear();
    nextTransaction.clear();
    if (!transactionInFlight.getOrDefault(false)) {
      continueProcessingBatchedRequests(context);
    }
  }

  private boolean isCurrentTransaction(String transactionId) {
    if (this.transactionId.getOrDefault("-").equals(transactionId)) {
      return true;
    }
    return false;
  }

  private void onAsyncResult(
      InternalContext context, AsyncOperationResult<ToFunction, FromFunction> asyncResult) {
    if (asyncResult.unknown()) {
      ToFunction batch = asyncResult.metadata();
      sendToFunction(context, batch);
      if (isTransactionQueued()) {
        context.awaitAsyncOperationComplete();
      }
      return;
    }

    InvocationResponse invocationResult = unpackInvocationOrThrow(context.self(), asyncResult);
    if (transactionInFlight.getOrDefault(false)) {
      transactionInFlight.clear();
      if (locked.getOrDefault(false)) {
        LOGGER.info("Received async result invocation for LOCKED TRANSACTION: " + context.self().type().toString());
        FromFunction.PreparePhaseResponse.Builder builder = FromFunction.PreparePhaseResponse.newBuilder();
        builder.setSuccess(!invocationResult.getFailed());
        builder.setTransactionId(transactionId.getOrDefault("-"));
        transactionResult.set(invocationResult);
        Address to = transactionResponseAddress.get();
        context.send(to, builder.build());
      } else {
        LOGGER.info("Received async result invocation for UNLOCKED TRANSACTION: " + context.self().type().toString());
        continueProcessingBatchedRequests(context);
      }
    } else {
      LOGGER.info("Received regular async result invocation: " + context.self().type().toString());
      handleInvocationResponse(context, invocationResult);
      continueProcessingBatchedRequests(context);
    }
  }

  private void continueProcessingBatchedRequests(InternalContext context) {
    // If there is a transaction waiting we never want to accept new requests since it may override
    if (isTransactionQueued()) {
      context.awaitAsyncOperationComplete();
    }

    if (!Iterables.isEmpty(batch.view())) {
      final InvocationBatchRequest.Builder nextBatch = getNextBatch();
      final int numBatched = requestState.getOrDefault(-1);
      if (numBatched < 0) {
        throw new IllegalStateException("Got an unexpected async result");
      }
      requestState.set(0);
      batch.clear();
      LOGGER.info("Sending out regular invocation to function: " + context.self().type().toString());
      context.functionTypeMetrics().consumeBacklogMessages(numBatched);
      sendToFunction(context, nextBatch);
    } else if (isTransactionQueued()) {
      startNextTransaction(context);
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

  private InvocationBatchRequest.Builder getNextBatch() {
    InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
    Iterable<Invocation> view = batch.view();
    builder.addAllInvocations(view);
    return builder;
  }

  private boolean isTransactionQueued() {
    return nextTransaction.getOrDefault(Invocation.getDefaultInstance()) != Invocation.getDefaultInstance();
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
