package org.apache.flink.statefun.flink.core.sagasfn;

import com.google.protobuf.Any;
import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.functions.StatefulFunctionInvocationException;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.polyglot.generated.Address;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.ResponseToTransactionFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.polyglotAddressToSdkAddress;
import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.sdkAddressToPolyglotAddress;

public class SagasFunction implements StatefulFunction {
    private final RequestReplyClient client;
    private final int maxNumBatchRequests;

    private static final Logger LOGGER = LoggerFactory.getLogger(org.apache.flink.statefun.flink.core.sagasfn.SagasFunction.class);

    @Persisted
    private final PersistedValue<List> queue =
            PersistedValue.of("queue", List.class);

    @Persisted
    private final PersistedValue<Boolean> transactionInProgress =
            PersistedValue.of("transaction-in-progress", Boolean.class);

    @Persisted
    private final PersistedValue<Boolean> transactionFailed =
            PersistedValue.of("transaction-failed", Boolean.class);

    @Persisted
    private final PersistedValue<String> currentTransactionId =
            PersistedValue.of("current-transaction-id", String.class);

    // 0 for not set, 1 for successful result, -1 for failure result
    @Persisted
    private final PersistedTable<Address, Integer> currentTransactionFunctions =
            PersistedTable.of("current-transaction-functions", Address.class, Integer.class);

    @Persisted
    private final PersistedValue<FromFunction.SagasFunctionInvocationResponse> currentTransactionResults =
            PersistedValue.of("current-transaction-results", FromFunction.SagasFunctionInvocationResponse.class);

    public SagasFunction(
            int maxNumBatchRequests,
            RequestReplyClient client) {
        this.client = Objects.requireNonNull(client);
        this.maxNumBatchRequests = maxNumBatchRequests;
    }

    @Override
    public void invoke(Context context, Object input) {
        LOGGER.info("Received invocaiton for SAGAs function: " + context.self().type().toString());
        InternalContext castedContext = (InternalContext) context;

        if (input instanceof AsyncOperationResult) {
            if (transactionInProgress.getOrDefault(false)) {
                AsyncOperationResult<ToFunction, FromFunction> result =
                        (AsyncOperationResult<ToFunction, FromFunction>) input;
                onAsyncResult(castedContext, result);
            } else {
                LOGGER.warn("Unexpected AsyncOperation Result for function: " + context.self().type().toString());
            }
            return;
        }

        if (input instanceof ResponseToTransactionFunction) {
            if (transactionInProgress.getOrDefault(false)) {
                ResponseToTransactionFunction responseToTransactionFunction =
                        (ResponseToTransactionFunction) input;
                handleResponseToTransactionFunction(castedContext, responseToTransactionFunction);
            } else {
                LOGGER.warn("Unexpected response to SAGAS function found: " + context.self().type().toString());
            }
            return;
        }

        onRequest(castedContext, (Any) input);
    }

    private void onRequest(InternalContext context, Any message) {
        ToFunction.Invocation.Builder invocationBuilder = singleInvocationBuilder(context, message);
        if (!transactionInProgress.getOrDefault(false)) {
            sendToFunction(context, invocationBuilder);
        } else {
            List<ToFunction.Invocation.Builder> q =
                    queue.getOrDefault(new ArrayList());
            q.add(invocationBuilder);
            queue.set(q);
        }
    }

    private void onAsyncResult(
            InternalContext context, AsyncOperationResult<ToFunction, FromFunction> asyncResult) {

        if (asyncResult.unknown()) {
            ToFunction batch = asyncResult.metadata();
            sendToFunction(context, batch);
            return;
        }

        FromFunction.SagasFunctionInvocationResponse invocationResult =
                unpackInvocationOrThrow(context.self(), asyncResult);
        List<FromFunction.SagasFunctionPair> invocationPairs = invocationResult.getInvocationPairsList();

        currentTransactionId.set(UUID.randomUUID().toString());

        for(FromFunction.SagasFunctionPair invocationPair : invocationPairs) {
            currentTransactionFunctions.set(
                    invocationPair.getInitialMessage().getTarget(), 0);
            final org.apache.flink.statefun.sdk.Address to =
                    polyglotAddressToSdkAddress(invocationPair.getInitialMessage().getTarget());
            final Any message = invocationPair.getInitialMessage().getArgument();
            context.sendTransactionMessage(to, message, currentTransactionId.get(),
                    Context.TransactionMessage.SAGAS);
        }
        currentTransactionResults.set(invocationResult);
    }

    private void handleResponseToTransactionFunction(
            InternalContext context, ResponseToTransactionFunction response) {
        if (!response.getTransactionId()
                .equals(currentTransactionId.getOrDefault("."))) {
            LOGGER.info("Received response to transaction for different (probably old) transaction");
            return;
        }

        Address caller = sdkAddressToPolyglotAddress(context.caller());

        if (currentTransactionFunctions.get(caller) != 0) {
            throw new StatefulFunctionInvocationException(
                    context.self().type(),
                    new IllegalAccessException("Invalid response to ongoing transaction."));
        }

        if (response.getSuccess()) {
            currentTransactionFunctions.set(caller, 1);
            if (didAllFunctionsSucceed()) {
                handleSuccess(context);
            }
            if (transactionFailed.getOrDefault(false)) {
                for (FromFunction.SagasFunctionPair invocationPair :
                        currentTransactionResults.get().getInvocationPairsList()) {
                    if (invocationPair.getInitialMessage().getTarget().equals(caller)) {
                        LOGGER.info("Later sending compensating message: " + context.self().type().toString());
                        sendCompensatingMessage(context, invocationPair);
                    }
                }
            }
        } else {
            currentTransactionFunctions.set(caller, -1);
            if (!transactionFailed.getOrDefault(false)) {
                transactionFailed.set(Boolean.TRUE);
                handleFailure(context);
            }
        }

        if (didAllFunctionsComplete()) {
            cleanUp(context);
        }
    }

    private void sendCompensatingMessage(InternalContext context, FromFunction.SagasFunctionPair invocationPair) {
        final org.apache.flink.statefun.sdk.Address to = polyglotAddressToSdkAddress(
                invocationPair.getCompensatingMessage().getTarget());
        final Any message = invocationPair.getCompensatingMessage().getArgument();
        context.send(to, message);

    }

    private void handleSuccess(InternalContext context) {
        LOGGER.info("Handling success case for SAGAS transaction: " + context.self().type().toString());
        handleResults(context, currentTransactionResults.get().getSuccessResponse());
    }

    private void handleFailure(InternalContext context) {
        LOGGER.info("Handling failure case for SAGAS transaction: " + context.self().type().toString());
        for (FromFunction.SagasFunctionPair invocationPair :
                currentTransactionResults.get().getInvocationPairsList()) {
            int success = currentTransactionFunctions.get(invocationPair.getInitialMessage().getTarget());
            if (success == 1) {
                LOGGER.info("Directly sending compensating message: " + context.self().type().toString());
                sendCompensatingMessage(context, invocationPair);
            }
        }
        handleResults(context, currentTransactionResults.get().getFailureResponse());
    }

    private void handleResults(InternalContext context, InvocationResponse invocationResponse) {
        handleEgressMessages(context, invocationResponse.getOutgoingEgressesList());
        handleOutgoingMessages(context, invocationResponse.getOutgoingMessagesList());
        handleOutgoingDelayedMessages(context, invocationResponse.getDelayedInvocationsList());
    }

    private void cleanUp(InternalContext context) {
        currentTransactionResults.clear();
        currentTransactionFunctions.clear();
        transactionInProgress.clear();
        transactionFailed.clear();
        currentTransactionId.clear();
        continueProcessingQueuedTransactions(context);
    }

    private void continueProcessingQueuedTransactions(InternalContext context) {
        List<ToFunction.Invocation.Builder> q = queue.getOrDefault(new ArrayList());

        if (q.size() == 0) {
            return;
        }

        ToFunction.Invocation.Builder elem = q.remove(0);
        queue.set(q);
        LOGGER.info("Executing next transaction from queue!");
        sendToFunction(context, elem);
    }

    private FromFunction.SagasFunctionInvocationResponse unpackInvocationOrThrow(
            org.apache.flink.statefun.sdk.Address self, AsyncOperationResult<ToFunction, FromFunction> result) {
        if (result.failure()) {
            throw new IllegalStateException(
                    "Failure forwarding a message to a remote function " + self, result.throwable());
        }
        LOGGER.info(result.toString());
        FromFunction fromFunction = result.value();
        if (fromFunction.hasSagasFunctionInvocationResult()) {
            LOGGER.info("Found SAGAs function invocation result.");
            return fromFunction.getSagasFunctionInvocationResult();
        }
        LOGGER.info("Did not find SAGAs function invocation result.");
        LOGGER.info(fromFunction.toString());
        return FromFunction.SagasFunctionInvocationResponse.getDefaultInstance();
    }

    private void handleEgressMessages(Context context, List<FromFunction.EgressMessage> egressMessages) {
        for (FromFunction.EgressMessage egressMessage : egressMessages) {
            EgressIdentifier<Any> id =
                    new EgressIdentifier<>(
                            egressMessage.getEgressNamespace(), egressMessage.getEgressType(), Any.class);
            context.send(id, egressMessage.getArgument());
        }
    }

    private void handleOutgoingMessages(Context context, List<FromFunction.Invocation> invocations) {
        for (FromFunction.Invocation invokeCommand : invocations) {
            final org.apache.flink.statefun.sdk.Address to = polyglotAddressToSdkAddress(invokeCommand.getTarget());
            final Any message = invokeCommand.getArgument();

            context.send(to, message);
        }
    }

    private void handleOutgoingDelayedMessages(Context context, List<FromFunction.DelayedInvocation> delayedMessages) {
        for (FromFunction.DelayedInvocation delayedInvokeCommand :
                delayedMessages) {
            final org.apache.flink.statefun.sdk.Address to = polyglotAddressToSdkAddress(delayedInvokeCommand.getTarget());
            final Any message = delayedInvokeCommand.getArgument();
            final long delay = delayedInvokeCommand.getDelayInMs();

            context.sendAfter(Duration.ofMillis(delay), to, message);
        }
    }

    // --------------------------------------------------------------------------------
    // Send Message to Remote Function
    // --------------------------------------------------------------------------------
    /**
     * Returns an {@link ToFunction.Invocation.Builder} set with the input {@code message} and the caller
     * information (is present).
     */
    private static ToFunction.Invocation.Builder singleInvocationBuilder(Context context, Any message) {
        ToFunction.Invocation.Builder invocationBuilder = ToFunction.Invocation.newBuilder();
        if (context.caller() != null) {
            invocationBuilder.setCaller(sdkAddressToPolyglotAddress(context.caller()));
        }
        invocationBuilder.setArgument(message);
        return invocationBuilder;
    }

    /**
     * Sends a {@link ToFunction.InvocationBatchRequest} to the remote function consisting out of a single
     * invocation represented by {@code invocationBuilder}.
     */
    private void sendToFunction(Context context, ToFunction.Invocation.Builder invocationBuilder) {
        transactionInProgress.set(true);
        ToFunction.InvocationBatchRequest.Builder batchBuilder = ToFunction.InvocationBatchRequest.newBuilder();
        batchBuilder.addInvocations(invocationBuilder);
        batchBuilder.setTarget(sdkAddressToPolyglotAddress(context.self()));
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

    private boolean didAllFunctionsSucceed() {
        for (int value : currentTransactionFunctions.values()) {
            if (value == 0 || value == -1) {
                return false;
            }
        }
        return true;
    }

    private boolean didAllFunctionsComplete() {
        for (int value : currentTransactionFunctions.values()) {
            if (value == 0) {
                return false;
            }
        }
        return true;
    }
}
