package org.apache.flink.statefun.flink.core.tpcfn;

import com.google.protobuf.Any;
import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.functions.StatefulFunctionInvocationException;
import org.apache.flink.statefun.flink.core.generated.Payload;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.polyglot.generated.Address;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.PreparePhaseResponse;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.polyglotAddressToSdkAddress;
import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.sdkAddressToPolyglotAddress;

public class TpcFunction implements StatefulFunction {
    private final RequestReplyClient client;
    private final int maxNumBatchRequests;

    private static final Logger LOGGER = LoggerFactory.getLogger(TpcFunction.class);

    @Persisted
    private final PersistedValue<LinkedList> batch =
            PersistedValue.of("batch", LinkedList.class);

    @Persisted
    private final PersistedValue<Boolean> transactionInProgress =
            PersistedValue.of("transaction-in-progress", Boolean.class);

    @Persisted
    private final PersistedValue<String> currentTransactionId =
            PersistedValue.of("current-transaction-id", String.class);

    @Persisted
    private final PersistedTable<Address, Boolean> currentTransactionFunctions =
            PersistedTable.of("current-transaction-functions", Address.class, Boolean.class);

    @Persisted
    private final PersistedValue<FromFunction.TpcFunctionInvocationResponse> currentTransactionResults =
            PersistedValue.of("current-transaction-results", FromFunction.TpcFunctionInvocationResponse.class);

    public TpcFunction(
            int maxNumBatchRequests,
            RequestReplyClient client) {
        this.client = Objects.requireNonNull(client);
        this.maxNumBatchRequests = maxNumBatchRequests;
    }

    @Override
    public void invoke(Context context, Object input) {
        LOGGER.info("Received invocation for transaction function: " + context.self().type().toString());
        InternalContext castedContext = (InternalContext) context;

        if (input instanceof AsyncOperationResult) {
            if (transactionInProgress.getOrDefault(false)) {
                AsyncOperationResult<ToFunction, FromFunction> result =
                        (AsyncOperationResult<ToFunction, FromFunction>) input;
                onAsyncResult(castedContext, result);
            }
            else {
                LOGGER.warn("Unexpected AsyncOperationResult received.");
            }
            return;
        }

        if (input instanceof PreparePhaseResponse) {
            if (transactionInProgress.getOrDefault(false)) {
                PreparePhaseResponse preparePhaseInvocationResponse =
                        (PreparePhaseResponse) input;
                handlePreparePhaseResponse(castedContext, preparePhaseInvocationResponse);
            } else {
                LOGGER.warn("Unexpected prepare phase result received.");
            }
            return;
        }

        onRequest(castedContext, (Any) input);
    }

    private void onRequest(InternalContext context, Any message) {
        ToFunction.Invocation.Builder invocationBuilder = singleInvocationBuilder(context, message);
        if (!transactionInProgress.getOrDefault(false)) {
            startTransaction(context, invocationBuilder);
        } else {
            LinkedList<ToFunction.Invocation.Builder> b =
                    batch.getOrDefault(new LinkedList<ToFunction.Invocation.Builder>());
            b.add(invocationBuilder);
            LOGGER.info("Add transaction request to queue.");
            batch.set(b);
        }
    }

    private void startTransaction(InternalContext context, ToFunction.Invocation.Builder invocationBuilder) {
        transactionInProgress.set(true);
        sendToFunction(context, invocationBuilder);
        return;
    }

    private void handlePreparePhaseResponse(InternalContext context, PreparePhaseResponse response) {
        if (!response.getTransactionId()
                .equals(currentTransactionId.getOrDefault("."))) {
            LOGGER.info("Received prepare phase response for different " +
                    "transaction (probably old).");
            return;
        }

        Address caller = sdkAddressToPolyglotAddress(context.caller());
        Boolean callerSuccess = currentTransactionFunctions.get(caller);

        if (callerSuccess == null || callerSuccess == true) {
            // Response should not be part of transaction
            throw new StatefulFunctionInvocationException(
                    context.self().type(),
                    new IllegalAccessException("Invalid response to ongoing transaction."));
        }

        if (response.getSuccess()) {
            currentTransactionFunctions.set(caller, Boolean.TRUE);
            if(didAllFunctionsSucceed()) {
                handleSuccess(context);
                cleanUp(context);
            }
        } else {
            handleFailure(context);
            cleanUp(context);
        }
    }

    private void onAsyncResult(
            InternalContext context, AsyncOperationResult<ToFunction, FromFunction> asyncResult) {

        if (asyncResult.unknown()) {
            ToFunction batch = asyncResult.metadata();
            sendToFunction(context, batch);
            return;
        }

        FromFunction.TpcFunctionInvocationResponse invocationResult =
                unpackInvocationOrThrow(context.self(), asyncResult);
        List<FromFunction.Invocation> atomicInvocations = invocationResult.getAtomicInvocationsList();

        for(FromFunction.Invocation invocation : atomicInvocations) {
            currentTransactionFunctions.set(invocation.getTarget(), Boolean.FALSE);
            final org.apache.flink.statefun.sdk.Address to = polyglotAddressToSdkAddress(invocation.getTarget());
            final Any message = invocation.getArgument();
            if (currentTransactionId.get() == null) {
                currentTransactionId.set(UUID.randomUUID().toString());
            }
            context.sendTpcMessage(to, message, currentTransactionId.getOrDefault("-"),
                        Context.TransactionMessage.PREPARE);
        }
        currentTransactionResults.set(invocationResult);
    }

    private FromFunction.TpcFunctionInvocationResponse unpackInvocationOrThrow(
            org.apache.flink.statefun.sdk.Address self, AsyncOperationResult<ToFunction, FromFunction> result) {
        if (result.failure()) {
            throw new IllegalStateException(
                    "Failure forwarding a message to a remote function " + self, result.throwable());
        }
        FromFunction fromFunction = result.value();
        if (fromFunction.hasTpcFunctionInvocationResult()) {
            LOGGER.info("Found TPC function invocation result.");
            return fromFunction.getTpcFunctionInvocationResult();
        }
        if (fromFunction.hasInvocationResult()) {
            LOGGER.info("Found HTTP function invocation result.");
        }
        LOGGER.info("Did not find TPC function invocation result.");
        return FromFunction.TpcFunctionInvocationResponse.getDefaultInstance();
    }

    private void handleSuccess(InternalContext context) {
        LOGGER.info("Handling success case for this transaction.");
        for(Address address : currentTransactionFunctions.keys()) {
            final org.apache.flink.statefun.sdk.Address to = polyglotAddressToSdkAddress(address);
            final Any message = Any.pack(Payload.getDefaultInstance());
                context.sendTpcMessage(to, message, currentTransactionId.getOrDefault("-"),
                        Context.TransactionMessage.COMMIT);
        }
        handleEgressMessages(context, currentTransactionResults.get().getOutgoingEgressesOnSuccessList());
        handleOutgoingMessages(context, currentTransactionResults.get().getOutgoingMessagesOnSuccessList());
        handleOutgoingDelayedMessages(context, currentTransactionResults.get().getDelayedInvocationsOnSuccessList());
    }

    private void handleFailure(InternalContext context) {
        LOGGER.info("Handling failure case for this transaction.");
        for(Address address : currentTransactionFunctions.keys()) {
            final org.apache.flink.statefun.sdk.Address to = polyglotAddressToSdkAddress(address);
            final Any message = Any.pack(Payload.getDefaultInstance());
            context.sendTpcMessage(to, message, currentTransactionId.getOrDefault("-"),
                    Context.TransactionMessage.ABORT);
        }
        handleEgressMessages(context, currentTransactionResults.get().getOutgoingEgressesOnFailureList());
        handleOutgoingMessages(context, currentTransactionResults.get().getOutgoingMessagesOnFailureList());
        handleOutgoingDelayedMessages(context, currentTransactionResults.get().getDelayedInvocationsOnFailureList());
    }

    private void cleanUp(InternalContext context) {
        currentTransactionResults.clear();
        currentTransactionFunctions.clear();
        currentTransactionId.set(UUID.randomUUID().toString());
        transactionInProgress.clear();
        continueProcessingQueuedTransactions(context);
    }

    private void continueProcessingQueuedTransactions(InternalContext context) {
        LinkedList<ToFunction.Invocation.Builder> b = batch.getOrDefault(new LinkedList<ToFunction.Invocation.Builder>());
        ToFunction.Invocation.Builder elem = b.poll();
        batch.set(b);
        if (elem != null) {
            LOGGER.info("Executing next transaction from queue!");
            startTransaction(context, elem);
        }
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
        ToFunction.InvocationBatchRequest.Builder batchBuilder = ToFunction.InvocationBatchRequest.newBuilder();
        batchBuilder.addInvocations(invocationBuilder);
        sendToFunction(context, batchBuilder);
    }

    /** Sends a {@link ToFunction.InvocationBatchRequest} to the remote function. */
    private void sendToFunction(Context context, ToFunction.InvocationBatchRequest.Builder batchBuilder) {
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
        for(boolean value : currentTransactionFunctions.values()) {
            if (value == false) {
                return false;
            }
        }
        return true;
    }
}

