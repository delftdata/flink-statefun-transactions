package org.apache.flink.statefun.flink.core.tpcfn;

import com.google.protobuf.Any;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.functions.StatefulFunctionInvocationException;
import org.apache.flink.statefun.flink.core.generated.Payload;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.polyglot.generated.Address;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.generated.ResponseToTransactionFunction;
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

public class TpcFunction implements StatefulFunction {
    private final RequestReplyClient client;
    private final int maxNumBatchRequests;

    private static final Logger LOGGER = LoggerFactory.getLogger(TpcFunction.class);

    @Persisted
    private final PersistedValue<List> batch =
            PersistedValue.of("batch", List.class);

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

    @Persisted
    private final PersistedValue<List> dependentAddresses =
            PersistedValue.of("dependent-addresses", List.class);

    @Persisted
    private final PersistedValue<List> blockingAddresses =
            PersistedValue.of("blocking-addresses", List.class);

    public TpcFunction(
            int maxNumBatchRequests,
            RequestReplyClient client) {
        this.client = Objects.requireNonNull(client);
        this.maxNumBatchRequests = maxNumBatchRequests;
    }

    @Override
    public void invoke(Context context, Object input) {
        // LOGGER.info("Received invocation for transaction function: " + context.self().type().toString());
        InternalContext castedContext = (InternalContext) context;

        if (context.getTransactionMessage() != null &&
                context.getTransactionMessage().equals(Context.TransactionMessage.BLOCKING)) {
            if (transactionInProgress.getOrDefault(false) &&
                    context.getTransactionId().equals(currentTransactionId.getOrDefault("-1"))) {
                List<Address> blocking = blockingAddresses.getOrDefault(new ArrayList());
                List<Address> dependent = dependentAddresses.getOrDefault(new ArrayList());
                // Multiple functions may return with same locking addresses, should not send message twice
                for (org.apache.flink.statefun.sdk.Address sdkAddresss : context.getAddresses()) {
                    Address polyAddress = sdkAddressToPolyglotAddress(sdkAddresss);
                    if (!blocking.contains(polyAddress)) {
                        // Send for self
                        context.sendDeadlockDetectionProbe(sdkAddresss, context.self());
                        // Send for any other dependencies recorded earlier
                        for (Address initiator : dependent) {
                            context.sendDeadlockDetectionProbe(sdkAddresss, polyglotAddressToSdkAddress(initiator));
                        }
                        blocking.add(polyAddress);
                    }
                }
                blockingAddresses.set(blocking);
            } else if (transactionInProgress.getOrDefault(false)) {
                org.apache.flink.statefun.sdk.Address initiator = context.getAddresses().get(0);
                if (initiator.equals(context.self())) {
                    // LOGGER.info("Found deadlock. Aborting transaction!");
                    handleRetryable((InternalContext) context);
                    cleanUp((InternalContext) context);
                    return;
                }

                List<Address> dependent = dependentAddresses.getOrDefault(new ArrayList());
                // Message already handled for this key
                if (dependent.contains(sdkAddressToPolyglotAddress(initiator))) {
                    return;
                }

                List<Address> blocking = blockingAddresses.getOrDefault(new ArrayList());
                for (Address target : blocking) {
                    context.sendDeadlockDetectionProbe(polyglotAddressToSdkAddress(target), initiator);
                }

                dependent.add(sdkAddressToPolyglotAddress(initiator));
                return;
            }
            return;
        }

        if (input instanceof AsyncOperationResult) {
            if (transactionInProgress.getOrDefault(false)) {
                AsyncOperationResult<ToFunction, FromFunction> result =
                        (AsyncOperationResult<ToFunction, FromFunction>) input;
                onAsyncResult(castedContext, result);
            }
            else {
                // LOGGER.warn("Unexpected AsyncOperation Result for function: " + context.self().type().toString());
            }
            return;
        }

        if (input instanceof ResponseToTransactionFunction) {
            if (transactionInProgress.getOrDefault(false)) {
                ResponseToTransactionFunction responseToTransactionFunction =
                        (ResponseToTransactionFunction) input;
                handleResponseToTransactionFunction(castedContext, responseToTransactionFunction);
            } else {
                // LOGGER.warn("Unexpected prepare phase result received.");
            }
            return;
        }

        onRequest(castedContext, (Any) input);
    }

    private void onRequest(InternalContext context, Any message) {
        ToFunction.Invocation.Builder invocationBuilder = singleInvocationBuilder(context, message);
        if (!transactionInProgress.getOrDefault(false)) {
            startTransaction(context, invocationBuilder, context.getTransactionId(), context.getAddresses());
        } else {
            List<ImmutableTriple<ToFunction.Invocation.Builder, String, List<org.apache.flink.statefun.sdk.Address>>> b =
                    batch.getOrDefault(new ArrayList());

            b.add(new ImmutableTriple(invocationBuilder, context.getTransactionId(), context.getAddresses()));
            // LOGGER.info("Add transaction request to queue.");
            batch.set(b);
        }
    }

    private void startTransaction(InternalContext context, ToFunction.Invocation.Builder invocationBuilder,
                                  String id,
                                  List<org.apache.flink.statefun.sdk.Address> addresses) {
        transactionInProgress.set(true);
        if (addresses != null) {
            for (org.apache.flink.statefun.sdk.Address address : addresses) {
                // Set all READ address values to TRUE, if they require another message they will be set to FALSE again
                // on async result
                currentTransactionFunctions.set(sdkAddressToPolyglotAddress(address), Boolean.TRUE);
            }
        }

        if (id != null) {
            currentTransactionId.set(id);
        }

        sendToFunction(context, invocationBuilder);
        return;
    }

    private void handleResponseToTransactionFunction(InternalContext context, ResponseToTransactionFunction response) {
        if (!response.getTransactionId()
                .equals(currentTransactionId.getOrDefault("."))) {
            // LOGGER.info("Received prepare phase response for different " +
            //         "transaction (probably old).");
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
        // LOGGER.info("Got async result for TPC!");
        if (asyncResult.unknown()) {
            ToFunction batch = asyncResult.metadata();
            sendToFunction(context, batch);
            return;
        }

        FromFunction.TpcFunctionInvocationResponse invocationResult =
                unpackInvocationOrThrow(context.self(), asyncResult);
        List<FromFunction.Invocation> atomicInvocations = invocationResult.getAtomicInvocationsList();

        if (currentTransactionId.get() == null) {
            currentTransactionId.set(UUID.randomUUID().toString());
        }

        for(FromFunction.Invocation invocation : atomicInvocations) {
            currentTransactionFunctions.set(invocation.getTarget(), Boolean.FALSE);
            final org.apache.flink.statefun.sdk.Address to = polyglotAddressToSdkAddress(invocation.getTarget());
            final Any message = invocation.getArgument();
            context.sendTransactionMessage(to, message, currentTransactionId.get(),
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
            // LOGGER.info("Found TPC function invocation result.");
            return fromFunction.getTpcFunctionInvocationResult();
        }
        // LOGGER.info("Did not find TPC function invocation result.");
        return FromFunction.TpcFunctionInvocationResponse.getDefaultInstance();
    }

    private void handleSuccess(InternalContext context) {
        // LOGGER.info("Handling success case for this transaction.");
        for(Address address : currentTransactionFunctions.keys()) {
            final org.apache.flink.statefun.sdk.Address to = polyglotAddressToSdkAddress(address);
            final Any message = Any.pack(Payload.getDefaultInstance());
                context.sendTransactionMessage(to, message, currentTransactionId.get(),
                        Context.TransactionMessage.COMMIT);
        }
        handleResults(context, currentTransactionResults.get().getSuccessResponse());
    }

    private void handleFailure(InternalContext context) {
        // LOGGER.info("Handling failure case for this transaction.");
        for(Address address : currentTransactionFunctions.keys()) {
            final org.apache.flink.statefun.sdk.Address to = polyglotAddressToSdkAddress(address);
            final Any message = Any.pack(Payload.getDefaultInstance());
            context.sendTransactionMessage(to, message, currentTransactionId.get(),
                    Context.TransactionMessage.ABORT);
        }
        handleResults(context, currentTransactionResults.get().getFailureResponse());
    }

    private void handleRetryable(InternalContext context) {
        // LOGGER.info("Handling deadlock case for this transaction.");
        for(Address address : currentTransactionFunctions.keys()) {
            final org.apache.flink.statefun.sdk.Address to = polyglotAddressToSdkAddress(address);
            final Any message = Any.pack(Payload.getDefaultInstance());
            context.sendTransactionMessage(to, message, currentTransactionId.get(),
                    Context.TransactionMessage.ABORT);
        }
        handleResults(context, currentTransactionResults.get().getRetryableResponse());
    }

    private void cleanUp(InternalContext context) {
        currentTransactionResults.clear();
        currentTransactionFunctions.clear();
        transactionInProgress.clear();
        currentTransactionId.clear();

        dependentAddresses.clear();
        blockingAddresses.clear();

        continueProcessingQueuedTransactions(context);
    }

    private void continueProcessingQueuedTransactions(InternalContext context) {
        List<ImmutableTriple<ToFunction.Invocation.Builder, String, List<org.apache.flink.statefun.sdk.Address>>> b =
                batch.getOrDefault(new ArrayList());

        if (b.size() == 0) {
            return;
        }

        ImmutableTriple<ToFunction.Invocation.Builder, String, List<org.apache.flink.statefun.sdk.Address>> elem = b.remove(0);
        batch.set(b);
        // LOGGER.info("Executing next transaction from queue!");
        startTransaction(context, elem.getLeft(), elem.getMiddle(), elem.getRight());
    }

    private void handleResults(Context context, InvocationResponse invocationResponse) {
        handleEgressMessages(context, invocationResponse.getOutgoingEgressesList());
        handleOutgoingMessages(context, invocationResponse.getOutgoingMessagesList());
        handleOutgoingDelayedMessages(context, invocationResponse.getDelayedInvocationsList());
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

