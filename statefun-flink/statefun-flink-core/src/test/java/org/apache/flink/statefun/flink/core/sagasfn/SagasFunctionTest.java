package org.apache.flink.statefun.flink.core.sagasfn;

import com.google.protobuf.Any;
import org.apache.flink.statefun.flink.core.TestUtils;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.junit.Test;

import static org.apache.flink.statefun.flink.core.TestUtils.FUNCTION_1_ADDR;
import static org.apache.flink.statefun.flink.core.TestUtils.FUNCTION_2_ADDR;

import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.sdkAddressToPolyglotAddress;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class SagasFunctionTest {
    private final TestUtils.FakeClient client = new TestUtils.FakeClient();
    private final TestUtils.FakeContext context = new TestUtils.FakeContext();

    private final SagasFunction functionUnderTest =
            new SagasFunction(10, client);

    @Test
    public void example() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        assertTrue(client.getWasSentToFunction().hasInvocation());
        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    @Test
    public void sendsInitialInvocations() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, standardAsyncOperationResult());
        assertThat(context.getTpcMessagesSent().size(), is(2));
    }

    @Test
    public void sendsMessagesOnSuccess() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, standardAsyncOperationResult());
        context.setCaller(FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, successResponse(context.getTransactionId()));
        context.setCaller(FUNCTION_2_ADDR);
        functionUnderTest.invoke(context, successResponse(context.getTransactionId()));

        assertThat(context.getMessagesSent().size(), is(3));
    }

    @Test
    public void sendsMessagesOnFirstFailure() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, standardAsyncOperationResult());
        context.setCaller(FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, failResponse(context.getTransactionId()));

        assertThat(context.getMessagesSent().size(), is(2));
    }

    @Test
    public void sendCompensatingMessageOnLaterSuccess() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, standardAsyncOperationResult());
        context.setCaller(FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, failResponse(context.getTransactionId()));
        context.clearMessagesSent();

        context.setCaller(FUNCTION_2_ADDR);
        functionUnderTest.invoke(context, successResponse(context.getTransactionId()));
        assertThat(context.getMessagesSent().size(), is (1));
    }

    @Test
    public void ignoreFailureMessageWithWrongId() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, standardAsyncOperationResult());
        context.setCaller(FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, successResponse(context.getTransactionId()));
        context.setCaller(FUNCTION_2_ADDR);
        functionUnderTest.invoke(context, failResponse(""));

        assertThat(context.getMessagesSent().size(), is(0));
    }

    @Test
    public void ignoreSuccessMessageWithWrongId() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, standardAsyncOperationResult());
        context.setCaller(FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, successResponse(context.getTransactionId()));
        context.setCaller(FUNCTION_2_ADDR);
        functionUnderTest.invoke(context, successResponse(""));

        assertThat(context.getMessagesSent().size(), is(0));
    }

    @Test
    public void sendsMessagesOnLastFailure() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, standardAsyncOperationResult());
        context.setCaller(FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, successResponse(context.getTransactionId()));
        context.setCaller(FUNCTION_2_ADDR);
        functionUnderTest.invoke(context, failResponse(context.getTransactionId()));

        // Send two messages for failure and 1 compensating message
        assertThat(context.getMessagesSent().size(), is(3));
    }

    @Test
    public void doNotSendUntilAllResponsesArrived() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, standardAsyncOperationResult());
        context.setCaller(FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, successResponse(context.getTransactionId()));

        assertThat(context.getMessagesSent().size(), is(0));
    }

    @Test
    public void ignoreLateFailureResponseMessage() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, standardAsyncOperationResult());
        context.setCaller(FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, failResponse(context.getTransactionId()));
        context.clearMessagesSent();

        context.setCaller(FUNCTION_2_ADDR);
        functionUnderTest.invoke(context, failResponse(context.getTransactionId()));

        assertThat(context.getMessagesSent().size(), is(0));
    }

    private FromFunction.ResponseToTransactionFunction successResponse(String id) {
        FromFunction.ResponseToTransactionFunction successResponse = FromFunction.ResponseToTransactionFunction
                .newBuilder().setSuccess(true).setTransactionId(id).build();
        return successResponse;
    }

    private FromFunction.ResponseToTransactionFunction failResponse(String id) {
        FromFunction.ResponseToTransactionFunction successResponse = FromFunction.ResponseToTransactionFunction
                .newBuilder().setSuccess(false).setTransactionId(id).build();
        return successResponse;
    }

    private AsyncOperationResult standardAsyncOperationResult() {
        FromFunction.InvocationResponse.Builder successResponse =
                FromFunction.InvocationResponse.newBuilder()
                        .addOutgoingMessages(FromFunction.Invocation.getDefaultInstance())
                        .addOutgoingMessages(FromFunction.Invocation.getDefaultInstance())
                        .addOutgoingMessages(FromFunction.Invocation.getDefaultInstance());

        FromFunction.InvocationResponse.Builder failureResponse =
                FromFunction.InvocationResponse.newBuilder()
                        .addOutgoingMessages(FromFunction.Invocation.getDefaultInstance())
                        .addOutgoingMessages(FromFunction.Invocation.getDefaultInstance());

        FromFunction.SagasFunctionInvocationResponse.Builder sagasFunctionInvocationResponse =
                FromFunction.SagasFunctionInvocationResponse.newBuilder()
                    .addInvocationPairs(
                            FromFunction.SagasFunctionPair.newBuilder()
                                    .setInitialMessage(FromFunction.Invocation.newBuilder()
                                            .setTarget(sdkAddressToPolyglotAddress(FUNCTION_1_ADDR)))
                                    .setCompensatingMessage(FromFunction.Invocation.newBuilder()
                                            .setTarget(sdkAddressToPolyglotAddress(FUNCTION_1_ADDR)))
                    ).addInvocationPairs(
                            FromFunction.SagasFunctionPair.newBuilder()
                                    .setInitialMessage(FromFunction.Invocation.newBuilder()
                                            .setTarget(sdkAddressToPolyglotAddress(FUNCTION_2_ADDR)))
                                    .setCompensatingMessage(FromFunction.Invocation.newBuilder()
                                            .setTarget(sdkAddressToPolyglotAddress(FUNCTION_2_ADDR))))
                    .setSuccessResponse(successResponse)
                    .setFailureResponse(failureResponse);


        FromFunction response =
                FromFunction.newBuilder()
                        .setSagasFunctionInvocationResult(
                                sagasFunctionInvocationResponse
                        ).build();

        return new AsyncOperationResult<>(
                new Object(), AsyncOperationResult.Status.SUCCESS, response, null);
    }
}
