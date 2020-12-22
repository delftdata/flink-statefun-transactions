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

import static org.apache.flink.statefun.flink.core.TestUtils.FUNCTION_1_ADDR;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.Map;

import org.apache.flink.statefun.flink.core.TestUtils;
import org.apache.flink.statefun.flink.core.httpfn.StateSpec;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.generated.ResponseToTransactionFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.PersistedValueMutation;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.PersistedValueMutation.MutationType;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.AsyncOperationResult.Status;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.Test;

public class RequestReplyFunctionTpcTest {
    private static final FunctionType FN_TYPE = new FunctionType("foo", "bar");

    private final TestUtils.FakeClient client = new TestUtils.FakeClient();
    private final TestUtils.FakeContext context = new TestUtils.FakeContext();
    private final PersistedRemoteFunctionValues states =
            new PersistedRemoteFunctionValues(Collections.singletonList(new StateSpec("session")));

    private final RequestReplyFunction functionUnderTest =
            new RequestReplyFunction(states, 10, client);

    @Test
    public void example() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        assertTrue(client.getWasSentToFunction().hasInvocation());
        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    @Test
    public void tpcPrepareMessageIsNotSentWithBatchBefore() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // First send regular messages batch
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        assertThat(client.capturedInvocationBatchSize(), is(2));
        functionUnderTest.invoke(context, successfulAsyncOperation(2));
        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    @Test
    public void tpcPrepareMessageIsNotSentWithBatchAfter() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    @Test
    public void tpcPrepareMessageIsNotSentWithBatchBeforeOrAfter() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        functionUnderTest.invoke(context, successfulAsyncOperation(2));

        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    @Test
    public void tpcNoBatchIsSentToFunctionAfterTpcPrepareResponse() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        functionUnderTest.invoke(context, successfulAsyncOperation(1));

        assertThat(client.capturedInvocationBatchSize(), not(2));
    }

    @Test
    public void tpcMessagesSeparateBatches() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Send 2 regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        context.setTpcPrepare("2", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Send 3 regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send first batch (1 message)
        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        assertThat(client.capturedInvocationBatchSize(), is(1));
        // Send first tpc (1 message)
        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        assertThat(client.capturedInvocationBatchSize(), is(1));
        // Complete tpc and remove lock and send next batch (2 messages)
        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        context.setTpcCommit("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        assertThat(client.capturedInvocationBatchSize(), is(2));
        // Complete this batch and send next transaction (1 message)
        functionUnderTest.invoke(context, successfulAsyncOperation(2));
        assertThat(client.capturedInvocationBatchSize(), is(1));
        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        context.setTpcAbort("2", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        assertThat(client.capturedInvocationBatchSize(), is(3));
    }

    @Test
    public void tpcResponseMessageIsSentOnSuccess() {
        // Send tpc prepare message
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        // Successful response
        functionUnderTest.invoke(context, successfulAsyncOperation(1));

        assertThat(context.getMessagesSent().size(), is(1));
        Map.Entry<Address, Object> messageSent = context.getMessagesSent().get(0);
        assertEquals(messageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(messageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertTrue(((ResponseToTransactionFunction) messageSent.getValue()).getSuccess());
    }

    @Test
    public void tpcResponseMessageIsSentOnFailure() {
        // Send tpc prepare message
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        // Failing response
        FromFunction fromFunction = FromFunction.getDefaultInstance().toBuilder()
                .setInvocationResult(
                        InvocationResponse.getDefaultInstance().toBuilder()
                                .addFailed(true)
                                .build())
                .build();
        functionUnderTest.invoke(context, successfulAsyncOperation(fromFunction));

        assertThat(context.getMessagesSent().size(), is(1));
        Map.Entry<Address, Object> messageSent = context.getMessagesSent().get(0);
        assertEquals(messageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(messageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertFalse(((ResponseToTransactionFunction) messageSent.getValue()).getSuccess());
    }

    @Test
    public void tpcResponseMessageIsSentForQueuedMessage() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        // Send tpc prepare message
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Successful response for original messages
        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        // Successful response for transaction message
        functionUnderTest.invoke(context, successfulAsyncOperation(1));


        assertThat(context.getMessagesSent().size(), is(1));
        Map.Entry<Address, Object> messageSent = context.getMessagesSent().get(0);
        assertEquals(messageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(messageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertTrue(((ResponseToTransactionFunction) messageSent.getValue()).getSuccess());
    }

    @Test
    public void tpcRegularRequestsFromBatch() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        functionUnderTest.invoke(context, successfulAsyncOperation(1));

        assertThat(context.getMessagesSent().size(), is(0));
        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        assertThat(context.getMessagesSent().size(), is(1));
    }

    @Test
    public void tpcStateIsModifiedOnCommit() {
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        // A message returned from the function
        // that asks to put "hello" into the session state.
        FromFunction response =
                FromFunction.newBuilder()
                        .setInvocationResult(
                                InvocationResponse.newBuilder()
                                        .addStateMutations(
                                                PersistedValueMutation.newBuilder()
                                                        .setStateValue(ByteString.copyFromUtf8("hello"))
                                                        .setMutationType(MutationType.MODIFY)
                                                        .setStateName("session"))
                                        .addFailed(false))
                        .build();
        functionUnderTest.invoke(context, successfulAsyncOperation(response));

        context.setTpcCommit("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        functionUnderTest.invoke(context, Any.getDefaultInstance());
        assertThat(client.capturedState(0), is(ByteString.copyFromUtf8("hello")));
    }

    @Test
    public void tpcStateIsNotModifiedOnAbort() {
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        // A message returned from the function
        // that asks to put "hello" into the session state.
        FromFunction response =
                FromFunction.newBuilder()
                        .setInvocationResult(
                                InvocationResponse.newBuilder()
                                        .addStateMutations(
                                                PersistedValueMutation.newBuilder()
                                                        .setStateValue(ByteString.copyFromUtf8("hello"))
                                                        .setMutationType(MutationType.MODIFY)
                                                        .setStateName("session"))
                                        .addFailed(false))
                        .build();
        functionUnderTest.invoke(context, successfulAsyncOperation(response));

        context.setTpcAbort("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        functionUnderTest.invoke(context, Any.getDefaultInstance());
        assertThat(client.capturedState(0), not(ByteString.copyFromUtf8("hello")));
    }

    @Test
    public void tpcAbortMessageWithWrongIdIgnored() {
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Queue up regular requests
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        FromFunction response =
                FromFunction.newBuilder()
                        .setInvocationResult(
                                InvocationResponse.newBuilder()
                                        .addStateMutations(
                                                PersistedValueMutation.newBuilder()
                                                        .setStateValue(ByteString.copyFromUtf8("hello"))
                                                        .setMutationType(MutationType.MODIFY)
                                                        .setStateName("session"))
                                        .addFailed(false))
                        .build();
        functionUnderTest.invoke(context, successfulAsyncOperation(response));

        context.setTpcAbort("2", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    @Test
    public void tpcCommitMessageWithWrongIdIgnored() {
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Queue up regular requests
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        FromFunction response =
                FromFunction.newBuilder()
                        .setInvocationResult(
                                InvocationResponse.newBuilder()
                                        .addStateMutations(
                                                PersistedValueMutation.newBuilder()
                                                        .setStateValue(ByteString.copyFromUtf8("hello"))
                                                        .setMutationType(MutationType.MODIFY)
                                                        .setStateName("session"))
                                        .addFailed(false))
                        .build();
        functionUnderTest.invoke(context, successfulAsyncOperation(response));

        context.setTpcCommit("2", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    @Test
    public void tpcIgnoreUnexpectedSuccessfulAsyncOperationWhileLocked() {
        context.setTpcPrepare("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Queue up regular requests
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Complete tpc remote execution
        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        // No new batch should be send (function should be locked)
        assertThat(client.capturedInvocationBatchSize(), is(1));

        // Unexpected successfulAsyncOperation
        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        // Should not send the next batch of 3
        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    private static AsyncOperationResult<Object, FromFunction> successfulAsyncOperation(int number) {
        FromFunction.Builder builder = FromFunction.getDefaultInstance().toBuilder();
        InvocationResponse.Builder responseBuilder = builder.getInvocationResult().toBuilder();
        for (int i = 0; i < number; i++) {
            responseBuilder.addFailed(false);
        }
        builder.setInvocationResult(responseBuilder);

        return new AsyncOperationResult<>(
                new Object(), Status.SUCCESS,
                builder.build(),
                null);
    }

    private static AsyncOperationResult<Object, FromFunction> successfulAsyncOperation(
            FromFunction fromFunction) {
        return new AsyncOperationResult<>(new Object(), Status.SUCCESS, fromFunction, null);
    }
}
