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

import static org.apache.flink.statefun.flink.core.TestUtils.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Any;
import java.util.*;

import org.apache.flink.statefun.flink.core.TestUtils;
import org.apache.flink.statefun.flink.core.httpfn.StateSpec;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.generated.ResponseToTransactionFunction;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.AsyncOperationResult.Status;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.Test;

public class RequestReplyFunctionSagasTest {
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
    public void sagasInMiddleOfBatchResponseIsSentSuccess() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        context.setSagas("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        functionUnderTest.invoke(context, successfulAsyncOperation(5));

        assertThat(context.getMessagesSent().size(), is(1));
        Map.Entry<Address, Object> messageSent = context.getMessagesSent().get(0);
        assertEquals(messageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(messageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertTrue(((ResponseToTransactionFunction) messageSent.getValue()).getSuccess());
    }

    @Test
    public void sagasInMiddleOfBatchResponseIsSentFailed() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        context.setSagas("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        functionUnderTest.invoke(context, successfulAsyncOperation(5, 2));

        assertThat(context.getMessagesSent().size(), is(1));
        Map.Entry<Address, Object> messageSent = context.getMessagesSent().get(0);
        assertEquals(messageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(messageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertFalse(((ResponseToTransactionFunction) messageSent.getValue()).getSuccess());
        assertFalse(((ResponseToTransactionFunction) messageSent.getValue()).getSuccess());
    }

    @Test
    public void sagasInBatchResponseIsSentSuccess() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        context.setSagas("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        functionUnderTest.invoke(context, successfulAsyncOperation(3));

        assertThat(context.getMessagesSent().size(), is(1));
        Map.Entry<Address, Object> messageSent = context.getMessagesSent().get(0);
        assertEquals(messageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(messageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertTrue(((ResponseToTransactionFunction) messageSent.getValue()).getSuccess());
    }

    @Test
    public void sagasInBatchResponseIsSentFailed() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        context.setSagas("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        functionUnderTest.invoke(context, successfulAsyncOperation(3, 0));

        assertThat(context.getMessagesSent().size(), is(1));
        Map.Entry<Address, Object> messageSent = context.getMessagesSent().get(0);
        assertEquals(messageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(messageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertFalse(((ResponseToTransactionFunction) messageSent.getValue()).getSuccess());
    }

    @Test
    public void sagasResponseMessageIsSentOnSuccess() {
        // Send sagas prepare message
        context.setSagas("1", FUNCTION_1_ADDR);
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
    public void sagasResponseMessageIsSentOnFailure() {
        // Send tpc prepare message
        context.setSagas("1", FUNCTION_1_ADDR);
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
    public void sagasResponseMessageIsSentForQueuedMessage() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        // Send sagas message
        context.setSagas("1", FUNCTION_1_ADDR);
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
    public void correctAddressSagasResponseMessageIsSentForQueuedMessage() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        // Send sagas message
        context.setSagas("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        context.setSagas("2", FUNCTION_2_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());


        // Successful response for original messages
        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        // Successful response for transaction message
        functionUnderTest.invoke(context, successfulAsyncOperation(8, 5));


        assertThat(context.getMessagesSent().size(), is(2));
        Map.Entry<Address, Object> firstMessageSent = context.getMessagesSent().get(0);
        assertEquals(firstMessageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(firstMessageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertTrue(((ResponseToTransactionFunction) firstMessageSent.getValue()).getSuccess());
        assertEquals(((ResponseToTransactionFunction) firstMessageSent.getValue()).getTransactionId(), "1");

        Map.Entry<Address, Object> secondMessageSent = context.getMessagesSent().get(1);
        assertEquals(secondMessageSent.getKey(), FUNCTION_2_ADDR);
        assertThat(secondMessageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertFalse(((ResponseToTransactionFunction) secondMessageSent.getValue()).getSuccess());
        assertEquals(((ResponseToTransactionFunction) secondMessageSent.getValue()).getTransactionId(), "2");
    }

    @Test
    public void correctAddressSagasResponseMessageIsSentForFirstInQueuedMessage() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send sagas message
        context.setSagas("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        context.setSagas("2", FUNCTION_2_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());


        // Successful response for original messages
        functionUnderTest.invoke(context, successfulAsyncOperation(1));
        // Successful response for transaction message
        functionUnderTest.invoke(context, successfulAsyncOperation(6, 3));


        assertThat(context.getMessagesSent().size(), is(2));
        Map.Entry<Address, Object> firstMessageSent = context.getMessagesSent().get(0);
        assertEquals(firstMessageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(firstMessageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertTrue(((ResponseToTransactionFunction) firstMessageSent.getValue()).getSuccess());
        assertEquals(((ResponseToTransactionFunction) firstMessageSent.getValue()).getTransactionId(), "1");

        Map.Entry<Address, Object> secondMessageSent = context.getMessagesSent().get(1);
        assertEquals(secondMessageSent.getKey(), FUNCTION_2_ADDR);
        assertThat(secondMessageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertFalse(((ResponseToTransactionFunction) secondMessageSent.getValue()).getSuccess());
        assertEquals(((ResponseToTransactionFunction) secondMessageSent.getValue()).getTransactionId(), "2");
    }

    @Test
    public void correctAddressSagasResponseMessageIsSentForSecondQueue() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send sagas message
        context.setSagas("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        context.setSagas("2", FUNCTION_2_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());


        // Successful response for original messages
        functionUnderTest.invoke(context, successfulAsyncOperation(1));

        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.setSagas("3", FUNCTION_3_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Successful response for first queue
        functionUnderTest.invoke(context, successfulAsyncOperation(6, 3));

        // Successful response for second queue
        functionUnderTest.invoke(context, successfulAsyncOperation(4));

        assertThat(context.getMessagesSent().size(), is(3));
        Map.Entry<Address, Object> firstMessageSent = context.getMessagesSent().get(0);
        assertEquals(firstMessageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(firstMessageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertTrue(((ResponseToTransactionFunction) firstMessageSent.getValue()).getSuccess());
        assertEquals(((ResponseToTransactionFunction) firstMessageSent.getValue()).getTransactionId(), "1");

        Map.Entry<Address, Object> secondMessageSent = context.getMessagesSent().get(1);
        assertEquals(secondMessageSent.getKey(), FUNCTION_2_ADDR);
        assertThat(secondMessageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertFalse(((ResponseToTransactionFunction) secondMessageSent.getValue()).getSuccess());
        assertEquals(((ResponseToTransactionFunction) secondMessageSent.getValue()).getTransactionId(), "2");

        Map.Entry<Address, Object> thirdMessageSent = context.getMessagesSent().get(2);
        assertEquals(thirdMessageSent.getKey(), FUNCTION_3_ADDR);
        assertThat(thirdMessageSent.getValue(), instanceOf(ResponseToTransactionFunction.class));
        assertTrue(((ResponseToTransactionFunction) thirdMessageSent.getValue()).getSuccess());
        assertEquals(((ResponseToTransactionFunction) thirdMessageSent.getValue()).getTransactionId(), "3");
    }

    @Test
    public void batchWithSagasIsSent() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send sagas message
        context.setSagas("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        context.setSagas("2", FUNCTION_2_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        context.clearTransaction();
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Successful response for original message
        functionUnderTest.invoke(context, successfulAsyncOperation(1));

        assertThat(client.capturedInvocationBatchSize(), is(6));
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

    private static AsyncOperationResult<Object, FromFunction> successfulAsyncOperation(int number, int indexFailed) {
        FromFunction.Builder builder = FromFunction.getDefaultInstance().toBuilder();
        InvocationResponse.Builder responseBuilder = builder.getInvocationResult().toBuilder();
        for (int i = 0; i < number; i++) {
            if (i == indexFailed) {
                responseBuilder.addFailed(true);
            } else {
                responseBuilder.addFailed(false);
            }
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
