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
import java.time.Duration;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.httpfn.StateSpec;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetrics;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.Invocation;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.AsyncOperationResult.Status;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.junit.Test;

public class RequestReplyFunctionSagasTest {
    private static final FunctionType FN_TYPE = new FunctionType("foo", "bar");

    private final FakeClient client = new FakeClient();
    private final FakeContext context = new FakeContext();
    private final PersistedRemoteFunctionValues states =
            new PersistedRemoteFunctionValues(Collections.singletonList(new StateSpec("session")));

    private final RequestReplyFunction functionUnderTest =
            new RequestReplyFunction(states, 10, client);

    @Test
    public void example() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        assertTrue(client.wasSentToFunction.hasInvocation());
        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    @Test
    public void sagasPrepareMessageIsNotSentWithBatchBefore() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // First send regular messages batch
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        setSagasInContext("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        clearTpcInContext();

        functionUnderTest.invoke(context, successfulAsyncOperation());
        assertThat(client.capturedInvocationBatchSize(), is(2));
        functionUnderTest.invoke(context, successfulAsyncOperation());
        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    @Test
    public void sagasPrepareMessageIsNotSentWithBatchAfter() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        setSagasInContext("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        clearTpcInContext();

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, successfulAsyncOperation());
        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    @Test
    public void sagasPrepareMessageIsNotSentWithBatchBeforeOrAfter() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        setSagasInContext("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        clearTpcInContext();

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, successfulAsyncOperation());
        functionUnderTest.invoke(context, successfulAsyncOperation());

        assertThat(client.capturedInvocationBatchSize(), is(1));
    }

    @Test
    public void sagasBatchIsSentToFunctionAfterSagasResponse() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        // Send tpc prepare message
        setSagasInContext("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        clearTpcInContext();

        // Send regular messages
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());

        functionUnderTest.invoke(context, successfulAsyncOperation());
        functionUnderTest.invoke(context, successfulAsyncOperation());

        assertThat(client.capturedInvocationBatchSize(), is(2));
    }

    @Test
    public void sagasResponseMessageIsSentOnSuccess() {
        // Send sagas prepare message
        setSagasInContext("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        clearTpcInContext();
        // Successful response
        functionUnderTest.invoke(context, successfulAsyncOperation());

        assertThat(context.messagesSent.size(), is(1));
        Map.Entry<Address, Object> messageSent = context.messagesSent.get(0);
        assertEquals(messageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(messageSent.getValue(), instanceOf(FromFunction.ResponseToTransactionFunction.class));
        assertTrue(((FromFunction.ResponseToTransactionFunction) messageSent.getValue()).getSuccess());
    }

    @Test
    public void sagasResponseMessageIsSentOnFailure() {
        // Send tpc prepare message
        setSagasInContext("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        clearTpcInContext();
        // Failing response
        FromFunction fromFunction = FromFunction.getDefaultInstance().toBuilder()
                .setInvocationResult(
                        InvocationResponse.getDefaultInstance().toBuilder()
                                .setFailed(true)
                                .build())
                .build();
        functionUnderTest.invoke(context, successfulAsyncOperation(fromFunction));

        assertThat(context.messagesSent.size(), is(1));
        Map.Entry<Address, Object> messageSent = context.messagesSent.get(0);
        assertEquals(messageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(messageSent.getValue(), instanceOf(FromFunction.ResponseToTransactionFunction.class));
        assertFalse(((FromFunction.ResponseToTransactionFunction) messageSent.getValue()).getSuccess());
    }

    @Test
    public void sagasResponseMessageIsSentForQueuedMessage() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        // Send tpc prepare message
        setSagasInContext("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        clearTpcInContext();

        // Successful response for original messages
        functionUnderTest.invoke(context, successfulAsyncOperation());
        // Successful response for transaction message
        functionUnderTest.invoke(context, successfulAsyncOperation());


        assertThat(context.messagesSent.size(), is(1));
        Map.Entry<Address, Object> messageSent = context.messagesSent.get(0);
        assertEquals(messageSent.getKey(), FUNCTION_1_ADDR);
        assertThat(messageSent.getValue(), instanceOf(FromFunction.ResponseToTransactionFunction.class));
        assertTrue(((FromFunction.ResponseToTransactionFunction) messageSent.getValue()).getSuccess());
    }

    @Test
    public void sagasRegularRequestsFromBatch() {
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        setSagasInContext("1", FUNCTION_1_ADDR);
        functionUnderTest.invoke(context, Any.getDefaultInstance());
        clearTpcInContext();
        functionUnderTest.invoke(context, successfulAsyncOperation());
        functionUnderTest.invoke(context, successfulAsyncOperation());

        assertThat(context.messagesSent.size(), is(0));
        functionUnderTest.invoke(context, successfulAsyncOperation());
        assertThat(context.messagesSent.size(), is(1));
    }

    private static AsyncOperationResult<Object, FromFunction> successfulAsyncOperation() {
        return new AsyncOperationResult<>(
                new Object(), Status.SUCCESS, FromFunction.getDefaultInstance(), null);
    }

    private static AsyncOperationResult<Object, FromFunction> successfulAsyncOperation(
            FromFunction fromFunction) {
        return new AsyncOperationResult<>(new Object(), Status.SUCCESS, fromFunction, null);
    }

    private void setSagasInContext(String id, Address caller) {
        context.setTransactionId(id);
        context.caller = caller;
        context.setTransactionMessage(Context.TransactionMessage.SAGAS);
    }

    private void clearTpcInContext() {
        context.setTransactionId(null);
        context.caller = null;
        context.setTransactionMessage(null);
    }

    private static final class FakeClient implements RequestReplyClient {
        ToFunction wasSentToFunction;
        Supplier<FromFunction> fromFunction = FromFunction::getDefaultInstance;

        @Override
        public CompletableFuture<FromFunction> call(
                ToFunctionRequestSummary requestSummary,
                RemoteInvocationMetrics metrics,
                ToFunction toFunction) {
            this.wasSentToFunction = toFunction;
            try {
                return CompletableFuture.completedFuture(this.fromFunction.get());
            } catch (Throwable t) {
                CompletableFuture<FromFunction> failed = new CompletableFuture<>();
                failed.completeExceptionally(t);
                return failed;
            }
        }

        /** return the n-th invocation sent as part of the current batch. */
        Invocation capturedInvocation(int n) {
            return wasSentToFunction.getInvocation().getInvocations(n);
        }

        ByteString capturedState(int n) {
            return wasSentToFunction.getInvocation().getState(n).getStateValue();
        }

        public int capturedInvocationBatchSize() {
            return wasSentToFunction.getInvocation().getInvocationsCount();
        }
    }

    private static final class FakeContext implements InternalContext {

        private final BacklogTrackingMetrics fakeMetrics = new BacklogTrackingMetrics();

        Address caller;
        boolean needsWaiting;

        TransactionMessage transactionMessage;
        String transactionId;

        // capture emitted messages
        List<Map.Entry<EgressIdentifier<?>, ?>> egresses = new ArrayList<>();
        List<Map.Entry<Duration, ?>> delayed = new ArrayList<>();
        List<Map.Entry<Address, Object>> messagesSent = new ArrayList<>();

        @Override
        public void awaitAsyncOperationComplete() {
            needsWaiting = true;
        }

        @Override
        public BacklogTrackingMetrics functionTypeMetrics() {
            return fakeMetrics;
        }

        @Override
        public Address self() {
            return new Address(FN_TYPE, "0");
        }

        @Override
        public Address caller() {
            return caller;
        }

        public void setTransactionMessage(TransactionMessage t) {
            transactionMessage = t;
        }

        @Override
        public TransactionMessage getTransactionMessage() {
            return transactionMessage;
        }

        @Override
        public boolean isTransaction() {
            if (transactionMessage == null) {
                return false;
            }
            return true;
        }

        public void setTransactionId(String id) {
            transactionId = id;
        }

        @Override
        public String getTransactionId() {
            if (transactionId != null && transactionId.equals("")) {
                return null;
            }
            return transactionId;
        }

        @Override
        public List<Address> getAddresses() {
            return null;
        }

        @Override
        public void sendTransactionMessage(Address to, Object message, String transactionId, TransactionMessage transactionMessage) {

        }

        @Override
        public void sendTransactionReadMessage(Address to, Object message, String transactionId, List<Address> addresses) {

        }

        @Override
        public void send(Address to, Object message) {
            messagesSent.add(new SimpleImmutableEntry<>(to, message));
        }

        @Override
        public <T> void send(EgressIdentifier<T> egress, T message) {
            egresses.add(new SimpleImmutableEntry<>(egress, message));
        }

        @Override
        public void sendAfter(Duration delay, Address to, Object message) {
            delayed.add(new SimpleImmutableEntry<>(delay, message));
        }

        @Override
        public <M, T> void registerAsyncOperation(M metadata, CompletableFuture<T> future) {}
    }

    private static final class BacklogTrackingMetrics implements FunctionTypeMetrics {

        private int numBacklog = 0;

        public int numBacklog() {
            return numBacklog;
        }

        @Override
        public void appendBacklogMessages(int count) {
            numBacklog += count;
        }

        @Override
        public void consumeBacklogMessages(int count) {
            numBacklog -= count;
        }

        @Override
        public void remoteInvocationFailures() {}

        @Override
        public void remoteInvocationLatency(long elapsed) {}

        @Override
        public void asyncOperationRegistered() {}

        @Override
        public void asyncOperationCompleted() {}

        @Override
        public void incomingMessage() {}

        @Override
        public void outgoingRemoteMessage() {}

        @Override
        public void outgoingEgressMessage() {}

        @Override
        public void outgoingLocalMessage() {}

        @Override
        public void blockedAddress() {}

        @Override
        public void unblockedAddress() {}
    }
}
