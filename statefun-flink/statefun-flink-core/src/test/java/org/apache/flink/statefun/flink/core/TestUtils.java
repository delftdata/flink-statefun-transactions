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
package org.apache.flink.statefun.flink.core;

import com.google.protobuf.ByteString;
import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.generated.EnvelopeAddress;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetrics;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@SuppressWarnings("WeakerAccess")
public class TestUtils {

  public static final MessageFactory ENVELOPE_FACTORY =
      MessageFactory.forKey(MessageFactoryKey.forType(MessageFactoryType.WITH_KRYO_PAYLOADS, null));

  public static final FunctionType FUNCTION_TYPE = new FunctionType("test", "a");
  public static final Address FUNCTION_1_ADDR = new Address(FUNCTION_TYPE, "a-1");
  public static final Address FUNCTION_2_ADDR = new Address(FUNCTION_TYPE, "a-2");
  public static final EnvelopeAddress DUMMY_PAYLOAD =
      EnvelopeAddress.newBuilder().setNamespace("com.foo").setType("greet").setId("user-1").build();

  public static final class FakeClient implements RequestReplyClient {
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
    ToFunction.Invocation capturedInvocation(int n) {
      return wasSentToFunction.getInvocation().getInvocations(n);
    }

    ByteString capturedState(int n) {
      return wasSentToFunction.getInvocation().getState(n).getStateValue();
    }

    public int capturedInvocationBatchSize() {
      return wasSentToFunction.getInvocation().getInvocationsCount();
    }

    public ToFunction getWasSentToFunction() {
      return wasSentToFunction;
    }

    public Supplier<FromFunction> getFromFunction() {
      return fromFunction;
    }
  }

  public static final class FakeContext implements InternalContext {

    private final TestUtils.BacklogTrackingMetrics fakeMetrics = new TestUtils.BacklogTrackingMetrics();

    Address caller;
    boolean needsWaiting;

    TransactionMessage transactionMessage;
    String transactionId;

    // capture emitted messages
    List<Map.Entry<EgressIdentifier<?>, ?>> egresses = new ArrayList<>();
    List<Map.Entry<Duration, ?>> delayed = new ArrayList<>();
    List<Map.Entry<Address, Object>> messagesSent = new ArrayList<>();
    List<Map.Entry<Address, Object>> tpcMessagesSent = new ArrayList<>();


    @Override
    public void awaitAsyncOperationComplete() {
      needsWaiting = true;
    }

    @Override
    public TestUtils.BacklogTrackingMetrics functionTypeMetrics() {
      return fakeMetrics;
    }

    @Override
    public Address self() {
      final FunctionType FN_TYPE = new FunctionType("foo", "bar");
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
      return transactionId;
    }

    @Override
    public List<Address> getAddresses() {
      return null;
    }

    public void setCaller(Address address) { caller = address; }

    public Address getCaller() {
      return caller;
    }

    public boolean isNeedsWaiting() {
      return needsWaiting;
    }

    public List<Map.Entry<EgressIdentifier<?>, ?>> getEgresses() {
      return egresses;
    }

    public List<Map.Entry<Duration, ?>> getDelayed() {
      return delayed;
    }

    public List<Map.Entry<Address, Object>> getMessagesSent() {
      return messagesSent;
    }

    public void clearMessagesSent() {
      messagesSent = new ArrayList<>();
    }


    public List<Map.Entry<Address, Object>> getTpcMessagesSent() {
      return tpcMessagesSent;
    }


    @Override
    public void sendTransactionMessage(Address to, Object message, String id, TransactionMessage transactionMessage) {
      tpcMessagesSent.add(new AbstractMap.SimpleImmutableEntry<>(to, message));
      transactionId = id;
    }

    @Override
    public void sendTransactionReadMessage(Address to, Object message, String transactionId, List<Address> addresses) {

    }

    @Override
    public void sendDeadlockDetectionProbe(Address to, Address initiator) {

    }

    @Override
    public void sendBlockingFunctions(Address to, String transactionId, List<Address> addresses) {

    }

    @Override
    public void send(Address to, Object message) {
      messagesSent.add(new AbstractMap.SimpleImmutableEntry<>(to, message));
    }

    @Override
    public <T> void send(EgressIdentifier<T> egress, T message) {
      egresses.add(new AbstractMap.SimpleImmutableEntry<>(egress, message));
    }

    @Override
    public void sendAfter(Duration delay, Address to, Object message) {
      delayed.add(new AbstractMap.SimpleImmutableEntry<>(delay, message));
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
