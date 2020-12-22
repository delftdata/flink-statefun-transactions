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
import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.polyglotAddressToSdkAddress;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Collections;
import org.apache.flink.statefun.flink.core.TestUtils;
import org.apache.flink.statefun.flink.core.httpfn.StateSpec;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.DelayedInvocation;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.EgressMessage;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.PersistedValueMutation;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.PersistedValueMutation.MutationType;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.Invocation;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.AsyncOperationResult.Status;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.junit.Test;

public class RequestReplyFunctionTest {
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
  public void callerIsSet() {
    context.setCaller(FUNCTION_1_ADDR);
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    Invocation anInvocation = client.capturedInvocation(0);
    Address caller = polyglotAddressToSdkAddress(anInvocation.getCaller());

    assertThat(caller, is(FUNCTION_1_ADDR));
  }

  @Test
  public void messageIsSet() {
    Any any = Any.pack(TestUtils.DUMMY_PAYLOAD);

    functionUnderTest.invoke(context, any);

    assertThat(client.capturedInvocation(0).getArgument(), is(any));
  }

  @Test
  public void batchIsAccumulatedWhileARequestIsInFlight() {
    // send one message
    functionUnderTest.invoke(context, Any.getDefaultInstance());
    // the following invocations should be queued and sent as a batch
    functionUnderTest.invoke(context, Any.getDefaultInstance());
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    // simulate a successful completion of the first operation
    functionUnderTest.invoke(context, successfulAsyncOperation());

    assertThat(client.capturedInvocationBatchSize(), is(2));
  }

  @Test
  public void batchIsAccumulatedWhileABatchedRequestIsInFlight() {
    // send one message
    functionUnderTest.invoke(context, Any.getDefaultInstance());
    // the following invocations should be queued and sent as a batch
    functionUnderTest.invoke(context, Any.getDefaultInstance());
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    // simulate a successful completion of the first operation
    functionUnderTest.invoke(context, successfulAsyncOperation());

    functionUnderTest.invoke(context, Any.getDefaultInstance());

    assertThat(client.capturedInvocationBatchSize(), is(2));
  }

  @Test
  public void reachingABatchLimitTriggersBackpressure() {
    RequestReplyFunction functionUnderTest = new RequestReplyFunction(states, 2, client);

    // send one message
    functionUnderTest.invoke(context, Any.getDefaultInstance());
    // the following invocations should be queued
    functionUnderTest.invoke(context, Any.getDefaultInstance());
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    // the following invocations should request backpressure
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    assertThat(context.getNeedsWaiting(), is(true));
  }

  @Test
  public void returnedMessageReleaseBackpressure() {
    RequestReplyFunction functionUnderTest = new RequestReplyFunction(states, 2, client);

    // the following invocations should cause backpressure
    functionUnderTest.invoke(context, Any.getDefaultInstance());
    functionUnderTest.invoke(context, Any.getDefaultInstance());
    functionUnderTest.invoke(context, Any.getDefaultInstance());
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    // complete one message, should send a batch of size 3
    context.setNeedsWaiting(false);
    functionUnderTest.invoke(context, successfulAsyncOperation());

    // the next message should not cause backpressure.
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    assertThat(context.getNeedsWaiting(), is(false));
  }

  @Test
  public void stateIsModified() {
    functionUnderTest.invoke(context, Any.getDefaultInstance());

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
                            .setStateName("session")))
            .build();

    functionUnderTest.invoke(context, successfulAsyncOperation(response));

    functionUnderTest.invoke(context, Any.getDefaultInstance());
    assertThat(client.capturedState(0), is(ByteString.copyFromUtf8("hello")));
  }

  @Test
  public void delayedMessages() {
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    FromFunction response =
        FromFunction.newBuilder()
            .setInvocationResult(
                InvocationResponse.newBuilder()
                    .addDelayedInvocations(
                        DelayedInvocation.newBuilder()
                            .setArgument(Any.getDefaultInstance())
                            .setDelayInMs(1)
                            .build()))
            .build();

    functionUnderTest.invoke(context, successfulAsyncOperation(response));

    assertFalse(context.getDelayed().isEmpty());
    assertEquals(Duration.ofMillis(1), context.getDelayed().get(0).getKey());
  }

  @Test
  public void egressIsSent() {
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    FromFunction response =
        FromFunction.newBuilder()
            .setInvocationResult(
                InvocationResponse.newBuilder()
                    .addOutgoingEgresses(
                        EgressMessage.newBuilder()
                            .setArgument(Any.getDefaultInstance())
                            .setEgressNamespace("org.foo")
                            .setEgressType("bar")))
            .build();

    functionUnderTest.invoke(context, successfulAsyncOperation(response));

    assertFalse(context.getEgresses().isEmpty());
    assertEquals(
        new EgressIdentifier<>("org.foo", "bar", Any.class), context.getEgresses().get(0).getKey());
  }

  @Test
  public void backlogMetricsIncreasedOnInvoke() {
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    // following should be accounted into backlog metrics
    functionUnderTest.invoke(context, Any.getDefaultInstance());
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    assertThat(context.numBacklog(), is(2));
  }

  @Test
  public void backlogMetricsDecreasedOnNextSuccess() {
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    // following should be accounted into backlog metrics
    functionUnderTest.invoke(context, Any.getDefaultInstance());
    functionUnderTest.invoke(context, Any.getDefaultInstance());

    // complete one message, should fully consume backlog
    context.setNeedsWaiting(false);
    functionUnderTest.invoke(context, successfulAsyncOperation());

    assertThat(context.numBacklog(), is(0));
  }

  private static AsyncOperationResult<Object, FromFunction> successfulAsyncOperation() {
    return new AsyncOperationResult<>(
        new Object(), Status.SUCCESS, FromFunction.getDefaultInstance(), null);
  }

  private static AsyncOperationResult<Object, FromFunction> successfulAsyncOperation(
      FromFunction fromFunction) {
    return new AsyncOperationResult<>(new Object(), Status.SUCCESS, fromFunction, null);
  }
}
