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
package org.apache.flink.statefun.flink.core.grpcfn;

import java.net.SocketAddress;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.jsonmodule.FunctionSpec;
import org.apache.flink.statefun.sdk.FunctionType;

public final class GrpcFunctionSpec implements FunctionSpec {
  private final FunctionType functionType;
  private final SocketAddress functionAddress;

  public GrpcFunctionSpec(FunctionType functionType, SocketAddress functionAddress) {
    this.functionType = Objects.requireNonNull(functionType);
    this.functionAddress = Objects.requireNonNull(functionAddress);
  }

  @Override
  public FunctionType functionType() {
    return functionType;
  }

  @Override
  public Kind kind() {
    return Kind.GRPC;
  }

  @Override
  public Transaction transaction() { return Transaction.NONE; }

  public SocketAddress address() {
    return functionAddress;
  }
}
