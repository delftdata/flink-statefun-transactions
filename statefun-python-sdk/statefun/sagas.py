################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from datetime import timedelta

from google.protobuf.any_pb2 import Any

from statefun.core import SdkAddress
from statefun.core import AnyStateHandle
from statefun.core import parse_typename

# generated function protocol
from statefun.request_reply_pb2 import FromFunction
from statefun.request_reply_pb2 import ToFunction

# import enum for function type
from enum import Enum


class SagasInvocationContext:
    def __init__(self, functions):
        self.functions = functions
        self.batch = None
        self.context = None
        self.target_function = None

    def setup(self, request_bytes):
        to_function = ToFunction()
        to_function.ParseFromString(request_bytes)
        #
        # setup
        #
        context = SagasBatchContext(to_function.invocation.target, to_function.invocation.state)
        target_function = self.functions.for_type(context.address.namespace, context.address.type)
        if target_function is None:
            raise ValueError("Unable to find a function of type ", target_function)

        self.batch = to_function.invocation.invocations
        self.context = context
        self.target_function = target_function

    def complete(self):
        from_function = FromFunction()
        invocation_result = from_function.sagas_function_invocation_result
        context = self.context
        self.add_invocation_pairs(context, invocation_result)
        self.add_success_outgoing_messages(context, invocation_result)
        self.add_failure_outgoing_messages(context, invocation_result)
        self.add_success_delayed_messages(context, invocation_result)
        self.add_failure_delayed_messages(context, invocation_result)
        self.add_success_egress(context, invocation_result)
        self.add_failure_egress(context, invocation_result)
        # reset the state for the next invocation
        self.batch = None
        self.context = None
        self.target_function = None
        # return the result
        return from_function.SerializeToString()

    @staticmethod
    def add_invocation_pairs(context, invocation_result):
        invocation_pairs = invocation_result.invocation_pairs
        for typename, id, message, compensating_message in context.invocation_pairs:
            invocation_pair = invocation_pairs.add()
            # Create initial
            namespace, type = parse_typename(typename)
            invocation_pair.initial_message.target.namespace = namespace
            invocation_pair.initial_message.target.type = type
            invocation_pair.initial_message.target.id = id
            invocation_pair.initial_message.argument.CopyFrom(message)

            # Create compensating
            invocation_pair.compensating_message.target.namespace = namespace
            invocation_pair.compensating_message.target.type = type
            invocation_pair.compensating_message.target.id = id
            invocation_pair.compensating_message.argument.CopyFrom(compensating_message)

    @staticmethod
    def add_success_outgoing_messages(context, invocation_result):
        outgoing_messages = invocation_result.outgoing_messages_on_success
        for typename, id, message in context.messages_on_success:
            outgoing = outgoing_messages.add()

            namespace, type = parse_typename(typename)
            outgoing.target.namespace = namespace
            outgoing.target.type = type
            outgoing.target.id = id
            outgoing.argument.CopyFrom(message)

    @staticmethod
    def add_failure_outgoing_messages(context, invocation_result):
        outgoing_messages = invocation_result.outgoing_messages_on_failure
        for typename, id, message in context.messages_on_failure:
            outgoing = outgoing_messages.add()

            namespace, type = parse_typename(typename)
            outgoing.target.namespace = namespace
            outgoing.target.type = type
            outgoing.target.id = id
            outgoing.argument.CopyFrom(message)


    @staticmethod
    def add_success_delayed_messages(context, invocation_result):
        delayed_invocations = invocation_result.delayed_invocations_on_success
        for delay, typename, id, message in context.delayed_messages_on_success:
            outgoing = delayed_invocations.add()

            namespace, type = parse_typename(typename)
            outgoing.target.namespace = namespace
            outgoing.target.type = type
            outgoing.target.id = id
            outgoing.delay_in_ms = delay
            outgoing.argument.CopyFrom(message)

    @staticmethod
    def add_failure_delayed_messages(context, invocation_result):
        delayed_invocations = invocation_result.delayed_invocations_on_failure
        for delay, typename, id, message in context.delayed_messages_on_failure:
            outgoing = delayed_invocations.add()

            namespace, type = parse_typename(typename)
            outgoing.target.namespace = namespace
            outgoing.target.type = type
            outgoing.target.id = id
            outgoing.delay_in_ms = delay
            outgoing.argument.CopyFrom(message)

    @staticmethod
    def add_success_egress(context, invocation_result):
        outgoing_egresses = invocation_result.outgoing_egresses_on_success
        for typename, message in context.egresses_on_success:
            outgoing = outgoing_egresses.add()

            namespace, type = parse_typename(typename)
            outgoing.egress_namespace = namespace
            outgoing.egress_type = type
            outgoing.argument.CopyFrom(message)

    @staticmethod
    def add_failure_egress(context, invocation_result):
        outgoing_egresses = invocation_result.outgoing_egresses_on_failure
        for typename, message in context.egresses_on_failure:
            outgoing = outgoing_egresses.add()

            namespace, type = parse_typename(typename)
            outgoing.egress_namespace = namespace
            outgoing.egress_type = type
            outgoing.argument.CopyFrom(message)


class SagasHandler:
    def __init__(self, functions):
        self.invocation_context = SagasInvocationContext(functions)

    def __call__(self, request_bytes):
        ic = self.invocation_context
        ic.setup(request_bytes)
        self.handle_invocation(ic)
        return ic.complete()

    @staticmethod
    def handle_invocation(ic: SagasInvocationContext):
        batch = ic.batch
        context = ic.context
        target_function = ic.target_function
        fun = target_function.func
        for invocation in batch:
            context.prepare(invocation)
            unpacked = target_function.unpack_any(invocation.argument)
            if not unpacked:
                fun(context, invocation.argument)
            else:
                fun(context, unpacked)


class AsyncSagasHandler:
    def __init__(self, functions):
        self.invocation_context = SagasInvocationContext(functions)

    async def __call__(self, request_bytes):
        ic = self.invocation_context
        ic.setup(request_bytes)
        await self.handle_invocation(ic)
        return ic.complete()

    @staticmethod
    async def handle_invocation(ic: SagasInvocationContext):
        batch = ic.batch
        context = ic.context
        target_function = ic.target_function
        fun = target_function.func
        for invocation in batch:
            context.prepare(invocation)
            unpacked = target_function.unpack_any(invocation.argument)
            if not unpacked:
                await fun(context, invocation.argument)
            else:
                await fun(context, unpacked)


class SagasBatchContext(object):
    def __init__(self, target, states):
        # remember own address
        self.address = SdkAddress(target.namespace, target.type, target.id)
        # the caller address would be set for each individual invocation in the batch
        self.caller = None
        # outgoing messages
        self.invocation_pairs = []
        self.messages_on_success = []
        self.messages_on_failure = []
        self.delayed_messages_on_success = []
        self.delayed_messages_on_failure = []
        self.egresses_on_success = []
        self.egresses_on_failure = []

    def prepare(self, invocation):
        """setup per invocation """
        if invocation.caller:
            caller = invocation.caller
            self.caller = SdkAddress(caller.namespace, caller.type, caller.id)
        else:
            self.caller = None

    # --------------------------------------------------------------------------------------
    # messages
    # --------------------------------------------------------------------------------------


    def send_invocation_pair(self, typename: str, id: str, message: Any, compensating_message: Any):
        """
        Send a message to a function of type and id.

        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
        """

        if not typename:
            raise ValueError("missing type name")
        if not id:
            raise ValueError("missing id")
        if not message:
            raise ValueError("missing message")
        out = (typename, id, message, compensating_message)
        self.atomic_invocations.append(out)


    def pack_and_send_invocation_pair(self, typename: str, id: str, message, compensating_message):
        """
        Send a Protobuf message to a function.

        This variant of send, would first pack this message
        into a google.protobuf.Any and then send it.

        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to pack into an Any and the send.
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        if not compensating_message:
                raise ValueError("missing message")
        compensating_any = Any()
        compensating_any.Pack(compensating_message)
        self.send_atomic_invocation(typename, id, any, compensating_any)


    def send(self, typename: str, id: str, message: Any, success: bool):
        """
        Send a message to a function of type and id.

        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
        :param success: whether to send this message on success or failure
        """

        if not typename:
            raise ValueError("missing type name")
        if not id:
            raise ValueError("missing id")
        if not message:
            raise ValueError("missing message")
        out = (typename, id, message)
        if success:
            self.messages_on_success.append(out)
        else:
            self.messages_on_failure.append(out)


    def pack_and_send(self, typename: str, id: str, message, success: bool):
        """
        Send a Protobuf message to a function.

        This variant of send, would first pack this message
        into a google.protobuf.Any and then send it.

        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to pack into an Any and the send.
        :param success: whether to send this message on success or failure
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        self.send(typename, id, any, success)

    def reply(self, message: Any, success: bool):
        """
        Reply to the sender (assuming there is a sender)

        :param message: the message to reply to.
        :param success: whether to send this message on success or failure
        """
        caller = self.caller
        if not caller:
            raise AssertionError(
                "Unable to reply without a caller. Was this message was sent directly from an ingress?")
        self.send(caller.typename(), caller.identity, message, success)

    def pack_and_reply(self, message, success: bool):
        """
        Reply to the sender (assuming there is a sender)

        :param message: the message to reply to.
        :param success: whether to send this message on success or failure
        """
        any = Any()
        any.Pack(message)
        self.reply(any, success)

    def send_after(self, delay: timedelta, typename: str, id: str, message: Any, success: bool):
        """
        Send a message to a function of type and id.

        :param delay: the amount of time to wait before sending this message.
        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
        :param success: whether to send this message on success or failure
        """
        if not delay:
            raise ValueError("missing delay")
        if not typename:
            raise ValueError("missing type name")
        if not id:
            raise ValueError("missing id")
        if not message:
            raise ValueError("missing message")
        duration_ms = int(delay.total_seconds() * 1000.0)
        out = (duration_ms, typename, id, message)
        if success:
            self.delayed_messages_on_success.append(out)
        else:
            self.delayed_messages_on_failure.append(out)

    def pack_and_send_after(self, delay: timedelta, typename: str, id: str, message, success: bool):
        """
        Send a message to a function of type and id.

        :param delay: the amount of time to wait before sending this message.
        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
        :param success: whether to send this message on success or failure
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        self.send_after(delay, typename, id, any, success)

    def send_egress(self, typename, message: Any, success: bool):
        """
        Sends a message to an egress defined by @typename
        :param typename: an egress identifier of the form <namespace>/<name>
        :param message: the message to send.
        """
        if not typename:
            raise ValueError("missing type name")
        if not message:
            raise ValueError("missing message")
        if success:
            self.egresses_on_success.append((typename, message))
        else:
            self.egresses_on_failure.append((typename, message))

    def pack_and_send_egress(self, typename, message, success: bool):
        """
        Sends a message to an egress defined by @typename
        :param typename: an egress identifier of the form <namespace>/<name>
        :param message: the message to send.
        :param success: whether to send this message on success or failure
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        self.send_egress(typename, any, success)

    # --------------------------------------------------------------------------------------
    # failure status for atomic messages
    # --------------------------------------------------------------------------------------
    def set_failed(self, failed_status):
        """
        Sets the fail attribute of the context to allow for atomic functions.
        :param failed_status: true for a failed invocation otherwise false
        """
        self.failed = failed_status