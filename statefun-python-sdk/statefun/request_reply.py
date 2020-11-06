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
import copy

from google.protobuf.any_pb2 import Any

from statefun.core import SdkAddress
from statefun.core import AnyStateHandle
from statefun.core import parse_typename
from statefun.core import StatefulFunctions

# generated function protocol
from statefun.request_reply_pb2 import FromFunction
from statefun.request_reply_pb2 import ToFunction
from statefun.exceptions import FunctionInvocationException


class InvocationContext:
    def __init__(self, functions: StatefulFunctions):
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
        context = BatchContext(to_function.invocation.target, to_function.invocation.state)
        target_function = self.functions.for_type(context.address.namespace, context.address.type)
        if target_function is None:
            raise ValueError("Unable to find a function of type ", target_function)

        self.batch = to_function.invocation.invocations
        self.context = context
        self.target_function = target_function

    def complete(self):
        from_function = FromFunction()
        invocation_result = from_function.invocation_result
        context = self.context
        self.add_mutations(context, invocation_result)
        self.add_outgoing_messages(context, invocation_result)
        self.add_delayed_messages(context, invocation_result)
        self.add_egress(context, invocation_result)
        # Add the failed status to the invocation result
        self.add_failed_status(context, invocation_result)
        self.add_transaction_invocation(context, invocation_result)
        self.add_unhandled_invocations(context, invocation_result)
        # reset the state for the next invocation
        self.batch = None
        self.context = None
        self.target_function = None
        # return the result
        return from_function.SerializeToString()

    @staticmethod
    def add_outgoing_messages(context, invocation_result):
        outgoing_messages = invocation_result.outgoing_messages
        for typename, id, message in context.messages:
            outgoing = outgoing_messages.add()

            namespace, type = parse_typename(typename)
            outgoing.target.namespace = namespace
            outgoing.target.type = type
            outgoing.target.id = id
            outgoing.argument.CopyFrom(message)

    @staticmethod
    def add_mutations(context, invocation_result):
        for name, handle in context.states.items():
            if not handle.modified:
                continue
            mutation = invocation_result.state_mutations.add()

            mutation.state_name = name
            if handle.deleted:
                mutation.mutation_type = FromFunction.PersistedValueMutation.MutationType.Value('DELETE')
            else:
                mutation.mutation_type = FromFunction.PersistedValueMutation.MutationType.Value('MODIFY')
                mutation.state_value = handle.bytes()

    @staticmethod
    def add_delayed_messages(context, invocation_result):
        delayed_invocations = invocation_result.delayed_invocations
        for delay, typename, id, message in context.delayed_messages:
            outgoing = delayed_invocations.add()

            namespace, type = parse_typename(typename)
            outgoing.target.namespace = namespace
            outgoing.target.type = type
            outgoing.target.id = id
            outgoing.delay_in_ms = delay
            outgoing.argument.CopyFrom(message)

    @staticmethod
    def add_egress(context, invocation_result):
        outgoing_egresses = invocation_result.outgoing_egresses
        for typename, message in context.egresses:
            outgoing = outgoing_egresses.add()

            namespace, type = parse_typename(typename)
            outgoing.egress_namespace = namespace
            outgoing.egress_type = type
            outgoing.argument.CopyFrom(message)

    # Add failed status to invocation result
    @staticmethod
    def add_failed_status(context, invocation_result):
        invocation_result.failed = context.failed

    # Add transaction_invocation to invocation_result
    @staticmethod
    def add_transaction_invocation(context, invocation_result):
        if context.transaction_invocation:
            typename, id, message = context.transaction_invocation
            namespace, type = parse_typename(typename)
            invocation_result.outgoing_message_to_transaction.target.namespace = namespace
            invocation_result.outgoing_message_to_transaction.target.type = type
            invocation_result.outgoing_message_to_transaction.target.id = id
            invocation_result.outgoing_message_to_transaction.argument.CopyFrom(message)

    @staticmethod
    def add_unhandled_invocations(context, invocation_result):
        unhandled_invocations = invocation_result.unhandled_invocations
        for invocation in context.unhandled_invocations:
            unhandled = unhandled_invocations.add()
            unhandled.target.namespace = invocation.caller.namespace
            unhandled.target.type = invocation.caller.type
            unhandled.target.id = invocation.caller.id
            unhandled.target.CopyFrom(invocation.caller)
            unhandled.argument.CopyFrom(invocation.argument)


class RequestReplyHandler:
    def __init__(self, functions):
        self.invocation_context = InvocationContext(functions)

    def __call__(self, request_bytes):
        ic = self.invocation_context
        ic.setup(request_bytes)
        self.handle_invocation(ic)
        return ic.complete()

    @staticmethod
    def handle_invocation(ic: InvocationContext):
        batch = ic.batch
        context = ic.context
        target_function = ic.target_function
        fun = target_function.func
        for invocation in batch:
            if not context.transaction_invocation:
                context.prepare(invocation)
                untouched_context = copy.deepcopy(context)
                try:
                    unpacked = target_function.unpack_any(invocation.argument)
                    if not unpacked:
                        fun(context, invocation.argument)
                    else:
                        fun(context, unpacked)
                except FunctionInvocationException as e:
                    context = untouched_context
                    ic.context = context
                    fun = ic.functions.for_exception(e.__class__)
                    if fun:
                        fun(context, e.message)
                    context.set_failed(True)
            else:
                context.add_unhandled_invocation(invocation)


class AsyncRequestReplyHandler:
    def __init__(self, functions):
        self.invocation_context = InvocationContext(functions)

    async def __call__(self, request_bytes):
        ic = self.invocation_context
        ic.setup(request_bytes)
        await self.handle_invocation(ic)
        return ic.complete()

    @staticmethod
    async def handle_invocation(ic: InvocationContext):
        batch = ic.batch
        context = ic.context
        target_function = ic.target_function
        fun = target_function.func
        for invocation in batch:
            if not context.transaction_invocation:
                context.prepare(invocation)
                untouched_context = copy.deepcopy(context)
                try:
                    unpacked = target_function.unpack_any(invocation.argument)
                    if not unpacked:
                        fun(context, invocation.argument)
                    else:
                        fun(context, unpacked)
                except FunctionInvocationException as e:
                    context = untouched_context
                    ic.context = context
                    fun = ic.functions.for_exception(e.__class__)
                    if fun:
                        fun(context, e.message)
                    context.set_failed(True)
            else:
                context.add_unhandled_invocation(invocation)


class BatchContext(object):
    def __init__(self, target, states):
        # populate the state store with the eager state values provided in the batch
        self.states = {s.state_name: AnyStateHandle(s.state_value) for s in states}
        # remember own address
        self.address = SdkAddress(target.namespace, target.type, target.id)
        # the caller address would be set for each individual invocation in the batch
        self.caller = None
        # outgoing messages
        self.messages = []
        self.delayed_messages = []
        self.egresses = []

        # add if a transaction was invocated with a "read" value, locking this function, so stopping this batch
        self.transaction_invocation = None
        self.unhandled_invocations = []

        # Add success status boolean to define success/failure state of atomic functions
        self.failed = False

    def prepare(self, invocation):
        """setup per invocation """
        if invocation.caller:
            caller = invocation.caller
            self.caller = SdkAddress(caller.namespace, caller.type, caller.id)
        else:
            self.caller = None

    # --------------------------------------------------------------------------------------
    # state access
    # --------------------------------------------------------------------------------------

    def state(self, name):
        if name not in self.states:
            raise KeyError('unknown state name ' + name + ', states needed to be explicitly registered in module.yaml')
        return self.states[name]

    def __getitem__(self, name):
        return self.state(name).value

    def __delitem__(self, name):
        state = self.state(name)
        del state.value

    def __setitem__(self, name, value):
        state = self.state(name)
        state.value = value

    # --------------------------------------------------------------------------------------
    # messages
    # --------------------------------------------------------------------------------------

    def send(self, typename: str, id: str, message: Any):
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
        out = (typename, id, message)
        self.messages.append(out)

    def pack_and_send(self, typename: str, id: str, message):
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
        self.send(typename, id, any)

    def send_transaction_invocation(self, typename: str, id: str, message: Any):
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
        out = (typename, id, message)
        self.transaction_invocation = out

    def pack_and_send_transaction_invocation(self, typename: str, id: str, message):
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
        self.send_transaction_invocation(typename, id, any)

    def reply(self, message: Any):
        """
        Reply to the sender (assuming there is a sender)

        :param message: the message to reply to.
        """
        caller = self.caller
        if not caller:
            raise AssertionError(
                "Unable to reply without a caller. Was this message was sent directly from an ingress?")
        self.send(caller.typename(), caller.identity, message)

    def pack_and_reply(self, message):
        """
        Reply to the sender (assuming there is a sender)

        :param message: the message to reply to.
        """
        any = Any()
        any.Pack(message)
        self.reply(any)

    def send_after(self, delay: timedelta, typename: str, id: str, message: Any):
        """
        Send a message to a function of type and id.

        :param delay: the amount of time to wait before sending this message.
        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
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
        self.delayed_messages.append(out)

    def pack_and_send_after(self, delay: timedelta, typename: str, id: str, message):
        """
        Send a message to a function of type and id.

        :param delay: the amount of time to wait before sending this message.
        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        self.send_after(delay, typename, id, any)

    def send_egress(self, typename, message: Any):
        """
        Sends a message to an egress defined by @typename
        :param typename: an egress identifier of the form <namespace>/<name>
        :param message: the message to send.
        """
        if not typename:
            raise ValueError("missing type name")
        if not message:
            raise ValueError("missing message")
        self.egresses.append((typename, message))

    def pack_and_send_egress(self, typename, message):
        """
        Sends a message to an egress defined by @typename
        :param typename: an egress identifier of the form <namespace>/<name>
        :param message: the message to send.
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        self.send_egress(typename, any)

    # --------------------------------------------------------------------------------------
    # failure status for atomic messages
    # --------------------------------------------------------------------------------------
    def set_failed(self, failed_status):
        """
        Sets the fail attribute of the context to allow for atomic functions.
        :param failed_status: true for a failed invocation otherwise false
        """
        self.failed = failed_status

    def add_unhandled_invocation(self, invocation):
        self.unhandled_invocations.append(invocation)

