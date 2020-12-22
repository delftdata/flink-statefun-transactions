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
import asyncio
import unittest
from datetime import timedelta

from google.protobuf.json_format import MessageToDict
from google.protobuf.any_pb2 import Any

from tests.examples_pb2 import LoginEvent, SeenCount
from statefun.request_reply_pb2 import ToFunction, FromFunction
from statefun import RequestReplyHandler, AsyncRequestReplyHandler
from statefun.core import StatefulFunctions, kafka_egress_record
from statefun.core import StatefulFunctions, kinesis_egress_record
from statefun import FunctionInvocationException


class InvocationBuilder(object):
    """builder for the ToFunction message"""

    def __init__(self):
        self.to_function = ToFunction()

    def with_target(self, ns, type, id):
        InvocationBuilder.set_address(ns, type, id, self.to_function.invocation.target)
        return self

    def with_state(self, name, value=None):
        state = self.to_function.invocation.state.add()
        state.state_name = name
        if value:
            any = Any()
            any.Pack(value)
            state.state_value = any.SerializeToString()
        return self

    def with_invocation(self, arg, caller=None):
        invocation = self.to_function.invocation.invocations.add()
        if caller:
            (ns, type, id) = caller
            InvocationBuilder.set_address(ns, type, id, invocation.caller)
        invocation.argument.Pack(arg)
        return self

    def SerializeToString(self):
        return self.to_function.SerializeToString()

    @staticmethod
    def set_address(namespace, type, id, address):
        address.namespace = namespace
        address.type = type
        address.id = id


def round_trip(typename, fn, to: InvocationBuilder) -> dict:
    functions = StatefulFunctions()
    functions.register(typename, fn)
    handler = RequestReplyHandler(functions)
    f = FromFunction()
    f.ParseFromString(handler(to.SerializeToString()))
    return MessageToDict(f, preserving_proto_field_name=True)


def async_round_trip(typename, fn, to: InvocationBuilder) -> dict:
    functions = StatefulFunctions()
    functions.register(typename, fn)
    handler = AsyncRequestReplyHandler(functions)

    in_bytes = to.SerializeToString()
    future = handler(in_bytes)
    out_bytes = asyncio.get_event_loop().run_until_complete(future)

    f = FromFunction()
    f.ParseFromString(out_bytes)
    return MessageToDict(f, preserving_proto_field_name=True)


def json_at(nested_structure: dict, path):
    try:
        for next in path:
            nested_structure = next(nested_structure)
        return nested_structure
    except KeyError:
        return None


def key(s: str):
    return lambda dict: dict[s]


def nth(n):
    return lambda list: list[n]


NTH_OUTGOING_MESSAGE = lambda n: [key("invocation_result"), key("outgoing_messages"), nth(n)]
NTH_STATE_MUTATION = lambda n: [key("invocation_result"), key("state_mutations"), nth(n)]
NTH_DELAYED_MESSAGE = lambda n: [key("invocation_result"), key("delayed_invocations"), nth(n)]
NTH_EGRESS = lambda n: [key("invocation_result"), key("outgoing_egresses"), nth(n)]


class SagasInBatchTestCase(unittest.TestCase):

    def test_integration(self):
        def fun(context, message):
            # state access
            seen = context.state('seen').unpack(SeenCount)
            if seen.seen > 0:
                raise FunctionInvocationException("test failure")
            else:
                seen.seen += 1
                context.state('seen').pack(seen)
                context.pack_and_reply(seen)

        #
        # build the invocation
        #
        builder = InvocationBuilder()
        builder.with_target("org.foo", "greeter", "0")

        seen = SeenCount()
        seen.seen = 0
        builder.with_state("seen", seen)

        arg = LoginEvent()
        arg.user_name = "user-1"
        builder.with_invocation(arg, ("org.foo", "greeter-java", "0"))
        builder.with_invocation(arg, ("org.foo", "greeter-java", "0"))

        #
        # invoke
        #
        result_json = round_trip("org.foo/greeter", fun, builder)

        # assert first outgoing message
        first_out_message = json_at(result_json, NTH_OUTGOING_MESSAGE(0))
        self.assertEqual(first_out_message['target']['namespace'], 'org.foo')
        self.assertEqual(first_out_message['target']['type'], 'greeter-java')
        self.assertEqual(first_out_message['target']['id'], '0')
        self.assertEqual(first_out_message['argument']['@type'], 'type.googleapis.com/k8s.demo.SeenCount')

        # assert state mutations
        first_mutation = json_at(result_json, NTH_STATE_MUTATION(0))
        self.assertEqual(first_mutation['mutation_type'], 'MODIFY')
        self.assertEqual(first_mutation['state_name'], 'seen')
        self.assertIsNotNone(first_mutation['state_value'])

        # assert failed working
        self.assertEqual(result_json['invocation_result']['failed'][0], False)
        self.assertEqual(result_json['invocation_result']['failed'][1], True)


class AsyncRequestReplyTestCase(unittest.TestCase):

    def test_integration(self):
        async def fun(context, message):
            # state access
            seen = context.state('seen').unpack(SeenCount)
            if seen.seen > 0:
                raise FunctionInvocationException("test failure")
            else:
                seen.seen += 1
                context.state('seen').pack(seen)
                context.pack_and_reply(seen)

        #
        # build the invocation
        #
        builder = InvocationBuilder()
        builder.with_target("org.foo", "greeter", "0")

        seen = SeenCount()
        seen.seen = 0
        builder.with_state("seen", seen)

        arg = LoginEvent()
        arg.user_name = "user-1"
        builder.with_invocation(arg, ("org.foo", "greeter-java", "0"))
        builder.with_invocation(arg, ("org.foo", "greeter-java", "0"))
        builder.with_invocation(arg, ("org.foo", "greeter-java", "0"))

        #
        # invoke
        #
        result_json = async_round_trip("org.foo/greeter", fun, builder)

        # assert first outgoing message
        first_out_message = json_at(result_json, NTH_OUTGOING_MESSAGE(0))
        self.assertEqual(first_out_message['target']['namespace'], 'org.foo')
        self.assertEqual(first_out_message['target']['type'], 'greeter-java')
        self.assertEqual(first_out_message['target']['id'], '0')
        self.assertEqual(first_out_message['argument']['@type'], 'type.googleapis.com/k8s.demo.SeenCount')

        # assert state mutations
        first_mutation = json_at(result_json, NTH_STATE_MUTATION(0))
        self.assertEqual(first_mutation['mutation_type'], 'MODIFY')
        self.assertEqual(first_mutation['state_name'], 'seen')
        self.assertIsNotNone(first_mutation['state_value'])

        # assert failed working
        self.assertEqual(result_json['invocation_result']['failed'][0], False)
        self.assertEqual(result_json['invocation_result']['failed'][1], True)
        self.assertEqual(result_json['invocation_result']['failed'][2], True)

class AsyncOnlySuccessRequestReplyTestCase(unittest.TestCase):

    def test_integration(self):
        async def fun(context, message):
            # state access
            seen = context.state('seen').unpack(SeenCount)
            seen.seen += 1
            context.state('seen').pack(seen)
            context.pack_and_reply(seen)

        #
        # build the invocation
        #
        builder = InvocationBuilder()
        builder.with_target("org.foo", "greeter", "0")

        seen = SeenCount()
        seen.seen = 0
        builder.with_state("seen", seen)

        arg = LoginEvent()
        arg.user_name = "user-1"
        builder.with_invocation(arg, ("org.foo", "greeter-java", "0"))
        builder.with_invocation(arg, ("org.foo", "greeter-java", "0"))
        builder.with_invocation(arg, ("org.foo", "greeter-java", "0"))

        #
        # invoke
        #
        result_json = async_round_trip("org.foo/greeter", fun, builder)

        # assert first outgoing message
        first_out_message = json_at(result_json, NTH_OUTGOING_MESSAGE(0))
        self.assertEqual(first_out_message['target']['namespace'], 'org.foo')
        self.assertEqual(first_out_message['target']['type'], 'greeter-java')
        self.assertEqual(first_out_message['target']['id'], '0')
        self.assertEqual(first_out_message['argument']['@type'], 'type.googleapis.com/k8s.demo.SeenCount')

        # assert state mutations
        first_mutation = json_at(result_json, NTH_STATE_MUTATION(0))
        self.assertEqual(first_mutation['mutation_type'], 'MODIFY')
        self.assertEqual(first_mutation['state_name'], 'seen')
        self.assertIsNotNone(first_mutation['state_value'])

        # assert failed working
        self.assertEqual(result_json['invocation_result']['failed'][0], False)
        self.assertEqual(result_json['invocation_result']['failed'][1], False)
        self.assertEqual(result_json['invocation_result']['failed'][2], False)