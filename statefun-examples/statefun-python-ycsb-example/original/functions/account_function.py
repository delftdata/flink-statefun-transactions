from statefun import StatefulFunctions
from statefun import RequestReplyHandler
from statefun import kafka_egress_record

import logging

from google.protobuf.any_pb2 import Any

from messages_pb2 import Response, Wrapper, State
from messages_pb2 import Insert, Read, Update

# Logging config
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

logger = logging.getLogger()

functions = StatefulFunctions()


@functions.bind("ycsb-example/account_function")
def account_function(context, request: Wrapper):
    # Get state
    state = context.state('state').unpack(State)

    # handle message
    if isinstance(request, Wrapper):
        # messages from outside
        request_id = request.request_id
        message = request.message

        if message.Is(Insert.DESCRIPTOR):
            insert = Insert()
            message.Unpack(insert)
            handle_insert(context, state, insert, request_id)
        elif message.Is(Read.DESCRIPTOR):
            read = Read()
            message.Unpack(read)
            handle_read(context, state, read, request_id)
        elif message.Is(Update.DESCRIPTOR):
            update = Update()
            message.Unpack(update)
            handle_update(context, state, update, request_id)
        else:
            send_response(context, request_id, 500)
    else:
        send_response(context, request_id, 500)


def handle_insert(context, state: State, message: Insert, request_id: str) -> None:
    if not state:
        state = State()
        state.CopyFrom(message.state)
        context.state('state').pack(state)
        send_response(context, request_id, 200, state)
    else:
        send_response(context, request_id, 500)


def handle_read(context, state: State, message: Read, request_id: str) -> None:
    if not state:
        send_response(context, request_id, 500)
    else:
        send_response(context, request_id, 200, state)


def handle_update(context, state: State, message: Update, request_id: str):
    if not state:
        send_response(context, request_id, 500)
    else:
        for key in message.updates:
            state.fields[key] = message.updates[key]
        context.state('state').pack(state)
        send_response(context, request_id, 200, state)


def send_response(context, request_id: str, status_code: int, message=None):
    response = Response(request_id=request_id, status_code=status_code)
    if message:
        out = Any()
        out.Pack(message)
        response.message.CopyFrom(out)
    egress_message = kafka_egress_record(topic="responses", key=request_id, value=response)
    context.pack_and_send_egress("ycsb-example/kafka-egress", egress_message)


handler = RequestReplyHandler(functions)

from flask import request
from flask import make_response
from flask import Flask

app = Flask(__name__)


@app.route('/statefun', methods=['POST'])
def handle():
    response_data = handler(request.data)
    response = make_response(response_data)
    response.headers.set('Content-Type', 'application/octet-stream')
    return response


if __name__ == "__main__":
    app.run()
