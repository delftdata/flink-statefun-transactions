from statefun import StatefulFunctions
from statefun import RequestReplyHandler
from statefun import kafka_egress_record

import typing
import logging

from google.protobuf.any_pb2 import Any

from messages_pb2 import Response, Wrapper, State
from messages_pb2 import Insert, Read, Update, DeleteAndTransferAll
from messages_pb2 import Transfer, Delete, AddCredit, SubtractCredit

from exceptions import NotFoundException
from exceptions import AlreadyExistsException
from exceptions import UnknownMessageException
from exceptions import NotEnoughCreditException

# Logging config
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

logger = logging.getLogger()

functions = StatefulFunctions()


@functions.bind("ycsb-example/account_function")
def account_function(context, request: typing.Union[Wrapper, Delete, AddCredit, SubtractCredit]):
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
        elif message.Is(DeleteAndTransferAll.DESCRIPTOR):
            delete = DeleteAndTransferAll()
            message.Unpack(delete)
            handle_delete_and_transfer_all(context, state, delete, request_id)
        else:
            raise UnknownMessageException(request_id)
    else:
        # internal messages
        if isinstance(request, Delete):
            handle_delete(context, state, request)
        elif isinstance(request, AddCredit):
            handle_add_credit(context, state, request)
        elif isinstance(request, SubtractCredit):
            handle_subtract_credit(context, state, request)
        else:
            raise UnknownMessageException(request_id)


@functions.bind_exception_handler(AlreadyExistsException)
def handle_already_exists(context, request_id):
    send_response(context, request_id, 400)


@functions.bind_exception_handler(NotFoundException)
def handle_not_found(context, request_id):
    send_response(context, request_id, 404)


@functions.bind_exception_handler(UnknownMessageException)
def handle_unkown_message(context, request_id):
    send_response(context, request_id, 500)


@functions.bind_exception_handler(NotEnoughCreditException)
def handle_not_enough_credit(context, request_id):
    send_response(context, request_id, 401)


def handle_insert(context, state: State, message: Insert, request_id: str) -> None:
    if not state:
        state = State()
        state.CopyFrom(message.state)
        context.state('state').pack(state)
        send_response(context, request_id, 200, state)
    else:
        raise AlreadyExistsException(request_id)


def handle_read(context, state: State, message: Read, request_id: str) -> None:
    if not state:
        raise NotFoundException(request_id)
    else:
        send_response(context, request_id, 200, state)


def handle_update(context, state: State, message: Update, request_id: str) -> None:
    if not state:
        raise NotFoundException(request_id)
    else:
        for key in message.updates:
            state.fields[key] = message.updates[key]
        context.state('state').pack(state)
        send_response(context, request_id, 200, state)


def handle_delete_and_transfer_all(context, state: State, message: DeleteAndTransferAll, request_id: str) -> None:
    if not state:
        raise NotFoundException(request_id)
    else:
        transfer = Transfer(outgoing_id=message.id, incoming_id=message.incoming_id, amount=state.balance)
        context.pack_and_send_transaction_invocation("ycsb-example/delete_function", request_id, transfer)


# Internal messages
def handle_delete(context, state: State, message: Delete) -> None:
    if not state:
        raise NotFoundException("NA")
    else:
        del context['state']


def handle_add_credit(context, state: State, message: AddCredit) -> None:
    if not state:
        raise NotFoundException("NA")
    else:
        state.balance += message.amount
        context.state('state').pack(state)


def handle_subtract_credit(context, state: State, message: SubtractCredit) -> None:
    if not state:
        raise NotFoundException("NA")
    else:
        if state.balance >= message.amount:
            state.balance -= message.amount
            context.state('state').pack(state)
        else:
            raise NotEnoughCreditException("NA")


def send_response(context, request_id: str, status_code: int, message=None) -> None:
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
