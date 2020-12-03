from statefun import StatefulFunctions
from statefun import AsyncRequestReplyHandler
from statefun import kafka_egress_record

import typing
import logging

from google.protobuf.any_pb2 import Any

from messages_pb2 import Response, Wrapper, State
from messages_pb2 import Insert, Read, Update
from messages_pb2 import AddCredit, SubtractCredit

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
async def account_function(context, request: typing.Union[Wrapper, AddCredit, SubtractCredit]):
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
            await handle_insert(context, state, insert, request_id)
        elif message.Is(Read.DESCRIPTOR):
            read = Read()
            message.Unpack(read)
            await handle_read(context, state, read, request_id)
        elif message.Is(Update.DESCRIPTOR):
            update = Update()
            message.Unpack(update)
            await handle_update(context, state, update, request_id)
        else:
            raise UnknownMessageException(request_id)
    else:
        # internal messages
        if isinstance(request, AddCredit):
            await handle_add_credit(context, state, request)
        elif isinstance(request, SubtractCredit):
            await handle_subtract_credit(context, state, request)
        else:
            raise UnknownMessageException(request_id)


@functions.bind_exception_handler(AlreadyExistsException)
async def handle_already_exists(context, request_id):
    await send_response(context, request_id, 400)


@functions.bind_exception_handler(NotFoundException)
async def handle_not_found(context, request_id):
    await send_response(context, request_id, 404)


@functions.bind_exception_handler(UnknownMessageException)
async def handle_unkown_message(context, request_id):
    await send_response(context, request_id, 500)


@functions.bind_exception_handler(NotEnoughCreditException)
async def handle_not_enough_credit(context, request_id):
    await send_response(context, request_id, 401)


async def handle_insert(context, state: State, message: Insert, request_id: str) -> None:
    if not state:
        state = State()
        state.CopyFrom(message.state)
        context.state('state').pack(state)
        await send_response(context, request_id, 200, state)
    else:
        raise AlreadyExistsException(request_id)


async def handle_read(context, state: State, message: Read, request_id: str) -> None:
    if not state:
        raise NotFoundException(request_id)
    else:
        await send_response(context, request_id, 200, state)


async def handle_update(context, state: State, message: Update, request_id: str) -> None:
    if not state:
        raise NotFoundException(request_id)
    else:
        for key in message.updates:
            state.fields[key] = message.updates[key]
        context.state('state').pack(state)
        await send_response(context, request_id, 200, state)


# Internal messages
async def handle_add_credit(context, state: State, message: AddCredit) -> None:
    if not state:
        raise NotFoundException("NA")
    else:
        state.balance += message.amount
        context.state('state').pack(state)


async def handle_subtract_credit(context, state: State, message: SubtractCredit):
    if not state:
        raise NotFoundException("NA")
    else:
        if state.balance >= message.amount:
            state.balance -= message.amount
            context.state('state').pack(state)
        else:
            raise NotEnoughCreditException("NA")


async def send_response(context, request_id: str, status_code: int, message=None):
    response = Response(request_id=request_id, status_code=status_code)
    if message:
        out = Any()
        out.Pack(message)
        response.message.CopyFrom(out)
    egress_message = kafka_egress_record(topic="responses", key=request_id, value=response)
    context.pack_and_send_egress("ycsb-example/kafka-egress", egress_message)

from aiohttp import web

handler = AsyncRequestReplyHandler(functions)


async def handle(request):
    req = await request.read()
    res = await handler(req)
    return web.Response(body=res, content_type="application/octet-stream")

app = web.Application()
app.add_routes([web.post('/statefun', handle)])

if __name__ == '__main__':
    web.run_app(app, port=80)
