from statefun import StatefulFunctions, AsyncTwoPhaseCommitHandler, kafka_egress_record
from google.protobuf.json_format import MessageToJson

import logging

from google.protobuf.any_pb2 import Any

from messages_pb2 import Response, Wrapper
from messages_pb2 import Transfer, AddCredit, SubtractCredit

# Logging config
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

logger = logging.getLogger()

functions = StatefulFunctions()


@functions.bind("ycsb-example/transfer_function")
async def transfer_function(context, request: Wrapper):
    # Unpack wrapper
    request_id = request.request_id
    message = request.message
    
    ### handle message
    # messages from outside
    if message.Is(Transfer.DESCRIPTOR):
        transfer = Transfer()
        message.Unpack(transfer)
        await handle_transfer(context, transfer, request_id)


async def handle_transfer(context, message: Transfer, request_id: str) -> None:
    # Send messages
    subtract_credit = SubtractCredit(amount = message.amount)
    context.pack_and_send_atomic_invocation("ycsb-example/account_function", message.outgoing_id, subtract_credit)
    add_credit = AddCredit(amount = message.amount)
    context.pack_and_send_atomic_invocation("ycsb-example/account_function", message.incoming_id, add_credit)

    # Send on success
    response = Response(request_id=request_id, status_code=200)
    egress_message = kafka_egress_record(topic="responses", key=request_id, value=response)
    context.pack_and_send_egress_on_success("ycsb-example/kafka-egress", egress_message)
    
    # Send on failure
    response = Response(request_id=request_id, status_code=422)
    egress_message = kafka_egress_record(topic="responses", key=request_id, value=response)
    context.pack_and_send_egress_on_failure("ycsb-example/kafka-egress", egress_message)

    # Send on retryable (e.g. deadlock)
    response = Response(request_id=request_id, status_code=401)
    egress_message = kafka_egress_record(topic="responses", key=request_id, value=response)
    context.pack_and_send_egress_on_retryable("ycsb-example/kafka-egress", egress_message)


from aiohttp import web

handler = AsyncTwoPhaseCommitHandler(functions)


async def handle(request):
    req = await request.read()
    res = await handler(req)
    return web.Response(body=res, content_type="application/octet-stream")

app = web.Application()
app.add_routes([web.post('/statefun', handle)])

if __name__ == '__main__':
    web.run_app(app, port=80)
