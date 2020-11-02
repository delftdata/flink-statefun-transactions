import logging
from typing import Dict, Awaitable

from aiohttp import web
from google.protobuf.json_format import MessageToJson
from google.protobuf.any_pb2 import Any

import kafka_utils
from messages_pb2 import Response, Wrapper, State
from messages_pb2 import Insert, Read, Update, Transfer, DeleteAndTransferAll

# Setup logging
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)


def value_serializer(value_object):
    return value_object.SerializeToString()


def value_deserializer(value_bytes):
    response = Response()
    response.ParseFromString(value_bytes)
    return response


def validate_amount(amount):
    try:
        amount = int(amount)
        if amount < 1:
            return web.Response(text="Amount should be > 0.", status=500)
        else:
            return amount
    except ValueError as e:
        return web.Response(text="No valid integer given as amount.", status=500)


def wrap(request_id, outgoing):
    wrapped = Wrapper()
    wrapped.request_id = request_id
    message = Any()
    message.Pack(outgoing)
    wrapped.message.CopyFrom(message)
    return wrapped


# Setup setup global variables for service
app = web.Application()
app.middlewares.append(kafka_utils.add_request_id_factory(app))
app['group_id'] = "http-gateway"
app['consumer_topic'] = "responses"
app['logger'] = logging.getLogger()
app['value_serializer'] = value_serializer
app['value_deserializer'] = value_deserializer
app['messages']: Dict[str, Awaitable[str]] = {}
app['timeout'] = 60

routes = web.RouteTableDef()


@routes.get('/')
async def hello_world(request):
    return web.Response(text="Hello from http gateway")


@routes.post('/usertable/insert')
async def insert(request):
    body = await request.json()
    insert = Insert()
    insert.id = body['id']
    insert.state.CopyFrom(State(balance=int(body['balance']), fields=body['fields']))
    
    wrapped = wrap(request['request_id'], insert)
    res = await kafka_utils.produce_and_wait_for_response(app, "insert", body['id'], wrapped)

    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/usertable/read/{id}')
async def read(request):
    read = Read()
    read.id = request.match_info['id']

    wrapped = wrap(request['request_id'], read)
    res = await kafka_utils.produce_and_wait_for_response(app, "read", request.match_info['id'], wrapped)
    
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.post('/usertable/update')
async def update(request):
    body = await request.json()
    update = Update(id=body['id'], updates=body['fields'])
        
    wrapped = wrap(request['request_id'], update)
    res = await kafka_utils.produce_and_wait_for_response(app, "update", body['id'], wrapped)

    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.post('/usertable/transfer')
async def transfer(request):
    body = await request.json()
    transfer = Transfer(
        outgoing_id=body['outgoing_id'], 
        incoming_id=body['incoming_id'],
        amount=int(body['amount']))
        
    wrapped = wrap(request['request_id'], transfer)
    res = await kafka_utils.produce_and_wait_for_response(app, "transfer", request['request_id'], wrapped)

    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.post('/usertable/delete')
async def delete(request):
    body = await request.json()
    if body['id'] == body['incoming_id']:
        return web.Response(status=500)
        
    delete = DeleteAndTransferAll(id=body['id'], incoming_id=body['incoming_id'])

    wrapped = wrap(request['request_id'], delete)
    res = await kafka_utils.produce_and_wait_for_response(app, "delete", body['id'], wrapped)

    return web.json_response(body=MessageToJson(res), status=res.status_code)


app.router.add_routes(routes)

app.on_startup.append(kafka_utils.create_producer)
app.on_startup.append(kafka_utils.create_consumer)

app.on_cleanup.append(kafka_utils.shutdown_kafka)

if __name__ == "__main__":
    web.run_app(app)
