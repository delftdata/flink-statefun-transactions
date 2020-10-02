import logging
import uuid
from typing import Dict, Awaitable

from aiohttp import web
from google.protobuf.json_format import MessageToJson

import os
import sys

curr = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(curr)
sys.path.append(parent)

import kafka_utils as kafka_utils
from protobuf.user_pb2 import UserRequest, UserResponse

# Setup logging
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)


def value_serializer(value_object: UserRequest):
    return value_object.SerializeToString()


def value_deserializer(value_bytes):
    user_response = UserResponse()
    user_response.ParseFromString(value_bytes)
    return user_response


def validate_amount(amount):
    try:
        amount = int(amount)
        if amount < 1:
            return web.Response(text="Amount should be > 0.", status=500)
        else:
            return amount
    except ValueError as e:
        return web.Response(text="No valid integer given as amount.", status=500)


# Setup setup global variables for service
app = web.Application()
app.middlewares.append(kafka_utils.add_request_id_factory(app))
app['group_id'] = "user-gateway"
app['consumer_topic'] = "user-responses"
app['producer_topic'] = "user-requests"
app['logger'] = logging.getLogger()
app['value_serializer'] = value_serializer
app['value_deserializer'] = value_deserializer
app['messages']: Dict[str, Awaitable[str]] = {}
app['timeout'] = 60

routes = web.RouteTableDef()


@routes.get('/user')
async def hello_world(request):
    return web.Response(text="Hello from user gateway")


@routes.get('/user/get/{user_id}')
async def get_user(request):
    user_request = UserRequest()
    user_request.user_id = request.match_info['user_id']
    user_request.request_id = request['request_id']
    user_request.get_user.SetInParent()
    res = await kafka_utils.produce_and_wait_for_response(app, user_request.user_id, user_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/user/create')
async def create_user(request):
    user_request = UserRequest()
    user_request.user_id = str(uuid.uuid4()).replace('-', '')
    user_request.request_id = request['request_id']
    user_request.create_user.SetInParent()
    res = await kafka_utils.produce_and_wait_for_response(app, user_request.user_id, user_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/user/delete/{user_id}')
async def delete_user(request):
    user_request = UserRequest()
    user_request.user_id = request.match_info['user_id']
    user_request.request_id = request['request_id']
    user_request.delete_user.SetInParent()
    res = await kafka_utils.produce_and_wait_for_response(app, user_request.user_id, user_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/user/add_credit/{user_id}/{amount}')
async def add_credit(request):
    amount = validate_amount(request.match_info['amount'])
    if type(amount) == web.Response:
        return amount

    user_request = UserRequest()
    user_request.user_id = request.match_info['user_id']
    user_request.request_id = request['request_id']
    user_request.add_user_credit.amount = amount
    res = await kafka_utils.produce_and_wait_for_response(app, user_request.user_id, user_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/user/subtract_credit/{user_id}/{amount}')
async def subtract_credit(request):
    amount = validate_amount(request.match_info['amount'])
    if type(amount) == web.Response:
        return amount

    user_request = UserRequest()
    user_request.user_id = request.match_info['user_id']
    user_request.request_id = request['request_id']
    user_request.subtract_user_credit.amount = amount
    res = await kafka_utils.produce_and_wait_for_response(app, user_request.user_id, user_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


app.router.add_routes(routes)

app.on_startup.append(kafka_utils.create_producer)
app.on_startup.append(kafka_utils.create_consumer)

app.on_cleanup.append(kafka_utils.shutdown_kafka)

if __name__ == "__main__":
    web.run_app(app)
