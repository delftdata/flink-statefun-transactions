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
from protobuf.order_pb2 import OrderRequest, OrderResponse, OrderItem

# Setup logging
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

def value_serializer(value_object: OrderRequest):
    return value_object.SerializeToString()


def value_deserializer(value_bytes):
    order_response = OrderResponse()
    order_response.ParseFromString(value_bytes)
    return order_response


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
app['group_id'] = "order-gateway"
app['consumer_topic'] = "order-responses"
app['producer_topic'] = "order-requests"
app['logger'] = logging.getLogger()
app['value_serializer'] = value_serializer
app['value_deserializer'] = value_deserializer
app['messages']: Dict[str, Awaitable[str]] = {}
app['timeout'] = 60

routes = web.RouteTableDef()


@routes.get('/order')
async def hello_world(request):
    return web.Response(text="Hello from order gateway")


@routes.get('/order/get/{order_id}')
async def get_order(request):
    order_request = OrderRequest()
    order_request.request_id = request['request_id']
    order_request.order_id = request.match_info['order_id']
    order_request.get_order.SetInParent()
    res = await kafka_utils.produce_and_wait_for_response(app, order_request.order_id, order_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/order/create/{user_id}')
async def create_order(request):
    order_request = OrderRequest()
    order_request.request_id = request['request_id']
    order_request.order_id = str(uuid.uuid4()).replace('-', '')
    order_request.create_order.user_id = request.match_info['user_id']
    res = await kafka_utils.produce_and_wait_for_response(app, order_request.order_id, order_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/order/delete/{order_id}')
async def delete_order(request):
    order_request = OrderRequest()
    order_request.request_id = request['request_id']
    order_request.order_id = request.match_info['order_id']
    order_request.delete_order.SetInParent()
    res = await kafka_utils.produce_and_wait_for_response(app, order_request.order_id, order_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/order/add/{order_id}/{product_id}/{amount}')
async def add_to_order(request):
    order_request = OrderRequest()
    order_request.request_id = request['request_id']
    order_request.order_id = request.match_info['order_id']
    amount = validate_amount(request.match_info['amount'])
    if type(amount) == web.Response:
        return amount
    order_request.add_to_order.order_item.product_id = request.match_info['product_id']
    order_request.add_to_order.order_item.amount = amount
    res = await kafka_utils.produce_and_wait_for_response(app, order_request.order_id, order_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/order/subtract/{order_id}/{product_id}/{amount}')
async def subtract_from_order(request):
    order_request = OrderRequest()
    order_request.request_id = request['request_id']
    order_request.order_id = request.match_info['order_id']
    amount = validate_amount(request.match_info['amount'])
    if type(amount) == web.Response:
        return amount
    order_request.subtract_from_order.order_item.amount = amount
    order_request.subtract_from_order.order_item.product_id = request.match_info['product_id']

    res = await kafka_utils.produce_and_wait_for_response(app, order_request.order_id, order_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/order/checkout/{order_id}')
async def checkout_order(request):
    order_request = OrderRequest()
    order_request.request_id = request['request_id']
    app['logger'].info("Checkout request ID : " + str(request['request_id']))
    order_request.order_id = request.match_info['order_id']
    order_request.checkout_order.SetInParent()
    res = await kafka_utils.produce_and_wait_for_response(app, order_request.order_id, order_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/order/cancel_checkout/{order_id}')
async def cancel_order(request):
    order_request = OrderRequest()
    order_request.request_id = request['request_id']
    app['logger'].info("Cancel checkout request ID : " + str(request['request_id']))
    order_request.order_id = request.match_info['order_id']
    order_request.cancel_checkout_order.SetInParent()
    res = await kafka_utils.produce_and_wait_for_response(app, order_request.order_id, order_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


app.router.add_routes(routes)

app.on_startup.append(kafka_utils.create_producer)
app.on_startup.append(kafka_utils.create_consumer)

app.on_cleanup.append(kafka_utils.shutdown_kafka)

if __name__ == "__main__":
    web.run_app(app)
