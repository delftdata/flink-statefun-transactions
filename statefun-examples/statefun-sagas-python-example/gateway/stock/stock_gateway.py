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
from protobuf.stock_pb2 import StockRequest, StockResponse

# Setup logging
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)


def value_serializer(value_object: StockRequest):
    return value_object.SerializeToString()


def value_deserializer(value_bytes):
    stock_response = StockResponse()
    stock_response.ParseFromString(value_bytes)
    return stock_response


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
app['group_id'] = "stock-gateway"
app['consumer_topic'] = "stock-responses"
app['producer_topic'] = "stock-requests"
app['logger'] = logging.getLogger()
app['value_serializer'] = value_serializer
app['value_deserializer'] = value_deserializer
app['messages']: Dict[str, Awaitable[str]] = {}
app['timeout'] = 60

routes = web.RouteTableDef()


@routes.get('/stock')
async def hello_world(request):
    return web.Response(text="Hello from stock gateway")


@routes.get('/stock/get/{product_id}')
async def get_product(request):
    stock_request = StockRequest()
    stock_request.product_id = request.match_info['product_id']
    stock_request.request_id = request['request_id']
    stock_request.get_product.SetInParent()
    res = await kafka_utils.produce_and_wait_for_response(app, stock_request.product_id, stock_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/stock/create/{price}')
async def create_product(request):
    price = validate_amount(request.match_info['price'])
    if type(price) == web.Response:
        return price
    stock_request = StockRequest()
    stock_request.product_id = str(uuid.uuid4()).replace('-', '')
    stock_request.request_id = request['request_id']
    stock_request.create_product.price = price

    res = await kafka_utils.produce_and_wait_for_response(app, stock_request.product_id, stock_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/stock/delete/{product_id}')
async def delete_product(request):
    stock_request = StockRequest()
    stock_request.product_id = request.match_info['product_id']
    stock_request.request_id = request['request_id']
    stock_request.delete_product.SetInParent()
    res = await kafka_utils.produce_and_wait_for_response(app, stock_request.product_id, stock_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/stock/add_stock/{product_id}/{amount}')
async def add_stock(request):
    amount = validate_amount(request.match_info['amount'])
    if type(amount) == web.Response:
        return amount

    stock_request = StockRequest()
    stock_request.product_id = request.match_info['product_id']
    stock_request.request_id = request['request_id']
    stock_request.add_product_stock.amount = amount
    res = await kafka_utils.produce_and_wait_for_response(app, stock_request.product_id, stock_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


@routes.get('/stock/subtract_stock/{product_id}/{amount}')
async def subtract_stock(request):
    amount = validate_amount(request.match_info['amount'])
    if type(amount) == web.Response:
        return amount

    stock_request = StockRequest()
    stock_request.product_id = request.match_info['product_id']
    stock_request.request_id = request['request_id']
    stock_request.subtract_product_stock.amount = amount
    res = await kafka_utils.produce_and_wait_for_response(app, stock_request.product_id, stock_request)
    return web.json_response(body=MessageToJson(res), status=res.status_code)


app.router.add_routes(routes)

app.on_startup.append(kafka_utils.create_producer)
app.on_startup.append(kafka_utils.create_consumer)

app.on_cleanup.append(kafka_utils.shutdown_kafka)

if __name__ == "__main__":
    web.run_app(app)
