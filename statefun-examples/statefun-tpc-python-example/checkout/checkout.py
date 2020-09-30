from order_pb2 import OrderRequest, OrderResponse, OrderState
from user_pb2 import UserRequest
from stock_pb2 import StockRequest
from statefun import StatefulFunctions, TwoPhaseCommitHandler, kafka_egress_record

import logging
import uuid

# Logging config
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

logger = logging.getLogger()

functions = StatefulFunctions()

@functions.bind("python-example/checkout")
def order_function(context, request: OrderResponse):
    order_response = OrderResponse()
    order_response.request_id = request.request_id
    order_response.order_id = request.order_id
    if request.state.paid:
        handle_cancel_checkout(context, request)
    else:
        handle_checkout(context , request)


def handle_cancel_checkout(context, request: OrderResponse):
    total_price = get_total_price(request.state.order_items)
    user_req = UserRequest()
    user_req.request_id = str(uuid.uuid4()).replace('-', '')
    user_req.user_id = request.state.user_id
    user_req.add_user_credit.amount = total_price
    context.pack_and_send_atomic_invocation("python-example/user", user_req.user_id, user_req)
    for key in request.state.order_items:
        value = request.state.order_items[key]
        stock_req = StockRequest()
        stock_req.request_id = str(uuid.uuid4()).replace('-', '')
        stock_req.product_id = value.product_id
        stock_req.add_product_stock.amount = value.amount
        context.pack_and_send_atomic_invocation("python-example/stock", stock_req.product_id, stock_req)
    order_req = OrderRequest()
    order_req.request_id = str(uuid.uuid4()).replace('-', '')
    order_req.order_id = request.order_id
    order_req.undo_pay_order.SetInParent()
    context.pack_and_send_atomic_invocation("python-example/order", order_req.order_id, order_req)
    # Send success message
    res = OrderResponse()
    res.request_id = request.request_id
    res.order_id = request.order_id
    res.string = "Successfully cancelled order."
    res.status_code = 200
    egress_message = kafka_egress_record(topic="order-responses", key=res.request_id, value=res)
    context.pack_and_send_egress("python-example/kafka-egress", egress_message, success=True)
    # Send failure message
    res.string = "Could not cancel order."
    res.status_code = 500
    egress_message = kafka_egress_record(topic="order-responses", key=res.request_id, value=res)
    context.pack_and_send_egress("python-example/kafka-egress", egress_message, success=False)


def handle_checkout(context, request: OrderResponse):
    total_price = get_total_price(request.state.order_items)
    user_req = UserRequest()
    user_req.request_id = str(uuid.uuid4()).replace('-', '')
    user_req.user_id = request.state.user_id
    user_req.subtract_user_credit.amount = total_price
    context.pack_and_send_atomic_invocation("python-example/user", user_req.user_id, user_req)
    for key in request.state.order_items:
        value = request.state.order_items[key]
        stock_req = StockRequest()
        stock_req.request_id = str(uuid.uuid4()).replace('-', '')
        stock_req.product_id = value.product_id
        stock_req.subtract_product_stock.amount = value.amount
        context.pack_and_send_atomic_invocation("python-example/stock", stock_req.product_id, stock_req)
    order_req = OrderRequest()
    order_req.request_id = str(uuid.uuid4()).replace('-', '')
    order_req.order_id = request.order_id
    order_req.pay_order.SetInParent()
    context.pack_and_send_atomic_invocation("python-example/order", order_req.order_id, order_req)
    # Send success message
    res = OrderResponse()
    res.request_id = request.request_id
    res.order_id = request.order_id
    res.string = "Successfully checked out order."
    res.status_code = 200
    egress_message = kafka_egress_record(topic="order-responses", key=res.request_id, value=res)
    context.pack_and_send_egress("python-example/kafka-egress", egress_message, success=True)
    # Send failure message
    res.string = "Could not checkout order."
    res.status_code = 500
    egress_message = kafka_egress_record(topic="order-responses", key=res.request_id, value=res)
    context.pack_and_send_egress("python-example/kafka-egress", egress_message, success=False)


def get_total_price(order_items):
    total = 0
    for key in order_items:
        value = order_items[key]
        total += value.amount * value.price_each
    return total

handler = TwoPhaseCommitHandler(functions)

from flask import request
from flask import make_response
from flask import Flask

app = Flask(__name__)

@app.route('/statefun', methods=['POST'])
def handle():
    logger.info("Found endpoint.")
    response_data = handler(request.data)
    response = make_response(response_data)
    response.headers.set('Content-Type', 'application/octet-stream')
    return response


if __name__ == "__main__":
    app.run()