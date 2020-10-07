from order_pb2 import OrderRequest, OrderResponse, OrderState
from user_pb2 import UserRequest, UserResponse
from stock_pb2 import StockRequest, StockResponse
from statefun import StatefulFunctions, RequestReplyHandler, kafka_egress_record
from google.protobuf.json_format import MessageToJson

import typing
import logging

# Logging config
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

logger = logging.getLogger()

functions = StatefulFunctions()

@functions.bind("python-example/order")
def order_function(context, request: typing.Union[OrderRequest, UserResponse, StockResponse]):
    if isinstance(request, OrderRequest):
        order_request_handler(context, request)
    elif isinstance(request, UserResponse):
        user_response_handler(context, request)
    elif isinstance(request, StockResponse):
        stock_response_handler(context, request)

def order_request_handler(context, request: OrderRequest):
    # Get state
    state = context.state('order').unpack(OrderState)
    # Init response
    response = OrderResponse()
    response.request_id = request.request_id
    response.order_id = request.order_id
    # Handle message
    message_type = request.WhichOneof('message')
    if message_type == 'create_order' and not state:
        state = OrderState()
        state.deleted = False
        state.paid = False
        user_request = UserRequest()
        user_request.request_id = request.request_id
        user_request.user_id = request.create_order.user_id
        user_request.get_user.SetInParent()
        context.pack_and_send('python-example/user', user_request.user_id, user_request)
        response = None
    elif message_type == 'create_order':
        response.status_code = 500
        response.string = "Order with id already existed."
    elif not state:
        response.status_code = 500
        response.string = "Order does not exist."
    elif state.deleted:
        response.status_code = 500
        response.string = "Order was deleted."
    elif message_type == 'get_order':
        response.status_code = 200
        response.state.CopyFrom(state)
    elif message_type == 'undo_pay_order' and state.paid:
        state.paid = False
        response = None
    elif message_type == 'cancel_checkout_order' and state.paid:
        # send checkout message
        response.status_code = 200
        response.state.CopyFrom(state)
        context.pack_and_send("python-example/checkout", response.order_id, response)
        response = None
    elif state.paid:
        response.status_code = 500
        response.string = "Order was already paid and completed. Could not perform operation."
    elif message_type == 'delete_order':
        state.deleted = True
        response.status_code = 200
        response.string = "Product deleted."
    elif message_type == 'add_to_order':
        order_item = request.add_to_order.order_item
        if order_item.product_id in state.order_items:
            state.order_items[order_item.product_id].amount += order_item.amount
            response.status_code = 200
            response.string = "Added to order."
        else:
            state.order_items[order_item.product_id].product_id = order_item.product_id 
            state.order_items[order_item.product_id].amount = order_item.amount
            stock_request = StockRequest()
            stock_request.request_id = request.request_id
            stock_request.product_id = order_item.product_id
            stock_request.get_product.SetInParent()
            context.pack_and_send('python-example/stock', stock_request.product_id, stock_request)
            response = None
    elif message_type == 'subtract_from_order':
        order_item = request.subtract_from_order.order_item
        if order_item.product_id in state.order_items:
            if state.order_items[order_item.product_id].amount >= order_item.amount:
                state.order_items[order_item.product_id].amount -= order_item.amount
                response.status_code = 200
                response.string = "Subtracted from order."
            else:
                response.status_code = 500
                response.string = "Not enough products in order to subtract."
        else:
            response.status_code = 500
            response.string = "No product with product id in order."
    elif message_type == 'pay_order' and not state.paid:
        state.paid = True
        response = None
    elif message_type == 'checkout_order' and not state.paid:
        # send checkout message
        response.status_code = 200
        response.state.CopyFrom(state)
        context.pack_and_send("python-example/checkout", response.order_id, response)
        response = None
    else:
        response.status_code = 500
        response.string = "Request not recognized."
    # Pack state and send message
    if state:
        context.state('order').pack(state)
    if response and not context.caller.type:
        logger.info("EGRESS KEY = "  + response.request_id)
        egress_message = kafka_egress_record(topic="order-responses", key=response.request_id, value=response)
        context.pack_and_send_egress("python-example/kafka-egress", egress_message)


def user_response_handler(context, request: UserResponse):
    # This method is called when a order is created to validate user_id
    # Get state
    state = context.state('order').unpack(OrderState)
    # Init response
    response = OrderResponse()
    response.request_id = request.request_id
    response.order_id = context.address.identity
    if int(str(request.status_code)[0]) == 2:
        state.user_id = request.user_id
        response.status_code = 200
        response.string = "Order created."
    else:
        del context['order']
        response.status_code = 500
        response.string = "Could not create order for non-existing user."
    context.state('order').pack(state)
    logger.info("EGRESS KEY (2) = "  + response.request_id)
    egress_message = kafka_egress_record(topic="order-responses", key=response.request_id, value=response)
    context.pack_and_send_egress("python-example/kafka-egress", egress_message)
    if response.status_code == 500:
        context.set_failed(True)


def stock_response_handler(context, request: StockResponse):
    # This method is called when a order is created to validate product_id
    # Get state
    state = context.state('order').unpack(OrderState)
    # Init response
    response = OrderResponse()
    response.request_id = request.request_id
    response.order_id = context.address.identity
    if int(str(request.status_code)[0]) == 2:
        state.order_items[request.product_id].price_each = request.state.price
        response.status_code = 200
        response.string = "Added to order."
    else:
        del state.order_items[request.product_id]
        response.status_code = 500
        response.string = "Could not add item for non-existing product."
    context.state('order').pack(state)
    logger.info("EGRESS KEY (3) = "  + response.request_id)
    egress_message = kafka_egress_record(topic="order-responses", key=response.request_id, value=response)
    context.pack_and_send_egress("python-example/kafka-egress", egress_message)
    if response.status_code == 500:
        context.set_failed(True)



handler = RequestReplyHandler(functions)

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