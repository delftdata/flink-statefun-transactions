from stock_pb2 import StockRequest, StockResponse, ProductState
from statefun import StatefulFunctions, RequestReplyHandler, kafka_egress_record

import logging

# Logging config
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

logger = logging.getLogger()

functions = StatefulFunctions()

@functions.bind("python-example/stock")
def stock_function(context, request: StockRequest):
    logger.info("Invoked!")
    # Get state
    state = context.state('product').unpack(ProductState)
    # Init response
    response = StockResponse()
    response.request_id = request.request_id
    response.product_id = request.product_id
    # Handle message
    message_type = request.WhichOneof('message')
    if message_type == 'create_product' and not state:
        state = ProductState()
        state.deleted = False
        state.price = request.create_product.price
        state.stock = 0
        response.status_code = 200
        response.string = "Product created."
    elif message_type == 'create_product':
        response.status_code = 500
        response.string = "Product with id already existed."
    elif not state:
        response.status_code = 500
        response.string = "Product does not exist."
    elif state.deleted:
        response.status_code = 500
        response.string = "Product was deleted."
    elif message_type == 'get_product':
        response.status_code = 200
        response.state.CopyFrom(state)
    elif message_type == 'delete_product':
        state.deleted = True
        response.status_code = 200
        response.string = "Product deleted."
    elif message_type == 'add_product_stock':
        state.stock += request.add_product_stock.amount
        response.status_code = 200
        response.string = "Stock added."
    elif message_type == 'subtract_product_stock':
        if state.stock >= request.subtract_product_stock.amount:
            state.stock -= request.subtract_product_stock.amount
            response.status_code = 200
            response.string = "Stock subtracted."
        else:
            response.status_code = 500
            response.string = "Not enough stock."
    else:
        response.status_code = 500
        response.string = "Request not recognized."
    # Pack state and send message
    if state:
        context.state('product').pack(state)
    if not context.caller.type:
        egress_message = kafka_egress_record(topic="stock-responses", key=response.request_id, value=response)
        context.pack_and_send_egress("python-example/kafka-egress", egress_message)
    elif context.caller.type == 'order':
        context.pack_and_reply(response)
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