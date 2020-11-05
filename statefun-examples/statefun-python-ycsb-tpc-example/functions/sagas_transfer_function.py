from statefun import StatefulFunctions, TwoPhaseCommitHandler, kafka_egress_record
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
def transfer_function(context, request: Wrapper):
    # Unpack wrapper
    request_id = request.request_id
    message = request.message
    
    ### handle message
    # messages from outside
    if message.Is(Transfer.DESCRIPTOR):
        transfer = Transfer()
        message.Unpack(transfer)
        handle_transfer(context, transfer, request_id)
    else:
        response = Response(request_id=request_id, status_code=500)
        egress_message = kafka_egress_record(topic="responses", key=request_id, value=response)
        context.pack_and_send_egress("ycsb-example/kafka-egress", egress_message, success=False)



def handle_transfer(context, message: Transfer, request_id: str) -> None:
    # Send invocation pairs
    subtract_credit = SubtractCredit(amount = message.amount)
    add_credit = AddCredit(amount = message.amount)
    context.pack_and_send_invocation_pair(
        "ycsb-example/account_function", 
        message.outgoing_id, 
        subtract_credit,
        add_credit)
    context.pack_and_send_invocation_pair(
        "ycsb-example/account_function", 
        message.incoming_id, 
        add_credit, 
        subtract_credit)

    # Send on success
    response = Response(request_id=request_id, status_code=200)
    egress_message = kafka_egress_record(topic="responses", key=request_id, value=response)
    context.pack_and_send_egress("ycsb-example/kafka-egress", egress_message, success=True)
    
    # Send on failure
    response = Response(request_id=request_id, status_code=422)
    egress_message = kafka_egress_record(topic="responses", key=request_id, value=response)
    context.pack_and_send_egress("ycsb-example/kafka-egress", egress_message, success=False)


handler = SagasHandler(functions)

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