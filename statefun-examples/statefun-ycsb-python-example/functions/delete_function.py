from statefun import StatefulFunctions, TwoPhaseCommitHandler, kafka_egress_record
from google.protobuf.json_format import MessageToJson

import typing
import logging

from google.protobuf.any_pb2 import Any

from messages_pb2 import Response
from messages_pb2 import Transfer, AddCredit, Delete

# Logging config
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

logger = logging.getLogger()

functions = StatefulFunctions()


@functions.bind("ycsb-example/delete_function")
def transfer_function(context, request: Transfer):
    # Send messages
    delete = Delete()
    context.pack_and_send_atomic_invocation("ycsb-example/account_function", request.outgoing_id, delete)
    add_credit = AddCredit(amount = request.amount)
    context.pack_and_send_atomic_invocation("ycsb-example/account_function", request.incoming_id, add_credit)

    # Send on success
    response = Response(request_id=context.address.identity, status_code=200)
    egress_message = kafka_egress_record(topic="responses", key=context.address.identity, value=response)
    context.pack_and_send_egress("ycsb-example/kafka-egress", egress_message, success=True)
    
    # Send on failure
    response = Response(request_id=context.address.identity, status_code=422)
    egress_message = kafka_egress_record(topic="responses", key=context.address.identity, value=response)
    context.pack_and_send_egress("ycsb-example/kafka-egress", egress_message, success=False)


handler = TwoPhaseCommitHandler(functions)

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