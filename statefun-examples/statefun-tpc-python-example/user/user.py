from user_pb2 import UserRequest, UserResponse, UserState
from statefun import StatefulFunctions, RequestReplyHandler, kafka_egress_record

import logging

# Logging config
FORMAT = '[%(asctime)s] %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

logger = logging.getLogger()

functions = StatefulFunctions()

@functions.bind("python-example/user")
def user_function(context, request: UserRequest):
    # Get state
    state = context.state('user').unpack(UserState)
    # Init response
    response = UserResponse()
    response.request_id = request.request_id
    response.user_id = request.user_id
    # Handle message
    message_type = request.WhichOneof('message')
    if message_type == 'create_user' and not state:
        state = UserState()
        state.deleted = False
        state.credit = 0
        response.status_code = 200
        response.string = "User created."
    elif message_type == 'create_user':
        response.status_code = 500
        response.string = "User with id already existed."
    elif not state:
        response.status_code = 500
        response.string = "User does not exist."
    elif state.deleted:
        response.status_code = 500
        response.string = "User is deleted."
    elif message_type == 'get_user':

        response.status_code = 200
        response.state.CopyFrom(state)
    elif message_type == 'delete_user':
        state.deleted = True
        response.status_code = 200
        response.string = "User deleted."
    elif message_type == 'add_user_credit':
        state.credit += request.add_user_credit.amount
        response.status_code = 200
        response.string = "Credit added."
    elif message_type == 'subtract_user_credit':
        if state.credit >= request.subtract_user_credit.amount:
            state.credit -= request.subtract_user_credit.amount
            response.status_code = 200
            response.string = "Credit subtracted."
        else:
            response.status_code = 500
            response.string = "Not enough credit."
    else:
        response.status_code = 500
        response.string = "Request not recognized."
    # Pack state and send message
    if state:
        context.state('user').pack(state)
    if not context.caller.type:
        egress_message = kafka_egress_record(topic="user-responses", key=response.request_id, value=response)
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
