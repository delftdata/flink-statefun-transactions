from statefun.exceptions import FunctionInvocationException


class AlreadyExistsException(FunctionInvocationException):
    def __init__(self, request_id):
        super().__init__(request_id)


class NotFoundException(FunctionInvocationException):
    def __init__(self, request_id):
        super().__init__(request_id)


class NotEnoughCreditException(FunctionInvocationException):
    def __init__(self, request_id):
        super().__init__(request_id)


class UnknownMessageException(FunctionInvocationException):
    def __init__(self):
        super().__init__(None)
