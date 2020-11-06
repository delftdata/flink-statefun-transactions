class FunctionInvocationException(Exception):
    """ Exception raised when a function fails to execute. This causes any
    transaction of which this function is part to fail and demands explicitly
    defined behaviour from the user when function is invocated without being
    part of a transaction.

    Attributes:
        context -- the invocation context of the invocation
        message -- the current message of the invocation
    """

    def __init__(self, message):
        self.message = message
        super().__init__("Function invocation was not successful due to user defined exception")
