from benji.exception import BenjiException, InternalError


class AMQPMessageDecodeError(BenjiException, IOError):
    pass


class AMQPMessageEncodeError(InternalError):
    pass
