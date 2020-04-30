import datetime
import uuid
from typing import Sequence, Dict, Any

import attr

MESSAGE_VERSION = '1.0'

MESSAGE_TYPE_RPC_RESULT = 'rpc-result'
MESSAGE_TYPE_RPC_ERROR = 'rpc-error'


class DecodingError(Exception):
    pass


def _is_iso_8601(instance, attribute: str, value: str) -> None:
    # Will raise ValueError when value is invalid.
    datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Message:

    version: str = attr.ib(default=MESSAGE_VERSION, validator=attr.validators.instance_of(str))
    message_id: str = attr.ib(default=attr.Factory(lambda: str(uuid.uuid4())),
                              validator=attr.validators.instance_of(str))

    def from_dict(self, message: Dict) -> 'Message':
        try:
            version = message['version']
        except KeyError:
            raise DecodingError('Message is missing the version field.')
        if version != MESSAGE_VERSION:
            raise DecodingError('Message has invalid version {version}')

        try:
            type = message['type']
        except KeyError:
            raise DecodingError('Message is missing the type field.')

        if type == MESSAGE_TYPE_RPC_RESULT:
            return RPCResult(**message)
        elif type == MESSAGE_TYPE_RPC_ERROR:
            return RPCError(**message)
        else:
            raise DecodingError('Message has an unknown type of {type}.')


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class RPCResult(Message):

    type: str = MESSAGE_TYPE_RPC_RESULT

    task_id: str = attr.ib(validator=attr.validators.instance_of(str))
    task_name: str = attr.ib(validator=attr.validators.instance_of(str))
    args: Sequence[str]
    kwargs: Dict[str, Any]
    start_time: str = attr.ib(validator=[attr.validators.instance_of(str), _is_iso_8601])
    completion_time: str = attr.ib(validator=[attr.validators.instance_of(str), _is_iso_8601])

    result: bytes = attr.ib(validator=attr.validators.instance_of(bytes))


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class RPCError(Message):

    type: str = MESSAGE_TYPE_RPC_ERROR

    task_id: str = attr.ib(validator=attr.validators.instance_of(str))
    task_name: str = attr.ib(validator=attr.validators.instance_of(str))
    args: Sequence[str] = attr.ib(factory=list)
    kwargs: Dict[str, Any] = attr.ib(factory=dict)
    start_time: str = attr.ib(validator=[attr.validators.instance_of(str), _is_iso_8601])
    completion_time: str = attr.ib(validator=[attr.validators.instance_of(str), _is_iso_8601])

    reason: str = attr.ib(validator=attr.validators.instance_of(str))
    message: str = attr.ib(validator=attr.validators.instance_of(str))
