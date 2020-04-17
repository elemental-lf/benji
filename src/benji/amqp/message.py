import datetime
import uuid
from typing import Any, Dict, ByteString, Union

import umsgpack

from benji.amqp.exception import AMQPMessageDecodeError, AMQPMessageEncodeError
from benji.repr import ReprMixIn

MESSAGE_FIELD_VERSION = 'version'
MESSAGE_VERSION = '1.0'

MESSAGE_FIELD_ID = 'id'

MESSAGE_FIELD_TYPE = 'type'
MESSAGE_TYPE_UNKNOWN = 'unknown'
MESSAGE_TYPE_RPC_CALL = 'rpc-call'
MESSAGE_TYPE_RPC_RESULT = 'rpc-result'
MESSAGE_TYPE_RPC_ERROR = 'rpc-error'
MESSAGE_TYPE_EVENT_VERSION_ADD = 'event-version-add'
MESSAGE_TYPE_EVENT_VERSION_REMOVE = 'event-version-remove'

MESSAGE_TYPES = (MESSAGE_TYPE_RPC_CALL, MESSAGE_TYPE_RPC_RESULT, MESSAGE_TYPE_RPC_ERROR, MESSAGE_TYPE_EVENT_VERSION_ADD,
                 MESSAGE_TYPE_EVENT_VERSION_REMOVE)

MESSAGE_FIELD_PAYLOAD = 'payload'

# rpc-call
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_CORRELATION_ID = 'correlation_id'
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_TASK = 'task'
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_ARGUMENTS = 'arguments'

# rpc-result
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_CORRELATION_ID = 'correlation_id'
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_RESULT = 'result'
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_START_TIME = 'start_time'
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_COMPLETION_TIME = 'completion_time'

# rpc-error
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_CORRELATION_ID = 'correlation_id'
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_REASON = 'reason'
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_MESSAGE = 'message'
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_START_TIME = 'start_time'
MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_COMPLETION_TIME = 'completion_time'

# event-version-add
MESSAGE_FIELD_MESSAGE_PAYLOAD_EVENT_VERSION_ADD_VERSION = 'version'

# event-version-remove
MESSAGE_FIELD_MESSAGE_PAYLOAD_EVENT_VERSION_REMOVE_VERSION = 'version'


def _is_iso_8601(time: str) -> bool:
    try:
        datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%fZ')
        return True
    except ValueError:
        return False


class AMQPMessage(ReprMixIn):

    def __init__(self, *, message_version: str, message_id: str, message_type: str, message_payload: Dict[str,
                                                                                                          Any]) -> None:
        if message_version != MESSAGE_VERSION:
            raise AMQPMessageEncodeError(f'Unsupported message version {message_version}.')
        self._version = message_version

        if not isinstance(message_id, str):
            raise AMQPMessageEncodeError(f'Message id has wrong type {type(message_id)}')
        if message_id == '':
            raise AMQPMessageEncodeError(f'Message id is None or empty.')
        self._id = message_id

        if not isinstance(message_type, str):
            raise AMQPMessageEncodeError(f'Message type has wrong type {type(message_id)}')
        if message_type not in MESSAGE_TYPES:
            raise AMQPMessageEncodeError(f'Message type {message_type} is invalid.')
        self._type = message_type

        if not isinstance(message_payload, dict):
            raise AMQPMessageDecodeError(f'Message payload has wrong type {type(message_payload)}.')
        self._payload = message_payload

    @property
    def version(self) -> str:
        return self._version

    @property
    def id(self) -> str:
        return self._id

    @property
    def type(self) -> str:
        return self._type

    @property
    def payload(self) -> Dict[str, Any]:
        return self._payload

    def marshal(self) -> bytes:
        message = {
            MESSAGE_FIELD_ID: self.id,
            MESSAGE_FIELD_VERSION: self.version,
            MESSAGE_FIELD_TYPE: self.type,
            MESSAGE_FIELD_PAYLOAD: self.payload,
        }

        return umsgpack.packb(message)

    @classmethod
    def unmarshall(cls, body: ByteString) -> 'AMQPMessage':
        try:
            message = umsgpack.unpackb(body)
        except (TypeError, umsgpack.UnpackException) as exception:
            raise AMQPMessageDecodeError(f'AMQP message body decoding error: {exception}.')

        if not isinstance(message, dict):
            raise AMQPMessageDecodeError(f'AMQP message body has the wrong type {type(message)}.')

        for field in (MESSAGE_FIELD_VERSION, MESSAGE_FIELD_TYPE, MESSAGE_FIELD_ID, MESSAGE_FIELD_PAYLOAD):
            if field not in message:
                raise AMQPMessageDecodeError(f'Required field {field} is missing.')

        message_version = message[MESSAGE_FIELD_VERSION]
        message_id = message[MESSAGE_FIELD_ID]
        message_type = message[MESSAGE_FIELD_TYPE]
        message_payload = message[MESSAGE_FIELD_PAYLOAD]

        if not isinstance(message_version, str):
            raise AMQPMessageDecodeError(f'Message version has wrong type {type(message_version)}.')
        if message_version != MESSAGE_VERSION:
            raise AMQPMessageDecodeError(f'Unsupported version {message_version}.')

        if not isinstance(message_id, str):
            raise AMQPMessageDecodeError(f'Message id has wrong type {type(message_id)}.')
        if message_id == '':
            raise AMQPMessageDecodeError(f'Message id is empty.')

        if not isinstance(message_type, str):
            raise AMQPMessageDecodeError(f'Message type has wrong type {type(message_type)}.')

        if not isinstance(message_payload, dict):
            raise AMQPMessageDecodeError(f'Message payload has wrong type {type(message_payload)}.')

        type_map = {
            MESSAGE_TYPE_RPC_CALL: AMQPRPCCall,
            MESSAGE_TYPE_RPC_RESULT: AMQPRPCResult,
            MESSAGE_TYPE_RPC_ERROR: AMQPRPCError,
            MESSAGE_TYPE_EVENT_VERSION_ADD: AMQPEventVersionAdd,
            MESSAGE_TYPE_EVENT_VERSION_REMOVE: AMQPEventVersionRemove,
        }

        if message_type not in MESSAGE_TYPES:
            raise AMQPMessageDecodeError(f'Unsupported type {message_type}.')

        return type_map[message_type].from_message(message_version=message_version,
                                                   message_id=message_id,
                                                   message_type=message_type,
                                                   message_payload=message_payload)


class AMQPRPCCall(AMQPMessage):

    def __init__(self, *, message_id: str = None, correlation_id: str, task: str, arguments: Dict[str, Any]) -> None:
        if not isinstance(correlation_id, str):
            raise AMQPMessageEncodeError(f'Correlation id has wrong type {correlation_id}.')
        if correlation_id == '':
            raise AMQPMessageEncodeError(f'Correlation id is empty.')

        if not isinstance(task, str):
            raise AMQPMessageEncodeError(f'Method has wrong type {task}.')
        if task == '':
            raise AMQPMessageEncodeError(f'Method is empty.')

        if not isinstance(arguments, dict):
            raise AMQPMessageEncodeError(f'Arguments has wrong type {correlation_id}.')

        calculated_message_id = message_id or str(uuid.uuid4())
        message_payload = {
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_CORRELATION_ID: correlation_id,
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_TASK: task,
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_ARGUMENTS: arguments
        }
        super().__init__(message_version=MESSAGE_VERSION,
                         message_id=calculated_message_id,
                         message_type=MESSAGE_TYPE_RPC_CALL,
                         message_payload=message_payload)
        self._correlation_id = correlation_id
        self._task = task
        self._arguments = arguments

    @classmethod
    def from_message(cls, *, message_version: str, message_id: str, message_type: str,
                     message_payload: Dict[str, Any]) -> 'AMQPRPCCall':
        for field in (MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_CORRELATION_ID,
                      MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_TASK, MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_ARGUMENTS):
            if field not in message_payload:
                raise AMQPMessageDecodeError(f'Required payload field {field} is missing.')

        correlation_id = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_CORRELATION_ID]
        task = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_TASK]
        arguments = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_CALL_ARGUMENTS]

        return cls(message_id=message_id, correlation_id=correlation_id, task=task, arguments=arguments)

    @property
    def correlation_id(self) -> str:
        return self._correlation_id

    @property
    def task(self) -> str:
        return self._task

    @property
    def arguments(self) -> Dict[str, Any]:
        return self._arguments


class AMQPRPCResult(AMQPMessage):

    def __init__(self,
                 *,
                 message_id: str = None,
                 correlation_id: str,
                 result: Union[str, ByteString],
                 start_time: str,
                 completion_time: str) -> None:
        if not isinstance(correlation_id, str):
            raise AMQPMessageEncodeError(f'Correlation id has wrong type {correlation_id}.')
        if correlation_id == '':
            raise AMQPMessageEncodeError(f'Correlation id is empty.')

        if not isinstance(result, ByteString):
            raise AMQPMessageEncodeError('Result has wrong type {type(result)}.')

        if not isinstance(start_time, str) or not _is_iso_8601(start_time):
            raise AMQPMessageEncodeError(f'Start time is invalid: {start_time}.')

        if not isinstance(completion_time, str) or not _is_iso_8601(completion_time):
            raise AMQPMessageEncodeError(f'Completion time is invalid: {completion_time}.')

        calculated_message_id = message_id or str(uuid.uuid4())
        message_payload = {
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_CORRELATION_ID: correlation_id,
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_RESULT: result,
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_START_TIME: start_time,
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_COMPLETION_TIME: completion_time,
        }
        super().__init__(message_version=MESSAGE_VERSION,
                         message_id=calculated_message_id,
                         message_type=MESSAGE_TYPE_RPC_RESULT,
                         message_payload=message_payload)
        self._correlation_id = correlation_id
        self._result = result

    @classmethod
    def from_message(cls, *, message_version: str, message_id: str, message_type: str,
                     message_payload: Dict[str, Any]) -> 'AMQPRPCResult':
        for field in (MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_CORRELATION_ID,
                      MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_RESULT):
            if field not in message_payload:
                raise AMQPMessageDecodeError(f'Required payload field {field} is missing.')

        correlation_id = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_CORRELATION_ID]
        result = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_RESULT]
        start_time = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_START_TIME]
        completion_time = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_COMPLETION_TIME]

        return cls(message_id=message_id,
                   correlation_id=correlation_id,
                   result=result,
                   start_time=start_time,
                   completion_time=completion_time)

    @property
    def correlation_id(self) -> str:
        return self._correlation_id

    @property
    def result(self) -> ByteString:
        return self._result


class AMQPRPCError(AMQPMessage):

    def __init__(self,
                 *,
                 message_id: str = None,
                 correlation_id: str,
                 reason: str,
                 message: str,
                 start_time: str,
                 completion_time: str) -> None:
        if not isinstance(correlation_id, str):
            raise AMQPMessageEncodeError(f'Correlation id has wrong type {correlation_id}.')
        if correlation_id == '':
            raise AMQPMessageEncodeError(f'Correlation id is empty.')

        if not isinstance(reason, str):
            raise AMQPMessageEncodeError(f'Reason has wrong type {reason}.')
        if reason == '':
            raise AMQPMessageEncodeError(f'Reason is empty.')

        if not isinstance(message, str):
            raise AMQPMessageEncodeError(f'Message has wrong type {message}.')
        if message == '':
            raise AMQPMessageEncodeError(f'Message is empty.')

        if not isinstance(start_time, str) or not _is_iso_8601(start_time):
            raise AMQPMessageEncodeError(f'Start time is invalid: {start_time}.')

        if not isinstance(completion_time, str) or not _is_iso_8601(completion_time):
            raise AMQPMessageEncodeError(f'Completion time is invalid: {completion_time}.')

        calculated_message_id = message_id or str(uuid.uuid4())
        message_payload = {
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_CORRELATION_ID: correlation_id,
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_REASON: reason,
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_MESSAGE: message,
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_START_TIME: start_time,
            MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_COMPLETION_TIME: completion_time,
        }
        super().__init__(message_version=MESSAGE_VERSION,
                         message_id=calculated_message_id,
                         message_type=MESSAGE_TYPE_RPC_ERROR,
                         message_payload=message_payload)
        self._correlation_id = correlation_id
        self._reason = reason
        self._message = message

    @classmethod
    def from_message(cls, *, message_version: str, message_id: str, message_type: str,
                     message_payload: Dict[str, Any]) -> 'AMQPRPCError':
        for field in (MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_CORRELATION_ID,
                      MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_REASON, MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_MESSAGE):
            if field not in message_payload:
                raise AMQPMessageDecodeError(f'Required payload field {field} is missing.')

        correlation_id = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_CORRELATION_ID]
        reason = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_REASON]
        message = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_ERROR_MESSAGE]
        start_time = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_START_TIME]
        completion_time = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_RPC_RESULT_COMPLETION_TIME]

        return cls(message_id=message_id,
                   correlation_id=correlation_id,
                   reason=reason,
                   message=message,
                   start_time=start_time,
                   completion_time=completion_time)

    @property
    def correlation_id(self) -> str:
        return self._correlation_id

    @property
    def reason(self) -> str:
        return self._reason

    @property
    def message(self) -> str:
        return self._message


class AMQPEventVersionAdd(AMQPMessage):

    def __init__(self, *, message_id: str = None, version: Dict[str, Any]) -> None:
        if not isinstance(version, dict):
            raise AMQPMessageEncodeError(f'Version has wrong type {version}.')

        calculated_message_id = message_id or str(uuid.uuid4())
        message_payload = {
            MESSAGE_FIELD_MESSAGE_PAYLOAD_EVENT_VERSION_ADD_VERSION: version,
        }
        super().__init__(message_version=MESSAGE_VERSION,
                         message_id=calculated_message_id,
                         message_type=MESSAGE_TYPE_EVENT_VERSION_ADD,
                         message_payload=message_payload)
        self._version = version

    @classmethod
    def from_message(cls, *, message_version: str, message_id: str, message_type: str,
                     message_payload: Dict[str, Any]) -> 'AMQPEventVersionAdd':
        if MESSAGE_FIELD_MESSAGE_PAYLOAD_EVENT_VERSION_ADD_VERSION not in message_payload:
            raise AMQPMessageDecodeError(f'Required payload field {MESSAGE_FIELD_MESSAGE_PAYLOAD_EVENT_VERSION_ADD_VERSION} is missing.')

        version = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_EVENT_VERSION_ADD_VERSION]

        return cls(message_id=message_id, version=version)

    @property
    def version(self) -> Dict[str, Any]:
        return self._version


class AMQPEventVersionRemove(AMQPMessage):

    def __init__(self, *, message_id: str = None, version: Dict[str, Any]) -> None:
        if not isinstance(version, dict):
            raise AMQPMessageEncodeError(f'Version has wrong type {version}.')

        calculated_message_id = message_id or str(uuid.uuid4())
        message_payload = {
            MESSAGE_FIELD_MESSAGE_PAYLOAD_EVENT_VERSION_REMOVE_VERSION: version,
        }
        super().__init__(message_version=MESSAGE_VERSION,
                         message_id=calculated_message_id,
                         message_type=MESSAGE_TYPE_EVENT_VERSION_REMOVE,
                         message_payload=message_payload)
        self._version = version

    @classmethod
    def from_message(cls, *, message_version: str, message_id: str, message_type: str,
                     message_payload: Dict[str, Any]) -> 'AMQPEventVersionRemove':
        if MESSAGE_FIELD_MESSAGE_PAYLOAD_EVENT_VERSION_REMOVE_VERSION not in message_payload:
            raise AMQPMessageDecodeError(f'Required payload field {MESSAGE_FIELD_MESSAGE_PAYLOAD_EVENT_VERSION_REMOVE_VERSION} is missing.')

        version = message_payload[MESSAGE_FIELD_MESSAGE_PAYLOAD_EVENT_VERSION_ADD_VERSION]

        return cls(message_id, message_id=message_id, version=version)

    @property
    def version(self) -> str:
        return self._version
