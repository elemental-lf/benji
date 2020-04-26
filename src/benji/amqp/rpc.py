import functools
import inspect
import json
import logging
import os
import threading
import time
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import AbstractContextManager
from datetime import datetime
from io import StringIO
from typing import Any, Dict, ByteString

import pika
from webargs import fields
from webargs.core import Parser, missing
from webargs.multidictproxy import MultiDictProxy

from benji.amqp.exception import AMQPMessageDecodeError
from benji.amqp.message import AMQPMessage, AMQPRPCCall, AMQPRPCResult, AMQPRPCError
from benji.logging import logger

AMQP_USERNAME = os.getenv('AMQP_USERNAME', 'guest')
AMQP_PASSWORD = os.getenv('AMQP_PASSWORD', 'guest')
AMQP_HOST = os.getenv('AMQP_HOST', 'localhost')
AMQP_VIRTUAL_HOST = os.getenv('AMQP_VIRTUAL_HOST', '/')
AMQP_PORT = os.getenv('AMQP_PORT', '5672')

# Constants used by server and client
AMQP_AUTO_DELETE_QUEUE_TIMEOUT = 60 * 60 * 1000  # in milliseconds
AMQP_MESSAGE_TTL = 12 * 60 * 60 * 1000  # in milliseconds
RECONNECT_SLEEP_TIME = 5  # in seconds
AMQP_HEARTBEAT_INTERVAL = 600  # in seconds
AMQP_BLOCKED_CONNECTION_TIMEOUT = 300  # in seconds
AMQP_REQUEST_DEFAULT_QUEUE = 'benji-rpc'
TERMINATE_TASK_NAME = 'terminate'

# Constants used by the server
SERVER_AMQP_QUEUE_CONSUMER_TAG = 'server'
SERVER_INACTIVITY_TIMER_CHECK_INTERVAL = 60  # in seconds
SERVER_DEFAULT_THREADS = 4

# Constants used by the client
CLIENT_AMQP_QUEUE_CONSUMER_TAG = 'client'
CLIENT_CLOSING_TIMER_CHECK_INTERVAL = 1  # in seconds
CLIENT_DEFAULT_RESPONSE_TIMEOUT = 10  # in seconds
CLIENT_DEFAULT_READY_TIMEOUT = 10  # in seconds

# Pika is quite verbose even at INFO level... tone it down a bit.
logging.getLogger('pika').setLevel(logging.WARNING)


def _create_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(AMQP_USERNAME, AMQP_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(AMQP_HOST,
                                  AMQP_PORT,
                                  virtual_host=AMQP_VIRTUAL_HOST,
                                  credentials=credentials,
                                  heartbeat=AMQP_HEARTBEAT_INTERVAL,
                                  blocked_connection_timeout=AMQP_BLOCKED_CONNECTION_TIMEOUT))
    return connection


def _setup_common_queues(channel, queue: str):
    if not queue.startswith('amq.'):
        arguments = {'x-message-ttl': AMQP_MESSAGE_TTL}
        auto_delete = False
        if queue == '':
            arguments['x-expires'] = AMQP_AUTO_DELETE_QUEUE_TIMEOUT
            auto_delete = True
        queue_declare_result = channel.queue_declare(queue=queue,
                                                     durable=True,
                                                     auto_delete=auto_delete,
                                                     arguments=arguments)
        return queue_declare_result.method.queue
    else:
        return queue


class _ArgumentsParser(Parser):

    def load_json(self, req, schema) -> Any:
        return req

    def load_json_or_form(self, req, schema):
        return missing

    def load_headers(self, req, schema) -> MultiDictProxy:
        return MultiDictProxy({'Content-Type': 'application/json; charset="utf-8"'})

    def handle_error(self, error, req, schema, *, error_status_code, error_headers) -> None:
        raise TypeError(error)

    def get_request_from_view_args(self, view, args, kwargs) -> Dict[str, Any]:
        return args[0]


# The server runs the AMQP client in the main thread and delegates the actual request handling to a future.
class AMQPRPCServer:

    def __init__(self,
                 *,
                 queue: str = AMQP_REQUEST_DEFAULT_QUEUE,
                 threads: int = SERVER_DEFAULT_THREADS,
                 inactivity_timeout: int = 0) -> None:
        self._queue = queue
        self.inactivity_timeout = inactivity_timeout
        logger.info(f'AMQP inactitivty timeout set to {self.inactivity_timeout}.')
        self._arguments_parser = _ArgumentsParser()
        self._tasks = {}
        self._connection = self._channel = None
        self._closing = False
        self._last_activity = time.monotonic()
        self._executor = ThreadPoolExecutor(max_workers=threads, thread_name_prefix='RPC-Thread')

        self._register_terminate_task()

    @property
    def queue(self) -> str:
        return self._queue

    def _setup_connection(self) -> None:
        self._connection = _create_connection()
        self._channel = self._connection.channel()
        self._queue = _setup_common_queues(self._channel, self._queue)

        if self.inactivity_timeout > 0:
            logger.debug(f'Scheduling inactivity timeout check every {SERVER_INACTIVITY_TIMER_CHECK_INTERVAL} seconds.')
            self._connection.call_later(SERVER_INACTIVITY_TIMER_CHECK_INTERVAL, self._inactivity_timeout_check)
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=self._queue,
                                    on_message_callback=self._message_handler,
                                    consumer_tag=SERVER_AMQP_QUEUE_CONSUMER_TAG)

    def _register_terminate_task(self):
        self.register_as_task({}, rpc_task_name=TERMINATE_TASK_NAME)(lambda: self.close())

    def _inactivity_timeout_check(self) -> None:
        logger.debug('Inactivity timeout check started.')
        if self.inactivity_timeout > 0:
            if time.monotonic() - self._last_activity > self.inactivity_timeout:
                logger.info('Requesting termination of the connection due to inactivity.')
                self.close()

            # Reschedule our self
            self._connection.call_later(SERVER_INACTIVITY_TIMER_CHECK_INTERVAL, self._inactivity_timeout_check)

    def _publish_response(self, *, body: str, method, properties) -> None:
        self._channel.basic_publish(exchange='',
                                    routing_key=properties.reply_to,
                                    properties=pika.BasicProperties(correlation_id=properties.correlation_id,
                                                                    delivery_mode=2),
                                    body=body)
        self._channel.basic_ack(delivery_tag=method.delivery_tag)

    # Runs as a future
    def _handle_rpc_call(self, *, message: AMQPRPCCall, method, properties) -> None:
        thread_name = threading.current_thread().name
        try:
            start_time = datetime.utcnow().isoformat(timespec='microseconds') + 'Z'
            logger.info(f'Thread {thread_name} - Calling task {message.task}({message.arguments}).')
            try:
                result = self._tasks[message.task](message.arguments)
            except Exception as exception:
                logger.info(f'Thread {thread_name} - Task threw {type(exception).__name__} exception: {str(exception)}')
                completion_time = datetime.utcnow().isoformat(timespec='microseconds') + 'Z'
                body = AMQPRPCError(correlation_id=properties.correlation_id,
                                    reason=type(exception).__name__,
                                    message=str(exception),
                                    start_time=start_time,
                                    completion_time=completion_time).marshal()
            else:
                logger.info(f'Thread {thread_name} - Task finished successfully.')
                completion_time = datetime.utcnow().isoformat(timespec='microseconds') + 'Z'
                body = AMQPRPCResult(correlation_id=properties.correlation_id,
                                     result=result,
                                     start_time=start_time,
                                     completion_time=completion_time).marshal()

            if not message.one_way:

                def publish_response():
                    nonlocal body, method, properties
                    self._publish_response(body=body, method=method, properties=properties)

                self._connection.add_callback_threadsafe(publish_response)
        except Exception as exception:
            logger.info(f'Thread {thread_name} - Unexpected exception {type(exception).__name__} while handling RPC call: {str(exception)}')

    def _message_handler(self, channel, method, properties, body) -> None:
        self._last_activity = time.monotonic()
        try:
            message = AMQPMessage.unmarshall(body)
        except AMQPMessageDecodeError:
            logger.error(f'Ignoring malformed message: {body}.')
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        if not isinstance(message, AMQPRPCCall):
            logger.error(f'Ignoring message of type {message.type}.')
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        if message.task not in self._tasks:
            logger.error(f'RPC call for unknown task: {message.task}.')
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        def handle_request():
            nonlocal message, method, properties
            self._handle_rpc_call(message=message, method=method, properties=properties)

        self._executor.submit(handle_request)

    @staticmethod
    def _encode_result_as_json(result: Any) -> bytes:
        # This assumes that a result of type StringIO already contains JSON formatted data and only
        # needs to be encoded.
        if isinstance(result, StringIO):
            encoded_result = result.getvalue().encode('utf-8')
        elif isinstance(result, ByteString):
            encoded_result = result
        else:
            encoded_result = json.dumps(result, check_circular=True, separators=(',', ': '), indent=2).encode('utf-8')
        return encoded_result

    def register_as_task(self,
                         *webargs_args,
                         rpc_task_name: str = None,
                         rpc_encode_as_json: bool = True,
                         **webargs_kwargs):

        def decorator(func):
            task = rpc_task_name or func.__name__
            if task in self._tasks:
                raise FileExistsError(f'Task {task} has already been registered.')

            if len(webargs_args) == 0:
                signature = inspect.Signature.from_callable(func, follow_wrapped=False)
                logger.info(f'Deriving parameters for task {task} from function definition.')
                argmap = {
                    name: value.annotation
                    for name, value in signature.parameters.items()
                    if isinstance(value.annotation, fields.Field)
                }
            elif len(webargs_args) == 1:
                argmap = webargs_args[0]
            else:
                raise TypeError('Only one positional argument (the webargs argmap) allowed.')

            logger.info(f'Installing task {task}.')

            @functools.wraps(func)
            def call_task(*task_args, **task_kwargs):
                nonlocal func, self
                # Drop the first argument, it's the original arguments dict.
                result = func(*task_args[1:], **task_kwargs)
                return self._encode_result_as_json(result) if rpc_encode_as_json else result

            func_webargs = self._arguments_parser.use_kwargs(argmap, **webargs_kwargs)(call_task)
            self._tasks[task] = func_webargs

            return func

        return decorator

    def serve(self) -> None:
        while True:
            try:
                if self._closing:
                    break

                self._setup_connection()

                try:
                    self._channel.start_consuming()
                except KeyboardInterrupt:
                    self._channel.stop_consuming()
                    break
            except pika.exceptions.ConnectionClosedByBroker:
                logger.warning(f'Connection closed by broker, retrying in {RECONNECT_SLEEP_TIME} seconds.')
                time.sleep(RECONNECT_SLEEP_TIME)
                # Continue when the server has closed the connection.
                continue
            except pika.exceptions.AMQPConnectionError:
                logger.warning(f'Connection to broker was closed because of an error, retrying in {RECONNECT_SLEEP_TIME} seconds.')
                time.sleep(RECONNECT_SLEEP_TIME)
                continue
        # shutdown() will wait for outstanding tasks to finish.
        self._executor.shutdown()
        self._connection.close()
        logger.info('Broker connection closed.')

    def close(self) -> None:
        logger.info('Closing broker connection.')
        self._channel.basic_cancel(consumer_tag=SERVER_AMQP_QUEUE_CONSUMER_TAG)
        self._closing = True


# The client runs the AMQP client in a seperate thread and so doesn't block the main thread.
# This also ensure that AMQP heartbeat messages are sent regularly.
class AMQPRPCClient(AbstractContextManager):

    def __init__(self,
                 *,
                 queue: str = AMQP_REQUEST_DEFAULT_QUEUE,
                 timeout: int = CLIENT_DEFAULT_RESPONSE_TIMEOUT,
                 ready_timeout: int = CLIENT_DEFAULT_READY_TIMEOUT) -> None:
        self._queue = queue
        self._timeout = timeout
        self._ready_timeout = ready_timeout
        self._connection_ready = threading.Event()
        self._connection_closing = threading.Event()
        self._responses = {}
        self._responses_cond = threading.Condition()
        self._queue = queue
        self._callback_queue = None
        self._amqp_thread = threading.Thread(target=self._serve, daemon=True)
        self._amqp_thread.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Run only in AMQP thread context
    def _serve(self):
        while True:
            try:
                if self._connection_closing.is_set():
                    break

                self._setup_connection()

                try:
                    self._connection_ready.set()
                    self._channel.start_consuming()
                except KeyboardInterrupt:
                    break
                finally:
                    self._connection_ready.clear()
                    self._channel.stop_consuming()
            except pika.exceptions.ConnectionClosedByBroker:
                logger.warning(f'Connection closed by broker, retrying in {RECONNECT_SLEEP_TIME} seconds.')
                time.sleep(RECONNECT_SLEEP_TIME)
                # Continue when the server has closed the connection.
                continue
            except pika.exceptions.AMQPConnectionError:
                logger.warning(f'Connection to broker was closed because of an error, retrying in {RECONNECT_SLEEP_TIME} seconds.')
                time.sleep(RECONNECT_SLEEP_TIME)
                continue
        self._connection.close()

    # Run only in AMQP thread context
    def _closing_check(self) -> None:
        if self._connection_closing.is_set():
            self._channel.basic_cancel(consumer_tag=CLIENT_AMQP_QUEUE_CONSUMER_TAG)
        self._connection.call_later(CLIENT_CLOSING_TIMER_CHECK_INTERVAL, self._closing_check)

    # Run only in AMQP thread context
    def _setup_connection(self) -> None:
        self._connection = _create_connection()
        self._channel = self._connection.channel()
        self._queue = _setup_common_queues(self._channel, self._queue)

        queue_declare_result = self._channel.queue_declare(queue='', exclusive=True)
        self._callback_queue = queue_declare_result.method.queue

        self._connection.call_later(CLIENT_CLOSING_TIMER_CHECK_INTERVAL, self._closing_check)
        self._channel.basic_consume(queue=self._callback_queue,
                                    on_message_callback=self._message_handler,
                                    auto_ack=True,
                                    consumer_tag=CLIENT_AMQP_QUEUE_CONSUMER_TAG)

    # Run only in AMQP thread context
    def _message_handler(self, channel, method, properties, body) -> None:
        try:
            message = AMQPMessage.unmarshall(body)
        except AMQPMessageDecodeError:
            logger.error(f'Ignoring malformed message: {body}.')
            return

        if not isinstance(message, (AMQPRPCResult, AMQPRPCError)):
            logger.error(f'Ignoring message of type {message.type}.')

        with self._responses_cond:
            self._responses[properties.correlation_id] = message
            self._responses_cond.notify_all()

    # Run only in AMQP thread context
    def _publish_request(self, correlation_id: str, body: Any) -> None:
        self._channel.basic_publish(exchange='',
                                    routing_key=self._queue,
                                    properties=pika.BasicProperties(
                                        reply_to=self._callback_queue,
                                        correlation_id=correlation_id,
                                        delivery_mode=2,
                                    ),
                                    body=body)

    @property
    def queue(self) -> str:
        return self._queue

    def call_async(self, task: str, one_way: bool = False, **kwargs) -> str:
        correlation_id = str(uuid.uuid4())
        body = AMQPRPCCall(correlation_id=correlation_id, task=task, arguments=kwargs, one_way=one_way).marshal()

        if self._connection_ready.wait(timeout=self._ready_timeout):
            # There is still a race here if the connection breaks down between getting ready above and using
            # the connection below.
            def publish_request():
                nonlocal correlation_id, body
                self._publish_request(correlation_id, body)

            self._connection.add_callback_threadsafe(publish_request)
        else:
            raise TimeoutError(f'Connection to broker did not become ready in {self._ready_timeout} seconds.')

        return correlation_id

    def response_ready(self, correlation_id: str) -> bool:
        with self._responses_cond:
            return correlation_id in self._responses

    def get_result(self, correlation_id: str) -> Any:
        with self._responses_cond:
            message = self._responses[correlation_id]
            del self._responses[correlation_id]

        if isinstance(message, AMQPRPCError):
            raise RuntimeError(f'RPC call for task with correlation id {correlation_id} failed: {message.reason} - {message.message}')

        return message.result

    def wait_for_response(self, correlation_id: str) -> None:
        with self._responses_cond:
            self._responses_cond.wait_for(lambda: self.response_ready(correlation_id), timeout=self._timeout)

        if not self.response_ready(correlation_id):
            raise TimeoutError(f'Timeout while waiting for response for task with correlation id {correlation_id}.')

    def call(self, task: str, **kwargs) -> Any:
        correlation_id = self.call_async(task, **kwargs)
        self.wait_for_response(correlation_id)
        return self.get_result(correlation_id)

    def close(self) -> None:
        logger.info('Requesting termination of broker connection.')
        self._connection_closing.set()
        self._amqp_thread.join()
