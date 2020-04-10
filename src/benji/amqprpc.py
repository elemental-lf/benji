import time
import uuid
from typing import Any, Dict, Callable
from io import StringIO
import json

import pika
from retry import retry

from rabbit_clients.clients.config import RABBIT_CONFIG
from webargs.core import Parser, missing
from webargs.multidictproxy import MultiDictProxy

from benji.logging import logger

# These are mainly set to unsure that stray queues and messages go aways even when lost.
AUTO_DELETE_QUEUE_TIMEOUT = 60 * 60 * 1000  # in milliseconds
MESSAGE_TTL = 12 * 60 * 60 * 1000  # in milliseconds

RECONNECT_SLEEP_TIME = 5  # in seconds
AMQP_HEARTBEAT_INTERVAL = 600  # in seconds
AMPQ_BLOCKED_CONNECTION_TIMEOUT = 300  # in seconds
INACTIVITY_TIMER_CHECK_INTERVAL = 60  # in seconds

AMPQ_SERVER_QUEUE_CONSUMER_TAG = 'server'
AMPQ_CLIENT_QUEUE_CONSUMER_TAG = 'client'


def _create_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBIT_CONFIG.RABBITMQ_USER, RABBIT_CONFIG.RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(RABBIT_CONFIG.RABBITMQ_HOST,
                                  virtual_host=RABBIT_CONFIG.RABBITMQ_VIRTUAL_HOST,
                                  credentials=credentials,
                                  heartbeat=AMQP_HEARTBEAT_INTERVAL,
                                  blocked_connection_timeout=AMPQ_BLOCKED_CONNECTION_TIMEOUT))
    return connection


class RabbitRPCParser(Parser):

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


class AMQPRPCServer:

    def __init__(self, queue: str, inactivity_timeout: int = 0) -> None:
        self._queue = queue
        self.inactivity_timeout = inactivity_timeout
        self._webargs_parser = RabbitRPCParser()
        self._tasks = {}
        self._connection = self._channel = None
        self._closing = False
        self._last_activity = time.monotonic()

    @property
    def queue(self) -> str:
        return self._queue

    def _setup_connection(self) -> None:
        self._connection = _create_connection()
        self._channel = self._connection.channel()
        queue_declare_result = self._channel.queue_declare(queue=self._queue, passive=True)
        if type(queue_declare_result.method) != pika.spec.Queue.DeclareOk:
            arguments = {'x-message-ttl': MESSAGE_TTL}
            if self._queue == '':
                arguments['x-expires'] = AUTO_DELETE_QUEUE_TIMEOUT
            queue_declare_result = self._channel.queue_declare(queue=self._queue,
                                                               durable=True,
                                                               auto_delete=(self._queue == ''),
                                                               arguments=arguments)
        self._queue = queue_declare_result.method.queue
        if self.inactivity_timeout > 0:
            self._connection.call_later(INACTIVITY_TIMER_CHECK_INTERVAL, self._inactivity_timeout_check)
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=self._queue,
                                    on_message_callback=self._message_handler,
                                    consumer_tag=AMPQ_SERVER_QUEUE_CONSUMER_TAG)

    def _inactivity_timeout_check(self) -> None:
        if self.inactivity_timeout > 0:
            if time.monotonic() - self._last_activity > self.inactivity_timeout:
                logger.info('Requesting termination of the connection due to inactivity.')
                self.close()

            # Reschedule our self
            self._connection.call_later(INACTIVITY_TIMER_CHECK_INTERVAL, self._inactivity_timeout_check)

    def _message_handler(self, channel, method, properties, body) -> None:
        self._last_activity = time.monotonic()
        try:
            request = json.loads(body.decode('utf-8'))
            if not isinstance(request, dict):
                raise TypeError(f'Request body has the wrong type {type(request)}.')
            if 'task' not in request:
                raise IndexError('Request is missing task key.')
            if 'args' not in request:
                raise IndexError('Request is missing args key.')
            if not isinstance(request['task'], str):
                raise TypeError(f'Request key task has the wrong type: {type(request["task"])}.')
            if not isinstance(request['args'], dict):
                raise TypeError(f'Request key args has the wrong type: {type(request["args"])}.')
            task = request['task']
            if task not in self._tasks:
                raise FileNotFoundError(f'Request to unknown task: {task}.')
        except Exception:
            # Ignore malformed messages
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        logger.info(f'Calling task {task}({request["args"]}).')
        response = self._tasks[task](request['args'])

        if isinstance(response, StringIO):
            body = response.getvalue()
        else:
            body = json.dumps(response, check_circular=True, separators=(',',
                                                                         ': '), indent=2) if response is not None else 'null'

        channel.basic_publish(exchange='',
                              routing_key=properties.reply_to,
                              properties=pika.BasicProperties(correlation_id=properties.correlation_id, delivery_mode=2),
                              body=body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        self._last_activity = time.monotonic()

    def register_task(self, task, *webargs_args, **webargs_kwargs) -> Callable:

        def wrapper(func):
            logger.info(f'Installing task {task}.')

            # Drop the first argument, it's the original message dict.
            def call_task(*args, **kwargs):
                return func(*args[1:], **kwargs)

            func_webargs = self._webargs_parser.use_kwargs(*webargs_args, **webargs_kwargs)(call_task)
            self._tasks[task] = func_webargs
            return func

        return wrapper

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
        self._connection.close()

    def close(self) -> None:
        logger.info('Closing connection to broker.')
        self._channel.basic_cancel(consumer_tag=AMPQ_SERVER_QUEUE_CONSUMER_TAG)
        self._closing = True


class AMQPRPCClient:

    def __init__(self, queue: str) -> None:

        @retry(pika.exceptions.AMQPConnectionError, tries=6, delay=10, jitter=(1, 3))
        def create_connection_with_retry():
            return _create_connection()

        self._connection = create_connection_with_retry()
        self._channel = self._connection.channel()
        queue_declare_result = self._channel.queue_declare(queue=queue, passive=True)
        if type(queue_declare_result.method) != pika.spec.Queue.DeclareOk:
            arguments = {'x-message-ttl': MESSAGE_TTL}
            if queue == '':
                arguments['x-expires'] = AUTO_DELETE_QUEUE_TIMEOUT
            queue_declare_result = self._channel.queue_declare(queue=queue,
                                                               durable=True,
                                                               auto_delete=(queue == ''),
                                                               arguments=arguments)
        self._queue = queue_declare_result.method.queue
        queue_declare_result = self._channel.queue_declare(queue='', exclusive=True)
        self._callback_queue = queue_declare_result.method.queue
        self._responses = {}

        def message_handler(channel, method, properties, body):
            response = json.loads(body.decode('utf-8'))
            self._responses[properties.correlation_id] = response

        self._channel.basic_consume(queue=self._callback_queue,
                                    on_message_callback=message_handler,
                                    auto_ack=True,
                                    consumer_tag=AMPQ_CLIENT_QUEUE_CONSUMER_TAG)

    @property
    def queue(self) -> str:
        return self._queue

    def response_ready(self, correlation_id: str) -> bool:
        return correlation_id in self._responses

    def process_data_events(self):
        self._connection.process_data_events()

    def call_async(self, task: str, **kwargs) -> str:
        request = {
            'task': task,
            'args': kwargs,
        }

        correlation_id = str(uuid.uuid4())

        self._channel.basic_publish(exchange='',
                                    routing_key=self._queue,
                                    properties=pika.BasicProperties(
                                        reply_to=self._callback_queue,
                                        correlation_id=correlation_id,
                                        delivery_mode=2,
                                    ),
                                    body=json.dumps(request))

        return correlation_id

    def get_response(self, correlation_id: str) -> Any:
        response = self._responses[correlation_id]
        del self._responses[correlation_id]
        return response

    def call(self, task: str, timeout: int = 10, **kwargs) -> Any:
        correlation_id = self.call_async(task, **kwargs)

        deadline = time.monotonic() + timeout
        while not self.response_ready(correlation_id) and time.monotonic() < deadline:
            self.process_data_events()

        if not self.response_ready(correlation_id):
            raise TimeoutError(f'Timeout while waiting for response for task {task}, correlation id {correlation_id}.')

        return self.get_response(correlation_id)

    def close(self) -> None:
        self._channel.basic_cancel(consumer_tag=AMPQ_CLIENT_QUEUE_CONSUMER_TAG)
        self._connection.close()
