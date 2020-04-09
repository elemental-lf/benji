import uuid
from typing import Any
from io import StringIO
import json

import pika
from benji.exception import InternalError
from retry import retry

from rabbit_clients.clients.config import RABBIT_CONFIG
from webargs.core import Parser, missing
from webargs.multidictproxy import MultiDictProxy

from benji.logging import logger

RPC_QUEUE = 'benji-rpc'


def _create_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBIT_CONFIG.RABBITMQ_USER, RABBIT_CONFIG.RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(RABBIT_CONFIG.RABBITMQ_HOST,
                                  virtual_host=RABBIT_CONFIG.RABBITMQ_VIRTUAL_HOST,
                                  credentials=credentials))
    return connection


class RabbitRPCParser(Parser):

    def load_json(self, req, schema):
        return req

    def load_json_or_form(self, req, schema):
        return missing

    def load_headers(self, req, schema):
        return MultiDictProxy({'Content-Type': 'application/json; charset="utf-8"'})

    def handle_error(self, error, req, schema, *, error_status_code, error_headers):
        raise RuntimeError(error)

    def get_request_from_view_args(self, view, args, kwargs):
        return args[0]


class AMQPRPCServer:

    AUTO_DELETE_QUEUE_TIMEOUT = 24 * 60 * 60 * 1000  # in milliseconds

    def __init__(self, queue: str = RPC_QUEUE):
        self._queue = queue
        self._webargs_parser = RabbitRPCParser()
        self._tasks = {}
        self._connection = self._channel = None

    @property
    def queue(self):
        return self._queue

    def _setup_connection(self):
        self._connection = _create_connection()
        self._channel = self._connection.channel()
        queue_declare_result = self._channel.queue_declare(queue=self._queue, passive=True)
        if queue_declare_result.method != pika.spec.Queue.DeclareOk:
            queue_declare_result = self._channel.queue_declare(
                queue=self._queue,
                durable=True,
                auto_delete=(self._queue == ''),
                arguments={'x-expires': self.AUTO_DELETE_QUEUE_TIMEOUT} if self._queue == '' else {})
        self._queue = queue_declare_result.method.queue
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=self._queue, on_message_callback=self._message_handler)

    def _message_handler(self, channel, method, properties, body):
        try:
            request = json.loads(body.decode('utf-8'))
            if not isinstance(request, dict):
                raise TypeError(f'Request body has the wrong type {type(request)}.')
            if 'task' not in request:
                raise ValueError('Request is missing task key.')
            if 'args' not in request:
                raise ValueError('Request is missing args key.')
            if not isinstance(request['task'], str):
                raise TypeError(f'Request key task has the wrong type: {type(request["task"])}.')
            if not isinstance(request['args'], dict):
                raise TypeError(f'Request key args has the wrong type: {type(request["args"])}.')
            task = request['task']
            if task not in self._tasks:
                raise FileNotFoundError(f'Request to unknown task: {task}.')
        except Exception:
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
                              properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                              body=body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def register_task(self, task, *webargs_args, **webargs_kwargs) -> Any:

        def wrapper(func):
            logger.info(f'Installing task {task}.')

            def call_task(*args, **kwargs):
                return func(*args[1:], **kwargs)

            func_webargs = self._webargs_parser.use_kwargs(*webargs_args, **webargs_kwargs)(call_task)
            self._tasks[task] = func_webargs
            return func

        return wrapper

    def serve(self) -> None:
        while True:
            try:

                @retry((pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker),
                       delay=10,
                       jitter=(1, 3))
                def setup_connection():
                    self._setup_connection()

                setup_connection()

                try:
                    self._channel.start_consuming()
                except KeyboardInterrupt:
                    self._channel.stop_consuming()
                    self._connection.close()
                    break
            except pika.exceptions.ConnectionClosedByBroker:
                # Continue when the server has closed the connection.
                continue
            except pika.exceptions.AMQPConnectionError:
                logger.warning(f'Connection to broker was close, retrying.')
                continue


class AMQPRPCClient:

    AUTO_DELETE_QUEUE_TIMEOUT = 24 * 60 * 60 * 1000  # in milliseconds

    def __init__(self, queue: str = RPC_QUEUE):

        @retry(pika.exceptions.AMQPConnectionError, tries=6, delay=10, jitter=(1, 3))
        def create_connection_with_retry():
            return _create_connection()

        self._connection = create_connection_with_retry()
        self._channel = self._connection.channel()
        queue_declare_result = self._channel.queue_declare(queue=queue, passive=True)
        if queue_declare_result.method != pika.spec.Queue.DeclareOk:
            queue_declare_result = self._channel.queue_declare(
                queue=queue,
                durable=True,
                auto_delete=(queue == ''),
                arguments={'x-expires': self.AUTO_DELETE_QUEUE_TIMEOUT} if queue == '' else {})
        self._queue = queue_declare_result.method.queue
        queue_declare_result = self._channel.queue_declare(queue='', exclusive=True)
        self._callback_queue = queue_declare_result.method.queue
        self._responses = {}

        def message_handler(channel, method, properties, body):
            response = json.loads(body.decode('utf-8'))
            self._responses[properties.correlation_id] = response

        self._channel.basic_consume(queue=self._callback_queue, on_message_callback=message_handler, auto_ack=True)

    @property
    def queue(self):
        return self._queue

    def response_ready(self, correlation_id: str) -> bool:
        return correlation_id in self._responses

    def process_data_events(self):
        self._connection.process_data_events()

    def call_async(self, task, **kwargs) -> str:
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
                                    ),
                                    body=json.dumps(request))

        return correlation_id

    def get_response(self, correlation_id: str):
        response = self._responses[correlation_id]
        del self._responses[correlation_id]
        return response

    def call(self, task, **kwargs):
        correlation_id = self.call_async(task, **kwargs)

        while not self.response_ready(correlation_id):
            self.process_data_events()

        return self.get_response(correlation_id)

    def close(self):
        self._connection.close()
