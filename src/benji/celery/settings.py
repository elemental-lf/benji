import os

amqp_username = os.getenv('AMQP_USERNAME', 'guest')
amqp_password = os.getenv('AMQP_PASSWORD', 'guest')
amqp_host = os.getenv('AMQP_HOST', 'localhost')
amqp_virtual_host = os.getenv('AMQP_VIRTUAL_HOST', '/')
amqp_port = os.getenv('AMQP_PORT', '5672')

broker_url = f'amqp://{amqp_username}:{amqp_password}@{amqp_host}:{amqp_port}/{amqp_virtual_host}'
broker_connection_max_retries = None

result_backend = 'rpc'
result_persistent = True

task_default_queue = 'benji-rpc'
task_time_limit = 24 * 60 * 60  # a day
task_soft_time_limit = 12 * 60 * 60  # half a day

task_serializer = 'msgpack'
result_serializer = 'msgpack'
event_serializer = 'msgpack'
accept_content = ['msgpack']

worker_pool = 'threads'
worker_send_task_events = False,
worker_concurrency = 1
worker_prefetch_multiplier = 1

worker_hijack_root_logger = False
