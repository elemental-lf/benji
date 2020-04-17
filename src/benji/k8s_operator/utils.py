import inspect
import random
import string
from typing import Any, Sequence

SERVICE_NAMESPACE_FILENAME = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'


def get_caller_name() -> str:
    """Returns the name of the calling function"""
    return inspect.getouterframes(inspect.currentframe())[1].function


def cr_to_job_name(body, suffix: str):
    if 'namespace' in body['metadata']:
        return f'crd:{body["kind"]}/{body["metadata"]["namespace"]}/{body["metadata"]["name"]}-{suffix}'
    else:
        return f'crd:{body["kind"]}/{body["metadata"]["name"]}-{suffix}'


def service_account_namespace() -> str:
    with open(SERVICE_NAMESPACE_FILENAME, 'r') as f:
        namespace = f.read()
        if namespace == '':
            raise RuntimeError(f'{SERVICE_NAMESPACE_FILENAME} is empty.')
    return namespace


def random_string(length: int, characters: str = string.ascii_lowercase + string.digits) -> str:
    return ''.join(random.choice(characters) for _ in range(length))


def keys_exist(obj: Any, keys: Sequence[str]) -> bool:
    split_keys = [attr.split('.') for attr in keys]

    for split_key in split_keys:
        position = obj
        for component in split_key:
            try:
                position = position.get(component, None)
            except AttributeError:
                return False
            if position is None:
                return False

    return True
