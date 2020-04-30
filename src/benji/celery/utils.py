import random
import string


def random_string(length: int, characters: str = string.ascii_lowercase + string.digits) -> str:
    return ''.join(random.choice(characters) for _ in range(length))
