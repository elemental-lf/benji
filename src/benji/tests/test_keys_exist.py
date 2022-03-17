import pytest

from benji.utils import keys_exist, key_get


class TestClass:

    def __init__(self, l):
        self.attr = l


def test_keys_exist() -> None:
    l4 = TestClass({'a': 7})
    l3 = {'a': 4, 'b': 5, 'c': 6, 'd': None, 'e': l4}
    l2 = {'a': l3, 'b': 4, 'c': 3}
    l1 = {'a': 1, 'b': 2, 'c': l2}

    assert keys_exist(l1, ['a'])
    assert not keys_exist(l1, ['d'])

    assert keys_exist(l1, ['c.a'])
    assert not keys_exist(l1, ['c.z'])
    assert not keys_exist(l1, ['a.z'])
    assert not keys_exist(l1, ['z.b'])

    assert keys_exist(l1, ['c.a.a'])
    assert not keys_exist(l1, ['c.a.z'])

    assert keys_exist(l1, ['c.a.a', 'c.a', 'c'])
    assert not keys_exist(l1, ['c.a.a', 'c.a', 'c', 'z'])

    assert keys_exist(l1, ['c.a.d'])

    assert keys_exist(l1, ['c.a.e.attr.a'])
    assert not keys_exist(l1, ['c.a.e.attr2.a'])


def test_key_get() -> None:
    l4 = TestClass({'a': 7})
    l3 = {'a': 4, 'b': 5, 'c': 6, 'd': None, 'e': l4}
    l2 = {'a': l3, 'b': 4, 'c': 3}
    l1 = {'a': 1, 'b': 2, 'c': l2}

    assert key_get(l1, 'c.a.e.attr.a') == 7
    with pytest.raises(AttributeError):
        key_get(l1, 'c.a.e.attr.b')
    with pytest.raises(AttributeError):
        key_get(l1, 'c.a.e.attr2.a')

    assert key_get(l1, 'c.a.e.attr.b', None) is None
    assert key_get(l1, 'c.a.e.attr2.a', 'test') == 'test'
