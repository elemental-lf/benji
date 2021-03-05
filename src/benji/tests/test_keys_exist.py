from benji.utils import keys_exist


def test_keys_exist() -> None:
    l3 = {'a': 4, 'b': 5, 'c': 6, 'd': None}
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
