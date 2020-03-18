from collections import namedtuple

from benji.helpers.utils import attrs_exist


def test_attrs_exist() -> None:
    nt = namedtuple('NT', ['a', 'b', 'c'])

    l3 = nt(a=4, b=5, c=6)
    l2 = nt(a=l3, b=4, c=3)
    l1 = nt(a=1, b=2, c=l2)

    assert attrs_exist(l1, ['a'])
    assert not attrs_exist(l1, ['d'])

    assert attrs_exist(l1, ['c.a'])
    assert not attrs_exist(l1, ['c.z'])
    assert not attrs_exist(l1, ['a.z'])
    assert not attrs_exist(l1, ['z.b'])

    assert attrs_exist(l1, ['c.a.a'])
    assert not attrs_exist(l1, ['c.a.z'])

    assert attrs_exist(l1, ['c.a.a', 'c.a', 'c'])
    assert not attrs_exist(l1, ['c.a.a', 'c.a', 'c', 'z'])
