# This is in large parts based on https://github.com/manicmaniac/sqlalchemy-repr
# License: MIT
# Copyright (c) 2016 Ryosuke Ito <rito.0305@gmail.com>
# And on https://github.com/alexprengere/reprmixin/blob/master/reprmixin.py
# License: Apache-2.0
# Copyright (c) 2015-2017 Alex Prengère
import io

from inspect import getmro
from typing import List, Set, Any

import sqlalchemy
from reprlib import Repr as _Repr


class Repr(_Repr):

    def repr1(self, obj, level: int) -> str:
        if level <= 0:
            return '<...>'
        elif isinstance(obj.__class__, sqlalchemy.ext.declarative.DeclarativeMeta):
            return self.repr_Base(obj, level)
        # Test if this is an object from one of our own modules
        elif hasattr(obj, '__module__') and obj.__module__.startswith(self.__module__.split('.')[0] + '.'):
            return self.repr_object(obj, level)
        else:
            return super().repr1(obj, level)

    def repr_Base(self, obj, level: int) -> str:
        return '%s(%s)' % (self._repr_class(obj), self._repr_attrs(obj, level))

    @staticmethod
    def _repr_class(obj) -> str:
        return obj.__class__.__name__

    def _repr_attrs(self, obj, level: int) -> str:
        represented_attrs = []
        for attr in self._iter_attrs(obj,
                                     obj.REPR_SQL_ATTR_SORT_FIRST if hasattr(obj, 'REPR_SQL_ATTR_SORT_FIRST') else []):
            represented_attr = self._repr_attr(attr, level)
            represented_attrs.append(represented_attr)
        return ', '.join(represented_attrs)

    def _repr_attr(self, obj, level: int) -> str:
        attr_name, attr_value = obj
        if hasattr(attr_value, 'isoformat'):
            return '%s=%r' % (attr_name, attr_value.isoformat())
        elif isinstance(obj.__class__, sqlalchemy.ext.declarative.DeclarativeMeta):
            return self.repr_Base(obj, level)
        else:
            return '%s=%s' % (attr_name, self.repr1(attr_value, level - 1))

    def repr_object(self, obj, level: int) -> str:
        return '{0}({1})'.format(
            obj.__class__.__name__, ', '.join('{0}={1}'.format(attr, self.repr1(getattr(obj, attr), level - 1))
                                              for attr in self._find_attrs(obj)
                                              if not attr.startswith('__')))

    @staticmethod
    def _iter_attrs(obj, sort_first: List[str] = []):
        attr_names = sorted(sqlalchemy.inspect(obj.__class__).columns.keys())
        for attr_name in reversed(sort_first):
            if attr_name in attr_names:
                attr_names.remove(attr_name)
                attr_names = [attr_name] + attr_names
        for attr_name in attr_names:
            yield (attr_name, getattr(obj, attr_name))

    @staticmethod
    def _find_attrs(obj):
        """Iterate over all attributes of objects."""
        visited: Set[Any] = set()

        if hasattr(obj, '__dict__'):
            for attr in sorted(obj.__dict__):
                if attr not in visited:
                    yield attr
                    visited.add(attr)

        for cls in reversed(getmro(obj.__class__)):
            if hasattr(cls, '__slots__'):
                for attr in cls.__slots__:
                    if hasattr(obj, attr):
                        if attr not in visited:
                            yield attr
                            visited.add(attr)


class PrettyRepr(Repr):

    def __init__(self, indent: int = 4) -> None:
        self.indent = ' ' * indent
        super().__init__()

    def repr_Base(self, obj, level: int) -> str:
        output = io.StringIO()
        output.write('%s(' % self._repr_class(obj))
        is_first_attr = True
        for attr in self._iter_attrs(obj):
            if not is_first_attr:
                output.write(',')
            is_first_attr = False
            represented_attr = self._repr_attr(attr, level)
            output.write('\n' + self.indent + represented_attr)
        output.write(')')
        return output.getvalue()


_shared_repr = Repr()
_shared_pretty_repr = PrettyRepr()


class ReprMixIn:

    def __repr__(self) -> str:
        return _shared_repr.repr(self)


class PrettyReprMixIn:

    def __repr__(self) -> str:
        return _shared_pretty_repr.repr(self)
