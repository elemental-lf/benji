import importlib
import sys

__all__ = ['__version__']
from ._version import __version__
del _version  # remove to avoid confusion with __version__


def lazy_import(fullname):
    try:
        return sys.modules[fullname]
    except KeyError:
        spec = importlib.util.find_spec(fullname)
        module = importlib.util.module_from_spec(spec)
        loader = importlib.util.LazyLoader(spec.loader)
        loader.exec_module(module)
        return module


lazy_import("pyparsing")
lazy_import("sqlalchemy")
lazy_import("sqlalchemy.orm")
lazy_import("sqlalchemy.ext.mutable")
lazy_import("sqlalchemy.ext.declarative")
lazy_import("dateparser")
