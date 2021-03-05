__path__ = __import__('pkgutil').extend_path(__path__, __name__)

__all__ = ['__version__', '__path__']
from ._version import __version__
del _version  # remove to avoid confusion with __version__
