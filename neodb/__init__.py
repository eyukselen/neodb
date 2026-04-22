from .core import NeoDB

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("neodb")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__all__ = ["NeoDB"]
