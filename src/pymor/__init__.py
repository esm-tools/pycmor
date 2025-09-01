"""pymor - Makes CMOR Simple"""

# Import module that registers all xarray accessors
from . import accessors  # noqa: F401
from . import (
    _version,
)

__author__ = "Paul Gierz <pgierz@awi.de>"
__all__ = []

__version__ = _version.get_versions()["version"]
