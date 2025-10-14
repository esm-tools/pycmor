"""Data Request module for pycmor.

This module provides interfaces to CMIP6 and CMIP7 data requests.
"""

from .collection import CMIP6DataRequest, CMIP7DataRequest, DataRequest
from .table import (
    CMIP6DataRequestTable,
    CMIP6DataRequestTableHeader,
    CMIP7DataRequestTable,
    CMIP7DataRequestTableHeader,
    DataRequestTable,
    DataRequestTableHeader,
)
from .variable import (
    CMIP6DataRequestVariable,
    CMIP7DataRequestVariable,
    DataRequestVariable,
)

# Import CMIP7 interface if available
try:
    from .cmip7_interface import (
        CMIP7_API_AVAILABLE,
        CMIP7Interface,
        get_cmip7_interface,
    )
except ImportError:
    CMIP7Interface = None
    get_cmip7_interface = None
    CMIP7_API_AVAILABLE = False

__all__ = [
    # Base classes
    "DataRequest",
    "DataRequestTable",
    "DataRequestTableHeader",
    "DataRequestVariable",
    # CMIP6 classes
    "CMIP6DataRequest",
    "CMIP6DataRequestTable",
    "CMIP6DataRequestTableHeader",
    "CMIP6DataRequestVariable",
    # CMIP7 classes
    "CMIP7DataRequest",
    "CMIP7DataRequestTable",
    "CMIP7DataRequestTableHeader",
    "CMIP7DataRequestVariable",
    # CMIP7 interface (official API)
    "CMIP7Interface",
    "get_cmip7_interface",
    "CMIP7_API_AVAILABLE",
]
