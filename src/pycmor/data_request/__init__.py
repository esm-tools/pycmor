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

# Import CMIP7 wrapper if available
try:
    from .cmip7_dreq_wrapper import (
        CMIP7DataRequestWrapper,
        CMIP7_DREQ_AVAILABLE,
        get_cmip7_data_request,
    )
except ImportError:
    CMIP7DataRequestWrapper = None
    get_cmip7_data_request = None
    CMIP7_DREQ_AVAILABLE = False

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
    # CMIP7 wrapper (official API)
    "CMIP7DataRequestWrapper",
    "get_cmip7_data_request",
    "CMIP7_DREQ_AVAILABLE",
]
