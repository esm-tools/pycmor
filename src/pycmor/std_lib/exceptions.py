"""
This module contains custom exceptions that you should raise when something specific
goes wrong in the standard library.
"""

# NOTE(PG): I am not sure what is better; to have this here, or to have each exception next to the step code...


class PymorizeError(Exception):
    """Base class for all errors raised by pycmor."""


class PymorizeResamplingError(PymorizeError):
    """Error raised when resampling fails."""


class PymorizeResamplingTimeAxisIncompatibilityError(
    PymorizeResamplingError, ValueError
):
    """Error raised when resampling fails due to time axis incompatibility."""
