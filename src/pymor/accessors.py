"""
Xarray accessor registration module for pymor.

This module imports and registers all xarray accessors used throughout the pymor project.
By importing this module, all accessors become available on xarray DataArrays and Datasets.
"""

from xarray import register_dataarray_accessor, register_dataset_accessor

# Import modules that register specialized xarray accessors
from .core.infer_freq import DatasetFrequencyAccessor, TimeFrequencyAccessor

# Future accessor imports can be added here as the project grows
# from .other_module import other_accessor  # noqa: F401


@register_dataarray_accessor("pymor")
class PymorDataArrayAccessor:
    """
    Unified pymor accessor for xarray DataArrays.

    This accessor provides access to all pymor functionality under a single namespace.
    It delegates to specialized accessors like timefreq while providing a unified interface.

    Examples
    --------
    # Time frequency operations
    data.pymor.resample_safe(target_approx_interval=30.0)  # ~monthly
    data.pymor.resample_safe(freq_str='3M')  # 3-monthly
    data.pymor.check_resolution(target_approx_interval=1.0)  # daily
    data.pymor.infer_frequency()  # infer frequency from data

    # Future pymor functionality will also be available here
    # data.pymor.other_feature()
    """

    def __init__(self, xarray_obj):
        self._obj = xarray_obj
        # Initialize specialized accessors
        self._timefreq = TimeFrequencyAccessor(xarray_obj)

    # Time frequency methods - delegate to TimeFrequencyAccessor
    def resample_safe(self, *args, **kwargs):
        """Resample data safely with temporal resolution validation.

        See TimeFrequencyAccessor.resample_safe for full documentation.
        """
        return self._timefreq.resample_safe(*args, **kwargs)

    def check_resolution(self, *args, **kwargs):
        """Check if temporal resolution is sufficient for resampling.

        See TimeFrequencyAccessor.check_resolution for full documentation.
        """
        return self._timefreq.check_resolution(*args, **kwargs)

    def infer_frequency(self, *args, **kwargs):
        """Infer frequency from time series data.

        See TimeFrequencyAccessor.infer_frequency for full documentation.
        """
        return self._timefreq.infer_frequency(*args, **kwargs)

    # Future pymor methods can be added here
    # def other_feature(self, *args, **kwargs):
    #     return self._other_accessor.other_feature(*args, **kwargs)


@register_dataset_accessor("pymor")
class PymorDatasetAccessor:
    """
    Unified pymor accessor for xarray Datasets.

    This accessor provides access to all pymor functionality under a single namespace.
    It delegates to specialized accessors like timefreq while providing a unified interface.

    Examples
    --------
    # Time frequency operations
    dataset.pymor.resample_safe(target_approx_interval=30.0)  # ~monthly
    dataset.pymor.resample_safe(freq_str='3M')  # 3-monthly
    dataset.pymor.check_resolution(target_approx_interval=1.0)  # daily
    dataset.pymor.infer_frequency()  # infer frequency from data

    # Future pymor functionality will also be available here
    # dataset.pymor.other_feature()
    """

    def __init__(self, xarray_obj):
        self._obj = xarray_obj
        # Initialize specialized accessors
        self._timefreq = DatasetFrequencyAccessor(xarray_obj)

    # Time frequency methods - delegate to DatasetFrequencyAccessor
    def resample_safe(self, *args, **kwargs):
        """Resample dataset safely with temporal resolution validation.

        See DatasetFrequencyAccessor.resample_safe for full documentation.
        """
        return self._timefreq.resample_safe(*args, **kwargs)

    def check_resolution(self, *args, **kwargs):
        """Check if temporal resolution is sufficient for resampling.

        See DatasetFrequencyAccessor.check_resolution for full documentation.
        """
        return self._timefreq.check_resolution(*args, **kwargs)

    def infer_frequency(self, *args, **kwargs):
        """Infer frequency from time series data.

        See DatasetFrequencyAccessor.infer_frequency for full documentation.
        """
        return self._timefreq.infer_frequency(*args, **kwargs)

    # Future pymor methods can be added here
    # def other_feature(self, *args, **kwargs):
    #     return self._other_accessor.other_feature(*args, **kwargs)
