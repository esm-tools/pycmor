"""
CMIP7 Data Request Interface using the official CMIP7_data_request_api.

This module provides a clean interface to work with CMIP7 data requests,
supporting both the new CMIP7 compound name structure and backward compatibility
with CMIP6 table-based lookups.

Key Concepts:
-------------
- CMIP7 Compound Name: realm.variable.branding.frequency.region
  Example: atmos.clt.tavg-u-hxy-u.mon.GLB

- CMIP6 Backward Compatibility: cmip6_table + cmip6_compound_name
  Example: Amon.clt

Usage:
------
>>> from pycmor.data_request import CMIP7Interface
>>> interface = CMIP7Interface()
>>>
>>> # Get metadata by CMIP7 compound name
>>> metadata = interface.get_variable_metadata('atmos.clt.tavg-u-hxy-u.mon.GLB')
>>>
>>> # Get metadata by CMIP6 compound name (backward compatibility)
>>> metadata = interface.get_variable_by_cmip6_name('Amon.clt')
>>>
>>> # Find all variants of a variable
>>> variants = interface.find_variable_variants('clt')
>>>
>>> # Get variables for an experiment
>>> vars_hist = interface.get_variables_for_experiment('historical', 'v1.2.2.2')
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Union

from ..core.logging import logger

# Try to import the official CMIP7 Data Request API
try:
    from data_request_api.command_line import export_dreq_lists_json
    from data_request_api.content import dreq_content

    CMIP7_API_AVAILABLE = True
    logger.debug("CMIP7 Data Request API loaded successfully")
except ImportError as e:
    CMIP7_API_AVAILABLE = False
    logger.warning(f"CMIP7 Data Request API not available: {e}. " "Install with: pip install CMIP7-data-request-api")
    dreq_content = None
    export_dreq_lists_json = None


class CMIP7Interface:
    """
    Interface to the CMIP7 Data Request using the official API.

    This class provides methods to:
    - Retrieve and cache CMIP7 data request content
    - Query variables by CMIP7 compound names
    - Query variables by CMIP6 compound names (backward compatibility)
    - Find all variants of a variable
    - Get variables for specific experiments

    Attributes
    ----------
    metadata : dict
        The loaded metadata dictionary from the data request
    version : str
        The currently loaded data request version

    Examples
    --------
    >>> interface = CMIP7Interface()
    >>> interface.load_metadata('v1.2.2.2')
    >>> metadata = interface.get_variable_metadata('atmos.tas.tavg-h2m-hxy-u.mon.GLB')
    >>> print(metadata['standard_name'])
    'air_temperature'
    """

    def __init__(self):
        """Initialize the CMIP7 interface."""
        if not CMIP7_API_AVAILABLE:
            raise ImportError(
                "CMIP7 Data Request API is not available. " "Install with: pip install CMIP7-data-request-api"
            )

        self._metadata = None
        self._version = None
        self._experiments_data = None

    def get_available_versions(self, offline: bool = False) -> List[str]:
        """
        Get list of available CMIP7 data request versions.

        Parameters
        ----------
        offline : bool, optional
            If True, only return cached versions. Default is False.

        Returns
        -------
        List[str]
            List of available version identifiers.
        """
        if offline:
            return dreq_content.get_cached()
        else:
            return dreq_content.get_versions(target="tags", offline=False)

    def load_metadata(
        self,
        version: str = "v1.2.2.2",
        metadata_file: Optional[Union[str, Path]] = None,
        force_reload: bool = False,
    ) -> Dict:
        """
        Load CMIP7 metadata for a specific version.

        Parameters
        ----------
        version : str, optional
            Version to load. Default is "v1.2.2.2".
        metadata_file : str or Path, optional
            Path to a local metadata JSON file. If provided, loads from file
            instead of using the API.
        force_reload : bool, optional
            If True, force reload even if already loaded. Default is False.

        Returns
        -------
        Dict
            The loaded metadata dictionary.
        """
        if not force_reload and self._metadata is not None and self._version == version:
            return self._metadata

        if metadata_file is not None:
            # Load from local file
            metadata_file = Path(metadata_file)
            logger.info(f"Loading CMIP7 metadata from file: {metadata_file}")
            with open(metadata_file, "r") as f:
                self._metadata = json.load(f)
            self._version = self._metadata.get("Header", {}).get("dreq content version", version)
        else:
            # Use the API to export metadata directly
            import subprocess
            import tempfile

            logger.info(f"Loading CMIP7 metadata for version: {version} using API")
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)
                output_file = tmpdir_path / "metadata.json"
                # Export metadata using the command-line tool
                # Signature: export_dreq_lists_json VERSION OUTPUT_FILE [options]
                logger.debug(f"Exporting CMIP7 data request to: {output_file}")
                result = subprocess.run(
                    ["export_dreq_lists_json", version, str(output_file)],
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    raise RuntimeError(
                        f"Failed to export CMIP7 metadata: {result.stderr}\n"
                        f"You may need to run: export_dreq_lists_json {version} <output_file>"
                    )
                # Load the generated metadata file
                metadata_file = output_file
                if not metadata_file.exists():
                    raise FileNotFoundError(
                        f"Metadata file not found after export: {metadata_file}. "
                        f"Expected files in {tmpdir_path}: {list(tmpdir_path.glob('*'))}"
                    )
                logger.debug(f"Reading metadata from: {metadata_file}")
                with open(metadata_file, "r") as f:
                    self._metadata = json.load(f)
                self._version = version

        logger.info(f"Loaded metadata for {len(self._metadata.get('Compound Name', {}))} variables")
        return self._metadata

    def load_experiments_data(self, experiments_file: Union[str, Path]) -> Dict:
        """
        Load experiment-to-variable mappings.

        Parameters
        ----------
        experiments_file : str or Path
            Path to the experiments JSON file (output of export_dreq_lists_json).

        Returns
        -------
        Dict
            The loaded experiments data.
        """
        experiments_file = Path(experiments_file)
        logger.info(f"Loading experiments data from: {experiments_file}")
        with open(experiments_file, "r") as f:
            self._experiments_data = json.load(f)
        return self._experiments_data

    def get_variable_metadata(self, cmip7_compound_name: str) -> Optional[Dict]:
        """
        Get metadata for a variable by its CMIP7 compound name.

        Parameters
        ----------
        cmip7_compound_name : str
            CMIP7 compound name in format: realm.variable.branding.frequency.region
            Example: 'atmos.tas.tavg-h2m-hxy-u.mon.GLB'

        Returns
        -------
        Optional[Dict]
            Variable metadata dictionary, or None if not found.

        Raises
        ------
        ValueError
            If metadata not loaded.
        """
        if self._metadata is None:
            raise ValueError("Metadata not loaded. Call load_metadata() first.")

        compound_names = self._metadata.get("Compound Name", {})
        return compound_names.get(cmip7_compound_name)

    def get_variable_by_cmip6_name(self, cmip6_compound_name: str) -> Optional[Dict]:
        """
        Get metadata for a variable by its CMIP6 compound name (backward compatibility).

        Parameters
        ----------
        cmip6_compound_name : str
            CMIP6 compound name in format: table.variable
            Example: 'Amon.tas'

        Returns
        -------
        Optional[Dict]
            Variable metadata dictionary, or None if not found.
            If multiple CMIP7 variants exist, returns the first match.

        Raises
        ------
        ValueError
            If metadata not loaded.
        """
        if self._metadata is None:
            raise ValueError("Metadata not loaded. Call load_metadata() first.")

        compound_names = self._metadata.get("Compound Name", {})
        for cmip7_name, metadata in compound_names.items():
            if metadata.get("cmip6_compound_name") == cmip6_compound_name:
                return metadata

        return None

    def find_variable_variants(
        self,
        variable_name: str,
        realm: Optional[str] = None,
        frequency: Optional[str] = None,
        region: Optional[str] = None,
    ) -> List[Dict]:
        """
        Find all variants of a variable across different frequencies, brandings, and regions.

        Parameters
        ----------
        variable_name : str
            The physical parameter name (e.g., 'tas', 'clt').
        realm : str, optional
            Filter by modeling realm (e.g., 'atmos', 'ocean').
        frequency : str, optional
            Filter by frequency (e.g., 'mon', 'day').
        region : str, optional
            Filter by region (e.g., 'GLB', '30S-90S').

        Returns
        -------
        List[Dict]
            List of metadata dictionaries for matching variants.
            Each dict includes the 'cmip7_compound_name' key.

        Raises
        ------
        ValueError
            If metadata not loaded.
        """
        if self._metadata is None:
            raise ValueError("Metadata not loaded. Call load_metadata() first.")

        variants = []
        compound_names = self._metadata.get("Compound Name", {})

        for cmip7_name, metadata in compound_names.items():
            # Parse compound name: realm.variable.branding.frequency.region
            parts = cmip7_name.split(".")
            if len(parts) != 5:
                continue

            var_realm, var_name, var_branding, var_freq, var_region = parts

            # Check if this matches our criteria
            if var_name != variable_name:
                continue
            if realm is not None and var_realm != realm:
                continue
            if frequency is not None and var_freq != frequency:
                continue
            if region is not None and var_region != region:
                continue

            # Add compound name to metadata for reference
            variant_meta = metadata.copy()
            variant_meta["cmip7_compound_name"] = cmip7_name
            variants.append(variant_meta)

        return variants

    def get_variables_for_experiment(
        self, experiment: str, priority: Optional[str] = None
    ) -> Union[Dict[str, List[str]], List[str]]:
        """
        Get variables requested for a specific experiment.

        Parameters
        ----------
        experiment : str
            Experiment name (e.g., 'historical', 'piControl').
        priority : str, optional
            Priority level to filter by: 'Core', 'High', 'Medium', 'Low'.
            If None, returns all priorities.

        Returns
        -------
        Dict[str, List[str]] or List[str]
            If priority is None: dict mapping priority levels to variable lists.
            If priority is specified: list of variables for that priority.

        Raises
        ------
        ValueError
            If experiments data not loaded or experiment not found.
        """
        if self._experiments_data is None:
            raise ValueError("Experiments data not loaded. Call load_experiments_data() first.")

        experiments = self._experiments_data.get("experiment", {})
        if experiment not in experiments:
            available = list(experiments.keys())
            raise ValueError(f"Experiment '{experiment}' not found. " f"Available experiments: {available[:10]}...")

        exp_data = experiments[experiment]

        if priority is None:
            return exp_data
        else:
            if priority not in exp_data:
                raise ValueError(
                    f"Priority '{priority}' not found for experiment '{experiment}'. "
                    f"Available priorities: {list(exp_data.keys())}"
                )
            return exp_data[priority]

    def get_all_experiments(self) -> List[str]:
        """
        Get list of all experiments in the loaded data.

        Returns
        -------
        List[str]
            List of experiment names.

        Raises
        ------
        ValueError
            If experiments data not loaded.
        """
        if self._experiments_data is None:
            raise ValueError("Experiments data not loaded. Call load_experiments_data() first.")

        return list(self._experiments_data.get("experiment", {}).keys())

    def get_all_compound_names(self) -> List[str]:
        """
        Get list of all CMIP7 compound names.

        Returns
        -------
        List[str]
            List of CMIP7 compound names.

        Raises
        ------
        ValueError
            If metadata not loaded.
        """
        if self._metadata is None:
            raise ValueError("Metadata not loaded. Call load_metadata() first.")

        return list(self._metadata.get("Compound Name", {}).keys())

    def parse_compound_name(self, cmip7_compound_name: str) -> Dict[str, str]:
        """
        Parse a CMIP7 compound name into its components.

        Parameters
        ----------
        cmip7_compound_name : str
            CMIP7 compound name to parse.

        Returns
        -------
        Dict[str, str]
            Dictionary with keys: 'realm', 'variable', 'branding', 'frequency', 'region'

        Raises
        ------
        ValueError
            If compound name format is invalid.
        """
        parts = cmip7_compound_name.split(".")
        if len(parts) != 5:
            raise ValueError(
                f"Invalid CMIP7 compound name: {cmip7_compound_name}. "
                "Expected format: realm.variable.branding.frequency.region"
            )

        return {
            "realm": parts[0],
            "variable": parts[1],
            "branding": parts[2],
            "frequency": parts[3],
            "region": parts[4],
        }

    def build_compound_name(self, realm: str, variable: str, branding: str, frequency: str, region: str) -> str:
        """
        Build a CMIP7 compound name from components.

        Parameters
        ----------
        realm : str
            Modeling realm (e.g., 'atmos', 'ocean').
        variable : str
            Variable name (e.g., 'tas', 'tos').
        branding : str
            Branding label (e.g., 'tavg-h2m-hxy-u').
        frequency : str
            Frequency (e.g., 'mon', 'day').
        region : str
            Region (e.g., 'GLB', '30S-90S').

        Returns
        -------
        str
            CMIP7 compound name.
        """
        return f"{realm}.{variable}.{branding}.{frequency}.{region}"

    @property
    def version(self) -> Optional[str]:
        """Get the currently loaded version."""
        return self._version

    @property
    def metadata(self) -> Optional[Dict]:
        """Get the currently loaded metadata."""
        return self._metadata

    @property
    def experiments_data(self) -> Optional[Dict]:
        """Get the currently loaded experiments data."""
        return self._experiments_data


# Convenience function
def get_cmip7_interface(version: str = "v1.2.2.2", metadata_file: Optional[Union[str, Path]] = None) -> CMIP7Interface:
    """
    Get a CMIP7Interface instance with metadata loaded.

    Parameters
    ----------
    version : str, optional
        Version to load. Default is "v1.2.2.2".
    metadata_file : str or Path, optional
        Path to metadata file. If None, attempts to use API.

    Returns
    -------
    CMIP7Interface
        Interface instance with metadata loaded.

    Examples
    --------
    >>> interface = get_cmip7_interface()  # Downloads and loads v1.2.2.2
    >>> metadata = interface.get_variable_metadata('atmos.tas.tavg-h2m-hxy-u.mon.GLB')
    >>> print(metadata['standard_name'])  # doctest: +ELLIPSIS
    'air_temperature'
    """
    interface = CMIP7Interface()
    interface.load_metadata(version, metadata_file=metadata_file)
    return interface
