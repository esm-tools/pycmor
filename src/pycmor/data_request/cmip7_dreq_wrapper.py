"""
Wrapper for the official CMIP7 Data Request Software.

This module provides a clean interface to the CMIP7_DReq_Software repository:
https://github.com/CMIP-Data-Request/CMIP7_DReq_Software

It wraps the official API to provide:
- Content retrieval and caching
- Variable queries by experiment and opportunity
- Integration with pycmor's data request classes
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Union

from ..core.logging import logger

# Try to import the CMIP7_DReq_Software modules
try:
    # Add the CMIP7_DReq_Software path to sys.path if needed
    _dreq_software_path = Path(__file__).parent.parent.parent.parent / "CMIP7_DReq_Software"
    if _dreq_software_path.exists():
        # Add multiple paths for the different modules
        _api_stable_path = _dreq_software_path / "data_request_api" / "stable"
        _api_content_path = _api_stable_path / "content" / "dreq_api"
        _api_query_path = _api_stable_path / "query"
        _api_transform_path = _api_stable_path / "transform"
        
        for path in [_api_stable_path, _api_content_path, _api_query_path, _api_transform_path]:
            if str(path) not in sys.path:
                sys.path.insert(0, str(path))
        
        # Import the official CMIP7 Data Request modules
        import dreq_content as dc
        import dreq_query as dq
        import dreq_classes
        
        CMIP7_DREQ_AVAILABLE = True
        logger.info("CMIP7_DReq_Software modules loaded successfully")
    else:
        CMIP7_DREQ_AVAILABLE = False
        logger.warning(
            f"CMIP7_DReq_Software not found at {_dreq_software_path}. "
            "Some functionality will be limited."
        )
        dc = None
        dq = None
        dreq_classes = None
except ImportError as e:
    CMIP7_DREQ_AVAILABLE = False
    logger.warning(f"Could not import CMIP7_DReq_Software: {e}")
    dc = None
    dq = None
    dreq_classes = None


class CMIP7DataRequestWrapper:
    """
    Wrapper class for the official CMIP7 Data Request Software.
    
    This class provides a simplified interface to:
    - Retrieve and cache CMIP7 data request content
    - Query variables by experiment, opportunity, and priority
    - Access variable metadata
    
    Examples
    --------
    >>> wrapper = CMIP7DataRequestWrapper()
    >>> wrapper.retrieve_content("v1.0")
    >>> variables = wrapper.get_variables_for_experiment("historical")
    >>> metadata = wrapper.get_variable_metadata("Amon.tas")
    """
    
    def __init__(self):
        """Initialize the wrapper."""
        if not CMIP7_DREQ_AVAILABLE:
            raise ImportError(
                "CMIP7_DReq_Software is not available. "
                "Please clone the repository to the project root:\n"
                "git clone https://github.com/CMIP-Data-Request/CMIP7_DReq_Software.git"
            )
        
        self._content = None
        self._version = None
        self._tables = None
    
    def get_available_versions(self, target: str = "tags") -> List[str]:
        """
        Get list of available CMIP7 data request versions.
        
        Parameters
        ----------
        target : str, optional
            Either "tags" for released versions or "branches" for development versions.
            Default is "tags".
        
        Returns
        -------
        List[str]
            List of available version identifiers.
        """
        return dc.get_versions(target=target)
    
    def get_cached_versions(self) -> List[str]:
        """
        Get list of locally cached versions.
        
        Returns
        -------
        List[str]
            List of cached version identifiers.
        """
        return dc.get_cached()
    
    def retrieve_content(
        self, 
        version: str = "latest_stable",
        export: str = "release",
        force_update: bool = False
    ) -> Dict:
        """
        Retrieve CMIP7 data request content.
        
        Parameters
        ----------
        version : str, optional
            Version to retrieve. Options:
            - "latest_stable": Latest stable release (default)
            - "latest": Latest version including pre-releases
            - "dev": Development version
            - Specific version tag, e.g., "v1.0"
        export : str, optional
            Export type: "release" or "raw". Default is "release".
        force_update : bool, optional
            If True, force re-download even if cached. Default is False.
        
        Returns
        -------
        Dict
            The loaded data request content.
        """
        if force_update or self._content is None or self._version != version:
            logger.info(f"Retrieving CMIP7 data request version: {version}")
            
            # Retrieve (downloads if not cached)
            dc.retrieve(version, export=export)
            
            # Load the content (consolidate=True merges the bases into one)
            self._content = dc.load(version, export=export, consolidate=True)
            self._version = version
            dq.DREQ_VERSION = version
            
            logger.info(f"Loaded CMIP7 data request version: {version}")
        
        return self._content
    
    def load_content(self, version: str = "latest_stable", **kwargs) -> Dict:
        """
        Load CMIP7 data request content (alias for retrieve_content).
        
        Parameters
        ----------
        version : str, optional
            Version to load. Default is "latest_stable".
        **kwargs
            Additional arguments passed to retrieve_content.
        
        Returns
        -------
        Dict
            The loaded data request content.
        """
        return self.retrieve_content(version, **kwargs)
    
    def get_requested_variables(
        self,
        opportunities: Union[str, List[str]] = "all",
        priority_cutoff: str = "Low",
        verbose: bool = False
    ) -> Dict:
        """
        Get variables requested for each experiment.
        
        Parameters
        ----------
        opportunities : str or List[str], optional
            Opportunities to include:
            - "all": All available opportunities (default)
            - List of opportunity titles
        priority_cutoff : str, optional
            Minimum priority level to include. Options: "Core", "High", "Medium", "Low".
            Default is "Low" (includes all priorities).
        verbose : bool, optional
            If True, print detailed information. Default is False.
        
        Returns
        -------
        Dict
            Dictionary with structure:
            {
                "Header": {
                    "Opportunities": [...],
                    "dreq version": "..."
                },
                "experiment": {
                    "historical": {
                        "Core": ["Amon.tas", ...],
                        "High": [...],
                        ...
                    },
                    ...
                }
            }
        """
        if self._content is None:
            raise ValueError(
                "Content not loaded. Call retrieve_content() first."
            )
        
        try:
            # Note: get_requested_variables modifies the content dict in place,
            # converting tables to dreq_table objects. To avoid issues with
            # repeated calls, we need to reload content if it's already been processed.
            import copy
            content_copy = copy.deepcopy(self._content)
            
            return dq.get_requested_variables(
                content_copy,
                use_opps=opportunities,
                priority_cutoff=priority_cutoff,
                verbose=verbose,
                consolidated=True
            )
        except AttributeError as e:
            if "compound_name" in str(e):
                logger.error(
                    f"Version {self._version} may have incompatible data structure. "
                    f"The 'compound_name' attribute is missing from variables. "
                    f"This is a known issue with some CMIP7 data request versions. "
                    f"Try using v1.0, v1.1, or v1.2 instead."
                )
                raise ValueError(
                    f"Incompatible data request version: {self._version}. "
                    f"Variable records are missing the 'compound_name' attribute. "
                    f"Please use v1.0, v1.1, or v1.2."
                ) from e
            else:
                raise
    
    def get_variables_for_experiment(
        self,
        experiment: str,
        opportunities: Union[str, List[str]] = "all",
        priority_cutoff: str = "Low"
    ) -> Dict[str, List[str]]:
        """
        Get variables requested for a specific experiment.
        
        Parameters
        ----------
        experiment : str
            Experiment name, e.g., "historical", "piControl".
        opportunities : str or List[str], optional
            Opportunities to include. Default is "all".
        priority_cutoff : str, optional
            Minimum priority level. Default is "Low".
        
        Returns
        -------
        Dict[str, List[str]]
            Dictionary mapping priority levels to variable lists:
            {
                "Core": ["Amon.tas", ...],
                "High": [...],
                ...
            }
        """
        all_vars = self.get_requested_variables(opportunities, priority_cutoff)
        
        if experiment not in all_vars["experiment"]:
            raise ValueError(
                f"Experiment '{experiment}' not found. "
                f"Available: {list(all_vars['experiment'].keys())}"
            )
        
        return all_vars["experiment"][experiment]
    
    def get_all_experiments(
        self,
        opportunities: Union[str, List[str]] = "all"
    ) -> List[str]:
        """
        Get list of all experiments in the data request.
        
        Parameters
        ----------
        opportunities : str or List[str], optional
            Opportunities to include. Default is "all".
        
        Returns
        -------
        List[str]
            List of experiment names.
        """
        all_vars = self.get_requested_variables(opportunities)
        return list(all_vars["experiment"].keys())
    
    def get_all_variables(
        self,
        opportunities: Union[str, List[str]] = "all",
        priority_cutoff: str = "Low"
    ) -> Set[str]:
        """
        Get set of all unique variables across all experiments.
        
        Parameters
        ----------
        opportunities : str or List[str], optional
            Opportunities to include. Default is "all".
        priority_cutoff : str, optional
            Minimum priority level. Default is "Low".
        
        Returns
        -------
        Set[str]
            Set of unique variable compound names (e.g., "Amon.tas").
        """
        all_vars = self.get_requested_variables(opportunities, priority_cutoff)
        
        unique_vars = set()
        for expt_vars in all_vars["experiment"].values():
            for priority_vars in expt_vars.values():
                unique_vars.update(priority_vars)
        
        return unique_vars
    
    def get_variable_metadata(self, compound_name: str) -> Optional[Dict]:
        """
        Get metadata for a specific variable.
        
        Parameters
        ----------
        compound_name : str
            Variable compound name, e.g., "Amon.tas".
        
        Returns
        -------
        Optional[Dict]
            Variable metadata dictionary, or None if not found.
        """
        if self._content is None:
            raise ValueError(
                "Content not loaded. Call retrieve_content() first."
            )
        
        # Create variable tables if not already done
        if self._tables is None:
            self._tables = dq.create_dreq_tables_for_variables(
                self._content, consolidated=True
            )
        
        # Parse compound name
        try:
            table_name, var_name = compound_name.split(".")
        except ValueError:
            raise ValueError(
                f"Invalid compound name: {compound_name}. "
                "Expected format: 'TableName.varname'"
            )
        
        # Get variable from tables
        if "Variables" not in self._tables:
            raise ValueError("Variables table not found in content")
        
        vars_table = self._tables["Variables"]
        
        # Search for the variable
        for var_record in vars_table.records.values():
            if (hasattr(var_record, "compound_name") and 
                var_record.compound_name == compound_name):
                # Convert record to dictionary
                return vars(var_record)
        
        return None
    
    def get_opportunities(self) -> List[str]:
        """
        Get list of all available opportunities.
        
        Returns
        -------
        List[str]
            List of opportunity titles.
        """
        if self._content is None:
            raise ValueError(
                "Content not loaded. Call retrieve_content() first."
            )
        
        tables = dq.create_dreq_tables_for_request(
            self._content, consolidated=True
        )
        
        opps_table = tables["Opportunity"]
        return [opp.title for opp in opps_table.records.values()]
    
    def get_priority_levels(self) -> List[str]:
        """
        Get list of all priority levels.
        
        Returns
        -------
        List[str]
            List of priority levels, ordered from highest to lowest.
        """
        return dq.get_priority_levels()
    
    def delete_cached_version(self, version: str = "all", keep_latest: bool = False):
        """
        Delete cached version(s).
        
        Parameters
        ----------
        version : str, optional
            Version to delete, or "all" for all versions. Default is "all".
        keep_latest : bool, optional
            If True and version="all", keep the latest versions. Default is False.
        """
        dc.delete(version=version, keep_latest=keep_latest)
        logger.info(f"Deleted cached version(s): {version}")
    
    def export_to_json(
        self,
        output_path: Union[str, Path],
        opportunities: Union[str, List[str]] = "all",
        priority_cutoff: str = "Low"
    ):
        """
        Export requested variables to JSON file.
        
        Parameters
        ----------
        output_path : str or Path
            Path to output JSON file.
        opportunities : str or List[str], optional
            Opportunities to include. Default is "all".
        priority_cutoff : str, optional
            Minimum priority level. Default is "Low".
        """
        output_path = Path(output_path)
        
        requested_vars = self.get_requested_variables(
            opportunities, priority_cutoff
        )
        
        with open(output_path, "w") as f:
            json.dump(requested_vars, f, indent=2)
        
        logger.info(f"Exported requested variables to: {output_path}")
    
    @property
    def version(self) -> Optional[str]:
        """Get the currently loaded version."""
        return self._version
    
    @property
    def content(self) -> Optional[Dict]:
        """Get the currently loaded content."""
        return self._content
    
    def check_version_compatibility(self) -> bool:
        """
        Check if the loaded version is compatible with the wrapper.
        
        Returns
        -------
        bool
            True if compatible, False otherwise.
        
        Notes
        -----
        Some newer versions (v1.2.2.1+) have incompatible data structures
        where variables lack the 'compound_name' attribute.
        """
        if self._content is None:
            logger.warning("No content loaded. Call retrieve_content() first.")
            return False
        
        try:
            # Reload content to avoid issues with in-place modifications
            import copy
            content_copy = copy.deepcopy(self._content)
            
            # Try to get variables for a simple query
            tables = dq.create_dreq_tables_for_variables(
                content_copy, consolidated=True
            )
            
            if "Variables" not in tables:
                logger.warning("Variables table not found in content")
                return False
            
            vars_table = tables["Variables"]
            
            # Check if variables have compound_name attribute
            for var_record in list(vars_table.records.values())[:5]:
                if not hasattr(var_record, "compound_name"):
                    logger.warning(
                        f"Version {self._version} is incompatible: "
                        "variables lack 'compound_name' attribute"
                    )
                    return False
            
            logger.info(f"Version {self._version} is compatible")
            return True
            
        except Exception as e:
            logger.error(f"Error checking compatibility: {e}")
            return False


# Convenience function for quick access
def get_cmip7_data_request(version: str = "latest_stable") -> CMIP7DataRequestWrapper:
    """
    Get a CMIP7DataRequestWrapper instance with content loaded.
    
    Parameters
    ----------
    version : str, optional
        Version to load. Default is "latest_stable".
    
    Returns
    -------
    CMIP7DataRequestWrapper
        Wrapper instance with content loaded.
    
    Examples
    --------
    >>> dreq = get_cmip7_data_request("v1.0")
    >>> experiments = dreq.get_all_experiments()
    >>> vars_hist = dreq.get_variables_for_experiment("historical")
    """
    wrapper = CMIP7DataRequestWrapper()
    wrapper.retrieve_content(version)
    return wrapper
