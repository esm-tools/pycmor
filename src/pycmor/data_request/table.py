import json
import pathlib
from abc import abstractmethod
from dataclasses import dataclass
from importlib.resources import files
from typing import List

import pendulum
from semver.version import Version

from ..core.factory import MetaFactory
from ..core.logging import logger
from .variable import CMIP6DataRequestVariable, CMIP7DataRequestVariable, DataRequestVariable

################################################################################
# BLUEPRINTS: Abstract classes for the data request tables
################################################################################


@dataclass
class DataRequestTable(metaclass=MetaFactory):
    """Abstract base class for a generic data request table."""

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Name of the table."""
        raise NotImplementedError

    @property
    def table_id(self) -> str:
        """Alias for table_name."""
        return self.table_name

    @property
    @abstractmethod
    def variables(self) -> List[DataRequestVariable]:
        """List of variables in the table."""
        raise NotImplementedError

    @abstractmethod
    def get_variable(self, name: str) -> DataRequestVariable:
        """Retrieve a variable's details by name."""
        raise NotImplementedError

    @property
    @abstractmethod
    def header(self) -> "DataRequestTableHeader":
        """Header of the table."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict) -> "DataRequestTable":
        """Create a DataRequestTable from a dictionary."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def table_dict_from_directory(cls, path: str) -> dict:
        """Create a dictionary of tables from a directory."""
        raise NotImplementedError


################################################################################


# TODO(PG): In general, this class needs to be reworked to determine which fields
# are generic and which are specific to CMIP6 or CMIP7. The current implementation
# was on CMIP6, under the assumption that all fields will also be present in CMIP7.
@dataclass
class DataRequestTableHeader(metaclass=MetaFactory):
    @property
    @abstractmethod
    def data_specs_version(self) -> Version:
        """Data specifications version."""
        raise NotImplementedError

    @property
    @abstractmethod
    def cmor_version(self) -> Version:
        """CMOR version."""
        raise NotImplementedError

    @property
    @abstractmethod
    def table_id(self) -> str:
        """Name of the table."""
        raise NotImplementedError

    @property
    @abstractmethod
    def realm(self) -> str:
        """Realm of the table."""
        raise NotImplementedError

    @property
    @abstractmethod
    def table_date(self) -> pendulum.date:
        """Date of the table."""
        raise NotImplementedError

    @property
    @abstractmethod
    def missing_value(self) -> float:
        """Missing Value"""
        raise NotImplementedError

    @property
    @abstractmethod
    def int_missing_value(self) -> int:
        """Integer missing value"""
        raise NotImplementedError

    @property
    @abstractmethod
    def product(self) -> str:
        """Product"""
        raise NotImplementedError

    @property
    @abstractmethod
    def approx_interval(self) -> float or None:
        """Approximate interval (time in days)"""
        raise NotImplementedError

    # TODO(PG): Find out if this is needed for *all* tables, or if it is
    # something specific only to CMIP6!
    @property
    @abstractmethod
    def generic_levels(self) -> List[str]:
        """Generic levels"""
        raise NotImplementedError

    @property
    @abstractmethod
    def mip_era(self) -> str:
        """MIP era"""
        raise NotImplementedError

    @property
    @abstractmethod
    def Conventions(self) -> str:
        """Conventions"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict) -> "DataRequestTableHeader":
        """Create a DataRequestTableHeader from a dictionary."""
        raise NotImplementedError


################################################################################
# END BLUEPRINTS
################################################################################


@dataclass
class CMIP7DataRequestTableHeader(DataRequestTableHeader):
    ############################################################################
    # Attributes without known defaults:
    _table_id: str
    _realm: List[str]
    _approx_interval: float  # Optional
    _generic_levels: List[str]

    @property
    def table_id(self) -> str:
        return self._table_id

    @property
    def realm(self) -> List[str]:
        return self._realm

    @property
    def approx_interval(self) -> float:
        return self._approx_interval

    @property
    def generic_levels(self) -> List[str]:
        return self._generic_levels

    ############################################################################

    ############################################################################
    # Attributes with known defaults:
    _data_specs_version: Version = Version.parse("1", optional_minor_and_patch=True)
    _cmor_version: Version = Version.parse("3.5", optional_minor_and_patch=True)
    _mip_era: str = "CMIP7"
    _Conventions: str = "CF-1.7 CMIP-7.0"
    _missing_value: float = 1.0e20
    _int_missing_value: int = -999
    _product: str = "model-output"
    # NOTE(PG): We refer here to the CMIP7 Data Request publication date, which
    # is on GitHub: https://github.com/CMIP-Data-Request/CMIP7_DReq_Software/tree/v1.0
    # Tag was created on: 22 Nov 2024
    _table_date: pendulum.Date = pendulum.Date(2024, 11, 22)

    @property
    def data_specs_version(self) -> Version:
        return self._data_specs_version

    @property
    def cmor_version(self) -> Version:
        return self._cmor_version

    @property
    def mip_era(self) -> str:
        return self._mip_era

    @property
    def Conventions(self) -> str:
        return self._Conventions

    @property
    def missing_value(self) -> float:
        return self._missing_value

    @property
    def int_missing_value(self) -> int:
        return self._int_missing_value

    @property
    def product(self) -> str:
        return self._product

    @property
    def table_date(self) -> pendulum.Date:
        return self._table_date

    ############################################################################

    ############################################################################
    # Constructor methods:
    @classmethod
    def from_dict(cls, data: dict) -> "CMIP7DataRequestTableHeader":
        """Create a CMIP7DataRequestTableHeader from a dictionary.

        Parameters
        ----------
        data : dict
            Dictionary containing header information from CMIP7 metadata.

        Returns
        -------
        CMIP7DataRequestTableHeader
            Table header instance.
        """
        # Extract required fields
        table_id = data.get("table_id", "unknown")
        realm = data.get("realm", [])
        if isinstance(realm, str):
            realm = [realm]

        # Extract optional fields with defaults
        approx_interval = data.get("approx_interval")
        generic_levels = data.get("generic_levels", [])
        if isinstance(generic_levels, str):
            generic_levels = generic_levels.split()

        return cls(
            _table_id=table_id,
            _realm=realm,
            _approx_interval=approx_interval,
            _generic_levels=generic_levels,
        )

    @classmethod
    def from_all_var_info(cls, table_name: str, all_var_info: dict = None) -> "CMIP7DataRequestTableHeader":
        """Create header from all_var_info.json for a specific table.

        This method is for backward compatibility with CMIP6 table structure.
        It groups CMIP7 variables by their CMIP6 table name.

        Parameters
        ----------
        table_name : str
            CMIP6 table name to filter by.
        all_var_info : dict, optional
            The all_var_info dictionary. If None, loads from vendored file.

        Returns
        -------
        CMIP7DataRequestTableHeader
            Table header instance.
        """
        if all_var_info is None:
            _all_var_info = files("pycmor.data.cmip7").joinpath("all_var_info.json")
            all_var_info = json.load(open(_all_var_info, "r"))

        # Filter by CMIP6 table name for backward compatibility
        all_vars_for_table = {
            k: v for k, v in all_var_info["Compound Name"].items() if v.get("cmip6_table") == table_name
        }

        if not all_vars_for_table:
            # Fallback: try prefix matching (old behavior)
            all_vars_for_table = {k: v for k, v in all_var_info["Compound Name"].items() if k.startswith(table_name)}

        attrs_for_table = {
            "realm": set(),
            "approx_interval": set(),
        }

        for var in all_vars_for_table.values():
            attrs_for_table["realm"].add(var["modeling_realm"])
            freq_interval = cls._approx_interval_from_frequency(var["frequency"])
            if freq_interval is not None:  # Skip None values (e.g., from 'fx')
                attrs_for_table["approx_interval"].add(freq_interval)

        # Get the most common approx_interval, or None if empty
        if attrs_for_table["approx_interval"]:
            # For tables with mixed frequencies, use the first one
            approx_interval = sorted(attrs_for_table["approx_interval"])[0]
        else:
            approx_interval = None

        # Build a table header, always using defaults for known fields
        return cls(
            _table_id=table_name,
            _realm=list(attrs_for_table["realm"]),
            _approx_interval=approx_interval,
            _generic_levels=[],
        )

    ############################################################################

    ############################################################################
    # Static methods:  Useful stuff that doesn't need to be on an instance

    @staticmethod
    def _approx_interval_from_frequency(frequency: str) -> float:
        if frequency == "1hr":
            return 1.0 / 24.0
        if frequency == "3hr":
            return 0.125
        if frequency == "6hr":
            return 0.25
        if frequency == "day":
            return 1.0
        if frequency == "dec":
            return 365.0 * 10.0
        if frequency == "fx":
            return None  # Maybe this should be 0.0?
        if frequency == "mon":
            return 30.0
        if frequency == "subhr":
            return 1.0 / 60.0  # Not sure about this one...
        if frequency == "yr":
            return 365.0
        raise ValueError(f"Frequency {frequency} not recognized.")


@dataclass
class CMIP6DataRequestTableHeader(DataRequestTableHeader):
    ############################################################################
    # NOTE(PG): The defaults here refer to the CMIP6 Data Request Tables
    # found in commit 1131220 of the cmip6-cmor-tables repository. Some
    # of these defaults might not be correct for later versions.
    #
    # Manual cleanup in the hard-coded defaults:
    # - data_specs_version: "01.00.33" -> "1.0.33" to match semver
    ############################################################################

    # Properties without defaults:
    # ----------------------------
    _table_id: str
    _realm: List[str]
    _table_date: pendulum.Date
    _approx_interval: float  # Optional
    _generic_levels: List[str]

    # Properties with known defaults:
    # -------------------------------
    # NOTE(PG): I don't like doing it this way, but it is fastest to
    #           implement for right by now...
    # Key: Value --> Old: New
    _HARD_CODED_DATA_SPECS_REPLACEMENTS = {
        "01.00.33": "1.0.33",
        "01.00.27": "1.0.27",
    }
    _data_specs_version: Version = Version.parse(
        "1.0.33",
        optional_minor_and_patch=True,
    )
    _cmor_version: Version = Version.parse(
        "3.5",
        optional_minor_and_patch=True,
    )
    _mip_era: str = "CMIP6"
    _Conventions: str = "CF-1.7 CMIP-6.2"
    _missing_value: float = 1.0e20
    _int_missing_value: int = -999
    _product: str = "model-output"

    @classmethod
    def from_dict(cls, data: dict) -> "CMIP6DataRequestTableHeader":
        # The input dict needs to have these, since we have no defaults:
        extracted_data = dict(
            _table_id=data["table_id"].lstrip("Table "),
            _realm=[data["realm"]],
            _table_date=pendulum.parse(data["table_date"], strict=False).date(),
            # This might be None, if the approx interval is an empty string...
            _approx_interval=(float(data["approx_interval"]) if data["approx_interval"] else None),
            _generic_levels=data["generic_levels"].split(" "),
        )
        # Optionally get the rest, which might not be present:
        for key in cls.__dataclass_fields__.keys():
            if key.lstrip("_") in data and key not in extracted_data:
                extracted_data[key] = data[key.lstrip("_")]
        # Handle Version conversions
        if "_data_specs_version" in extracted_data:
            for old_value, new_value in cls._HARD_CODED_DATA_SPECS_REPLACEMENTS.items():
                extracted_data["_data_specs_version"] = extracted_data["_data_specs_version"].replace(
                    old_value, new_value
                )
            extracted_data["_data_specs_version"] = Version.parse(
                extracted_data["_data_specs_version"],
                optional_minor_and_patch=True,
            )
        if "_cmor_version" in extracted_data:
            extracted_data["_cmor_version"] = Version.parse(
                extracted_data["_cmor_version"],
                optional_minor_and_patch=True,
            )
        # Handle types for missing_value and int_missing_value
        if "_missing_value" in extracted_data:
            extracted_data["_missing_value"] = float(extracted_data["_missing_value"])
        if "_int_missing_value" in extracted_data:
            extracted_data["_int_missing_value"] = int(extracted_data["_int_missing_value"])
        return cls(**extracted_data)

    @property
    def table_id(self) -> str:
        return self._table_id

    @property
    def realm(self) -> List[str]:
        return self._realm

    @property
    def table_date(self) -> pendulum.Date:
        return self._table_date

    @property
    def missing_value(self) -> float:
        return self._missing_value

    @property
    def int_missing_value(self) -> int:
        return self._int_missing_value

    @property
    def product(self) -> str:
        return self._product

    @property
    def approx_interval(self) -> float:
        return self._approx_interval

    @property
    def generic_levels(self) -> List[str]:
        return self._generic_levels

    @property
    def mip_era(self) -> str:
        return self._mip_era

    @property
    def Conventions(self) -> str:
        return self._Conventions

    @property
    def data_specs_version(self) -> Version:
        return self._data_specs_version

    @property
    def cmor_version(self) -> Version:
        return self._cmor_version


################################################################################


@dataclass
class CMIP6JSONDataRequestTableHeader(CMIP6DataRequestTableHeader):
    @classmethod
    def from_json_file(cls, jfile) -> "CMIP6JSONDataRequestTableHeader":
        with open(jfile, "r") as f:
            data = json.load(f)
            header = data["Header"]
            return cls.from_dict(header)


################################################################################


class CMIP6DataRequestTable(DataRequestTable):
    """DataRequestTable for CMIP6."""

    # FIXME(PG): This might bite itself in the ass...
    def __init__(
        self,
        header: CMIP6DataRequestTableHeader,
        variables: List[DataRequestVariable],
    ):
        self._header = header
        self._variables = variables

    @property
    def variables(self) -> List[str]:
        return self._variables

    @property
    def header(self) -> CMIP6DataRequestTableHeader:
        return self._header

    @property
    def table_name(self) -> str:
        return self.header.table_id

    def get_variable(self, name: str, find_by="name") -> DataRequestVariable:
        """Returns the first variable with the matching name.

        Parameters
        ----------
        name : str

        Returns
        -------
        DataRequestVariable
        """
        for v in self._variables:
            if getattr(v, find_by) == name:
                return v
        raise ValueError(f"A Variable with the attribute {find_by}={name} not found in the table.")

    @classmethod
    def from_dict(cls, data: dict) -> "CMIP6DataRequestTable":
        header = CMIP6DataRequestTableHeader.from_dict(data["Header"])
        variables = [CMIP6DataRequestVariable.from_dict(v) for v in data["variable_entry"].values()]
        return cls(header, variables)

    @classmethod
    def table_dict_from_directory(cls, path) -> dict:
        # We need to know which files to skip...
        _skip_files = [
            "CMIP6_CV_test.json",
            "CMIP6_coordinate.json",
            "CMIP6_CV.json",
            "CMIP6_formula_terms.json",
            "CMIP6_grids.json",
            "CMIP6_input_example.json",
        ]
        path = pathlib.Path(path)  # noop if already a Path
        tables = {}
        for file in path.iterdir():
            if file.name in _skip_files:
                continue
            if file.is_file() and file.suffix == ".json":
                table = cls.from_json_file(file)
                tables[table.table_id] = table
        return tables

    @classmethod
    def from_json_file(cls, jfile) -> "CMIP6DataRequestTable":
        with open(jfile, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)


################################################################################


@dataclass
class CMIP7DataRequestTable(DataRequestTable):
    """DataRequestTable for CMIP7."""

    # FIXME(PG): This might bite itself in the ass...
    def __init__(
        self,
        header: CMIP7DataRequestTableHeader,
        variables: List[DataRequestVariable],
    ):
        self._header = header
        self._variables = variables

    @property
    def variables(self) -> List[str]:
        return self._variables

    @property
    def header(self) -> CMIP7DataRequestTableHeader:
        return self._header

    @property
    def table_name(self) -> str:
        return self.header.table_id

    def get_variable(self, name: str, find_by="name") -> DataRequestVariable:
        """Returns the first variable with the matching name.

        Parameters
        ----------
        name : str

        Returns
        -------
        DataRequestVariable
        """
        for v in self._variables:
            if getattr(v, find_by) == name:
                return v
        raise ValueError(f"A Variable with the attribute {find_by}={name} not found in the table.")

    @classmethod
    def from_dict(cls, data: dict) -> "CMIP7DataRequestTable":
        header = CMIP7DataRequestTableHeader.from_dict(data["Header"])
        variables = []
        for var_key, var_data in data["Compound Name"].items():
            table_name, var_name = var_key.split(".")
            var_data["table_name"] = table_name
            var_data["name"] = var_name
            variables.append(CMIP7DataRequestVariable.from_dict(var_data))
        return cls(header, variables)

    @classmethod
    def from_all_var_info_json(cls, table_name: str) -> "CMIP7DataRequestTable":
        _all_var_info = files("pycmor.data.cmip7").joinpath("all_var_info.json")
        all_var_info = json.load(open(_all_var_info, "r"))
        return cls.from_all_var_info(table_name, all_var_info)

    @classmethod
    def from_all_var_info(cls, table_name: str, all_var_info: dict = None):
        if all_var_info is None:
            _all_var_info = files("pycmor.data.cmip7").joinpath("all_var_info.json")
            all_var_info = json.load(open(_all_var_info, "r"))
        header = CMIP7DataRequestTableHeader.from_all_var_info(table_name, all_var_info)
        variables = []
        for var_name, var_dict in all_var_info["Compound Name"].items():
            if var_dict["cmip6_cmor_table"] == table_name:
                variables.append(CMIP7DataRequestVariable.from_dict(var_dict))
        return cls(header, variables)

    @classmethod
    def table_dict_from_directory(cls, path) -> dict:
        """
        Create tables from directory or use packaged data.

        For CMIP7, this method uses the packaged all_var_info.json
        instead of looking in the directory, since CMIP7 data is
        distributed with pycmor rather than in a separate repository.

        Parameters
        ----------
        path : str or Path
            Path parameter (ignored for CMIP7, kept for API compatibility)

        Returns
        -------
        dict
            Dictionary mapping table_id to CMIP7DataRequestTable objects
        """
        # Use packaged data for CMIP7
        _all_var_info = files("pycmor.data.cmip7").joinpath("all_var_info.json")
        with open(_all_var_info, "r") as f:
            all_var_info = json.load(f)

        tables = {}
        table_ids = set(
            v.get("cmip6_cmor_table") for v in all_var_info["Compound Name"].values() if v.get("cmip6_cmor_table")
        )
        for table_id in table_ids:
            table = cls.from_all_var_info(table_id, all_var_info)
            tables[table_id] = table
        return tables

    @classmethod
    def from_json_file(cls, jfile) -> "CMIP7DataRequestTable":
        with open(jfile, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)

    @property
    def table_id(self) -> str:
        """Alias for table_name."""
        return self.table_name


################################################################################
