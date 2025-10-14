"""
This module defines the ``DataRequestVariable`` abstract base class and its
concrete implementation ``CMIP6DataRequestVariable``.

The ``DataRequestVariable`` class outlines the necessary properties and methods
that any variable class should implement. It includes properties such as frequency,
modeling realm, standard name, units, cell methods, cell measures, long name,
comment, dimensions, out name, type, positive direction, valid minimum and
maximum values, acceptable minimum and maximum mean absolute values, and the
table name.

The ``CMIP6DataRequestVariable`` class is a concrete implementation of the
``DataRequestVariable`` class, specifically for CMIP6 variables. It uses the
``dataclass`` decorator to automatically generate the ``__init__``, ``__repr__``,
and other special methods.

The module also provides class methods for constructing ``DataRequestVariable``
instances from dictionaries and JSON files, as well as a method for converting
a ``DataRequestVariable`` instance to a dictionary representation.
"""

import copy
import json
from abc import abstractmethod
from dataclasses import dataclass
from importlib.resources import files
from typing import Optional

from ..core.factory import MetaFactory


@dataclass
class DataRequestVariable(metaclass=MetaFactory):
    """Abstract base class for a generic variable."""

    _type_strings = {
        "real": float,
        "integer": int,
        "string": str,
        "double": float,
        "float": float,
        "char": str,
        "int": int,
        "long": int,
        "short": int,
        "boolean": bool,
        "logical": bool,
        "character": str,
    }
    """dict: conversion of string names in the tables to actual Python types"""

    #################################################################
    # Properties a DataRequestVariable needs to have
    #################################################################

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the variable"""
        raise NotImplementedError

    @property
    def variable_id(self) -> str:
        """Variable ID"""
        return self.name

    @property
    @abstractmethod
    def frequency(self) -> str:  # Or should this return Frequency?
        """Frequency of this variable"""
        raise NotImplementedError

    @property
    @abstractmethod
    def modeling_realm(self) -> str:
        """Modeling Realm of this variable"""
        raise NotImplementedError

    @property
    @abstractmethod
    def standard_name(self) -> str:
        """The CF standard name of the variable"""
        raise NotImplementedError

    @property
    @abstractmethod
    def units(self) -> str:
        """The units of the variable"""
        raise NotImplementedError

    @property
    @abstractmethod
    def cell_methods(self) -> str:
        """Methods applied to the cell"""
        # FIXME(PG): I have no idea what this is
        raise NotImplementedError

    @property
    @abstractmethod
    def cell_measures(self) -> str:
        """What this cell measure"""
        # FIXME(PG): I have no idea what this is
        raise NotImplementedError

    @property
    @abstractmethod
    def long_name(self) -> str:
        """The CF long name for this variable"""
        raise NotImplementedError

    @property
    @abstractmethod
    def comment(self) -> str:
        """Comment for NetCDF attributes"""
        raise NotImplementedError

    @property
    @abstractmethod
    def dimensions(self) -> tuple[str, ...]:
        """Dimensions of this variable"""
        raise NotImplementedError

    @property
    @abstractmethod
    def out_name(self) -> str:
        """Short name (array name) of this variable"""
        raise NotImplementedError

    @property
    @abstractmethod
    def typ(self) -> type:
        """The type of this array: int, float, str"""
        raise NotImplementedError

    @property
    @abstractmethod
    def positive(self) -> str:
        """For 3-D variables, which direction is up/down"""
        raise NotImplementedError

    @property
    @abstractmethod
    def valid_min(self) -> float:
        """Valid minimum"""
        raise NotImplementedError

    @property
    @abstractmethod
    def valid_max(self) -> float:
        """Valid maximum"""
        raise NotImplementedError

    @property
    @abstractmethod
    def ok_min_mean_abs(self) -> float:
        """ok minimum, mean, and absolute value"""
        raise NotImplementedError

    @property
    @abstractmethod
    def ok_max_mean_abs(self) -> float:
        """ok maximum, mean, and absolute value"""
        raise NotImplementedError

    @property
    @abstractmethod
    def table_name(self) -> Optional[str]:
        """The table this variable is define in"""
        raise NotImplementedError

    @property
    @abstractmethod
    def attrs(self) -> dict:
        """Attributes to update the Xarray DataArray with"""
        raise NotImplementedError

    #################################################################
    # Class methods for construction
    #################################################################
    @classmethod
    def from_dict(cls, data: dict) -> "DataRequestVariable":
        """Create a DataRequestVariable instance from a dictionary."""
        raise NotImplementedError

    @classmethod
    def from_json_file(cls, jfile: str, varname: str) -> "DataRequestVariable":
        """Create a DataRequestVariable instance from a JSON file."""
        raise NotImplementedError

    #################################################################
    # Methods for serialization
    #################################################################
    def to_dict(self) -> dict:
        """Convert the variable to a dictionary representation"""
        return self.__dict__

    #################################################################
    # Other methods
    #################################################################
    @abstractmethod
    def global_attrs(self, override_dict: dict = None) -> dict:
        """Global attributes for this variable, used to set on the xr.Dataset"""
        raise NotImplementedError

    @abstractmethod
    def clone(self) -> "DataRequestVariable":
        """Create a copy of this variable"""
        raise NotImplementedError


@dataclass
class CMIP6DataRequestVariable(DataRequestVariable):
    _variable_id: str
    _name: str
    _frequency: str
    _modeling_realm: str
    _standard_name: str
    _units: str
    _cell_methods: str
    _cell_measures: str
    _long_name: str
    _comment: str
    _dimensions: tuple[str, ...]
    _out_name: str
    _typ: type
    _positive: str
    _valid_min: float
    _valid_max: float
    _ok_min_mean_abs: float
    _ok_max_mean_abs: float
    _table_name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "CMIP6DataRequestVariable":
        """Create a DataRequestVariable instance from a dictionary."""
        typ = cls._type_strings.get(data["type"])
        if typ is None:
            raise ValueError(f"Unsupported type: {data['type']}")
        return cls(
            # NOTE(PG): This one is self-defined, ``name`` is not in the dict, but useful
            _name=data["out_name"],
            _variable_id=data["out_name"],
            _frequency=data["frequency"],
            _modeling_realm=data["modeling_realm"],
            _standard_name=data["standard_name"],
            _units=data["units"],
            _cell_methods=data["cell_methods"],
            _cell_measures=data["cell_measures"],
            _long_name=data["long_name"],
            _comment=data["comment"],
            # NOTE(PG): tuple, because of immutability
            _dimensions=tuple(data["dimensions"].split(" ")),
            _out_name=data["out_name"],
            _typ=cls._type_strings[data["type"]],
            _positive=data["positive"],
            _valid_min=data["valid_min"],
            _valid_max=data["valid_max"],
            _ok_min_mean_abs=data["ok_min_mean_abs"],
            _ok_max_mean_abs=data["ok_max_mean_abs"],
            _table_name=data.get("table_name"),
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def frequency(self) -> str:
        return self._frequency

    @property
    def modeling_realm(self) -> str:
        return self._modeling_realm

    @property
    def standard_name(self) -> str:
        return self._standard_name

    @property
    def units(self) -> str:
        return self._units

    @property
    def cell_methods(self) -> str:
        return self._cell_methods

    @property
    def cell_measures(self) -> str:
        return self._cell_measures

    @property
    def long_name(self) -> str:
        return self._long_name

    @property
    def comment(self) -> str:
        return self._comment

    @property
    def dimensions(self) -> tuple[str, ...]:
        return self._dimensions

    @property
    def out_name(self) -> str:
        return self._out_name

    @property
    def typ(self) -> type:
        return self._typ

    @property
    def positive(self) -> str:
        return self._positive

    @property
    def valid_min(self) -> float:
        return self._valid_min

    @property
    def valid_max(self) -> float:
        return self._valid_max

    @property
    def ok_min_mean_abs(self) -> float:
        return self._ok_min_mean_abs

    @property
    def ok_max_mean_abs(self) -> float:
        return self._ok_max_mean_abs

    @property
    def table_name(self) -> Optional[str]:
        return self._table_name

    @property
    def attrs(self) -> dict:
        return {
            "standard_name": self.standard_name,
            "long_name": self.long_name,
            "units": self.units,
            "cell_methods": self.cell_methods,
            "cell_measures": self.cell_measures,
            "_FillValue": getattr(self, "_FillValue", None),
            "missing_value": getattr(self, "missing_value", None),
        }

    def global_attrs(self, override_dict: dict = None) -> dict:
        """Return a dictionary of global attributes for a CMIP6 variable

        Parameters
        ----------
        override_dict : dict
            A dictionary of attributes to override the default values
        """
        override_dict = override_dict or {}
        # FIXME: This needs to come from the CVs somehow
        rdict = {
            "Conventions": None,
            "activity_id": None,
            "creation_date": None,
            "data_specs_version": None,
            "experiment": None,
            "experiment_id": None,
            "forcing_index": None,
            "frequency": None,
            "further_info_url": None,
            "grid": None,
            "grid_label": None,
            "initialization_index": None,
            "institution": None,
            "institution_id": None,
            "license": None,
            "mip_era": None,
            "nominal_resolution": None,
            "physics_index": None,
            "product": None,
            "realization_index": None,
            "realm": None,
            "source": None,
            "source_id": None,
            "source_type": None,
            "sub_experiment": None,
            "sub_experiment_id": None,
            "table_id": None,
            "tracking_id": None,
            "variable_id": None,
            "variant_label": None,
        }
        rdict.update(override_dict)
        return rdict

    def clone(self) -> "CMIP6DataRequestVariable":
        clone = copy.deepcopy(self)
        return clone


class CMIP6JSONDataRequestVariable(CMIP6DataRequestVariable):
    @classmethod
    def from_json_file(cls, jfile: str, varname: str) -> "CMIP6DataRequestVariable":
        with open(jfile, "r") as f:
            data = json.load(f)
            header = data["Header"]
            table_name = header["table_id"].replace("Table ", "")
            var_data = data["variable_entry"][varname]
            var_data["table_name"] = table_name
            return cls.from_dict(var_data)


@dataclass
class CMIP7DataRequestVariable(DataRequestVariable):
    """DataRequestVariable for CMIP7.
    
    CMIP7 uses a compound name structure: realm.variable.branding.frequency.region
    Example: atmos.tas.tavg-h2m-hxy-u.mon.GLB
    
    For backward compatibility, CMIP6 table and compound name are also stored.
    """

    # Attributes without defaults
    _frequency: str
    _modeling_realm: str
    _standard_name: str
    _units: str
    _cell_methods: str
    _cell_measures: str
    _long_name: str
    _comment: str
    _dimensions: tuple[str, ...]
    _out_name: str
    _typ: type
    _positive: str
    _spatial_shape: str
    _temporal_shape: str
    _name: str
    
    # CMIP7-specific attributes
    _cmip7_compound_name: Optional[str] = None
    _branding_label: Optional[str] = None
    _region: Optional[str] = None
    
    # CMIP6 backward compatibility
    _cmip6_table: Optional[str] = None
    _cmip6_compound_name: Optional[str] = None
    _table_name: Optional[str] = None

    @classmethod
    def from_dict(cls, data):
        """Create a CMIP7DataRequestVariable from a dictionary.
        
        Parameters
        ----------
        data : dict
            Dictionary containing variable metadata from CMIP7 data request.
            Expected keys include all standard metadata fields plus CMIP7-specific
            fields like 'cmip7_compound_name', 'branding_label', 'region', etc.
        
        Returns
        -------
        CMIP7DataRequestVariable
            Variable instance.
        """
        extracted_data = dict(
            _name=data.get("out_name", data.get("physical_parameter_name")),
            _frequency=data["frequency"],
            _modeling_realm=data["modeling_realm"],
            # FIXME(PG): Not all variables appear to have standard_name
            _standard_name=data.get("standard_name"),
            _units=data["units"],
            _cell_methods=data["cell_methods"],
            _cell_measures=data["cell_measures"],
            _long_name=data["long_name"],
            _comment=data["comment"],
            _dimensions=tuple(data["dimensions"].split(" ")),
            _out_name=data["out_name"],
            _typ=cls._type_strings[data["type"]],
            _positive=data["positive"],
            _spatial_shape=data["spatial_shape"],
            _temporal_shape=data["temporal_shape"],
            # CMIP7-specific fields
            _cmip7_compound_name=data.get("cmip7_compound_name"),
            _branding_label=data.get("branding_label"),
            _region=data.get("region"),
            # CMIP6 backward compatibility
            _cmip6_table=data.get("cmip6_table"),
            _cmip6_compound_name=data.get("cmip6_compound_name"),
            _table_name=data.get("cmip6_table", data.get("table_name")),
        )
        return cls(**extracted_data)

    @classmethod
    def from_all_var_info_json(cls, compound_name: str, use_cmip6_name: bool = False):
        """Load a variable from the vendored all_var_info.json file.
        
        Parameters
        ----------
        compound_name : str
            Either CMIP7 compound name (realm.variable.branding.frequency.region)
            or CMIP6 compound name (table.variable) if use_cmip6_name=True.
        use_cmip6_name : bool, optional
            If True, treat compound_name as CMIP6 format. Default is False.
        
        Returns
        -------
        CMIP7DataRequestVariable
            Variable instance.
        """
        _all_var_info = files("pycmor.data.cmip7").joinpath("all_var_info.json")
        all_var_info = json.load(open(_all_var_info, "r"))
        
        if use_cmip6_name:
            # Search for CMIP6 compound name
            for cmip7_name, data in all_var_info["Compound Name"].items():
                if data.get("cmip6_compound_name") == compound_name:
                    return cls.from_dict(data)
            raise ValueError(f"CMIP6 compound name '{compound_name}' not found")
        else:
            # Use CMIP7 compound name directly
            data = all_var_info["Compound Name"].get(compound_name)
            if data is None:
                raise ValueError(f"CMIP7 compound name '{compound_name}' not found")
            return cls.from_dict(data)

    @property
    def attrs(self) -> dict:
        """Return attributes dictionary for xarray DataArray."""
        attrs = {
            "standard_name": self.standard_name,
            "long_name": self.long_name,
            "units": self.units,
            "cell_methods": self.cell_methods,
            "comment": self.comment,
        }
        # Remove None values
        return {k: v for k, v in attrs.items() if v is not None}

    @property
    def cell_measures(self) -> str:
        return self._cell_measures

    @property
    def cell_methods(self) -> str:
        return self._cell_methods

    @property
    def comment(self) -> str:
        return self._comment

    @property
    def dimensions(self) -> tuple[str, ...]:
        return self._dimensions

    @property
    def frequency(self) -> str:
        return self._frequency

    def global_attrs(self, override_dict: dict = None) -> dict:
        """Return global attributes for CMIP7 variable.
        
        Parameters
        ----------
        override_dict : dict, optional
            Dictionary of attributes to override defaults.
        
        Returns
        -------
        dict
            Global attributes dictionary.
        """
        override_dict = override_dict or {}
        rdict = {
            "Conventions": "CF-1.7 CMIP-7.0",
            "mip_era": "CMIP7",
            "frequency": self.frequency,
            "realm": self.modeling_realm,
            "variable_id": self.out_name,
            "table_id": self.table_name,
            # Additional CMIP7-specific attributes
            "cmip7_compound_name": self.cmip7_compound_name,
            "branding_label": self.branding_label,
            "region": self.region,
        }
        # Remove None values
        rdict = {k: v for k, v in rdict.items() if v is not None}
        rdict.update(override_dict)
        return rdict

    @property
    def long_name(self) -> str:
        return self._long_name

    @property
    def modeling_realm(self) -> str:
        return self._modeling_realm

    @property
    def name(self) -> str:
        return self._name

    @property
    def ok_max_mean_abs(self) -> float:
        """Acceptable maximum mean absolute value (not defined in CMIP7)."""
        return float('inf')

    @property
    def ok_min_mean_abs(self) -> float:
        """Acceptable minimum mean absolute value (not defined in CMIP7)."""
        return 0.0

    @property
    def out_name(self) -> str:
        return self._out_name

    @property
    def positive(self) -> str:
        return self._positive

    @property
    def standard_name(self) -> str:
        return self._standard_name

    @property
    def table_name(self) -> Optional[str]:
        """Return CMIP6 table name for backward compatibility."""
        return self._table_name
    
    @property
    def cmip7_compound_name(self) -> Optional[str]:
        """CMIP7 compound name: realm.variable.branding.frequency.region"""
        return self._cmip7_compound_name
    
    @property
    def cmip6_compound_name(self) -> Optional[str]:
        """CMIP6 compound name for backward compatibility: table.variable"""
        return self._cmip6_compound_name
    
    @property
    def branding_label(self) -> Optional[str]:
        """CMIP7 branding label describing temporal/spatial processing."""
        return self._branding_label
    
    @property
    def region(self) -> Optional[str]:
        """CMIP7 region code (e.g., 'GLB', '30S-90S')."""
        return self._region

    @property
    def typ(self) -> type:
        return self._typ

    @property
    def units(self) -> str:
        return self._units

    @property
    def valid_max(self) -> float:
        """Valid maximum value (not strictly defined in CMIP7)."""
        return float('inf')

    @property
    def valid_min(self) -> float:
        """Valid minimum value (not strictly defined in CMIP7)."""
        return float('-inf')

    def clone(self) -> "CMIP7DataRequestVariable":
        clone = copy.deepcopy(self)
        return clone
