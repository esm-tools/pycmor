import json
import pathlib
from abc import abstractmethod
from enum import Enum
from importlib.resources import files
from typing import Dict

import deprecation

from ..core.factory import MetaFactory
from ..core.utils import download_json_tables_from_url, list_files_in_directory
from .table import CMIP6DataRequestTable, CMIP7DataRequestTable, DataRequestTable
from .variable import CMIP7DataRequestVariable


class DataRequest(metaclass=MetaFactory):
    @classmethod
    @abstractmethod
    def from_tables(cls, tables: Dict[str, DataRequestTable]) -> "DataRequest":
        """Create a DataRequest from a dictionary of tables."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_directory(cls, directory: str) -> "DataRequest":
        """Create a DataRequest from a directory of tables."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_git(cls, url: str, branch: str = "master") -> "DataRequest":
        """Create a DataRequest from a git repository."""
        raise NotImplementedError

    @classmethod
    @deprecation.deprecated(details="Use from_directory instead.")
    @abstractmethod
    def from_tables_dir(cls, directory: str) -> "DataRequest":
        """Create a DataRequest from a directory of tables."""
        raise NotImplementedError


class CMIP7DataRequest(DataRequest):
    GIT_URL = "https://github.com/CMIP-Data-Request/CMIP7_DReq_Software/"
    """str: The URL of the CMIP7 data request repository."""

    def __init__(
        self,
        tables: Dict[str, DataRequestTable],
        variables: Dict[str, CMIP7DataRequestVariable] = None,
    ):
        self.tables = tables
        self.variables = variables

    @classmethod
    def from_json_file(cls, jfile: str) -> "CMIP7DataRequest":
        """Creates a CMIP7DataRequest instance from a single JSON file"""
        # At the moment, we assume that this file is the "all_vars_info" file
        with open(jfile, "r") as f:
            data = json.load(f)
        return cls.from_all_var_info(data)

    @classmethod
    def from_vendored_json(cls):
        _all_var_info = files("pycmor.data.cmip7").joinpath("all_var_info.json")
        all_var_info = json.load(open(_all_var_info, "r"))
        return cls.from_all_var_info(all_var_info)

    @classmethod
    def from_all_var_info(cls, data):
        tables = {}
        variables = {}
        table_ids = set(k.split(".")[0] for k in data["Compound Name"].keys())
        for table_id in table_ids:
            table = CMIP7DataRequestTable.from_all_var_info(table_id, data)
            tables[table_id] = table
            for variable in table.variables:
                variable.table_header = table.header
                variables[variable.variable_id] = variable
        return cls(tables, variables)

    @classmethod
    def from_tables(cls, tables: Dict[str, DataRequestTable]) -> "CMIP7DataRequest":
        for table in tables.values():
            if not isinstance(table, DataRequestTable):
                raise ValueError("All tables must be instances of DataRequestTable.")
        return cls(tables)

    @classmethod
    def from_directory(cls, directory: str) -> "CMIP7DataRequest":
        """Creates the CMIP7 data request from a directory"""
        directory = pathlib.Path(directory)
        for file in directory.iterdir():
            # We assume that the directory contains only 1 JSON file, the "all_vars_info" file
            if file.is_file() and file.suffix == ".json":
                return cls.from_json_file(file)

    @classmethod
    @deprecation.deprecated(details="Use from_directory instead.")
    def from_tables_dir(cls, directory: str) -> "CMIP7DataRequest":
        return cls.from_directory(directory)


class CMIP6DataRequest(DataRequest):

    GIT_URL = "https://github.com/PCMDI/cmip6-cmor-tables/"
    """str: The URL of the CMIP6 data request repository."""

    _IGNORE_TABLE_FILES = [
        "CMIP6_CV_test.json",
        "CMIP6_coordinate.json",
        "CMIP6_CV.json",
        "CMIP6_formula_terms.json",
        "CMIP6_grids.json",
        "CMIP6_input_example.json",
    ]
    """List[str]: Table files to ignore when reading from a directory."""

    def __init__(
        self,
        tables: Dict[str, CMIP6DataRequestTable],
        flattable_variables: bool = True,
        include_table_headers_in_variables: bool = True,
    ):
        """
        Create a CMIP6DataRequest instance.

        Parameters
        ----------
        tables : Dict[str, DataRequestTable]
            A dictionary of tables.
        flattable_variables: bool, optional
            Whether or not to "flatten" tables by key, generating a unique key for each
            variable. This is composed of the table_id and variable_id. Default is True.
        include_table_headers_in_variables: bool, optional
            Whether or not to include the table header in the variable object. Default is False.
        """
        self.tables = tables
        self.variables = {}
        for table in tables.values():
            for variable in table.variables:
                if flattable_variables:
                    var_key = f"{table.table_id}.{variable.variable_id}"
                else:
                    var_key = variable.variable_id
                if include_table_headers_in_variables:
                    variable.table_header = table.header
                self.variables[var_key] = variable

    @classmethod
    def from_tables(cls, tables: Dict[str, DataRequestTable]) -> "CMIP6DataRequest":
        for table in tables.values():
            if not isinstance(table, DataRequestTable):
                raise ValueError("All tables must be instances of DataRequestTable.")
        return cls(tables)

    @classmethod
    def from_directory(cls, directory: str) -> "CMIP6DataRequest":
        tables = {}
        directory = pathlib.Path(directory)
        for file in directory.iterdir():
            if file.is_file() and file.suffix == ".json":
                if file.name in cls._IGNORE_TABLE_FILES:
                    continue
                table = CMIP6DataRequestTable.from_json_file(file)
                tables[table.table_id] = table

        for table in tables.values():
            if table in CMIP6IgnoreTableFiles.values():
                tables.pop(table)  # Remove the table from the dictionary

        return cls(tables)

    @classmethod
    def from_git(cls, url: str = None, branch: str = "main") -> "CMIP6DataRequest":
        if url is None:
            url = cls.GIT_URL
        raw_url = f"{url}/{branch}/Tables".replace(
            "github.com", "raw.githubusercontent.com"
        )
        # Something for parsing the tables at the URL
        tables = list_files_in_directory(url, "Tables", branch=branch)
        # Something for downloading
        dir = download_json_tables_from_url(raw_url, tables)
        return cls.from_directory(dir)

    @classmethod
    @deprecation.deprecated(details="Use from_directory instead.")
    def from_tables_dir(cls, directory: str) -> "CMIP6DataRequest":
        return cls.from_directory(directory)

    @classmethod
    def from_variables(cls, variables: Dict[str, Dict[str, str]]) -> "CMIP6DataRequest":
        tables = {}
        instance = cls(tables)
        instance.variables = variables
        return instance


class CMIP6IgnoreTableFiles(Enum):
    """Table files to ignore when reading from a directory."""

    CV_TEST = "CMIP6_CV_test.json"
    COORDINATE = "CMIP6_coordinate.json"
    CV = "CMIP6_CV.json"
    FORMULA_TERMS = "CMIP6_formula_terms.json"
    GRIDS = "CMIP6_grids.json"
    INPUT_EXAMPLE = "CMIP6_input_example.json"

    @classmethod
    def values(cls):
        return [item.value for item in cls]
