"""
Controlled vocabularies for CMIP6
"""

import glob
import json
import os
import re
from pathlib import Path

import requests

from .factory import MetaFactory


class ControlledVocabularies(dict, metaclass=MetaFactory):
    @classmethod
    def from_directory(cls, directory: str) -> "ControlledVocabularies":
        """Create ControlledVocabularies from a directory of CV files"""
        raise NotImplementedError

    @classmethod
    def load_from_git(cls, tag: str) -> "ControlledVocabularies":
        """Load the ControlledVocabularies from the git repository"""
        raise NotImplementedError

    @classmethod
    def load(cls, table_dir: str) -> "ControlledVocabularies":
        """Load the ControlledVocabularies using the default method"""
        raise NotImplementedError


class CMIP6ControlledVocabularies(ControlledVocabularies):
    """Controlled vocabularies for CMIP6"""

    def __init__(self, json_files):
        """Create a new ControlledVocabularies object from a list of json files

        Parameters
        ----------
        json_files : list
            List of json files to load

        Returns
        -------
        ControlledVocabularies
            A new ControlledVocabularies object, behaves like a dictionary.
        """
        super().__init__()
        for f in json_files:
            d = self.dict_from_json_file(f)
            self.update(d)

    @classmethod
    def load(cls, table_dir=None):
        """Load the controlled vocabularies from the CMIP6_CVs directory"""
        cv_dir = Path(table_dir)
        return cls.from_directory(cv_dir)

    @classmethod
    def from_directory(cls, directory):
        """Create a new ControlledVocabularies object from a directory of json files

        Parameters
        ----------
        directory : str
            Path to the directory containing the json files
        """
        json_files = glob.glob(os.path.join(directory, "*.json"))
        return cls(json_files)

    def print_experiment_ids(self):
        """Print experiment ids with start and end years and parent experiment ids"""
        for k, v in self["experiment_id"].items():
            print(
                f"{k} {v['start_year']}-{v['end_year']} parent:{', '.join(v['parent_experiment_id'])}"
            )

    @staticmethod
    def dict_from_json_file(path):
        """Load a json file into a dictionary object

        Parameters
        ----------
        path : str
            Path to the json file to load

        Raises
        ------
        ValueError
            If the file cannot be loaded
        """
        try:
            with open(path, "r") as file:
                return json.load(file)
        except json.JSONDecodeError as e:
            raise ValueError(f"file {path}: {e.msg}")

    @classmethod
    def load_from_git(cls, tag: str = "6.2.58.64"):
        """Load the controlled vocabularies from the git repository

        Parameters
        ----------
        tag : str
            The git tag to use. Default is 6.2.58.64
            If tag is None, the main branch is used.
        Returns
        -------
        ControlledVocabularies
            A new ControlledVocabularies object, behaves like a dictionary.
        """
        if tag is None:
            tag = "refs/heads/main"
        else:
            tag = "refs/tags/" + tag
        url = f"https://raw.githubusercontent.com/WCRP-CMIP/CMIP6_CVs/{tag}"
        filenames = (
            "CMIP6_DRS.json",
            "CMIP6_activity_id.json",
            "CMIP6_experiment_id.json",
            "CMIP6_frequency.json",
            "CMIP6_grid_label.json",
            "CMIP6_institution_id.json",
            "CMIP6_license.json",
            "CMIP6_nominal_resolution.json",
            "CMIP6_realm.json",
            "CMIP6_required_global_attributes.json",
            "CMIP6_source_id.json",
            "CMIP6_source_type.json",
            "CMIP6_sub_experiment_id.json",
            "CMIP6_table_id.json",
            "mip_era.json",
        )
        name_pattern = re.compile(r"^(?:CMIP6_)?(?P<name>[^\.]+)\.json$").match
        data = {}
        for fname in filenames:
            name = name_pattern(fname).groupdict().get("name")
            fpath = "/".join([url, fname])
            r = requests.get(fpath)
            r.raise_for_status()
            content = r.content.decode()
            content = json.loads(content)
            data[name] = content.get(name)
        obj = cls([])
        obj.update(data)
        return obj


class CMIP7ControlledVocabularies(ControlledVocabularies):
    pass
