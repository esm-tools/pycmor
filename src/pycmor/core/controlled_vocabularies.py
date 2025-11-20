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
from .resource_loader import CVLoader


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
    def load(cls, table_dir=None, version=None):
        """Load the controlled vocabularies from the CMIP6_CVs directory

        Uses ResourceLoader with priority:
        1. table_dir (if provided)
        2. XDG cache
        3. Remote git
        4. Packaged CMIP6_CVs submodule

        Parameters
        ----------
        table_dir : str or Path, optional
            User-specified CV_Dir path
        version : str, optional
            CV version tag (default: "6.2.58.64")

        Returns
        -------
        CMIP6ControlledVocabularies
            Loaded controlled vocabularies
        """
        loader = CVLoader(cmor_version="CMIP6", version=version, user_path=table_dir)
        cv_path = loader.load()

        if cv_path is None:
            raise FileNotFoundError(
                "Could not load CMIP6 controlled vocabularies from any source. "
                "Check that git submodules are initialized or internet connection is available."
            )

        return cls.from_directory(cv_path)

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
            print(f"{k} {v['start_year']}-{v['end_year']} parent:{', '.join(v['parent_experiment_id'])}")

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
    """Controlled vocabularies for CMIP7

    CMIP7 CVs are organized differently from CMIP6:
    - Each CV entry is a separate JSON file (e.g., experiment/picontrol.json)
    - Files are organized in subdirectories (experiment/, project/)
    - Uses JSON-LD format with @context and @type fields
    - Project-level CVs use list-based structures (e.g., frequency-list.json)
    """

    def __init__(self, cv_data: dict):
        """Create a new CMIP7ControlledVocabularies object

        Parameters
        ----------
        cv_data : dict
            Dictionary containing the controlled vocabularies organized by category
            (e.g., {'experiment': {...}, 'frequency': [...], ...})
        """
        super().__init__()
        self.update(cv_data)

    @classmethod
    def load(cls, table_dir=None, version=None):
        """Load the controlled vocabularies from the CMIP7_CVs directory

        Uses ResourceLoader with priority:
        1. table_dir (if provided)
        2. XDG cache
        3. Remote git
        4. Packaged CMIP7-CVs submodule

        Parameters
        ----------
        table_dir : str or Path, optional
            User-specified CV_Dir path
        version : str, optional
            Git branch/tag (default: "src-data")

        Returns
        -------
        CMIP7ControlledVocabularies
            A new CMIP7ControlledVocabularies object
        """
        loader = CVLoader(cmor_version="CMIP7", version=version, user_path=table_dir)
        cv_path = loader.load()

        if cv_path is None:
            raise FileNotFoundError(
                "Could not load CMIP7 controlled vocabularies from any source. "
                "Check that git submodules are initialized or internet connection is available."
            )

        return cls.from_directory(cv_path)

    @staticmethod
    def _get_vendored_cv_path():
        """Get the path to the vendored CMIP7-CVs submodule

        Returns
        -------
        Path
            Path to the CMIP7-CVs submodule directory
        """
        # Get the path to this file, then navigate to the repository root
        current_file = Path(__file__)
        # Assuming structure: repo_root/src/pycmor/core/controlled_vocabularies.py
        repo_root = current_file.parent.parent.parent.parent
        cv_path = repo_root / "CMIP7-CVs"

        if not cv_path.exists():
            raise FileNotFoundError(
                f"CMIP7-CVs submodule not found at {cv_path}. "
                "Please initialize the submodule with: "
                "git submodule update --init CMIP7-CVs"
            )

        return cv_path

    @classmethod
    def from_directory(cls, directory):
        """Create a new CMIP7ControlledVocabularies object from a directory

        Parameters
        ----------
        directory : str or Path
            Path to the directory containing CMIP7 CV subdirectories
            (experiment/, project/, etc.)

        Returns
        -------
        CMIP7ControlledVocabularies
            A new CMIP7ControlledVocabularies object
        """
        directory = Path(directory)
        cv_data = {}

        # Load experiment CVs (one file per experiment)
        experiment_dir = directory / "experiment"
        if experiment_dir.exists():
            cv_data["experiment"] = cls._load_individual_files(experiment_dir)

        # Load project-level CVs (list-based files)
        project_dir = directory / "project"
        if project_dir.exists():
            cv_data.update(cls._load_project_files(project_dir))

        return cls(cv_data)

    @staticmethod
    def _load_individual_files(directory):
        """Load individual JSON files from a directory into a dictionary

        Each file represents one CV entry (e.g., experiment/picontrol.json)

        Parameters
        ----------
        directory : Path
            Directory containing individual JSON files

        Returns
        -------
        dict
            Dictionary mapping entry IDs to their data
        """
        entries = {}
        json_files = directory.glob("*.json")

        for json_file in json_files:
            # Skip special files
            if (
                json_file.name.startswith("@")
                or json_file.name == "graph.jsonld"
                or json_file.name == "graph.min.jsonld"
            ):
                continue

            try:
                with open(json_file, "r") as f:
                    data = json.load(f)
                    # Use 'id' field as the key, or filename without extension as fallback
                    entry_id = data.get("id", json_file.stem)
                    entries[entry_id] = data
            except json.JSONDecodeError as e:
                raise ValueError(f"file {json_file}: {e.msg}")

        return entries

    @staticmethod
    def _load_project_files(directory):
        """Load project-level CV files (list-based structures)

        Project files like frequency-list.json contain arrays of values

        Parameters
        ----------
        directory : Path
            Directory containing project-level JSON files

        Returns
        -------
        dict
            Dictionary mapping CV types to their data
        """
        cv_data = {}
        json_files = directory.glob("*-list.json")

        for json_file in json_files:
            try:
                with open(json_file, "r") as f:
                    data = json.load(f)
                    # Extract the CV type from filename (e.g., "frequency-list" -> "frequency")
                    cv_type = json_file.stem.replace("-list", "")

                    # The actual data is usually in a field matching the cv_type
                    # e.g., frequency-list.json has a "frequency" field with the list
                    if cv_type in data:
                        cv_data[cv_type] = data[cv_type]
                    else:
                        # Fallback: store the entire data
                        cv_data[cv_type] = data
            except json.JSONDecodeError as e:
                raise ValueError(f"file {json_file}: {e.msg}")

        return cv_data

    @classmethod
    def load_from_git(cls, tag: str = None, branch: str = "src-data"):
        """Load the controlled vocabularies from the git repository

        Parameters
        ----------
        tag : str, optional
            The git tag to use. If None, uses the branch specified.
        branch : str, optional
            The branch to use. Default is "src-data" which contains the CMIP7 CVs.

        Returns
        -------
        CMIP7ControlledVocabularies
            A new CMIP7ControlledVocabularies object
        """
        # Use tag if provided, otherwise use branch
        if tag is not None:
            base_url = f"https://raw.githubusercontent.com/WCRP-CMIP/CMIP7-CVs/{tag}"
        else:
            base_url = f"https://raw.githubusercontent.com/WCRP-CMIP/CMIP7-CVs/{branch}"

        cv_data = {}

        # Load experiments (sample key experiments)
        experiment_files = [
            "picontrol.json",
            "historical.json",
            "1pctco2.json",
            "abrupt-4xco2.json",
            "amip.json",
        ]

        experiments = {}
        for fname in experiment_files:
            url = f"{base_url}/experiment/{fname}"
            try:
                r = requests.get(url)
                r.raise_for_status()
                data = r.json()
                entry_id = data.get("id", fname.replace(".json", ""))
                experiments[entry_id] = data
            except requests.RequestException:
                # Skip files that don't exist
                continue

        if experiments:
            cv_data["experiment"] = experiments

        # Load project-level CVs
        project_files = [
            "frequency-list.json",
            "license-list.json",
            "mip-era-list.json",
            "product-list.json",
            "tables-list.json",
        ]

        for fname in project_files:
            url = f"{base_url}/project/{fname}"
            try:
                r = requests.get(url)
                r.raise_for_status()
                data = r.json()
                cv_type = fname.replace("-list.json", "")

                # Extract the actual list from the data
                if cv_type in data:
                    cv_data[cv_type] = data[cv_type]
                else:
                    cv_data[cv_type] = data
            except requests.RequestException:
                continue

        return cls(cv_data)

    def print_experiment_ids(self):
        """Print experiment ids with start and end years and parent experiment ids"""
        if "experiment" not in self:
            print("No experiment data available")
            return

        for exp_id, exp_data in self["experiment"].items():
            start = exp_data.get("start", exp_data.get("start-year", "N/A"))
            end = exp_data.get("end", exp_data.get("end-year", "N/A"))
            parent = exp_data.get("parent-experiment", exp_data.get("parent_experiment_id", []))

            # Handle parent experiment format
            if isinstance(parent, list):
                parent_str = ", ".join(parent)
            else:
                parent_str = str(parent)

            print(f"{exp_id} {start}-{end} parent:{parent_str}")
