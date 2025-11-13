import datetime
import re
import uuid
from abc import abstractmethod

import xarray as xr

from ..core.factory import MetaFactory


class GlobalAttributes(metaclass=MetaFactory):
    @abstractmethod
    def global_attributes(self):
        raise NotImplementedError()

    @abstractmethod
    def subdir_path(self):
        raise NotImplementedError()


class CMIP7GlobalAttributes(GlobalAttributes):
    """
    Global attributes handler for CMIP7.

    CMIP7 uses a different structure than CMIP6:
    - Variable metadata from CMIP7 Data Request API
    - Controlled vocabularies from CMIP7-CVs repository
    - Some CVs (source_id, institution_id) not yet available in CMIP7

    Parameters
    ----------
    drv : CMIP7DataRequestVariable or dict
        Variable metadata from CMIP7 data request
    cv : CMIP7ControlledVocabularies
        CMIP7 controlled vocabularies
    rule_dict : dict
        User-provided configuration including:
        - source_id: Model identifier
        - institution_id: Institution identifier
        - experiment_id: Experiment identifier
        - variant_label: Ensemble member (e.g., 'r1i1p1f1')
        - grid_label: Grid identifier
        - creation_date: File creation timestamp
        - cmor_variable: Variable name
    """

    def __init__(self, drv, cv, rule_dict):
        self.drv = drv
        self.cv = cv
        self.rule_dict = rule_dict

    @property
    def required_global_attributes(self):
        """
        Return list of required global attributes.

        CMIP7 CV's required-global-attributes-list.json is currently empty,
        so we use the CMIP6 list as a baseline for compatibility.
        """
        # Check if CMIP7 CV has the list
        if "required_global_attributes" in self.cv and self.cv["required_global_attributes"]:
            return self.cv["required_global_attributes"]

        # Fallback to CMIP6-compatible list
        return [
            "Conventions",
            "activity_id",
            "creation_date",
            "data_specs_version",
            "experiment",
            "experiment_id",
            "forcing_index",
            "frequency",
            "further_info_url",
            "grid",
            "grid_label",
            "initialization_index",
            "institution",
            "institution_id",
            "license",
            "mip_era",
            "nominal_resolution",
            "physics_index",
            "product",
            "realization_index",
            "realm",
            "source",
            "source_id",
            "source_type",
            "sub_experiment",
            "sub_experiment_id",
            "table_id",
            "tracking_id",
            "variable_id",
            "variant_label",
        ]

    def global_attributes(self) -> dict:
        """Generate all required global attributes for CMIP7"""
        d = {}
        for key in self.required_global_attributes:
            func = getattr(self, f"get_{key}")
            d[key] = func()
        return d

    def subdir_path(self) -> str:
        """
        Generate CMIP7 directory structure path.

        CMIP7 DRS is similar to CMIP6:
        <mip_era>/<activity_id>/<institution_id>/<source_id>/<experiment_id>/
        <member_id>/<table_id>/<variable_id>/<grid_label>/<version>
        """
        mip_era = self.get_mip_era()
        activity_id = self.get_activity_id()
        institution_id = self.get_institution_id()
        source_id = self.get_source_id()
        experiment_id = self.get_experiment_id()
        member_id = self.get_variant_label()
        sub_experiment_id = self.get_sub_experiment_id()
        if sub_experiment_id != "none":
            member_id = f"{member_id}-{sub_experiment_id}"
        table_id = self.get_table_id()
        variable_id = self.get_variable_id()
        grid_label = self.get_grid_label()
        version = f"v{datetime.datetime.today().strftime('%Y%m%d')}"
        directory_path = f"{mip_era}/{activity_id}/{institution_id}/{source_id}/{experiment_id}/{member_id}/{table_id}/{variable_id}/{grid_label}/{version}"  # noqa: E501
        return directory_path

    # ========================================================================
    # Variant label and component extraction
    # ========================================================================

    def _variant_label_components(self, label: str):
        """Parse variant label into components (r, i, p, f indices)"""
        pattern = re.compile(
            r"r(?P<realization_index>\d+)"
            r"i(?P<initialization_index>\d+)"
            r"p(?P<physics_index>\d+)"
            r"f(?P<forcing_index>\d+)"
            r"$"
        )
        d = pattern.match(label)
        if d is None:
            raise ValueError(f"`label` must be of the form 'r<int>i<int>p<int>f<int>', Got: {label}")
        d = {name: int(val) for name, val in d.groupdict().items()}
        return d

    def get_variant_label(self):
        return self.rule_dict["variant_label"]

    def get_physics_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return str(components["physics_index"])

    def get_forcing_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return str(components["forcing_index"])

    def get_initialization_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return str(components["initialization_index"])

    def get_realization_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return str(components["realization_index"])

    # ========================================================================
    # Source and institution attributes
    # ========================================================================

    def get_source_id(self):
        return self.rule_dict["source_id"]

    def get_source(self):
        """
        Get source description.

        CMIP7 doesn't yet have a source_id CV, so we use user-provided
        description or construct from available information.
        """
        # Check if user provided source description
        user_source = self.rule_dict.get("source", None)
        if user_source:
            return user_source

        # Fallback: construct from source_id and realm
        source_id = self.get_source_id()
        realm = self.get_realm()

        # Check if user provided release year
        release_year = self.rule_dict.get("release_year", None)
        if release_year:
            return f"{realm} ({release_year})"

        # Minimal fallback
        return f"{source_id} {realm}"

    def get_institution_id(self):
        return self.rule_dict["institution_id"]

    def get_institution(self):
        """
        Get institution name.

        CMIP7 doesn't yet have an institution_id CV, so we use
        user-provided institution name.
        """
        # Check if user provided institution name
        user_institution = self.rule_dict.get("institution", None)
        if user_institution:
            return user_institution

        # Fallback to institution_id
        return self.get_institution_id()

    # ========================================================================
    # Realm and grid attributes
    # ========================================================================

    def get_realm(self):
        """
        Get modeling realm.

        In CMIP7, this comes from variable metadata's 'modeling_realm' field.
        """
        # Check if drv is a dict or object
        if isinstance(self.drv, dict):
            realm = self.drv.get("modeling_realm", None)
        else:
            realm = getattr(self.drv, "modeling_realm", None)

        if realm is None:
            # Fallback to user-provided value
            realm = self.rule_dict.get("realm", self.rule_dict.get("model_component", None))

        if realm is None:
            raise ValueError("Realm/modeling_realm not found in variable metadata or rule_dict")

        return realm

    def get_grid_label(self):
        return self.rule_dict["grid_label"]

    def get_grid(self):
        """
        Get grid description.

        CMIP7 doesn't yet have source_id CV with grid info,
        so we use user-provided grid description.
        """
        user_grid = self.rule_dict.get("grid", self.rule_dict.get("description", None))
        if user_grid:
            return user_grid

        # Minimal fallback
        return "none"

    def get_nominal_resolution(self):
        """
        Get nominal resolution.

        CMIP7 doesn't yet have source_id CV with resolution info,
        so we use user-provided nominal resolution.
        """
        user_resolution = self.rule_dict.get("nominal_resolution", self.rule_dict.get("resolution", None))
        if user_resolution:
            return user_resolution

        # Minimal fallback
        return "none"

    # ========================================================================
    # License attribute
    # ========================================================================

    def get_license(self):
        """
        Get license text.

        CMIP7 license structure is different from CMIP6.
        Uses license-list.json from project CVs.
        """
        # Check if CMIP7 license CV is available
        if "license" in self.cv and self.cv["license"]:
            licenses = self.cv["license"]
            # CMIP7 license is a list of license objects
            if isinstance(licenses, list) and len(licenses) > 0:
                # Check if user provided custom license text
                user_license = self.rule_dict.get("license", None)
                if user_license:
                    return user_license

                # Construct license text
                institution_id = self.get_institution_id()
                license_text = (
                    f"CMIP7 model data produced by {institution_id} is licensed under "
                    f"a Creative Commons Attribution 4.0 International License "
                    f"(https://creativecommons.org/licenses/by/4.0/). "
                    f"Consult https://pcmdi.llnl.gov/CMIP7/TermsOfUse for terms of use "
                    f"governing CMIP7 output, including citation requirements and proper "
                    f"acknowledgment. The data producers and data providers make no warranty, "
                    f"either express or implied, including, but not limited to, warranties of "
                    f"merchantability and fitness for a particular purpose. All liabilities "
                    f"arising from the supply of the information (including any liability "
                    f"arising in negligence) are excluded to the fullest extent permitted by law."
                )
                return license_text

        # Fallback: use user-provided license or default
        user_license = self.rule_dict.get("license", None)
        if user_license:
            return user_license

        # Default CMIP7 license
        institution_id = self.get_institution_id()
        return (
            f"CMIP7 model data produced by {institution_id} is licensed under "
            f"a Creative Commons Attribution 4.0 International License "
            f"(https://creativecommons.org/licenses/by/4.0/)."
        )

    # ========================================================================
    # Experiment attributes
    # ========================================================================

    def get_experiment_id(self):
        return self.rule_dict["experiment_id"]

    def get_experiment(self):
        """
        Get experiment description.

        In CMIP7, experiments are in individual JSON files.
        """
        experiment_id = self.get_experiment_id()

        # Check if CMIP7 experiment CV is available
        if "experiment" in self.cv and experiment_id in self.cv["experiment"]:
            exp_data = self.cv["experiment"][experiment_id]
            # CMIP7 uses 'description' field
            return exp_data.get("description", experiment_id)

        # Fallback to user-provided or experiment_id
        return self.rule_dict.get("experiment", experiment_id)

    def get_activity_id(self):
        """
        Get activity ID.

        In CMIP7, this comes from experiment CV's 'activity' field.
        """
        experiment_id = self.get_experiment_id()

        # Check if CMIP7 experiment CV is available
        if "experiment" in self.cv and experiment_id in self.cv["experiment"]:
            exp_data = self.cv["experiment"][experiment_id]
            activities = exp_data.get("activity", [])

            if len(activities) > 1:
                # Multiple activities - check if user specified one
                user_activity_id = self.rule_dict.get("activity_id", None)
                if user_activity_id:
                    if user_activity_id not in activities:
                        raise ValueError(
                            f"Activity ID '{user_activity_id}' is not valid. " f"Allowed values: {activities}"
                        )
                    return user_activity_id
                raise ValueError(f"Multiple activities are not supported, got: {activities}")

            if len(activities) == 1:
                return activities[0]

        # Fallback to user-provided
        user_activity_id = self.rule_dict.get("activity_id", None)
        if user_activity_id:
            return user_activity_id

        raise ValueError(f"Could not determine activity_id for experiment '{experiment_id}'")

    def get_sub_experiment_id(self):
        """
        Get sub-experiment ID.

        CMIP7 structure may differ from CMIP6 for sub-experiments.
        """
        experiment_id = self.get_experiment_id()

        # Check if CMIP7 experiment CV is available
        if "experiment" in self.cv and experiment_id in self.cv["experiment"]:
            exp_data = self.cv["experiment"][experiment_id]
            # CMIP7 may use different field name
            sub_exp = exp_data.get("sub-experiment", exp_data.get("sub_experiment_id", ["none"]))
            if isinstance(sub_exp, list):
                return " ".join(sub_exp)
            return str(sub_exp)

        # Fallback to user-provided or "none"
        return self.rule_dict.get("sub_experiment_id", "none")

    def get_sub_experiment(self):
        """Get sub-experiment description"""
        sub_experiment_id = self.get_sub_experiment_id()
        if sub_experiment_id == "none":
            return "none"
        else:
            # Return first sub-experiment if multiple
            return sub_experiment_id.split()[0]

    def get_source_type(self):
        """
        Get source type (required model components).

        In CMIP7, this comes from experiment CV's 'model-realms' field.
        """
        experiment_id = self.get_experiment_id()

        # Check if CMIP7 experiment CV is available
        if "experiment" in self.cv and experiment_id in self.cv["experiment"]:
            exp_data = self.cv["experiment"][experiment_id]
            model_realms = exp_data.get("model-realms", [])

            # Extract realm IDs from model-realms objects
            if isinstance(model_realms, list):
                realm_ids = []
                for realm in model_realms:
                    if isinstance(realm, dict):
                        realm_id = realm.get("id", "")
                        if realm_id:
                            realm_ids.append(realm_id)
                    else:
                        realm_ids.append(str(realm))

                if realm_ids:
                    return " ".join(realm_ids)

        # Fallback to user-provided
        user_source_type = self.rule_dict.get("source_type", None)
        if user_source_type:
            return user_source_type

        # Minimal fallback
        return "AOGCM"

    # ========================================================================
    # Table and variable attributes
    # ========================================================================

    def get_table_id(self):
        """
        Get table ID.

        In CMIP7, we use the cmip6_table field for backward compatibility.
        """
        # Check if drv is a dict or object
        if isinstance(self.drv, dict):
            table_id = self.drv.get("cmip6_table", None)
        else:
            table_id = getattr(self.drv, "cmip6_table", None)

        if table_id is None:
            # Fallback to user-provided
            table_id = self.rule_dict.get("table_id", None)

        if table_id is None:
            raise ValueError("table_id not found in variable metadata or rule_dict")

        return table_id

    def get_mip_era(self):
        """Get MIP era (CMIP7)"""
        # Check if CMIP7 CV has mip-era
        if "mip-era" in self.cv:
            mip_era_data = self.cv["mip-era"]
            if isinstance(mip_era_data, list) and len(mip_era_data) > 0:
                return mip_era_data[0]

        # Fallback to user-provided or default
        return self.rule_dict.get("mip_era", "CMIP7")

    def get_frequency(self):
        """Get output frequency from variable metadata"""
        # Check if drv is a dict or object
        if isinstance(self.drv, dict):
            frequency = self.drv.get("frequency", None)
        elif self.drv is not None:
            frequency = getattr(self.drv, "frequency", None)
        else:
            frequency = None

        # Fall back to rule_dict if not found in drv
        if frequency is None:
            frequency = self.rule_dict.get("frequency", None)

        if frequency is None:
            raise ValueError("frequency not found in variable metadata or rule")

        return frequency

    def get_Conventions(self):
        """Get CF Conventions version"""
        # CMIP7 uses CF-1.10 and CMIP-7.0
        return self.rule_dict.get("Conventions", "CF-1.10 CMIP-7.0")

    def get_product(self):
        """Get product type"""
        # Check if CMIP7 CV has product list
        if "product" in self.cv:
            product_data = self.cv["product"]
            if isinstance(product_data, list) and len(product_data) > 0:
                return product_data[0]

        # Fallback to user-provided or default
        return self.rule_dict.get("product", "model-output")

    def get_data_specs_version(self):
        """Get data specifications version"""
        # This could come from the CMIP7 data request version
        # Check if drv has version info
        if isinstance(self.drv, dict):
            version = self.drv.get("dreq content version", None)
        else:
            version = getattr(self.drv, "version", None)

        if version:
            return str(version)

        # Fallback to user-provided or default
        return self.rule_dict.get("data_specs_version", "1.0.0")

    def get_creation_date(self):
        return self.rule_dict["creation_date"]

    def get_tracking_id(self):
        """Generate a unique tracking ID"""
        return "hdl:21.14100/" + str(uuid.uuid4())

    def get_variable_id(self):
        return self.rule_dict["cmor_variable"]

    def get_further_info_url(self):
        """Construct further info URL"""
        mip_era = self.get_mip_era()
        institution_id = self.get_institution_id()
        source_id = self.get_source_id()
        experiment_id = self.get_experiment_id()
        sub_experiment_id = self.get_sub_experiment_id()
        variant_label = self.get_variant_label()

        # CMIP7 may use different URL structure
        # For now, use similar structure to CMIP6
        return (
            f"https://furtherinfo.es-doc.org/"
            f"{mip_era}.{institution_id}.{source_id}.{experiment_id}.{sub_experiment_id}.{variant_label}"
        )


class CMIP6GlobalAttributes(GlobalAttributes):
    def __init__(self, drv, cv, rule_dict):
        self.drv = drv
        self.cv = cv
        self.rule_dict = rule_dict

    @property
    def required_global_attributes(self):
        return self.cv["required_global_attributes"]

    def global_attributes(self) -> dict:
        d = {}
        for key in self.required_global_attributes:
            func = getattr(self, f"get_{key}")
            d[key] = func()
        return d

    def subdir_path(self) -> str:
        mip_era = self.get_mip_era()
        activity_id = self.get_activity_id()
        institution_id = self.get_institution_id()
        source_id = self.get_source_id()
        experiment_id = self.get_experiment_id()
        member_id = self.get_variant_label()
        sub_experiment_id = self.get_sub_experiment_id()
        if sub_experiment_id != "none":
            member_id = f"{member_id}-{sub_experiment_id}"
        table_id = self.get_table_id()
        variable_id = self.get_variable_id()
        grid_label = self.get_grid_label()
        version = f"v{datetime.datetime.today().strftime('%Y%m%d')}"
        directory_path = f"{mip_era}/{activity_id}/{institution_id}/{source_id}/{experiment_id}/{member_id}/{table_id}/{variable_id}/{grid_label}/{version}"  # noqa: E501
        return directory_path

    def _variant_label_components(self, label: str):
        pattern = re.compile(
            r"r(?P<realization_index>\d+)"
            r"i(?P<initialization_index>\d+)"
            r"p(?P<physics_index>\d+)"
            r"f(?P<forcing_index>\d+)"
            r"$"
        )
        d = pattern.match(label)
        if d is None:
            raise ValueError(f"`label` must be of the form 'r<int>i<int>p<int>f<int>', Got: {label}")
        d = {name: int(val) for name, val in d.groupdict().items()}
        return d

    def get_variant_label(self):
        return self.rule_dict["variant_label"]

    def get_physics_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return str(components["physics_index"])

    def get_forcing_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return str(components["forcing_index"])

    def get_initialization_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return str(components["initialization_index"])

    def get_realization_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return str(components["realization_index"])

    def get_source_id(self):
        return self.rule_dict["source_id"]

    def get_source(self):
        # TODO: extend this to include all model components
        model_component = self.get_realm()
        source_id = self.get_source_id()
        cv_source_id = self.cv["source_id"][source_id]
        release_year = cv_source_id["release_year"]
        # return f"{source_id} ({release_year})"
        return f"{model_component} ({release_year})"

    def get_institution_id(self):
        source_id = self.get_source_id()
        cv_source_id = self.cv["source_id"][source_id]
        institution_ids = cv_source_id["institution_id"]
        if len(institution_ids) > 1:
            user_institution_id = self.rule_dict.get("institution_id", None)
            if user_institution_id:
                if user_institution_id not in institution_ids:
                    raise ValueError(
                        f"Institution ID '{user_institution_id}' is not valid. " f"Allowed values: {institution_ids}"
                    )
                return user_institution_id
            raise ValueError(f"Multiple institutions are not supported, got: {institution_ids}")
        return institution_ids[0]

    def get_institution(self):
        institution_id = self.get_institution_id()
        return self.cv["institution_id"][institution_id]

    def get_realm(self):
        # `realm`` from table header turns out to be incorrect in some of the cases.
        # So instead read it from the user input to ensure the correct value
        #
        # return self.drv.table_header.realm
        model_component = self.rule_dict.get("model_component", None)
        if model_component is None:
            model_component = self.drv.model_component
            if len(model_component.split()) > 1:
                model_component = self.drv.table_header.realm
        return model_component

    def get_grid_label(self):
        return self.rule_dict["grid_label"]

    def get_grid(self):
        source_id = self.get_source_id()
        cv_source_id = self.cv["source_id"][source_id]
        model_component = self.get_realm()
        grid_description = cv_source_id["model_component"][model_component]["description"]
        if grid_description == "none":
            # check if user has provided grid description
            user_grid_description = self.rule_dict.get("description", self.rule_dict.get("grid", None))
            if user_grid_description:
                grid_description = user_grid_description
        return grid_description

    def get_nominal_resolution(self):
        source_id = self.get_source_id()
        cv_source_id = self.cv["source_id"][source_id]
        model_component = self.get_realm()
        cv_model_component = cv_source_id["model_component"][model_component]
        if "native_nominal_resolution" in cv_model_component:
            nominal_resolution = cv_model_component["native_nominal_resolution"]
        if "native_ominal_resolution" in cv_model_component:
            nominal_resolution = cv_model_component["native_ominal_resolution"]
        if nominal_resolution == "none":
            # check if user has provided nominal resolution
            user_nominal_resolution = self.rule_dict.get("nominal_resolution", self.rule_dict.get("resolution", None))
            if user_nominal_resolution:
                nominal_resolution = user_nominal_resolution
        return nominal_resolution

    def get_license(self):
        institution_id = self.get_institution_id()
        source_id = self.get_source_id()
        cv_source_id = self.cv["source_id"][source_id]
        license_id = cv_source_id["license_info"]["id"]
        license_url = self.cv["license"]["license_options"][license_id]["license_url"]
        license_id = self.cv["license"]["license_options"][license_id]["license_id"]
        license_text = self.cv["license"]["license"]
        # make placeholders in license text
        license_text = re.sub(r"<.*?>", "{}", license_text)
        further_info_url = self.rule_dict.get("further_info_url", None)
        if further_info_url is None:
            license_text = re.sub(r"\[.*?\]", "", license_text)
            license_text = license_text.format(institution_id, license_id, license_url)
        else:
            license_text = license_text.format(institution_id, license_id, license_url, further_info_url)
        return license_text

    def get_experiment_id(self):
        return self.rule_dict["experiment_id"]

    def get_experiment(self):
        experiment_id = self.get_experiment_id()
        return self.cv["experiment_id"][experiment_id]["experiment"]

    def get_activity_id(self):
        experiment_id = self.get_experiment_id()
        cv_experiment_id = self.cv["experiment_id"][experiment_id]
        activity_ids = cv_experiment_id["activity_id"]
        if len(activity_ids) > 1:
            user_activity_id = self.rule_dict.get("activity_id", None)
            if user_activity_id:
                if user_activity_id not in activity_ids:
                    raise ValueError(
                        f"Activity ID '{user_activity_id}' is not valid. " f"Allowed values: {activity_ids}"
                    )
                return user_activity_id
            raise ValueError(f"Multiple activities are not supported, got: {activity_ids}")
        return activity_ids[0]

    def get_sub_experiment_id(self):
        experiment_id = self.get_experiment_id()
        cv_experiment_id = self.cv["experiment_id"][experiment_id]
        sub_experiment_ids = cv_experiment_id["sub_experiment_id"]
        sub_experiment_id = " ".join(sub_experiment_ids)
        return sub_experiment_id

    def get_sub_experiment(self):
        sub_experiment_id = self.get_sub_experiment_id()
        if sub_experiment_id == "none":
            sub_experiment = "none"
        else:
            sub_experiment = sub_experiment_id.split()[0]
        return sub_experiment

    def get_source_type(self):
        experiment_id = self.get_experiment_id()
        cv_experiment_id = self.cv["experiment_id"][experiment_id]
        source_type = " ".join(cv_experiment_id["required_model_components"])
        return source_type

    def get_table_id(self):
        return self.drv.table_header.table_id

    def get_mip_era(self):
        return self.drv.table_header.mip_era

    def get_frequency(self):
        return self.drv.frequency

    def get_Conventions(self):
        header = self.drv.table_header
        return header.Conventions

    def get_product(self):
        header = self.drv.table_header
        return header.product

    def get_data_specs_version(self):
        header = self.drv.table_header
        return str(header.data_specs_version)

    def get_creation_date(self):
        return self.rule_dict["creation_date"]

    def get_tracking_id(self):
        return "hdl:21.14100/" + str(uuid.uuid4())

    def get_variable_id(self):
        return self.rule_dict["cmor_variable"]

    def get_further_info_url(self):
        mip_era = self.get_mip_era()
        institution_id = self.get_institution_id()
        source_id = self.get_source_id()
        experiment_id = self.get_experiment_id()
        sub_experiment_id = self.get_sub_experiment_id()
        variant_label = self.get_variant_label()
        return (
            f"https://furtherinfo.es-doc.org/"
            f"{mip_era}.{institution_id}.{source_id}.{experiment_id}.{sub_experiment_id}.{variant_label}"
        )


def set_global_attributes(ds, rule):
    """Set global attributes for the dataset"""
    if isinstance(ds, xr.DataArray):
        ds = ds.to_dataset()
    ds.attrs.update(rule.ga.global_attributes())
    return ds
