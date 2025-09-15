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
    def global_attributes(self):
        raise NotImplementedError()

    def subdir_path(self):
        raise NotImplementedError()


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
            raise ValueError(
                f"`label` must be of the form 'r<int>i<int>p<int>f<int>', Got: {label}"
            )
        d = {name: int(val) for name, val in d.groupdict().items()}
        return d

    def get_variant_label(self):
        return self.rule_dict["variant_label"]

    def get_physics_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return components["physics_index"]

    def get_forcing_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return components["forcing_index"]

    def get_initialization_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return components["initialization_index"]

    def get_realization_index(self):
        variant_label = self.get_variant_label()
        components = self._variant_label_components(variant_label)
        return components["realization_index"]

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
                        f"Institution ID '{user_institution_id}' is not valid. "
                        f"Allowed values: {institution_ids}"
                    )
                return user_institution_id
            raise ValueError(
                f"Multiple institutions are not supported, got: {institution_ids}"
            )
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
        grid_description = cv_source_id["model_component"][model_component][
            "description"
        ]
        if grid_description == "none":
            # check if user has provided grid description
            user_grid_description = self.rule_dict.get(
                "description", self.rule_dict.get("grid", None)
            )
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
            user_nominal_resolution = self.rule_dict.get(
                "nominal_resolution", self.rule_dict.get("resolution", None)
            )
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
            license_text = license_text.format(
                institution_id, license_id, license_url, further_info_url
            )
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
                        f"Activity ID '{user_activity_id}' is not valid. "
                        f"Allowed values: {activity_ids}"
                    )
                return user_activity_id
            raise ValueError(
                f"Multiple activities are not supported, got: {activity_ids}"
            )
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
