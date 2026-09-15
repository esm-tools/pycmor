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

        # Fallback to CMIP6-compatible list, extended with CMIP7 branded-variable globals
        return [
            "Conventions",
            "activity_id",
            "area_label",
            "branch_time_in_child",
            "branch_time_in_parent",
            "branded_variable",
            "branding_suffix",
            "creation_date",
            "data_specs_version",
            "drs_specs",
            "experiment",
            "experiment_id",
            "forcing_index",
            "frequency",
            "grid_label",
            "history",
            "horizontal_label",
            "initialization_index",
            "institution",
            "institution_id",
            "license",
            "license_id",
            "mip_era",
            "nominal_resolution",
            "parent_activity_id",
            "parent_experiment_id",
            "parent_mip_era",
            "parent_source_id",
            "parent_time_units",
            "parent_variant_label",
            "physics_index",
            "product",
            "realization_index",
            "realm",
            "region",
            "source_id",
            "temporal_label",
            "title",
            "tracking_id",
            "variable_id",
            "variant_label",
            "vertical_label",
        ]
        # CMIP7 Appendix 2 (Global_Attributes) eliminated the following
        # CMIP6 global attributes. They are "not forbidden" per the doc
        # but no longer part of the official CMIP7 standard; we drop them
        # rather than emit spec-stale metadata:
        #   branch_method, comment, contact, further_info_url, grid,
        #   source_type, sub_experiment, sub_experiment_id, table_id
        # Also dropped: ``source`` — required in CMIP6, now optional in
        # CMIP7. Our previous value was ``"<source_id> <realm>"`` which
        # duplicated source_id without adding information. Users who want
        # a CMIP6-style full-component description can override in the
        # recipe.
        # sub_experiment_id is still used internally by subdir_path() to
        # extend the DRS member_id when non-"none"; the removal here only
        # affects the on-disk attribute set, not the DRS path.

    #: Attributes the CMIP7 guidance marks "conditionally required", with the
    #: condition "Parent experiment exists". Written only when the CV gives
    #: this experiment a parent; omitted entirely otherwise, since an empty
    #: or zero value would assert a branch that never happened.
    _PARENT_CONDITIONAL_ATTRS = (
        "branch_time_in_child",
        "branch_time_in_parent",
        "parent_activity_id",
        "parent_experiment_id",
        "parent_mip_era",
        "parent_source_id",
        "parent_time_units",
        "parent_variant_label",
    )

    def global_attributes(self) -> dict:
        """Generate all required global attributes for CMIP7"""
        from ..core.logging import logger

        d = {}
        has_parent = self.has_parent_experiment()
        for key in self.required_global_attributes:
            func = getattr(self, f"get_{key}")
            value = func()
            if key in self._PARENT_CONDITIONAL_ATTRS:
                if not has_parent:
                    continue
                # A parent exists but this attribute was not supplied.
                # parent_time_units and branch_time_in_parent cannot be
                # derived, so warn rather than write a placeholder.
                if value is None or value == "":
                    logger.warning(
                        f"global_attributes: {key!r} is required because "
                        f"{self.rule_dict.get('experiment_id')!r} has parent "
                        f"{self.get_parent_experiment_id()!r}, but no value was "
                        f"supplied; set it in the recipe."
                    )
                    continue
            d[key] = value
        return d

    def subdir_path(self) -> str:
        """
        Generate CMIP7 directory structure path (13 components, per WCRP DRS).

        <drs_specs>/<mip_era>/<activity>/<institution>/<source>/<experiment>/
        <variant_label>/<region>/<frequency>/<variable>/<branding_suffix>/
        <grid_label>/<directory_date>
        """
        drs_specs = self.get_drs_specs()
        mip_era = self.get_mip_era()
        activity_id = self.get_activity_id()
        institution_id = self.get_institution_id()
        source_id = self.get_source_id()
        experiment_id = self.get_experiment_id()
        member_id = self.get_variant_label()
        sub_experiment_id = self.get_sub_experiment_id()
        if sub_experiment_id != "none":
            member_id = f"{member_id}-{sub_experiment_id}"
        region = self.get_region() or "glb"
        frequency = self.get_frequency()
        variable_id = self.get_variable_id()
        branding_suffix = self.get_branding_suffix() or "unknown"
        grid_label = self.get_grid_label()
        directory_date = f"v{datetime.datetime.today().strftime('%Y%m%d')}"
        return (
            f"{drs_specs}/{mip_era}/{activity_id}/{institution_id}/{source_id}/"
            f"{experiment_id}/{member_id}/{region}/{frequency}/{variable_id}/"
            f"{branding_suffix}/{grid_label}/{directory_date}"
        )

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

        # An explicit rule-level value wins over the registry.
        user = self.rule_dict.get("experiment")
        if user:
            return user

        # ``self.cv`` is not populated with the CMIP7 experiment collection in
        # normal runs, so the branch above rarely fires and we used to fall
        # back to the bare experiment_id ("piControl"). wcrp ATTR007b compares
        # this attribute against the registry's description and flagged every
        # file: 536 MEDIUM findings on a single cli108 year. Resolve it from
        # esgvoc the same way the parent attributes do.
        term = self._cv_experiment_term()
        description = getattr(term, "description", None) if term else None
        if description:
            return description

        return experiment_id

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

        For CMIP7: table_id is not a core concept. We derive it from compound name
        or return None. The cmip6_table field is only used for backward compatibility.

        Priority:
        1. table_id from rule configuration (user override)
        2. Derive from compound_name if available (CMIP7 standard)
        3. cmip6_table field from variable metadata (backward compatibility only)
        """
        from ..core.logging import logger

        # Priority 1: User-provided table_id
        table_id = self.rule_dict.get("table_id", None)
        if table_id:
            logger.debug(f"table_id from rule_dict: {table_id}")
            return table_id

        # Priority 2: Derive from compound_name (CMIP7 native approach)
        compound_name = self.rule_dict.get("compound_name", None)
        if compound_name:
            logger.debug(f"Attempting to derive table_id from compound_name: {compound_name}")
            parts = compound_name.split(".")
            logger.debug(f"compound_name split into {len(parts)} parts: {parts}")
            if len(parts) >= 5:
                component = parts[0]  # e.g., ocean, atmos
                frequency = parts[3]  # e.g., mon, day

                # Map component to realm letter
                realm_map = {
                    "atmos": "A",
                    "ocean": "O",
                    "ocn": "O",
                    "ocnBgchem": "O",
                    "seaIce": "SI",
                    "land": "L",
                    "landIce": "LI",
                }
                realm_letter = realm_map.get(component, component[0].upper())
                table_id = f"{realm_letter}{frequency}"
                logger.debug(f"Derived table_id: {table_id} (realm={realm_letter}, freq={frequency})")
                return table_id

        # Priority 3: Check for cmip6_table (backward compatibility only)
        if isinstance(self.drv, dict):
            table_id = self.drv.get("cmip6_table", None)
        else:
            table_id = getattr(self.drv, "cmip6_table", None)

        if table_id:
            logger.debug(f"table_id from variable metadata (cmip6_table - backward compat): {table_id}")
            return table_id

        # If still not found, try to derive from compound_name (works for both CMIP6 and CMIP7)
        if table_id is None:
            compound_name = self.rule_dict.get("compound_name", None)
            logger.debug(f"Attempting to derive table_id from compound_name: {compound_name}")
            if compound_name:
                # compound_name formats:
                # CMIP6-style: Table.variable (e.g., Amon.tas, Omon.thetao)
                # CMIP7-style: component.variable.cell_methods.frequency.grid
                parts = compound_name.split(".")
                logger.debug(f"compound_name split into {len(parts)} parts: {parts}")
                if len(parts) == 2:
                    # CMIP6-style compound name: table_id is the first part
                    table_id = parts[0]
                    logger.debug(f"Derived table_id from CMIP6-style compound_name: {table_id}")
                elif len(parts) >= 5:
                    component = parts[0]  # e.g., ocnBgchem
                    frequency = parts[3]  # e.g., mon

                    # Map component prefix to realm letter
                    realm_map = {
                        "atmos": "A",
                        "ocean": "O",
                        "ocn": "O",
                        "ocnBgchem": "O",
                        "seaIce": "SI",
                        "land": "L",
                        "landIce": "LI",
                    }

                    # Get realm letter (default to first letter if not in map)
                    realm_letter = realm_map.get(component, component[0].upper())

                    # Capitalize frequency and combine with realm
                    # mon -> Omon, day -> Oday, etc.
                    table_id = f"{realm_letter}{frequency}"
                    logger.debug(f"Derived table_id: {table_id} (realm={realm_letter}, freq={frequency})")
                else:
                    logger.warning(f"compound_name has {len(parts)} parts, expected at least 5")

        if table_id is None:
            logger.error(f"Could not determine table_id. rule_dict keys: {list(self.rule_dict.keys())}")
            raise ValueError("table_id not found in variable metadata or rule_dict")

        logger.debug(f"Final table_id: {table_id}")
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
        """Get CF Conventions version.

        CMIP7 Conventions CV lists CF versions only (``CF-1.11``, ``CF-1.12``,
        ``CF-1.13``); the ``CMIP-7.0`` suffix used by CMIP6 is not a CMIP7 term.
        """
        return self.rule_dict.get("Conventions", "CF-1.11")

    # ========================================================================
    # CMIP7 branded-variable attributes (parsed from compound_name)
    # compound_name format: <realm>.<variable>.<branding_suffix>.<frequency>.<region>
    # branding_suffix: <temporal>-<vertical>-<horizontal>-<area>
    # ========================================================================

    def _compound_parts(self):
        compound_name = self.rule_dict.get("compound_name")
        if not compound_name:
            return None
        parts = compound_name.split(".")
        if len(parts) < 5:
            return None
        return parts

    def _branding_tokens(self):
        parts = self._compound_parts()
        if parts is None:
            return None
        tokens = parts[2].split("-")
        if len(tokens) != 4:
            return None
        return tokens

    def get_branded_variable(self):
        # CMIP7 branded_variable CV format: <variable_id>_<branding_suffix>
        # (e.g. ``sidmassth_tavg-u-hxy-si``). Internal compound_name uses the
        # dotted 5-part form <realm>.<variable>.<branding_suffix>.<frequency>.<region>;
        # transform to the DRS form when emitting the global attribute.
        user = self.rule_dict.get("branded_variable")
        if user:
            return user
        parts = self._compound_parts()
        if parts is not None:
            return f"{parts[1]}_{parts[2]}"
        return self.rule_dict.get("compound_name")

    def get_branding_suffix(self):
        parts = self._compound_parts()
        return parts[2] if parts else None

    def get_temporal_label(self):
        tokens = self._branding_tokens()
        return tokens[0] if tokens else None

    def get_vertical_label(self):
        tokens = self._branding_tokens()
        return tokens[1] if tokens else None

    def get_horizontal_label(self):
        tokens = self._branding_tokens()
        return tokens[2] if tokens else None

    def get_area_label(self):
        tokens = self._branding_tokens()
        return tokens[3] if tokens else None

    def get_region(self):
        # CMIP7 region CV preserves case from the compound: simple codes
        # are lowercase (glb, nh, sh, ...) but latitude-band tokens stay
        # uppercase (30S-90S, 30N-90N, ...). The data-request corpus
        # carries the canonical case; lower-casing trips wcrp FILE001
        # and ATTR004 region CV checks for the uppercase tokens.
        parts = self._compound_parts()
        region = parts[4] if parts else self.rule_dict.get("region", "glb")
        return region

    def get_drs_specs(self):
        return self.rule_dict.get("drs_specs", "MIP-DRS7")

    def get_license_id(self):
        return self.rule_dict.get("license_id", "CC-BY-4.0")

    def _cv_experiment_term(self):
        """Return this experiment's CMIP7 CV term, or None.

        Cached on the instance; the CV lookup walks the whole experiment
        collection and several parent getters need the same term.
        """
        if hasattr(self, "_experiment_term_cache"):
            return self._experiment_term_cache
        term = None
        experiment_id = self.rule_dict.get("experiment_id")
        if experiment_id:
            try:
                from esgvoc.api.projects import get_all_terms_in_collection

                for t in get_all_terms_in_collection("cmip7", "experiment"):
                    if getattr(t, "drs_name", None) == experiment_id:
                        term = t
                        break
            except Exception:
                term = None
        self._experiment_term_cache = term
        return term

    def has_parent_experiment(self) -> bool:
        """True when the CMIP7 CV assigns this experiment a parent.

        The parent/branch global attributes are "conditionally required"
        per the CMIP7 Global Attributes guidance, with the condition given
        as "Parent experiment exists". The CV is what decides that: the
        ``experiment`` term for ``piControl`` carries ``parent_experiment:
        "picontrol-spinup"``, while ``piControl-spinup`` itself carries
        ``parent_experiment: null`` and so terminates the chain.
        """
        return bool(self.get_parent_experiment_id())

    def get_parent_activity_id(self):
        """Activity of the parent experiment, from the CV unless overridden."""
        user = self.rule_dict.get("parent_activity_id")
        if user:
            return user
        term = self._cv_experiment_term()
        parent_activity = getattr(term, "parent_activity", None) if term else None
        if parent_activity is not None:
            drs = getattr(parent_activity, "drs_name", None)
            if drs:
                return drs
            # CV stores the bare id ("cmip"); DRS form is upper-case.
            if isinstance(parent_activity, str) and parent_activity:
                return parent_activity.upper()
        return ""

    def get_parent_mip_era(self):
        """MIP era of the parent run.

        Defaults to this run's own ``mip_era``: a parent from an earlier
        era is possible but rare, so it has to be set explicitly.
        """
        return self.rule_dict.get("parent_mip_era") or self.get_mip_era()

    def get_parent_source_id(self):
        """Model that produced the parent run, normally the same model."""
        return self.rule_dict.get("parent_source_id") or self.get_source_id()

    def get_parent_variant_label(self):
        """Variant label of the parent run.

        Defaults to this run's own ``variant_label``. That is right for a
        straight continuation off a single unbranched parent; set it
        explicitly if the parent used a different variant.
        """
        return self.rule_dict.get("parent_variant_label") or self.get_variant_label()

    def get_parent_time_units(self):
        """Time units as recorded in the parent run.

        Cannot be derived: it is a property of the parent simulation's own
        time axis, which this run has no access to. Must be supplied in
        the recipe when the experiment has a parent, e.g.
        ``parent_time_units: "days since 1350-01-01"``.
        """
        return self.rule_dict.get("parent_time_units") or ""

    def get_branch_time_in_parent(self):
        """Branch time expressed in the parent's time units and time model.

        Cannot be derived; supply ``branch_time_in_parent`` in the recipe
        alongside ``parent_time_units``.
        """
        value = self.rule_dict.get("branch_time_in_parent")
        if value is None or value == "":
            return None
        return float(value)

    def get_branch_time_in_child(self):
        """Branch time expressed in this run's time units and time model.

        Defaults to 0.0, which is correct whenever the child begins at the
        branch point (the usual case, and true for piControl off its
        spinup). This is only meaningful because the pipeline now carries
        one time epoch for the whole dataset; see the note in
        ``std_lib.timeaverage.timeavg`` about the per-file epochs that
        previously made a bare 0.0 ambiguous.
        """
        value = self.rule_dict.get("branch_time_in_child", 0.0)
        if value is None or value == "":
            return None
        return float(value)

    def get_parent_experiment_id(self):
        # CMIP7 experiment CV assigns each experiment its parent; the CMIP6
        # sentinel "no parent" is not a valid CMIP7 value. If the user supplied
        # "no parent" (or left it unset), look up the parent from the CV.
        user = self.rule_dict.get("parent_experiment_id")
        if user and user.strip().lower() not in ("no parent", "none", ""):
            return user
        experiment_id = self.rule_dict.get("experiment_id")
        if experiment_id:
            try:
                from esgvoc.api.projects import get_all_terms_in_collection

                for term in get_all_terms_in_collection("cmip7", "experiment"):
                    if getattr(term, "drs_name", None) == experiment_id:
                        parent = getattr(term, "parent_experiment", None)
                        if parent is not None:
                            return getattr(parent, "drs_name", "") or ""
                        return ""
            except Exception:
                pass
        return user or ""

    def get_title(self):
        user = self.rule_dict.get("title")
        if user:
            return user
        return f"{self.get_source_id()} output prepared for CMIP7"

    def get_history(self):
        user = self.rule_dict.get("history")
        if user:
            return user
        return f"{self.rule_dict.get('creation_date','')}: pycmor CMIP7 rewrite"

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
        """Get data specifications version.

        Priority: user override → drv version → parse from CMIP7_DReq_metadata path
        (e.g. '/…/v1.2.2.2/metadata.json' → '1.2.2.2') → '1.0.0'.
        """
        user = self.rule_dict.get("data_specs_version")
        if user:
            return str(user)
        if isinstance(self.drv, dict):
            version = self.drv.get("dreq content version", None)
        else:
            version = getattr(self.drv, "version", None)
        if version:
            return str(version)
        dreq_path = self.rule_dict.get("CMIP7_DReq_metadata") or self.rule_dict.get("general", {}).get(
            "CMIP7_DReq_metadata"
        )
        if dreq_path:
            m = re.search(r"/v(\d+(?:\.\d+)+)/", str(dreq_path))
            if m:
                return f"MIP-DS7.{m.group(1)}"
        return "MIP-DS7.1.0.0"

    def get_creation_date(self):
        return self.rule_dict["creation_date"]

    def get_tracking_id(self):
        """Generate a unique tracking ID (prefix overridable via rule_dict).

        The CMIP7 tracking_id CV requires the ``hdl:21.14107/<uuid>`` prefix
        (21.14107, not the 21.14100 handle used in CMIP6).
        """
        prefix = self.rule_dict.get("tracking_id_prefix", "hdl:21.14107/")
        return prefix + str(uuid.uuid4())

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


def _sanitize_attr_value(v):
    """Replace non-ASCII dashes with ASCII hyphen in string attrs.

    HDF5 / older libhdf5 builds can mis-encode em-dash (U+2014, "—"),
    en-dash (U+2013, "–"), and figure-dash (U+2012) in attribute strings
    when the writer's locale disagrees with the reader's. Substitute
    them here so downstream tools reading with a stricter encoding
    (e.g. panoply, ncview on older platforms) don't trip.
    DKRZ review, 2026-06-30.
    """
    if not isinstance(v, str):
        return v
    return v.translate(str.maketrans({"—": "-", "–": "-", "‒": "-"}))


def set_global_attributes(ds, rule):
    """Set global attributes for the dataset"""
    if isinstance(ds, xr.DataArray):
        ds = ds.to_dataset()
    global_attrs = rule.ga.global_attributes()
    # Filter out None values -- xarray accepts them in memory but
    # netCDF serialization rejects non-string/non-numeric attributes.
    # Also sanitize non-ASCII dashes in string values.
    global_attrs = {
        k: _sanitize_attr_value(v)
        for k, v in global_attrs.items() if v is not None
    }
    ds.attrs.update(global_attrs)
    return ds


def _collect_external_cell_measures(ds):
    """Return cell_measures variable names referenced but not present in ``ds``.

    Parses every data variable's ``cell_measures`` attribute (CF format
    ``"key1: name1 [key2: name2 ...]"``) and returns the set of referenced
    variable names that do not appear in ``ds`` itself. Typical ocean
    outputs reference ``areacello`` / ``volcello``; atmos reference
    ``areacella``; these are shipped as separate fx files.
    """
    names: set = set()
    for var in ds.data_vars:
        cm = ds[var].attrs.get("cell_measures")
        if not isinstance(cm, str):
            continue
        # "area: areacello volume: volcello" -> ["areacello", "volcello"]
        tokens = cm.replace(",", " ").split()
        for i, tok in enumerate(tokens):
            if tok.endswith(":"):
                continue
            if i > 0 and tokens[i - 1].endswith(":"):
                if tok not in ds.variables:
                    names.add(tok)
    return names
