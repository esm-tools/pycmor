import re

from pycmor.core.controlled_vocabularies import ControlledVocabularies
from pycmor.core.factory import create_factory
from pycmor.std_lib.global_attributes import GlobalAttributes

# Name, expected pass
creation_date_format = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"  # noqa: W605
tracking_id_format = r"^hdl:\d{2}\.\d{5}/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$"  # noqa: W605
global_attributes = {
    "Conventions": "CF-1.7 CMIP-6.2",
    "activity_id": "CMIP",
    "data_specs_version": "1.0.27",
    "experiment": "pre-industrial control",
    "experiment_id": "piControl",
    "forcing_index": "1",
    "frequency": "day",
    "grid": "FESOM 1.4 (unstructured grid in the horizontal with 1306775 wet " "nodes; 46 levels; top grid cell 0-5 m)",
    "grid_label": "gn",
    "initialization_index": "1",
    "institution": "Alfred Wegener Institute, Helmholtz Centre for Polar and "
    "Marine Research, Am Handelshafen 12, 27570 Bremerhaven, "
    "Germany",
    "institution_id": "AWI",
    "license": "CMIP6 model data produced by AWI is licensed under a Creative "
    "Commons Attribution 4.0 International License "
    "(https://creativecommons.org/licenses/by/4.0/). Consult "
    "https://pcmdi.llnl.gov/CMIP6/TermsOfUse for terms of use "
    "governing CMIP6 output, including citation requirements and "
    "proper acknowledgment. Further information about this data, "
    "including some limitations, can be found via the further_info_url "
    "(recorded as a global attribute in this file). The data producers "
    "and data providers make no warranty, either express or implied, "
    "including, but not limited to, warranties of merchantability and "
    "fitness for a particular purpose. All liabilities arising from "
    "the supply of the information (including any liability arising in "
    "negligence) are excluded to the fullest extent permitted by law.",
    "mip_era": "CMIP6",
    "nominal_resolution": "25 km",
    "physics_index": "1",
    "product": "model-output",
    "realization_index": "1",
    # use `modeling_realm` from variable instead of `realm` in table header
    # "realm": "ocnBgchem",
    "realm": "ocean",
    "source": "ocean (2018)",
    "source_id": "AWI-CM-1-1-HR",
    "source_type": "AOGCM",
    "sub_experiment": "none",
    "sub_experiment_id": "none",
    "table_id": "Oday",
    "variable_id": "tos",
    "variant_label": "r1i1p1f1",
    "further_info_url": "https://furtherinfo.es-doc.org/CMIP6.AWI.AWI-CM-1-1-HR.piControl.none.r1i1p1f1",
}


def test_global_attributes(CV_dir, rule_after_cmip6_cmorizer_init):
    """Pseudo-integration test for the global attributes"""

    # Set the fixture as the rule
    rule = rule_after_cmip6_cmorizer_init

    ControlledVocabularies_factory = create_factory(ControlledVocabularies)
    ControlledVocabulariesClass = ControlledVocabularies_factory.get("CMIP6")
    cv = ControlledVocabulariesClass.load(CV_dir)
    # breakpoint()
    # Get the global attributes set on rule. Maybe move it somewhere else
    rule_attrs = rule.global_attributes_set_on_rule()
    GlobalAttributes_factory = create_factory(GlobalAttributes)
    GlobalAttributesClass = GlobalAttributes_factory.get("CMIP6")
    ga = GlobalAttributesClass(rule.data_request_variable, cv, rule_attrs)
    # Get the global attributes

    d = ga.global_attributes()

    # This is here only for the purpose of debugging
    for key, value in global_attributes.items():
        if key not in d:
            print(f"key: {key} missing in d")
        if d[key] != value:
            print(f"key: {key}, value: {value}     {d[key]}")

    # Remove GA that change on every run
    creation_date = d["creation_date"]
    del d["creation_date"]
    tracking_id = d["tracking_id"]
    del d["tracking_id"]

    # Assert that the formats of creation_date and tracking_id are correct
    assert bool(re.match(creation_date_format, creation_date))
    assert bool(re.match(tracking_id_format, tracking_id))

    # Assert that the rest of the global attributes are correct
    assert d == global_attributes
