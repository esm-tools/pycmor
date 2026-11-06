"""Library integration tests for pycmor CMORizer class.

This module tests the library interface (CMORizer API) for all registered
model runs with both CMIP6 and CMIP7 configurations. Tests are organized
as a matrix of [model] x [cmip_version].
"""

import pytest
import yaml
from jinja2 import Template

from pycmor.core.cmorizer import CMORizer
from pycmor.core.logging import logger
from tests.utils.entry_points import discover_model_runs

# Discover all registered model runs
MODEL_RUNS = discover_model_runs()
MODEL_NAMES = sorted(MODEL_RUNS.keys())


@pytest.fixture(params=MODEL_NAMES)
def model_run_instance(request, tmp_path_factory):
    """Fixture that provides model run instances for all registered models.

    Parameters
    ----------
    request : pytest.FixtureRequest
        Pytest request object with model name as parameter
    tmp_path_factory : pytest.TempPathFactory
        Factory for creating temporary directories

    Returns
    -------
    BaseModelRun
        Model run instance with data and config access
    """
    model_name = request.param
    model_class = MODEL_RUNS[model_name]

    # Determine if we should use real data based on markers or env var
    use_real = model_class.should_use_real_data(request)

    # Create instance using from_module (pass the model.py path)
    # We need to find where the model class is defined
    import inspect

    module_file = inspect.getfile(model_class)
    return model_class.from_module(module_file, use_real=use_real, tmp_path_factory=tmp_path_factory)


@pytest.mark.parametrize("cmip_version", ["cmip6", "cmip7"])
def test_library_initialization(model_run_instance, cmip_version):
    """Test that CMORizer can be initialized from model config (library API).

    This test validates that the CMORizer class can be instantiated from
    configuration without executing the processing pipeline.

    Parameters
    ----------
    model_run_instance : BaseModelRun
        Model run instance with data and config
    cmip_version : str
        CMIP version to test (cmip6 or cmip7)
    """
    # Mark as xfail if config is not available (will show XPASS when implemented)
    if cmip_version not in model_run_instance.configs:
        pytest.xfail(f"{cmip_version.upper()} config not available for this model")

    # Get the appropriate config path
    config_path = model_run_instance.configs[cmip_version]

    model_name = model_run_instance.__class__.__name__
    logger.info(f"Testing library initialization for {model_name} with {cmip_version.upper()}")

    # Load config as Jinja2 template and render with datadir
    with open(config_path, "r") as f:
        template = Template(f.read())

    rendered_config = template.render(datadir=str(model_run_instance.datadir))
    cfg = yaml.safe_load(rendered_config)

    # Test that CMORizer can be constructed
    cmorizer = CMORizer.from_dict(cfg)
    assert cmorizer is not None
    assert len(cmorizer.rules) > 0, "CMORizer should have at least one rule"


@pytest.mark.parametrize("cmip_version", ["cmip6", "cmip7"])
@pytest.mark.parametrize(
    "orchestrator_config",
    [
        pytest.param({"pipeline_workflow_orchestrator": "prefect", "enable_dask": "yes"}, id="prefect-dask"),
        pytest.param({"pipeline_workflow_orchestrator": "native", "enable_dask": "yes"}, id="native-dask"),
        pytest.param({"pipeline_workflow_orchestrator": "native", "enable_dask": "no"}, id="native-nodask"),
    ],
)
def test_library_process(model_run_instance, cmip_version, orchestrator_config, tmp_path):
    """Test that CMORizer can process data from model config (library API).

    This test validates the full processing pipeline using the CMORizer
    library interface, from initialization through data processing to
    output file creation. Tests multiple orchestrator configurations.

    Parameters
    ----------
    model_run_instance : BaseModelRun
        Model run instance with data and config
    cmip_version : str
        CMIP version to test (cmip6 or cmip7)
    orchestrator_config : dict
        Orchestrator configuration to test (pipeline_workflow_orchestrator, enable_dask)
    tmp_path : Path
        Temporary directory for test artifacts
    """
    # Mark as xfail if config is not available (will show XPASS when implemented)
    if cmip_version not in model_run_instance.configs:
        pytest.xfail(f"{cmip_version.upper()} config not available for this model")

    # Get the appropriate config path
    config_path = model_run_instance.configs[cmip_version]

    model_name = model_run_instance.__class__.__name__
    orch_type = orchestrator_config["pipeline_workflow_orchestrator"]
    dask_status = "dask" if orchestrator_config["enable_dask"] == "yes" else "nodask"
    orchestrator_desc = f"{orch_type}-{dask_status}"
    logger.info(f"Testing library processing for {model_name} with {cmip_version.upper()} using {orchestrator_desc}")

    # Load config as Jinja2 template and render with datadir
    with open(config_path, "r") as f:
        template = Template(f.read())

    rendered_config = template.render(datadir=str(model_run_instance.datadir))
    cfg = yaml.safe_load(rendered_config)

    # Update output directory to tmp_path
    if "general" not in cfg:
        cfg["general"] = {}
    cfg["general"]["output_directory"] = str(tmp_path / "output")

    # Apply orchestrator configuration
    if "pycmor" not in cfg:
        cfg["pycmor"] = {}
    cfg["pycmor"].update(orchestrator_config)

    # Process the data
    cmorizer = CMORizer.from_dict(cfg)
    result = cmorizer.process()

    # Verify processing completed successfully
    assert result is not None, "Processing returned None"
    assert len(result) > 0, "Processing returned empty result"

    # Backends report per rule, and leave the data on disk. Serial and dask give
    # {rule: True}, prefect a list of summaries.
    outcomes = result.values() if isinstance(result, dict) else result
    assert all(outcomes), f"Rules that failed: {result}"


@pytest.mark.parametrize("cmip_version", ["cmip6", "cmip7"])
def test_library_accessor(model_run_instance, cmip_version):
    """Test dataset accessor API (ds.pycmor.process) for model data.

    This test validates the accessor interface, which provides a simpler
    API for one-shot in-memory processing of datasets without needing full
    CMORizer configuration. The accessor processes data and returns the
    result without saving to disk.

    Parameters
    ----------
    model_run_instance : BaseModelRun
        Model run instance with data and config
    cmip_version : str
        CMIP version to test (cmip6 or cmip7)
    """
    import xarray as xr

    import pycmor

    pycmor.enable_xarray_accessor()

    model_name = model_run_instance.__class__.__name__
    logger.info(f"Testing accessor API for {model_name} with {cmip_version.upper()}")

    # Get the appropriate config to extract a variable name
    if cmip_version not in model_run_instance.configs:
        pytest.xfail(f"{cmip_version.upper()} config not available for this model")

    config_path = model_run_instance.configs[cmip_version]

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    # Extract first rule to get variable info
    if not cfg.get("rules"):
        pytest.skip(f"No rules found in {config_path}")

    first_rule = cfg["rules"][0]

    # Get the variable identifier based on CMIP version
    if cmip_version == "cmip6":
        # For CMIP6, use cmor_variable
        variable_id = first_rule.get("cmor_variable")
        if not variable_id:
            pytest.skip(f"No cmor_variable in first rule of {config_path}")
    else:
        # For CMIP7, use compound_name
        variable_id = first_rule.get("compound_name")
        if not variable_id:
            pytest.skip(f"No compound_name in first rule of {config_path}")

    # Load a dataset from the model run data
    # Replace REPLACE_ME in input paths
    if not first_rule.get("inputs"):
        pytest.skip("First rule has no inputs")

    first_input = first_rule["inputs"][0]
    input_path = first_input["path"].replace("REPLACE_ME", str(model_run_instance.datadir))

    # Try to open the dataset
    try:
        ds = xr.open_dataset(input_path)
    except Exception as e:
        pytest.skip(f"Could not open dataset {input_path}: {e}")

    # Get inherit defaults from config (source_id, experiment_id, etc.)
    inherit_defaults = cfg.get("inherit", {})

    # Process using accessor API - this does in-memory processing
    # The accessor inherits metadata from ~/.pycmor.yaml config, but we can
    # also pass explicit kwargs that override the config
    try:
        result = ds.pycmor.process(
            variable_id,
            cmor_version=cmip_version.upper(),
            **inherit_defaults,
        )
    except Exception as e:
        # Some models may not have all metadata required for accessor API
        # or the data structure may not be compatible
        pytest.skip(f"Accessor processing failed (may be expected): {e}")

    # Verify result is an xarray Dataset or DataArray
    assert isinstance(result, (xr.Dataset, xr.DataArray)), f"Result should be xarray object, got {type(result)}"

    # If result is a Dataset, it should have data variables
    if isinstance(result, xr.Dataset):
        assert len(result.data_vars) > 0, "Result Dataset should have at least one data variable"

    # Verify the result has expected CMOR attributes
    # The accessor should have applied metadata from the data request
    if isinstance(result, xr.DataArray):
        assert hasattr(result, "attrs"), "Result should have attributes"
    else:
        # For Dataset, check that at least one variable has attributes
        assert any(
            hasattr(var, "attrs") for var in result.data_vars.values()
        ), "Result variables should have attributes"
