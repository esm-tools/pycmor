# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Structure

This is a **git worktree** setup with a bare repository structure:
- The bare repo is in `.bare/` with multiple worktrees for parallel development
- Each worktree represents a different branch (prep-release, PR branches, etc.)
- Source code layout (within any worktree):
  - `src/pycmor/` - Main package code
  - `tests/` - Test suite
  - `doc/` - Sphinx documentation
  - `examples/` - Example configurations

**Important**: When working with this repository:
- You may be in any worktree (prep-release, pr-226, pr-223, etc.)
- All worktrees share the same `.bare/` git repository
- Standard `src/` layout applies to all worktrees
- See `pycmor-parallel-merge-workflow.md` for worktree workflow details

## Project Overview

`pycmor` is a Python package that simplifies the standardization of climate model output into CMOR (Climate Model Output Rewriter) format for CMIP6/CMIP7 compliance. It provides a modular, extensible pipeline-based system for transforming Earth System Model output into standardized formats.

Key features:
- Workflow engine based on Prefect with Dask for distributed computing
- Plugin architecture for custom processing steps and CLI subcommands
- Support for FESOM ocean model output and unstructured grids
- YAML-based configuration for rules and processing pipelines

## Development Commands

### Installation
```bash
# From within any worktree
pip install -e ".[dev,fesom]"
```

### Testing
```bash
# Run all tests
pytest

# Run specific test suites
pytest tests/unit/
pytest tests/integration/
pytest tests/meta/

# Run with coverage
pytest --cov=src/pycmor --cov-report=html

# Run single test file
pytest tests/unit/test_pipeline.py

# Run with verbose output
pytest -vvv -s
```

### Code Quality
```bash
# Format code
black .
isort --profile black .

# Lint
flake8 . --exclude ./cmip6-cmor-tables,./build,_version.py,./src/pycmor/webapp.py,./CMIP7_DReq_Software

# YAML linting
yamllint .

# Run all checks (as CI does)
pre-commit run --all-files
```

### Documentation
```bash
cd doc
make html
# Output is in doc/_build/html/
```

### Working with Worktrees
```bash
# List all worktrees
git worktree list

# Check current branch
git branch --show-current

# Switch between worktrees (navigate to directory)
cd ../pycmor-prep-release  # or ../pycmor-pr226, etc.
```

### Running the CLI
```bash
# Process a configuration file
pycmor process <config_file.yaml>

# Validate configuration
pycmor validate config <config_file.yaml>

# Launch table explorer (Streamlit UI)
pycmor table-explorer
```

## Core Architecture

### Main Classes

1. **`CMORizer`** (`src/pycmor/core/cmorizer.py`)
   - Central orchestrator that manages rules, pipelines, and Dask cluster
   - Loads configuration from YAML and validates with Cerberus schemas
   - Entry point: `CMORizer.from_dict(config_dict)`
   - Key method: `process()` - executes all rules with their pipelines

2. **`Rule`** (`src/pycmor/core/rule.py`)
   - Represents one CMOR variable and how to produce it from model output
   - Contains: input patterns (regex), CMOR variable name, pipeline references
   - Additional attributes can be added and accessed during processing
   - All rules can inherit common attributes from `inherit` config section

3. **`Pipeline`** (`src/pycmor/core/pipeline.py`)
   - Sequence of processing steps (Python functions) applied to data
   - Steps are converted to Prefect tasks for workflow management
   - Can be defined inline (list of step qualnames) or via `uses` directive
   - Uses Dask for parallel execution
   - Call signature for steps: `def step(data: Any, rule: Rule) -> Any`

4. **Configuration** (`src/pycmor/core/config.py`)
   - Hierarchical config using Everett: defaults → user file → run config → env vars → CLI
   - User config locations (priority order):
     1. `${PYCMOR_CONFIG_FILE}`
     2. `${XDG_CONFIG_HOME}/pycmor.yaml`
     3. `${XDG_CONFIG_HOME}/pycmor/pycmor.yaml`
     4. `~/.pycmor.yaml`

### Configuration Structure

YAML config files have 5 main sections:

1. **`pycmor`**: CLI settings (logging, cluster type, parallelization)
2. **`general`**: Global info (data paths, CMOR tables, CV directories, experiment metadata)
3. **`pipelines`**: Pipeline definitions with steps or `uses` directives
4. **`rules`**: List of rules mapping model output to CMOR variables
5. **`inherit`**: Key-value pairs added to all rules (unless rule overrides)

### Processing Flow

1. `CMORizer.from_dict()` loads and validates configuration
2. Creates Dask cluster (local, SLURM, SSH tunnel)
3. For each rule:
   - Gathers input files matching patterns
   - Applies each pipeline step sequentially
   - Steps are Prefect tasks with caching enabled
   - Results saved according to CMOR conventions

### Standard Library (`src/pycmor/std_lib/`)

Pre-built processing steps for common operations:
- `gather_inputs.py`: Load data with xarray `open_mfdataset`
- `generic.py`: Get variables from datasets, basic operations
- `units.py`: Unit conversion with pint-xarray
- `timeaverage.py`: Temporal aggregation/resampling
- `setgrid.py`: Grid information and coordinates
- `global_attributes.py`: CMOR-compliant metadata
- `variable_attributes.py`: Variable-level metadata
- `files.py`: Saving datasets to NetCDF
- `bounds.py`: Coordinate bounds (including vertical bounds via `add_vertical_bounds`)

### Plugin System

Two plugin types:

1. **CLI Subcommands**: Entry point groups `pycmor.cli_subcommands` or `pymor.cli_subcommands`
2. **Pipeline Steps**: Any importable Python function following step protocol

### Dask Cluster Support

Cluster types (via `dask_cluster` in `pycmor` config):
- `local`: Single-machine distributed processing
- `slurm`: SLURM HPC cluster with `dask-jobqueue`
- `ssh_tunnel`: Remote cluster via SSH tunnel

Scaling modes:
- `fixed`: Fixed number of workers (`fixed_jobs`)
- `adaptive`: Auto-scaling between `minimum_jobs` and `maximum_jobs`

## Special Considerations

### Python Version Support
- Requires Python 3.9-3.12
- Uses `versioneer` for version management (don't edit `_version.py`)

### CI/CD
- GitHub Actions workflow: `.github/workflows/CI-test.yaml`
- Tests run on Python 3.9, 3.10, 3.11, 3.12
- Linting (black, isort, flake8, yamllint) must pass before tests
- Coverage uploaded to Codecov

### Pre-commit Hooks
- Configured in `.pre-commit-config.yaml`
- Excludes: `versioneer.py`, `_version.py`, `webapp.py`, `cmip7/` data files

### Testing
- Fixtures are modular via `conftest.py` and `tests/fixtures/`
- Test categories: unit, integration, meta (environment checks)
- Uses pytest with coverage, async support, mock, xdist

### Model-Specific Code
- FESOM 1.4 support: `src/pycmor/fesom_1p4/` (nodes to levels conversion)
- FESOM 2.1+ support: `src/pycmor/fesom_2p1/` (regridding with pyfesom2)

### Data Request Handling
- CMIP6 tables: Git submodule at `cmip6-cmor-tables/`
- CMIP7 data: JSON format in `src/pycmor/data/cmip7/`
- Classes: `DataRequest`, `DataRequestTable`, `DataRequestVariable`

## Code Style

- Line length: 120 characters
- Formatter: Black
- Import sorting: isort with Black profile
- Docstring style: ReStructuredText for Sphinx
- Type hints: Optional but encouraged

## Important Patterns

### Creating a Custom Step
```python
def my_step(data, rule):
    """Process data according to rule specifications."""
    # Access rule attributes
    cmor_var = rule.cmor_variable
    # Modify data
    data = data.sel(time=slice("2000", "2010"))
    return data
```

### Adding a Step to Config
```yaml
pipelines:
  - name: custom_pipeline
    steps:
      - "my_module.my_step"
      - "pycmor.std_lib.convert_units"
```

### Using DefaultPipeline
```yaml
pipelines:
  - name: standard
    uses: pycmor.pipeline.DefaultPipeline
```

## Documentation Location

- User docs: `doc/*.rst`
- API docs: Auto-generated from docstrings
- ReadTheDocs: https://pycmor.readthedocs.io/
- Parallel workflow guide: `pycmor-parallel-merge-workflow.md` (at repository root)

## GPG Signing

When committing, GPG key D763C0EA86718612 should be used if unlocked. Never use `--no-gpg-sign` in automated workflows.
