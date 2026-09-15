import functools
import os
import secrets
import sys
from importlib import resources
from importlib.metadata import entry_points
from pathlib import Path
from typing import List

import rich_click as click
import yaml
from click import get_current_context as _cur_ctx
from click_loguru import ClickLoguru
from dask.distributed import Client
from loguru import logger as _loguru_logger
from rich.traceback import install as rich_traceback_install
from streamlit.web import cli as stcli

from . import _version
from .core import caching
from .core.cmorizer import CMORizer
from .core.filecache import fc
from .core.logging import add_report_logger, logger
from .core.ssh_tunnel import ssh_tunnel_cli
from .core.validate import GENERAL_VALIDATOR, PIPELINES_VALIDATOR, RULES_VALIDATOR
from .dev import utils as dev_utils
from .fesom_1p4.nodes_to_levels import convert
from .scripts.update_dimensionless_mappings import update_dimensionless_mappings


def _patch_click_loguru_unique_logfiles(cl_instance):
    """Replace ClickLoguru.init_logger with a race-free variant.

    The upstream implementation (click_loguru 1.3.7) picks a log filename by
    scanning ``logs/pycmor-process_N.log`` and choosing ``N+1``, then unlinks
    old files for retention. When multiple pycmor jobs share a working
    directory (typical on HPC/SLURM) this TOCTOU races: several processes pick
    the same N, and retention-unlink can hit a file another job just removed,
    raising ``FileNotFoundError``. Here we append ``<pid>_<token>`` to the log
    filename so every process gets a unique slot, and we skip the retention
    sweep (which is inherently racy with shared cwd).
    """

    def init_logger(log_dir_parent=None, logfile=True):
        def decorator(user_func):
            @functools.wraps(user_func)
            def wrapper(*args, **kwargs):
                state = _cur_ctx().find_object(cl_instance.LogState)
                if state.verbose:
                    log_level = "DEBUG"
                elif state.quiet:
                    log_level = "ERROR"
                else:
                    log_level = cl_instance._stderr_log_level
                _loguru_logger.remove()
                _loguru_logger.add(
                    sys.stderr, level=log_level, format=cl_instance.stderr_format_func
                )
                if logfile and state.logfile:
                    if log_dir_parent is not None:
                        cl_instance._log_dir_parent = log_dir_parent
                    if cl_instance._log_dir_parent is None:
                        log_dir_path = Path(".") / "logs"
                    else:
                        log_dir_path = Path(cl_instance._log_dir_parent)
                    subcommand = _cur_ctx().invoked_subcommand or state.subcommand
                    if subcommand is not None:
                        logfile_prefix = f"{cl_instance._name}-{subcommand}"
                    else:
                        logfile_prefix = f"{cl_instance._name}"
                    log_dir_path.mkdir(parents=True, exist_ok=True)
                    unique_tag = f"{os.getpid()}_{secrets.token_hex(4)}"
                    state.logfile_path = (
                        log_dir_path / f"{logfile_prefix}_{unique_tag}.log"
                    )
                    state.logfile_handler_id = _loguru_logger.add(
                        str(state.logfile_path), level=cl_instance._file_log_level
                    )
                _loguru_logger.debug(f'Command line: "{" ".join(sys.argv)}"')
                _loguru_logger.debug(f"{cl_instance._name} version {cl_instance._version}")
                return user_func(*args, **kwargs)

            return wrapper

        return decorator

    cl_instance.init_logger = init_logger

MAX_FRAMES = int(os.environ.get("PYCMOR_ERROR_MAX_FRAMES", os.environ.get("PYMOR_ERROR_MAX_FRAMES", 3)))
"""
str: The maximum number of frames to show in the traceback if there is an error. Default to 3
"""
# install rich traceback
rich_traceback_install(show_locals=True, max_frames=MAX_FRAMES)

VERSION = _version.get_versions()["version"]

# global constants
LOG_FILE_RETENTION = 3
NAME = "pycmor"
# define the CLI
click_loguru = ClickLoguru(
    NAME,
    VERSION,
    retention=LOG_FILE_RETENTION,
    # log_dir_parent="tests/data/logs",
    timer_log_level="info",
)
# Make log-file allocation race-free across concurrent pycmor invocations
# that share a working directory (e.g. multiple SLURM jobs in the same dir).
_patch_click_loguru_unique_logfiles(click_loguru)


# FIXME(PG): Doesn't work as intended :-(
def pymor_cli_group(func):
    """
    Decorator to add the click_loguru logging options to a click group
    """
    func = click_loguru.logging_options(func)
    func = click.group()(func)
    func = click_loguru.stash_subcommand()(func)
    func = click.version_option(version=VERSION, prog_name="PyCMOR - Makes CMOR Simple")(func)
    return func


def find_subcommands():
    """
    Finds CLI Subcommands for installed plugins in both legacy and new groups.
    """
    groups = ["pycmor.cli_subcommands", "pymor.cli_subcommands"]
    discovered_subcommands = {}
    for group in groups:
        try:
            # Python 3.10+ - use keyword argument
            eps = entry_points(group=group)
        except TypeError:
            # Python 3.9 - returns dict-like object
            eps = entry_points().get(group, [])
        for entry_point in eps:
            discovered_subcommands[entry_point.name] = {
                "plugin_name": entry_point.value.split(":")[0].split(".")[0],
                "callable": entry_point.load(),
            }
    return discovered_subcommands


@click_loguru.logging_options
@click.group(name="pycmor", help="PyCMOR - Makes CMOR Simple")
@click_loguru.stash_subcommand()
@click.version_option(version=VERSION, prog_name=NAME)
def cli(verbose, quiet, logfile, profile_mem):
    return 0


################################################################################
################################################################################
################################################################################

################################################################################
# Direct Commands
################################################################################


@cli.command()
@click_loguru.init_logger()
@click.argument("config_file", type=click.Path(exists=True))
@click.option(
    "--data-path",
    default=None,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help=(
        "New model run root (e.g. /scratch/.../Run_99). Anchored "
        "prefix-substitution rewrites every path string in the cfg "
        "that starts with the old run root."
    ),
)
@click.option(
    "--old-data-path",
    default=None,
    help=(
        "Old run-root prefix to replace. Auto-derived from inherit.data_path "
        "by stripping the trailing /outdata/<component>; pass explicitly when "
        "the yaml has no inherit.data_path."
    ),
)
@click.option("--year-start", default=None, type=int, help="Override start year on every rule.")
@click.option("--year-end", default=None, type=int, help="Override end year on every rule.")
@click.option(
    "--mesh-path",
    default=None,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Override FESOM mesh directory.",
)
@click.option(
    "--output-directory",
    default=None,
    # NOT exists=True — pycmor creates the directory.
    type=click.Path(file_okay=False, dir_okay=True),
    help="Override CMORized-output destination directory.",
)
@click.option(
    "--memory",
    default=None,
    help=(
        "Override SLURM per-job memory request (e.g. '512GB'). "
        "When omitted, the yaml's jobqueue.slurm.memory is left as-is."
    ),
)
def process(
    config_file,
    data_path,
    old_data_path,
    year_start,
    year_end,
    mesh_path,
    output_directory,
    memory,
):
    # NOTE(PG): The ``init_logger`` decorator above removes *ALL* previously configured loggers,
    #           so we need to re-create the report logger here. Paul does not like this at all.
    add_report_logger()
    from .core.banner import show_banner
    from .core.env_check import run_env_check
    from .core.overrides import CliOverrides, OverrideError, apply_overrides

    show_banner()
    run_env_check()
    logger.info(f"Processing {config_file}")
    with open(config_file, "r") as f:
        cfg = yaml.safe_load(f)
    try:
        cfg = apply_overrides(
            cfg,
            CliOverrides(
                data_path=data_path,
                old_data_path=old_data_path,
                year_start=year_start,
                year_end=year_end,
                mesh_path=mesh_path,
                output_directory=output_directory,
                memory=memory,
            ),
        )
    except OverrideError as e:
        raise click.UsageError(str(e))
    logger.debug(f"Effective config after CLI overrides:\n{yaml.safe_dump(cfg)}")
    cmorizer = CMORizer.from_dict(cfg)
    client = Client(cmorizer._cluster)  # noqa: F841
    cmorizer.process()


@cli.command()
@click_loguru.init_logger()
@click.argument("config_file", type=click.Path(exists=True))
def prefect_check(config_file):
    add_report_logger()
    logger.info(f"Checking prefect with dummy flow using {config_file}")
    with open(config_file, "r") as f:
        cfg = yaml.safe_load(f)
        cmorizer = CMORizer.from_dict(cfg)
        client = Client(cmorizer._cluster)  # noqa: F841
        cmorizer.check_prefect()


@cli.command()
@click_loguru.init_logger()
def table_explorer():
    logger.info("Launching table explorer...")
    try:
        with resources.path("pycmor", "webapp.py") as webapp_path:
            sys.argv = ["streamlit", "run", str(webapp_path)]
            stcli.main()
            return
    except Exception:
        pass
    with resources.path("pymor", "webapp.py") as webapp_path:
        sys.argv = ["streamlit", "run", str(webapp_path)]
        stcli.main()


################################################################################
# SUBCOMMANDS
################################################################################
@click_loguru.logging_options
@click.group()
@click_loguru.stash_subcommand()
@click.version_option(version=VERSION, prog_name=NAME)
def validate(verbose, quiet, logfile, profile_mem):
    return 0


@click_loguru.logging_options
@click.group()
@click_loguru.stash_subcommand()
@click.version_option(version=VERSION, prog_name=NAME)
def develop(verbose, quiet, logfile, profile_mem):
    return 0


@click_loguru.logging_options
@click.group()
@click_loguru.stash_subcommand()
@click.version_option(version=VERSION, prog_name=NAME)
def cache(verbose, quiet, logfile, profile_mem):
    return 0


@click.group()
def scripts():
    """Various utility scripts for Pycmor."""
    return 0


################################################################################
################################################################################

################################################################################
# COMMANDS FOR develop
################################################################################


@develop.command()
@click_loguru.init_logger()
@click.argument("directory", type=click.Path(exists=True))
@click.argument("output_file", type=click.File("w"), required=False, default=None)
def ls(directory, output_file):
    yaml_str = dev_utils.ls_to_yaml(directory)
    # Append to beginning of output file
    if output_file is not None:
        output_file.write(f"# Created with: pycmor develop ls {directory}\n")
        output_file.write(yaml_str)
    return 0


################################################################################
################################################################################
################################################################################
################################################################################
# COMMANDS FOR validate
################################################################################


@validate.command()
@click_loguru.init_logger()
@click.argument("config_file", type=click.Path(exists=True))
def config(config_file):
    logger.info(f"Checking if a CMORizer can be built from {config_file}")
    with open(config_file, "r") as f:
        cfg = yaml.safe_load(f)
        if "pipelines" in cfg:
            pipelines = cfg["pipelines"]
            PIPELINES_VALIDATOR.validate({"pipelines": pipelines})
        if "rules" in cfg:
            rules = cfg["rules"]
            RULES_VALIDATOR.validate({"rules": rules})
        if "general" in cfg:
            general = cfg["general"]
            GENERAL_VALIDATOR.validate({"general": general})
        if not any(
            [
                PIPELINES_VALIDATOR.errors,
                RULES_VALIDATOR.errors,
                GENERAL_VALIDATOR.errors,
            ]
        ):
            logger.success(f"Configuration {config_file} is valid for general settings, rules, and pipelines!")
        for key, error in {
            **GENERAL_VALIDATOR.errors,
            **PIPELINES_VALIDATOR.errors,
            **RULES_VALIDATOR.errors,
        }.items():
            logger.error(f"{key}: {error}")


@validate.command()
@click_loguru.init_logger()
@click.argument("config_file", type=click.Path(exists=True))
@click.argument("table_name", type=click.STRING)
def table(config_file, table_name):
    logger.info(f"Processing {config_file}")
    with open(config_file, "r") as f:
        cfg = yaml.safe_load(f)
        cmorizer = CMORizer.from_dict(cfg)
        cmorizer.check_rules_for_table(table_name)


@validate.command()
@click_loguru.init_logger()
@click.argument("config_file", type=click.Path(exists=True))
@click.argument("output_dir", type=click.STRING)
def directory(config_file, output_dir):
    logger.info(f"Processing {config_file}")
    with open(config_file, "r") as f:
        cfg = yaml.safe_load(f)
        cmorizer = CMORizer.from_dict(cfg)
        cmorizer.check_rules_for_output_dir(output_dir)


################################################################################
################################################################################
################################################################################

################################################################################
# COMMANDS FOR scripts
################################################################################


@scripts.group()
def fesom1():
    pass


fesom1.add_command(convert, name="nodes-to-levels")

# Add scripts commands
scripts.add_command(update_dimensionless_mappings)

################################################################################
################################################################################
################################################################################

################################################################################
# COMMANDS FOR cache
################################################################################


@cache.command()
@click_loguru.init_logger()
@click.argument(
    "cache_dir",
    default=f"{os.environ['HOME']}/.prefect/storage/",
    type=click.Path(exists=True, dir_okay=True),
)
def inspect_prefect_global(cache_dir):
    """Print information about items in Prefect's storage cache"""
    logger.info(f"Inspecting Prefect Cache at {cache_dir}")
    caching.inspect_cache(cache_dir)
    return 0


@cache.command()
@click_loguru.init_logger()
@click.argument(
    "result",
    type=click.Path(exists=True),
)
def inspect_prefect_result(result):
    obj = caching.inspect_result(result)
    logger.info(obj)
    return 0


@cache.command()
@click.argument("files", type=click.Path(exists=True), nargs=-1)
def populate_cache(files: List):
    fc.add_files(files)
    fc.save()


################################################################################
################################################################################
################################################################################

################################################################################
# CMIP7 Testing Commands
################################################################################


@cli.command()
@click_loguru.init_logger()
@click.argument("compound_name", type=click.STRING)
@click.option(
    "--version",
    "-v",
    default="v1.2.2.5",
    help="CMIP7 data request version to test against",
    show_default=True,
)
@click.option(
    "--metadata-file",
    "-m",
    type=click.Path(exists=True),
    help="Path to local metadata JSON file (optional)",
)
@click.option(
    "--show-all-variants",
    "-a",
    is_flag=True,
    help="Show all variants of the variable if found",
)
def cmip7_name_test(compound_name, version, metadata_file, show_all_variants):
    """
    Test a CMIP7 compound name against the data request.

    Checks if the given compound name exists in the CMIP7 data request
    and displays metadata information.

    Example compound name format: realm.variable.branding.frequency.region
    Example: atmos.tas.tavg-h2m-hxy-u.mon.GLB
    """
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    from .data_request.cmip7_interface import CMIP7Interface

    console = Console()

    try:
        # Initialize interface
        console.print("[bold]Loading CMIP7 Data Request...[/bold]")
        interface = CMIP7Interface()
        interface.load_metadata(version=version, metadata_file=metadata_file)
        console.print(f"[green]✓[/green] Loaded metadata for version: {version}\n")

        # Try to find the compound name
        console.print(f"[bold]Testing compound name:[/bold] {compound_name}\n")
        metadata = interface.get_variable_metadata(compound_name)

        if metadata:
            # Found it!
            console.print(Panel("[bold green]✓ Compound name FOUND in data request[/bold green]", border_style="green"))

            # Display metadata in a table
            table = Table(title="Variable Metadata", show_header=True, header_style="bold magenta")
            table.add_column("Property", style="cyan", no_wrap=True)
            table.add_column("Value", style="white")

            # Key properties to display
            display_props = [
                "variable_id",
                "standard_name",
                "long_name",
                "units",
                "frequency",
                "modeling_realm",
                "cmip6_compound_name",
                "cell_methods",
                "cell_measures",
            ]

            for prop in display_props:
                if prop in metadata:
                    value = str(metadata[prop])
                    # Truncate very long values
                    if len(value) > 80:
                        value = value[:77] + "..."
                    table.add_row(prop, value)

            console.print(table)

            # Show all variants if requested
            if show_all_variants:
                parts = compound_name.split(".")
                if len(parts) == 5:
                    realm, variable, branding, frequency, region = parts
                    console.print(f"\n[bold]Finding all variants of variable '{variable}' in realm '{realm}'...[/bold]")
                    variants = interface.find_variable_variants(variable, realm=realm)

                    if len(variants) > 1:
                        console.print(f"Found {len(variants)} total variants:\n")
                        for var in variants:
                            console.print(f"  • {var['cmip7_compound_name']}")
                    else:
                        console.print("No other variants found.")

        else:
            # Not found
            console.print(Panel("[bold red]✗ Compound name NOT FOUND in data request[/bold red]", border_style="red"))

            # Try to provide helpful information
            parts = compound_name.split(".")
            if len(parts) != 5:
                console.print(
                    f"\n[yellow]Warning:[/yellow] Compound name should have 5 parts "
                    f"(realm.variable.branding.frequency.region), but got {len(parts)} parts."
                )
            else:
                realm, variable, branding, frequency, region = parts
                console.print("\n[bold]Searching for similar variables...[/bold]")

                # Try to find variants of this variable
                variants = interface.find_variable_variants(variable, realm=realm)
                if variants:
                    console.print(f"\nFound {len(variants)} variant(s) of '{variable}' in realm '{realm}':")
                    for var in variants:
                        console.print(f"  • {var['cmip7_compound_name']}")
                    console.print("\n[yellow]Hint:[/yellow] Check if one of these matches what you're looking for.")
                else:
                    console.print(f"\n[yellow]No variants found for variable '{variable}' in realm '{realm}'.[/yellow]")
                    console.print("\n[yellow]Suggestions:[/yellow]")
                    console.print("  1. Check spelling of variable name")
                    console.print("  2. Verify the realm is correct")
                    console.print("  3. Use 'pycmor table-explorer' to browse available variables")

        return 0

    except ImportError as e:
        console.print(
            Panel(
                "[bold red]Error: CMIP7 Data Request API not installed[/bold red]\n\n"
                f"{str(e)}\n\n"
                "Install with: pip install CMIP7-data-request-api",
                border_style="red",
            )
        )
        return 1
    except Exception as e:
        console.print(Panel(f"[bold red]Error:[/bold red] {str(e)}", border_style="red"))
        logger.exception("Failed to test compound name")
        return 1


################################################################################
################################################################################
################################################################################

################################################################################
# Imported subcommands
################################################################################

cli.add_command(ssh_tunnel_cli, name="ssh-tunnel")
cli.add_command(scripts)

################################################################################

################################################################################
# Defined subcommands
################################################################################

cli.add_command(develop)
cli.add_command(validate)
cli.add_command(cache)

################################################################################
################################################################################
################################################################################


def main():
    for entry_point_name, entry_point in find_subcommands().items():
        cli.add_command(entry_point["callable"], name=entry_point_name)
    # Prefer new env var prefix, but keep backward compatibility
    cli(auto_envvar_prefix="PYCMOR")


if __name__ == "__main__":
    sys.exit(main())
