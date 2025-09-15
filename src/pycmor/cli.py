import os
import sys
from importlib import resources
from typing import List

import pkg_resources
import rich_click as click
import yaml
from click_loguru import ClickLoguru
from dask.distributed import Client
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

MAX_FRAMES = int(
    os.environ.get(
        "PYCMOR_ERROR_MAX_FRAMES", os.environ.get("PYMOR_ERROR_MAX_FRAMES", 3)
    )
)
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


# FIXME(PG): Doesn't work as intended :-(
def pymor_cli_group(func):
    """
    Decorator to add the click_loguru logging options to a click group
    """
    func = click_loguru.logging_options(func)
    func = click.group()(func)
    func = click_loguru.stash_subcommand()(func)
    func = click.version_option(version=VERSION, prog_name="PyMOR - Make CMOR Simple")(
        func
    )
    return func


def find_subcommands():
    """
    Finds CLI Subcommands for installed plugins in both legacy and new groups.
    """
    groups = ["pycmor.cli_subcommands", "pymor.cli_subcommands"]
    discovered_subcommands = {}
    for group in groups:
        for entry_point in pkg_resources.iter_entry_points(group):
            discovered_subcommands[entry_point.name] = {
                "plugin_name": entry_point.module_name.split(".")[0],
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
def process(config_file):
    # NOTE(PG): The ``init_logger`` decorator above removes *ALL* previously configured loggers,
    #           so we need to re-create the report logger here. Paul does not like this at all.
    add_report_logger()
    logger.info(f"Processing {config_file}")
    with open(config_file, "r") as f:
        cfg = yaml.safe_load(f)
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
    """Various utility scripts for Pymor."""
    return 0


################################################################################
################################################################################

################################################################################
# COMMANDS FOR develop
################################################################################


@develop.command()
@click_loguru.logging_options
@click_loguru.init_logger()
@click.argument("directory", type=click.Path(exists=True))
@click.argument("output_file", type=click.File("w"), required=False, default=None)
def ls(directory, output_file, verbose, quiet, logfile, profile_mem):
    yaml_str = dev_utils.ls_to_yaml(directory)
    # Append to beginning of output file
    if output_file is not None:
        output_file.write(f"# Created with: pymor develop ls {directory}\n")
        output_file.write(yaml_str)
    return 0


################################################################################
################################################################################
################################################################################

################################################################################
# COMMANDS FOR validate
################################################################################


@validate.command()
@click_loguru.logging_options
@click_loguru.init_logger()
@click.argument("config_file", type=click.Path(exists=True))
def config(config_file, verbose, quiet, logfile, profile_mem):
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
            logger.success(
                f"Configuration {config_file} is valid for general settings, rules, and pipelines!"
            )
        for key, error in {
            **GENERAL_VALIDATOR.errors,
            **PIPELINES_VALIDATOR.errors,
            **RULES_VALIDATOR.errors,
        }.items():
            logger.error(f"{key}: {error}")


@validate.command()
@click_loguru.logging_options
@click_loguru.init_logger()
@click.argument("config_file", type=click.Path(exists=True))
@click.argument("table_name", type=click.STRING)
def table(config_file, table_name, verbose, quiet, logfile, profile_mem):
    logger.info(f"Processing {config_file}")
    with open(config_file, "r") as f:
        cfg = yaml.safe_load(f)
        cmorizer = CMORizer.from_dict(cfg)
        cmorizer.check_rules_for_table(table_name)


@validate.command()
@click_loguru.logging_options
@click_loguru.init_logger()
@click.argument("config_file", type=click.Path(exists=True))
@click.argument("output_dir", type=click.STRING)
def directory(config_file, output_dir, verbose, quiet, logfile, profile_mem):
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
@click_loguru.logging_options
@click_loguru.init_logger()
@click.argument(
    "cache_dir",
    default=f"{os.environ['HOME']}/.prefect/storage/",
    type=click.Path(exists=True, dir_okay=True),
)
def inspect_prefect_global(cache_dir, verbose, quiet, logfile, profile_mem):
    """Print information about items in Prefect's storage cache"""
    logger.info(f"Inspecting Prefect Cache at {cache_dir}")
    caching.inspect_cache(cache_dir)
    return 0


@cache.command()
@click_loguru.logging_options
@click_loguru.init_logger()
@click.argument(
    "result",
    type=click.Path(exists=True),
)
def inspect_prefect_result(result, verbose, quiet, logfile, profile_mem):
    obj = caching.inspect_result(result)
    logger.info(obj)
    return 0


@cache.command()
@click_loguru.logging_options
@click.argument("files", type=click.Path(exists=True), nargs=-1)
def populate_cache(files: List, verbose, quiet, logfile, profile_mem):
    fc.add_files(files)
    fc.save()


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
