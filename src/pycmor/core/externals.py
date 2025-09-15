import subprocess

import rich_click as click

from ..cli import find_subcommands  # noqa: F401
from ..cli import NAME, VERSION, click_loguru  # , pymor_cli_group
from .logging import logger


@click_loguru.logging_options
@click.group(invoke_without_command=True)
@click.pass_context
@click_loguru.stash_subcommand()
@click_loguru.init_logger()
@click.version_option(version=VERSION, prog_name=NAME)
# @pymor_cli_group
def externals(ctx, verbose, quiet, logfile, profile_mem):
    """
    Information about external dependencies
    """
    ctx.externals = {
        "CDO": subprocess.run(["cdo", "-V"], capture_output=True).stdout.decode(),
        "NCO": subprocess.run(["ncap2", "-r"], capture_output=True).stdout.decode(),
    }
    if ctx.invoked_subcommand is None:
        for NAME, VERSION in ctx.externals.items():  # noqa: F402
            logger.info(f"{NAME}: {VERSION}")


# @plugins.command(name="list")
# @click_loguru.init_logger()
# def _list():
#     """
#     List all installed pymor plugins. These can be to help CMORize a specific data
#     collection (e.g. produced by FESOM, ICON, etc.)
#     """
#     discovered_plugins = find_subcommands()
#     logger.info("The pymor plugins are installed and available:")
#     for plugin_name in discovered_plugins:
#         plugin_code = discovered_plugins[plugin_name]["callable"]
#         logger.info(f"# {plugin_name}", extra={"markup": True})
#         doc = plugin_code.__doc__
#         if doc:
#             logger.info(doc)
