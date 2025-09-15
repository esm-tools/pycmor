import rich_click as click

from ..cli import NAME, VERSION, click_loguru, find_subcommands  # , pymor_cli_group
from .logging import logger


@click_loguru.logging_options
@click.group
@click_loguru.stash_subcommand()
@click.version_option(version=VERSION, prog_name=NAME)
# @pymor_cli_group
def plugins(verbose, quiet, logfile, profile_mem):
    """
    Manage pymor plugins
    """
    pass


@plugins.command(name="list")
@click_loguru.init_logger()
def _list():
    """
    List all installed pymor plugins. These can be to help CMORize a specific data
    collection (e.g. produced by FESOM, ICON, etc.)
    """
    discovered_plugins = find_subcommands()
    logger.info("The pymor plugins are installed and available:")
    for plugin_name in discovered_plugins:
        plugin_code = discovered_plugins[plugin_name]["callable"]
        logger.info(f"# {plugin_name}", extra={"markup": True})
        doc = plugin_code.__doc__
        if doc:
            logger.info(doc)
