import rich_click as click

from ..cli import NAME, VERSION, find_subcommands
from .logging import logger


@click.group
@click.version_option(version=VERSION, prog_name=NAME)
def plugins():
    """
    Manage pycmor plugins
    """
    pass


@plugins.command(name="list")
def _list():
    """
    List all installed pycmor plugins. These can be to help CMORize a specific data
    collection (e.g. produced by FESOM, ICON, etc.)
    """
    discovered_plugins = find_subcommands()
    logger.info("The pycmor plugins are installed and available:")
    for plugin_name in discovered_plugins:
        plugin_code = discovered_plugins[plugin_name]["callable"]
        logger.info(f"# {plugin_name}", extra={"markup": True})
        doc = plugin_code.__doc__
        if doc:
            logger.info(doc)
