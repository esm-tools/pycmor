"""ASCII banner for PyCMOR"""

from .. import __version__
from .logging import logger

BANNER = r"""
 ____        ____ __  __  ___  ____
|  _ \ _   _/ ___|  \/  |/ _ \|  _ \
| |_) | | | | |   | |\/| | | | | |_) |
|  __/| |_| | |___| |  | | |_| |  _ <
|_|    \__, |\____|_|  |_|\___/|_| \_\
       |___/
"""


def show_banner():
    """Display PyCMOR banner with version information"""
    version = __version__
    logger.info(BANNER)
    logger.info(f"PyCMOR v{version} - Makes CMOR Simple")
    logger.info("")
