import warnings
from functools import wraps

from loguru import logger
from rich.logging import RichHandler


def showwarning(message, *args, **kwargs):
    """Set up warnings to use logger"""
    logger.warning(message)


def report_filter(record):
    """Checks if the record should be added to the report log or not"""
    return record["extra"].get("add_to_report", False)


def add_to_report_log(func):
    """Decorator for logging to the report log"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        with logger.contextualize(add_to_report=True):
            return func(*args, **kwargs)

    return wrapper


def add_report_logger():
    logger.add(
        "pymor_report.log", format="{time} {level} {message}", filter=report_filter
    )


warnings.showwarning = showwarning
logger.remove()
rich_handler_id = logger.add(RichHandler(), format="{message}", level="INFO")
