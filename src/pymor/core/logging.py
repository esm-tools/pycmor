import logging
import os
import warnings
from functools import wraps

from loguru import logger
from rich.logging import RichHandler


class InterceptHandler(logging.Handler):
    """Intercept standard library logging and route to Loguru."""

    def emit(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging_interception():
    """Set up interception of standard library logging to route to Loguru."""
    # Remove any existing handlers from root logger
    logging.root.handlers = []

    # Add our intercept handler
    intercept_handler = InterceptHandler()
    logging.root.addHandler(intercept_handler)

    # Set the root logger level to capture all messages
    logging.root.setLevel(logging.NOTSET)

    # Specifically intercept common loggers that Prefect uses
    for name in ["prefect", "prefect.flow", "prefect.task", "dask", "distributed"]:
        logging.getLogger(name).handlers = []
        logging.getLogger(name).addHandler(intercept_handler)
        logging.getLogger(name).propagate = False


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


def configure_logging_from_config():
    """Configure logging based on PymorConfig settings.

    This attempts to read log settings from the configuration and applies them.
    If configuration is not available, it falls back to environment variables
    and finally to defaults.
    """

    # Try to get configuration from PymorConfig
    log_level = os.environ.get("PYMOR_LOG_LEVEL", "INFO").upper()
    log_file_level = os.environ.get("PYMOR_LOG_FILE_LEVEL", "DEBUG").upper()
    log_format = os.environ.get("PYMOR_LOG_FORMAT", "rich").lower()

    try:
        # Try to import and use PymorConfig if available
        from .config import PymorConfigManager

        # Create a minimal config manager to get logging settings
        config_mgr = PymorConfigManager.from_pymor_cfg({})
        log_level = config_mgr("log_level", default=log_level)
        log_file_level = config_mgr("log_file_level", default=log_file_level)
        log_format = config_mgr("log_format", default=log_format)

    except (ImportError, Exception):
        # Configuration not available, use environment variables and defaults
        pass

    return log_level, log_file_level, log_format


def init_cli_logger(log_level=None, log_format=None):
    """Initialize CLI logging while preserving Prefect loggers.

    This is a safer alternative to click_loguru.init_logger() that doesn't
    remove Prefect's loggers when they exist.

    Args:
        log_level (str): Log level for console output (if None, reads from config)
        log_format (str): Format for console output ("rich", "simple", "detailed") (if None, reads from config)
    """
    # Get configuration if not provided
    if log_level is None or log_format is None:
        (
            config_log_level,
            config_log_file_level,
            config_log_format,
        ) = configure_logging_from_config()
        log_level = log_level or config_log_level
        log_format = log_format or config_log_format

    # Don't remove all handlers - just ensure our setup is in place
    if not hasattr(logger, "_rich_handler_added"):
        # Only add our handler if it hasn't been added yet
        logger.remove()

        if log_format == "rich":
            logger.add(RichHandler(), format="{message}", level=log_level)
        elif log_format == "simple":
            logger.add(
                lambda msg: print(msg),
                format="{time:%H:%M:%S} | {level} | {message}",
                level=log_level,
            )
        elif log_format == "detailed":
            logger.add(
                lambda msg: print(msg),
                format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
                level=log_level,
            )
        else:
            # Fallback to rich
            logger.add(RichHandler(), format="{message}", level=log_level)

        logger._rich_handler_added = True

    # Re-add report logger if needed
    add_report_logger()


def get_logger():
    """Get a logger that works in both regular and Prefect contexts.

    This function returns the PyMOR Loguru logger, but when used inside
    a Prefect flow or task, it will also send messages to Prefect's logger
    to ensure they appear in the Prefect UI.

    Returns:
        loguru.Logger: The configured logger instance
    """
    try:
        # Try to get the Prefect run logger if we're in a Prefect context
        from prefect import get_run_logger

        prefect_logger = get_run_logger()

        # Create a wrapper that logs to both PyMOR and Prefect
        class HybridLogger:
            def __init__(self, pymor_logger, prefect_logger):
                self._pymor = pymor_logger
                self._prefect = prefect_logger

            def __getattr__(self, name):
                # Delegate to PyMOR logger for all other attributes/methods
                return getattr(self._pymor, name)

            def _log_both(self, level, message, *args, **kwargs):
                # Log to PyMOR (Loguru)
                getattr(self._pymor, level)(message, *args, **kwargs)
                # Also log to Prefect if the level exists there
                if hasattr(self._prefect, level):
                    getattr(self._prefect, level)(message, *args, **kwargs)

            def debug(self, message, *args, **kwargs):
                self._log_both("debug", message, *args, **kwargs)

            def info(self, message, *args, **kwargs):
                self._log_both("info", message, *args, **kwargs)

            def warning(self, message, *args, **kwargs):
                self._log_both("warning", message, *args, **kwargs)

            def error(self, message, *args, **kwargs):
                self._log_both("error", message, *args, **kwargs)

            def critical(self, message, *args, **kwargs):
                self._log_both("critical", message, *args, **kwargs)

            def success(self, message, *args, **kwargs):
                # Success is Loguru-specific
                self._pymor.success(message, *args, **kwargs)
                # Fall back to info for Prefect
                self._prefect.info(f"SUCCESS: {message}", *args, **kwargs)

        return HybridLogger(logger, prefect_logger)

    except Exception:
        # Not in a Prefect context, return regular logger
        return logger


warnings.showwarning = showwarning

# Set up interception before configuring Loguru
setup_logging_interception()

# Configure Loguru logger with configuration-based settings
logger.remove()

# Get initial configuration
try:
    log_level, log_file_level, log_format = configure_logging_from_config()
except Exception:
    # Fallback to defaults if configuration fails
    log_level, log_file_level, log_format = "INFO", "DEBUG", "rich"

# Add console handler based on configuration
if log_format == "rich":
    rich_handler_id = logger.add(RichHandler(), format="{message}", level=log_level)
elif log_format == "simple":
    rich_handler_id = logger.add(
        lambda msg: print(msg),
        format="{time:%H:%M:%S} | {level} | {message}",
        level=log_level,
    )
elif log_format == "detailed":
    rich_handler_id = logger.add(
        lambda msg: print(msg),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
        level=log_level,
    )
else:
    # Fallback to rich
    rich_handler_id = logger.add(RichHandler(), format="{message}", level=log_level)
