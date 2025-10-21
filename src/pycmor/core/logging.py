"""
Unified logging system for pycmor that integrates with Prefect.

This module provides a clean logging interface that:
1. Uses loguru for the primary logging API (simple, clean)
2. Bridges to Python's logging system for Prefect integration
3. Shows logs in both console and Prefect UI
4. Allows optional file logging via environment variable

Key design decisions:
- Loguru is the primary API (logger.info(), logger.error(), etc.)
- All logs automatically flow to Prefect when running in a flow/task context
- No more click-loguru complexity
- No more manual add_report_logger() calls
- Clear, predictable log destinations
"""

import logging
import os
import sys
from collections import namedtuple
from pathlib import Path
from typing import Optional

from loguru import logger

# Create a simple file record type that's picklable
FileRecord = namedtuple("FileRecord", ["name", "path"])

DEFAULT_LOG_LEVEL = os.environ.get("PYCMOR_LOG_LEVEL", "INFO")
LOG_FILE_PATH = os.environ.get("PYCMOR_LOG_FILE", None)
LOG_TO_PREFECT = os.environ.get("PYCMOR_LOG_TO_PREFECT", "true").lower() == "true"

# Store handler IDs for dynamic log file management
_file_handler_id = None
_rule_log_handlers = {}


class InterceptHandler(logging.Handler):
    """
    Handler that intercepts standard logging calls and redirects to loguru.
    This allows Prefect's logging to flow through loguru for unified output.
    """

    def emit(self, record: logging.LogRecord):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Use the record's location information directly instead of trying to
        # calculate depth. Create a proper file record compatible with loguru.
        file_record = FileRecord(
            name=Path(record.pathname).name,
            path=record.pathname,
        )

        logger.patch(
            lambda r: r.update(
                name=record.name,
                file=file_record,
                function=record.funcName,
                line=record.lineno,
            )
        ).log(level, record.getMessage())


class PrefectHandler(logging.Handler):
    """
    Handler that sends loguru logs to Prefect's logging system.
    Ensures logs appear in the Prefect UI when running inside flows/tasks.
    """

    def emit(self, record: logging.LogRecord):
        try:
            from prefect import get_run_logger
            from prefect.context import FlowRunContext, TaskRunContext

            flow_context = FlowRunContext.get()
            task_context = TaskRunContext.get()

            if flow_context or task_context:
                prefect_logger = get_run_logger()
                level_map = {
                    "TRACE": logging.DEBUG,
                    "DEBUG": logging.DEBUG,
                    "INFO": logging.INFO,
                    "SUCCESS": logging.INFO,
                    "WARNING": logging.WARNING,
                    "ERROR": logging.ERROR,
                    "CRITICAL": logging.CRITICAL,
                }
                log_level = level_map.get(record.levelname, logging.INFO)
                prefect_logger.log(log_level, record.getMessage())
        except Exception:
            pass


def setup_logging(
    level: str = DEFAULT_LOG_LEVEL,
    log_file: Optional[str] = LOG_FILE_PATH,
    enable_prefect_integration: bool = LOG_TO_PREFECT,
):
    """
    Configure the unified logging system for pycmor.

    Parameters
    ----------
    level : str
        Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    log_file : str, optional
        Path to log file. If None, no file logging.
    enable_prefect_integration : bool
        Whether to send logs to Prefect UI when in a flow/task context

    Example
    -------
    >>> from pycmor.core.logging import setup_logging, logger
    >>> setup_logging(level="DEBUG", log_file="pycmor.log")
    >>> logger.info("This appears in console, file, and Prefect UI")
    """
    logger.remove()

    console_format = (
        "<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )
    logger.add(
        sys.stderr,
        format=console_format,
        level=level,
        colorize=True,
        enqueue=True,
    )

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            log_file,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
            level="DEBUG",
            rotation="10 MB",
            retention="7 days",
            compression="gz",
        )
        logger.info(f"File logging enabled: {log_file}")

    if enable_prefect_integration:
        logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

        for logger_name in ["prefect", "prefect.flow_runs", "prefect.task_runs"]:
            prefect_logger = logging.getLogger(logger_name)
            prefect_logger.handlers = [InterceptHandler()]
            prefect_logger.propagate = False

        logger.add(
            PrefectHandler(),
            format="{message}",
            level=level,
            enqueue=True,
        )
        logger.debug("Prefect logging integration enabled")

    logger.success(f"Logging initialized at level: {level}")


def add_rule_log_file(rule_name: str, log_dir: Optional[Path] = None) -> int:
    """
    Add a per-rule log file for parallel processing.

    Parameters
    ----------
    rule_name : str
        Name of the rule (will be sanitized for filename)
    log_dir : Path, optional
        Directory to store rule logs. If None, uses current directory + 'logs'

    Returns
    -------
    int
        Handler ID for the added log file
    """
    if log_dir is None:
        log_dir = Path.cwd() / "logs" / "rules"
    else:
        log_dir = Path(log_dir)

    log_dir.mkdir(parents=True, exist_ok=True)

    # Sanitize rule name for filename
    safe_rule_name = rule_name.replace("/", "_").replace("\\", "_").replace(" ", "_")
    log_file = log_dir / f"pycmor_{safe_rule_name}.log"

    handler_id = logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        enqueue=True,
    )

    _rule_log_handlers[rule_name] = handler_id
    logger.debug(f"Added per-rule log file: {log_file}")
    return handler_id


def remove_rule_log_file(rule_name: str):
    """
    Remove a per-rule log file handler.

    Parameters
    ----------
    rule_name : str
        Name of the rule whose log handler to remove
    """
    if rule_name in _rule_log_handlers:
        handler_id = _rule_log_handlers[rule_name]
        logger.remove(handler_id)
        del _rule_log_handlers[rule_name]
        logger.debug(f"Removed per-rule log handler for: {rule_name}")


def merge_rule_logs(
    rule_names: list, output_file: Optional[Path] = None, log_dir: Optional[Path] = None
):
    """
    Merge per-rule log files into a single chronologically-sorted log.

    Parameters
    ----------
    rule_names : list
        List of rule names whose logs to merge
    output_file : Path, optional
        Output file for merged logs. If None, uses 'pycmor_merged.log'
    log_dir : Path, optional
        Directory containing rule logs. If None, uses current directory + 'logs/rules'
    """
    import re
    from datetime import datetime

    if log_dir is None:
        log_dir = Path.cwd() / "logs" / "rules"
    else:
        log_dir = Path(log_dir)

    if output_file is None:
        output_file = Path.cwd() / "logs" / "pycmor_merged.log"
    else:
        output_file = Path(output_file)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Collect all log lines with timestamps
    all_lines = []
    timestamp_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})")

    for rule_name in rule_names:
        safe_rule_name = (
            rule_name.replace("/", "_").replace("\\", "_").replace(" ", "_")
        )
        log_file = log_dir / f"pycmor_{safe_rule_name}.log"

        if not log_file.exists():
            logger.warning(f"Rule log file not found: {log_file}")
            continue

        with open(log_file, "r") as f:
            for line in f:
                match = timestamp_pattern.match(line)
                if match:
                    timestamp_str = match.group(1)
                    try:
                        timestamp = datetime.strptime(
                            timestamp_str, "%Y-%m-%d %H:%M:%S.%f"
                        )
                        all_lines.append((timestamp, f"[{rule_name}] {line}"))
                    except ValueError:
                        # Couldn't parse timestamp, append anyway
                        all_lines.append((datetime.min, line))
                else:
                    # No timestamp, append anyway
                    all_lines.append((datetime.min, line))

    # Sort by timestamp
    all_lines.sort(key=lambda x: x[0])

    # Write merged log
    with open(output_file, "w") as f:
        for _, line in all_lines:
            f.write(line)

    logger.info(f"Merged {len(rule_names)} rule logs into: {output_file}")


setup_logging()

__all__ = [
    "logger",
    "setup_logging",
    "add_rule_log_file",
    "remove_rule_log_file",
    "merge_rule_logs",
]
