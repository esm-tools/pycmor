"""
Tests for PyMOR logging functionality including Prefect integration.
"""

import logging
import os
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from loguru import logger

from pymor.core.logging import (
    InterceptHandler,
    configure_logging_from_config,
    get_logger,
    init_cli_logger,
    setup_logging_interception,
)


class TestInterceptHandler:
    """Tests for the InterceptHandler class that bridges stdlib logging to Loguru."""

    def test_intercept_handler_init(self):
        """Test that InterceptHandler can be instantiated."""
        handler = InterceptHandler()
        assert isinstance(handler, logging.Handler)

    def test_intercept_handler_emit(self):
        """Test that InterceptHandler properly emits log records to Loguru."""
        with patch.object(logger, "opt") as mock_opt:
            mock_opt_instance = MagicMock()
            mock_opt.return_value = mock_opt_instance

            handler = InterceptHandler()
            record = logging.LogRecord(
                name="test_logger",
                level=logging.INFO,
                pathname="/test/path.py",
                lineno=42,
                msg="Test message",
                args=(),
                exc_info=None,
            )

            handler.emit(record)

            # Verify Loguru's opt method was called with correct parameters
            mock_opt.assert_called_once()
            mock_opt_instance.log.assert_called_once_with("INFO", "Test message")


class TestLoggingInterception:
    """Tests for logging interception setup."""

    def test_setup_logging_interception(self):
        """Test that logging interception sets up correctly."""
        # Store original handlers for cleanup
        original_handlers = logging.root.handlers.copy()

        try:
            setup_logging_interception()

            # Check that root logger has our intercept handler
            assert len(logging.root.handlers) == 1
            assert isinstance(logging.root.handlers[0], InterceptHandler)

            # Check that root logger level is set to capture all messages
            assert logging.root.level == logging.NOTSET

        finally:
            # Restore original handlers
            logging.root.handlers = original_handlers

    def test_prefect_logger_interception(self):
        """Test that Prefect-specific loggers are intercepted."""
        original_handlers = logging.root.handlers.copy()

        try:
            setup_logging_interception()

            # Check Prefect loggers are configured
            prefect_logger = logging.getLogger("prefect")
            assert len(prefect_logger.handlers) == 1
            assert isinstance(prefect_logger.handlers[0], InterceptHandler)
            assert not prefect_logger.propagate

            # Check dask logger is configured
            dask_logger = logging.getLogger("dask")
            assert len(dask_logger.handlers) == 1
            assert isinstance(dask_logger.handlers[0], InterceptHandler)
            assert not dask_logger.propagate

        finally:
            logging.root.handlers = original_handlers


class TestConfigurationFromConfig:
    """Tests for configuration reading functionality."""

    def test_configure_logging_from_config_env_vars(self):
        """Test configuration reading from environment variables."""
        # The config system actually looks for just LOG_LEVEL, not PYMOR_LOG_LEVEL
        with patch.dict(
            os.environ,
            {"LOG_LEVEL": "DEBUG", "LOG_FILE_LEVEL": "ERROR", "LOG_FORMAT": "simple"},
        ):
            log_level, log_file_level, log_format = configure_logging_from_config()

            assert log_level == "DEBUG"
            assert log_file_level == "ERROR"
            assert log_format == "simple"

    def test_configure_logging_from_config_defaults(self):
        """Test configuration defaults when no environment variables are set."""
        with patch.dict(os.environ, {}, clear=True):
            # Ensure no PYMOR logging env vars are set
            for key in list(os.environ.keys()):
                if key.startswith("PYMOR_LOG"):
                    del os.environ[key]

            log_level, log_file_level, log_format = configure_logging_from_config()

            assert log_level == "INFO"
            assert log_file_level == "DEBUG"
            assert log_format == "rich"

    def test_configure_logging_with_config_manager(self):
        """Test configuration when PymorConfig is available."""
        with patch("pymor.core.logging.PymorConfigManager") as mock_config_manager:
            mock_config = MagicMock()
            mock_config.return_value = "CUSTOM_VALUE"
            mock_config_manager.from_pymor_cfg.return_value = mock_config

            log_level, log_file_level, log_format = configure_logging_from_config()

            # Should have attempted to get config values
            mock_config.assert_any_call("log_level", default="INFO")
            mock_config.assert_any_call("log_file_level", default="DEBUG")
            mock_config.assert_any_call("log_format", default="rich")


class TestCliLogger:
    """Tests for CLI logger initialization."""

    def test_init_cli_logger_default(self):
        """Test CLI logger initialization with defaults."""
        # Mock logger to avoid actual initialization
        with patch("pymor.core.logging.logger") as mock_logger, patch(
            "pymor.core.logging.add_report_logger"
        ) as mock_report:
            mock_logger.add = MagicMock()
            mock_logger.remove = MagicMock()
            # Mock hasattr to return False so it tries to initialize
            with patch("builtins.hasattr", return_value=False):
                init_cli_logger()

                # Should have been called to set up logger
                mock_logger.remove.assert_called_once()
                mock_logger.add.assert_called()
                mock_report.assert_called_once()

    def test_init_cli_logger_with_params(self):
        """Test CLI logger initialization with specific parameters."""
        with patch("pymor.core.logging.logger") as mock_logger, patch(
            "pymor.core.logging.add_report_logger"
        ) as mock_report:
            mock_logger.add = MagicMock()
            mock_logger.remove = MagicMock()
            # Mock hasattr to return False so it tries to initialize
            with patch("builtins.hasattr", return_value=False):
                init_cli_logger(log_level="DEBUG", log_format="simple")

                mock_logger.remove.assert_called_once()
                mock_logger.add.assert_called()
                mock_report.assert_called_once()


class TestHybridLogger:
    """Tests for the hybrid logger functionality."""

    @contextmanager
    def mock_prefect_context(self):
        """Context manager to simulate being in a Prefect flow/task."""
        mock_prefect_logger = MagicMock()

        # We need to patch the import inside get_logger function
        with patch("prefect.get_run_logger", return_value=mock_prefect_logger):
            yield mock_prefect_logger

    def test_get_logger_outside_prefect(self):
        """Test get_logger when not in a Prefect context."""
        with patch(
            "prefect.get_run_logger", side_effect=Exception("Not in Prefect context")
        ):
            hybrid_logger = get_logger()

            # Should return the regular logger
            assert hybrid_logger is logger

    def test_get_logger_inside_prefect(self):
        """Test get_logger when inside a Prefect context."""
        with self.mock_prefect_context():
            hybrid_logger = get_logger()

            # Should return the hybrid logger wrapper
            assert hybrid_logger is not logger
            assert hasattr(hybrid_logger, "_pymor")
            assert hasattr(hybrid_logger, "_prefect")

    def test_hybrid_logger_info_logging(self):
        """Test that hybrid logger logs to both PyMOR and Prefect."""
        with self.mock_prefect_context() as mock_prefect_logger:
            with patch("pymor.core.logging.logger") as mock_pymor_logger:
                hybrid_logger = get_logger()

                # Test info logging
                hybrid_logger.info("Test message")

                # Should have logged to both loggers
                mock_pymor_logger.info.assert_called_once_with("Test message")
                mock_prefect_logger.info.assert_called_once_with("Test message")

    def test_hybrid_logger_error_logging(self):
        """Test that hybrid logger handles error logging."""
        with self.mock_prefect_context() as mock_prefect_logger:
            with patch("pymor.core.logging.logger") as mock_pymor_logger:
                hybrid_logger = get_logger()

                # Test error logging
                hybrid_logger.error("Error message")

                # Should have logged to both loggers
                mock_pymor_logger.error.assert_called_once_with("Error message")
                mock_prefect_logger.error.assert_called_once_with("Error message")

    def test_hybrid_logger_success_logging(self):
        """Test that hybrid logger handles Loguru-specific success logging."""
        with self.mock_prefect_context() as mock_prefect_logger:
            with patch("pymor.core.logging.logger") as mock_pymor_logger:
                hybrid_logger = get_logger()

                # Test success logging (Loguru-specific)
                hybrid_logger.success("Success message")

                # Should have logged success to PyMOR and info to Prefect
                mock_pymor_logger.success.assert_called_once_with("Success message")
                mock_prefect_logger.info.assert_called_once_with(
                    "SUCCESS: Success message"
                )

    def test_hybrid_logger_delegation(self):
        """Test that hybrid logger delegates other attributes to PyMOR logger."""
        with self.mock_prefect_context():
            with patch("pymor.core.logging.logger") as mock_pymor_logger:
                mock_pymor_logger.bind.return_value = "bound_logger"

                hybrid_logger = get_logger()

                # Test delegation of bind method
                result = hybrid_logger.bind(key="value")

                # Should delegate to PyMOR logger
                mock_pymor_logger.bind.assert_called_once_with(key="value")
                assert result == "bound_logger"


class TestPrefectIntegration:
    """Integration tests for Prefect logging functionality."""

    def test_intercept_handler_with_prefect_messages(self):
        """Test that InterceptHandler properly handles Prefect log messages."""
        with patch.object(logger, "opt") as mock_opt:
            mock_opt_instance = MagicMock()
            mock_opt.return_value = mock_opt_instance

            handler = InterceptHandler()

            # Create a Prefect-style log record
            record = logging.LogRecord(
                name="prefect.flow",
                level=logging.INFO,
                pathname="/prefect/flow.py",
                lineno=123,
                msg="Flow starting: test_flow",
                args=(),
                exc_info=None,
            )

            handler.emit(record)

            # Should have been forwarded to Loguru
            mock_opt.assert_called_once()
            mock_opt_instance.log.assert_called_once_with(
                "INFO", "Flow starting: test_flow"
            )
