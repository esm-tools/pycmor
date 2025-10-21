# Logging in pycmor

The pycmor logging system is designed to be simple, predictable, and integrated with Prefect's workflow UI.

## Quick Start

```python
from pycmor.core.logging import logger

# Use loguru's simple API
logger.info("Processing data...")
logger.success("Processing complete!")
logger.warning("Check this value")
logger.error("Something went wrong")
logger.debug("Detailed debugging info")
```

## Log Destinations

By default, logs go to **3 places**:

1. **Console (stderr)** - Always enabled, colored output
2. **Prefect UI** - Automatic when running inside Prefect flows/tasks
3. **File (optional)** - Set `PYCMOR_LOG_FILE` environment variable

## Configuration

### Environment Variables

- `PYCMOR_LOG_LEVEL` - Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default: INFO
- `PYCMOR_LOG_FILE` - Path to log file. If not set, no file logging.
- `PYCMOR_LOG_TO_PREFECT` - Enable Prefect integration (true/false). Default: true

### Example

```bash
# Set log level to DEBUG and enable file logging
export PYCMOR_LOG_LEVEL=DEBUG
export PYCMOR_LOG_FILE=/path/to/pycmor.log

# Run your command
pycmor process config.yaml
```

### Programmatic Configuration

```python
from pycmor.core.logging import setup_logging

# Reconfigure logging at runtime
setup_logging(
    level="DEBUG",
    log_file="my_run.log",
    enable_prefect_integration=True
)
```

## Integration with Prefect

When running inside a Prefect flow or task, all pycmor logs automatically appear in the Prefect UI:

```python
from prefect import flow, task
from pycmor.core.logging import logger

@task
def process_rule(rule):
    logger.info(f"Processing {rule.name}")
    # ... do work ...
    logger.success(f"{rule.name} completed successfully")

@flow
def cmorize():
    logger.info("Starting CMORization")
    for rule in rules:
        process_rule(rule)
    logger.success("All rules processed successfully")
```

### Viewing Logs in Prefect UI

1. Start Prefect server: `prefect server start`
2. Open UI: `http://localhost:4200`
3. Run your flow
4. Click on the flow run to see all logs with correct file:line references

## Log Format

### Console Output

```
09:45:02 | INFO     | pycmor.core.cmorizer:process:648 - Processing config.yaml
09:45:03 | SUCCESS  | pycmor.std_lib.files:save_dataset:285 - Saved output.nc
```

Format: `TIME | LEVEL | module:function:line - message`

### File Output

```
2025-10-21 09:45:02.123 | INFO     | pycmor.core.cmorizer:process:648 - Processing config.yaml
```

Format: `TIMESTAMP | LEVEL | module:function:line - message`

- Includes milliseconds for precise timing
- Rotates at 10 MB
- Keeps logs for 7 days
- Compresses old logs with gzip

## Common Patterns

### Basic Usage

```python
from pycmor.core.logging import logger

logger.info("Starting process")
try:
    result = do_something()
    logger.success(f"Got result: {result}")
except Exception as e:
    logger.error(f"Failed: {e}")
    logger.exception("Full traceback:")  # Includes stack trace
```

### Logging in Pipeline Steps

```python
def my_pipeline_step(data, rule_spec):
    """A custom pipeline step"""
    logger.debug(f"Input data shape: {data.shape}")

    # Do processing
    result = transform(data)

    logger.info(f"Processed {len(result)} items")
    return result
```

### Conditional Debug Logging

```python
import os
from pycmor.core.logging import logger

if os.getenv("PYCMOR_LOG_LEVEL") == "DEBUG":
    logger.debug(f"Detailed info: {expensive_to_compute()}")
```

## Troubleshooting

### Logs not appearing in Prefect UI

Check that `PYCMOR_LOG_TO_PREFECT=true` (default). If you disabled it:

```bash
export PYCMOR_LOG_TO_PREFECT=true
```

### Too many logs

Increase log level:

```bash
export PYCMOR_LOG_LEVEL=WARNING  # Only warnings and errors
```

### Want logs in a file

```bash
export PYCMOR_LOG_FILE=./pycmor.log
```

### Logs from Prefect/httpx/other libraries

These are automatically captured and shown with correct file:line references. To filter them out:

```bash
# In your shell, filter stderr
pycmor process config.yaml 2>&1 | grep -v "httpx"
```

Or increase log level to reduce noise:

```bash
export PYCMOR_LOG_LEVEL=WARNING
```

## For Developers

### Adding logging to new code

```python
from pycmor.core.logging import logger

def my_function():
    logger.info("Doing something")
    # ... code ...
    logger.success("Done")
```

### Best Practices

Use the centralized pycmor logger instead of creating your own:

```python
# Not recommended
import logging
my_logger = logging.getLogger(__name__)

# Recommended
from pycmor.core.logging import logger
```

### Testing with different log levels

```python
from pycmor.core.logging import setup_logging

def test_my_feature():
    setup_logging(level="DEBUG")  # Temporary DEBUG for this test
    # ... test code ...
```
