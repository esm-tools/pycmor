# AWI-CM RECOM Test Data Fixture Usage

## Overview

The `awicm_1p0_recom_data` fixture has been refactored to support both stub data (fast, lightweight) and real data (full downloads) with automatic routing.

## Fixtures Available

### 1. `awicm_1p0_recom_data` (Router - Use This!)
The main fixture that automatically routes to stub or real data based on configuration.

**Default behavior**: Returns stub data generated from YAML manifests

**Returns**: `Path` to the data directory (`awi-esm-1-1-lr_kh800/piControl`)

### 2. `awicm_1p0_recom_stub_data` (Stub Data)
Generates lightweight stub NetCDF files from YAML manifest.
- Fast execution
- No network required
- Minimal disk space
- Generated in temporary directory

### 3. `awicm_1p0_recom_real_data` (Real Data)
Downloads and extracts full test dataset.
- Requires network connection
- Downloads ~XX MB tar file
- Cached in `~/.cache/pycmor/test_data/`
- Full, realistic NetCDF data

### 4. `awicm_1p0_recom_download_data` (Download Helper)
Internal fixture that handles downloading. Usually not used directly.

## Usage Examples

### Default: Use Stub Data (Recommended for Most Tests)
```python
def test_my_feature(awicm_1p0_recom_data):
    # By default, this uses stub data
    data_path = awicm_1p0_recom_data
    # data_path points to stub data directory
    assert data_path.exists()
```

### Option 1: Use Real Data via Environment Variable
```bash
# Run all tests with real data
export PYCMOR_USE_REAL_TEST_DATA=1
pytest tests/

# Or inline
PYCMOR_USE_REAL_TEST_DATA=true pytest tests/integration/test_awicm_recom.py
```

### Option 2: Use Real Data via pytest Marker
```python
import pytest

@pytest.mark.real_data
def test_with_real_data(awicm_1p0_recom_data):
    # This test always uses real downloaded data
    data_path = awicm_1p0_recom_data
    # data_path points to real data directory
    assert data_path.exists()
```

### Option 3: Explicitly Use Stub or Real Data
```python
def test_explicit_stub(awicm_1p0_recom_stub_data):
    # Always uses stub data, regardless of env var or marker
    data_path = awicm_1p0_recom_stub_data
    assert data_path.exists()

def test_explicit_real(awicm_1p0_recom_real_data):
    # Always uses real data, regardless of env var or marker
    data_path = awicm_1p0_recom_real_data
    assert data_path.exists()
```

## When to Use Which?

### Use Stub Data (Default) When:
- Developing new tests
- Running tests locally
- Testing logic/flow rather than data integrity
- CI/CD pipelines (fast feedback)
- You want fast test execution

### Use Real Data When:
- Validating data processing accuracy
- Testing edge cases in real NetCDF files
- Debugging issues specific to actual data
- Final validation before release
- Testing file format compatibility

## Migration Guide

**No changes needed!** Existing tests using `awicm_1p0_recom_data` will automatically use stub data by default.

To explicitly test with real data, either:
1. Set `PYCMOR_USE_REAL_TEST_DATA=1` environment variable
2. Add `@pytest.mark.real_data` decorator to specific tests

## Stub Data Manifest

Stub data is generated from the YAML manifest at:
```
tests/fixtures/stub_data/awicm_1p0_recom.yaml
```

This manifest defines:
- File paths and structure
- Variable names and dimensions
- Coordinate metadata
- Global attributes

The stub generator creates NetCDF files with random data matching these specifications.

## Benefits

1. **Faster tests**: Stub data generation is ~100x faster than downloading
2. **No network dependency**: Tests work offline
3. **Smaller CI cache**: Stub data is generated on-demand, no need to cache
4. **Flexible**: Easy to switch between stub and real data
5. **Backward compatible**: Existing tests work without changes
