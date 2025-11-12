# CMIP7 Data Request - Quick Start Guide

## Installation

```bash
cd /path/to/pymorize
git clone https://github.com/CMIP-Data-Request/CMIP7_DReq_Software.git
```

## 30-Second Start

```python
from pycmor.data_request import get_cmip7_data_request

# Load and query in 3 lines
dreq = get_cmip7_data_request("v1.0")
experiments = dreq.get_all_experiments()
vars_hist = dreq.get_variables_for_experiment("historical")
```

## Common Tasks

### Get Variables for an Experiment

```python
from pycmor.data_request import get_cmip7_data_request

dreq = get_cmip7_data_request("v1.0")
vars_hist = dreq.get_variables_for_experiment("historical", priority_cutoff="High")

# Output: {'Core': [...], 'High': [...], 'Medium': [], 'Low': []}
```

### List All Experiments

```python
experiments = dreq.get_all_experiments()
# Output: ['historical', 'piControl', 'amip', ...]
```

### Get All Unique Variables

```python
all_vars = dreq.get_all_variables(priority_cutoff="Low")
# Output: {'Amon.tas', 'Omon.tos', ...}
```

### Export to JSON

```python
dreq.export_to_json("cmip7_vars.json", opportunities="all", priority_cutoff="Low")
```

### Check Version Compatibility

```python
from pycmor.data_request import CMIP7DataRequestWrapper

wrapper = CMIP7DataRequestWrapper()
wrapper.retrieve_content("v1.2")
if wrapper.check_version_compatibility():
    print("Compatible!")
```

## Recommended Versions

✅ **Use these**: v1.0, v1.1, v1.2  
⚠️ **Avoid these**: v1.2.2.1, v1.2.2.2 (incompatible)

## Import Cheat Sheet

```python
# Quick access
from pycmor.data_request import get_cmip7_data_request

# Full wrapper
from pycmor.data_request import CMIP7DataRequestWrapper

# Built-in classes (no external repo needed)
from pycmor.data_request import CMIP7DataRequest

# Check availability
from pycmor.data_request import CMIP7_DREQ_AVAILABLE
```

## Troubleshooting

**Problem**: `CMIP7_DREQ_AVAILABLE` is `False`  
**Solution**: Clone CMIP7_DReq_Software to project root

**Problem**: `AttributeError: 'dreq_record' object has no attribute 'compound_name'`  
**Solution**: Use v1.0, v1.1, or v1.2 instead

**Problem**: Slow queries  
**Solution**: Results are cached per wrapper instance, reuse the instance

## Full Documentation

- **Usage Guide**: `docs/cmip7_wrapper_usage.md`
- **Import Examples**: `docs/cmip7_import_examples.md`
- **Summary**: `CMIP7_WRAPPER_SUMMARY.md`

## Support

- **GitHub Issues**: [CMIP7_DReq_Software Issues](https://github.com/CMIP-Data-Request/CMIP7_DReq_Software/issues)
- **Discussions**: [CMIP7_DReq_Software Discussions](https://github.com/CMIP-Data-Request/CMIP7_DReq_Software/discussions)
