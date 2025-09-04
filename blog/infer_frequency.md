# Stop Guessing: A Smarter Way to Infer Time Frequencies in Climate Data

*A practical guide to a robust Python module for handling tricky real-world time series.*

Time series are the backbone of climate science. From historical temperature records to future scenario simulations, understanding the **temporal resolution** (frequency) of your data is a vital first step. In the Python ecosystem, `xarray` is the workhorse for labeled, multi-dimensional datasets. It’s intuitive, powerful, and tightly integrated with the scientific stack.

But here’s the catch: one of the simplest-sounding tasks—figuring out the frequency of your time coordinate—often turns into a roadblock. If you’ve ever called `xarray.infer_freq()` on climate model output, you’ve probably seen it return the dreaded `None`. Why does this happen, and how can we do better?

---

## Why `xarray.infer_freq()` Often Returns `None`

Libraries like `pandas` and `xarray` excel with clean, perfectly regular time series. Real climate data rarely looks like that. In practice, three patterns commonly break inference and lead to a silent `None`:

1. **Non-standard calendars** (e.g., `noleap`, `360_day`) that standard datetime objects can’t represent.
2. **Unanchored or shifted timestamps** (e.g., monthly means stamped mid-month) that appear “irregular.”
3. **Minor gaps or duplicates** (e.g., a missing month or a duplicated timestamp) that strict algorithms reject.

The result is guesswork or fragile custom code—tedious and risky, especially for automated pipelines.

Here’s how we address these real-world cases in a robust, calendar-aware way.

### Background: NetCDF/CF and Model Calendars

- NetCDF is a common file format for climate and geoscience data.
- The CF (Climate and Forecast) conventions standardize metadata such as variable names, units, and time coordinates so tools can interpret datasets consistently. See the CF conventions at: [cfconventions.org](https://cfconventions.org)
- Many climate models use special calendars, such as:
  - standard/gregorian — real-world calendar with leap years
  - noleap — 365 days every year (no Feb 29)
  - 360_day — 12 months × 30 days = 360 days
- Standard datetime types can’t represent these calendars directly, so libraries often rely on `cftime` to handle them (docs: [cftime](https://unidata.github.io/cftime)). This is a key reason naive frequency inference may return `None`, even for perfectly regular model output.

Typical CF-compliant time metadata looks like:

```text
time: units = "days since 1850-01-01 00:00:00"
time: calendar = "noleap"  # or "360_day", "gregorian", etc.
```

---

## A Smarter Alternative: `pycmor.core.infer_freq`

To solve this, we built a robust frequency inference engine, tailored for climate data. The approach is simple but effective:

- Compute **deltas between all time points**.
- Use the **median step** to smooth over small irregularities.
- Convert all timestamps (including `cftime`) into a comparable numerical format.

This design makes it resilient to outliers, irregular calendars, and slight misalignments.

---

### Feature 1: Works with Any Calendar

Example: monthly data on a `360_day` calendar. In many cases you don’t need to pass the calendar explicitly—if you’re using `xarray`, CF-compliant attributes on the time coordinate will be detected and handled under the hood.

```python
import cftime
from pycmor.core.infer_freq import infer_frequency

times = [
    cftime.Datetime360Day(2000, 1, 16),
    cftime.Datetime360Day(2000, 2, 16),
    cftime.Datetime360Day(2000, 3, 16),
]

print(infer_frequency(times))
# Output: 'M'
```

Where `xarray.infer_freq` fails, `infer_frequency` succeeds.

---

### Feature 2: Rich Diagnostics, Not Silence

You can ask for detailed metadata:

```python
from pycmor.core.infer_freq import infer_frequency

times = ["2000-01-01", "2000-02-01", "2000-02-28", "2000-04-01"]

result = infer_frequency(times, return_metadata=True, strict=True)
print(result)
```

Output:

```python
FrequencyResult(
  frequency='M',
  delta_days=30.0,
  step=1,
  is_exact=False,
  status='missing_steps'
)
```

Instead of `None`, you now know:

- The intended frequency (monthly)
- The median spacing (30 days)
- Whether the series is perfectly regular (here: no)
- Why not (missing steps)

This feedback is immediately actionable.

These diagnostics help you prevent subtle downstream errors (like accidental upsampling) before they happen.

---

### Feature 3: Handles Data Overlaps and Duplicates

A common scenario: you're concatenating multiple NetCDF files or accidentally process the same file twice. This creates duplicate timestamps that break most frequency inference tools.

```python
import cftime
import numpy as np
from pymor.core.infer_freq import infer_frequency

# Original monthly data
data = [
    cftime.Datetime360Day(2000, 1, 16),
    cftime.Datetime360Day(2000, 2, 16), 
    cftime.Datetime360Day(2000, 3, 16)
]

# Simulate concatenating the same file twice (common mistake!)
duplicated_data = np.tile(data, 2)  # [Jan, Feb, Mar, Jan, Feb, Mar]

result = infer_frequency(duplicated_data, return_metadata=True)
print(result)
# FrequencyResult(frequency='M', delta_days=30.0, step=1, is_exact=False, status='irregular')
```

**Key insight**: pymor still correctly identifies the monthly frequency (`'M'`) but flags the data as `'irregular'` due to the duplicates. This prevents silent errors in downstream analysis while giving you the information needed to clean your data.

**Practical benefit**: Instead of mysterious `None` results, you get actionable diagnostics that help you identify and fix data quality issues before they corrupt your analysis.

---

### Understanding the FrequencyResult

Here’s what the fields mean:

- **`frequency`**: The inferred frequency string (e.g., `'D'` for daily, `'M'` for monthly).
- **`delta_days`**: The median spacing between time steps (in days).
- **`step`**: Multiplier for the frequency (e.g., `2` means `'2D'`).
- **`is_exact`**: Whether the series is perfectly regular (`True`) or not.
- **`status`**: Diagnostic message (`'valid'`, `'missing_steps'`, `'irregular'`, `'too_short'`).

👉 **How to interpret this:**

- `status="valid"` and `is_exact=True` → dataset is safe for downstream resampling/analysis.
- `status="missing_steps"` → data has gaps; consider filling or handling before analysis.
- `status="irregular"` → underlying frequency exists, but beware of inconsistencies.
- `status="too_short"` → not enough points to determine frequency.

---

## Why This Matters: Preventing Subtle Errors

Resampling is central in climate workflows—aggregating daily data to monthly, or comparing outputs across models. A silent mistake here (e.g., accidentally upsampling) can invalidate an analysis without you noticing.

By inferring frequency robustly, you can **programmatically block invalid resampling** before it happens. That’s a safeguard against silent data corruption.

- After concatenating multiple files, invoke `infer_frequency` on the combined time coordinate. This helps detect hidden issues (overlaps, missing chunks, or misaligned steps) before they propagate into your analysis.

---

## Takeaway

Real-world climate data is messy. We need tools that are:

- **Resilient** to irregularities
- **Transparent** in their diagnostics
- **Tailored** to non-standard calendars

The `infer_freq` module in `pycmor` delivers exactly that. It turns guesswork into a reliable, automated process—so you can spend less time debugging and more time doing science.

Stop guessing. Start inferring—smarter.

---

## Project Repository

- GitHub: [esm-tools/pymor](https://github.com/esm-tools/pymor)
- PyPI: [py-cmor](https://pypi.org/project/py-cmor/)

---

## Authors

This work was developed by the High Performance Computing and Data Processing group at the Alfred Wegener Institute for Polar and Marine Research (AWI), Bremerhaven, Germany.

- Paul Gierz (AWI)
- Pavan Siligam (AWI)
- Miguel Andrés-Martínez (AWI)
