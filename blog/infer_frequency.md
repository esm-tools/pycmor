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

---

## A Smarter Alternative: `pymor.core.infer_freq`

To solve this, we built a robust frequency inference engine, tailored for climate data. The approach is simple but effective:

- Compute **deltas between all time points**.
- Use the **median step** to smooth over small irregularities.
- Convert all timestamps (including `cftime`) into a comparable numerical format.

This design makes it resilient to outliers, irregular calendars, and slight misalignments.

---

### Feature 1: Works with Any Calendar

Example: monthly data on a `360_day` calendar.

```python
import cftime
from pymor.core.infer_freq import infer_frequency

times = [
    cftime.Datetime360Day(2000, 1, 16),
    cftime.Datetime360Day(2000, 2, 16),
    cftime.Datetime360Day(2000, 3, 16),
]

print(infer_frequency(times, calendar='360_day'))
# Output: 'M'
```

Where `xarray.infer_freq` fails, `infer_frequency` succeeds.

---

### Feature 2: Rich Diagnostics, Not Silence

You can ask for detailed metadata:

```python
from pymor.core.infer_freq import infer_frequency

times = ["2000-01-01", "2000-02-01", "2000-02-28", "2000-04-01"]

result = infer_frequency(times, return_metadata=True, strict=True)
print(result)
```

Output:

```
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

The `infer_freq` module in PyMOR delivers exactly that. It turns guesswork into a reliable, automated process—so you can spend less time debugging and more time doing science.

Stop guessing. Start inferring—smarter.
