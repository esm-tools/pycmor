"""Pack conversion products into Slurm array tasks by expected run time.

A conversion of even a trivial product costs about ten seconds, most of it
starting the array task and importing pycmor. Several small products share one
array task (one Python process); a product expected to take at least the
target time runs alone. Array tasks are numbered longest first, so the slowest
products start at once instead of forming a tail (longest processing time
first). Packing only decides where and when a product runs, never how.
"""

from datetime import datetime
import json
from pathlib import Path

# Model-level ocean and atmosphere fields at daily or finer frequency took
# 8-17 minutes in the one-year AWI-ESM3 LR benchmark.
SLOW_LEVELS = {"al", "ol"}


def expected_seconds(compound):
    """Rough cost of one product-year from its frequency and vertical domain.

    Only used for products without a recorded time (see :func:`load_history`);
    the classes follow the medians of the one-year AWI-ESM3 LR benchmark.
    """
    parts = compound.split(".")
    frequency = parts[-2] if len(parts) > 2 else ""
    branding = parts[2].split("-") if len(parts) > 2 else []
    vertical = branding[1] if len(branding) > 1 else "u"
    levels = vertical in SLOW_LEVELS
    profile = levels or vertical.startswith("p") or vertical == "sl"
    if frequency in ("1hr", "3hr", "6hr"):
        return 900 if levels else 250 if profile else 150
    if frequency == "day":
        return 900 if levels else 250 if profile else 40
    if frequency == "mon":
        return 30 if profile else 20
    return 20


def record_seconds(record):
    try:
        return (datetime.fromisoformat(record["finished"]) - datetime.fromisoformat(record["started"])).total_seconds()
    except (KeyError, TypeError, ValueError):
        return None


def load_history(sources):
    """Longest recorded time per compound from earlier campaigns' status files."""
    seconds = {}
    for source in sources or ():
        source = Path(source)
        for path in sorted([*source.glob("status/*.json"), *source.glob("representative_test/status/*.json")]):
            try:
                record = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            key = record.get("key") or path.stem
            compound = record.get("compound") or key.split("__", 1)[-1]
            value = record_seconds(record)
            if value is not None and value >= 0:
                seconds[compound] = max(value, seconds.get(compound, 0))
    return seconds


def cost(task, history):
    return history.get(task["compound"], expected_seconds(task["compound"]))


def plan_batches(costs, max_products, target_seconds):
    """Group ``[(item, seconds), ...]``; return ``[(items, seconds), ...]`` longest first.

    Products are taken longest first and placed into the first open group that
    stays within ``target_seconds`` and ``max_products``; a product at or above
    the target gets its own group. With ``max_products == 1`` every product is
    alone and the original order is kept.
    """
    if max_products < 1 or target_seconds <= 0:
        raise ValueError("batch size and target time must be positive")
    if max_products == 1:
        return [([item], seconds) for item, seconds in costs]
    order = sorted(range(len(costs)), key=lambda i: -costs[i][1])
    groups = []
    for i in order:
        item, seconds = costs[i]
        if seconds < target_seconds:
            for group in groups:
                if group[1] + seconds <= target_seconds and len(group[0]) < max_products:
                    group[0].append(item)
                    group[1] += seconds
                    break
            else:
                groups.append([[item], seconds])
        else:
            groups.append([[item], seconds])
    groups.sort(key=lambda group: -group[1])
    return [(items, seconds) for items, seconds in groups]


def describe(groups):
    """Small JSON-able manifest of the packing, kept next to the array logs."""
    return dict(groups=[items for items, _ in groups], expected_seconds=[round(seconds, 1) for _, seconds in groups])
