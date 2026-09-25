"""Decadal means from a year-by-year cmorization.

The HR submission processes one model year per job and pins every input
pattern to that year. A decadal mean needs ten. Rather than keep state
about which decades are done, every yearly job carries the dec rules and
:func:`decadal_gate` decides again each time:

- the run's first year is recounted from the input files on disk,
- decades are counted from that year (1850-1859, 1860-1869, ... for a run
  starting in 1850; 2024-2033, ... for one starting in 2024),
- the rule only proceeds in the year that closes a decade, and then reads
  all ten years; in every other year it ends without output.

A trailing partial decade is never written. CMIP7 ``dec`` is one sample per
ten years, and the coordinate checks in cc-plugin-wcrp#81 reject a shorter
cell ("does not match regular 10-year cells").
"""

import re

from ..core.logging import logger
from ..core.skip import RuleSkipped

_PLACEHOLDER = r"\d{4}"


def _years_on_disk(collection):
    grouped = re.compile(collection.pattern_str.replace(_PLACEHOLDER, r"(\d{4})", 1))
    years = set()
    for f in collection.path.iterdir():
        m = grouped.match(f.name)
        if m:
            years.add(int(m.group(1)))
    return years


def decadal_gate(data, rule):
    """Let a decadal rule through only in the year that closes a decade.

    Put this first in the pipeline of every ``dec`` rule. It needs
    ``rule.year``, which ``repoint_hr_year.py`` injects into ``inherit``.

    In the closing year the year-pinned input patterns are widened back to
    all years and ``year_start``/``year_end`` are set to the decade, so
    ``load_mfdataset`` reads exactly those ten years and
    ``set_time_bounds`` writes the decade as the cell. Missing years in a
    closing decade are an error, not a skip.
    """
    year = rule.get("year")
    if year is None:
        raise ValueError(
            f"decadal_gate: rule {rule.get('name')!r} has no 'year'. "
            "repoint_hr_year.py sets it in inherit for every yearly submission."
        )
    year = int(year)

    per_collection = []
    for coll in rule.inputs:
        if coll.pattern is None:
            raise ValueError(f"decadal_gate: glob input {coll.pattern_str!r} is not supported")
        if _PLACEHOLDER not in coll.pattern_str:
            widened = coll.pattern_str.replace(str(year), _PLACEHOLDER)
            if widened == coll.pattern_str:
                raise ValueError(
                    f"decadal_gate: input pattern {coll.pattern_str!r} contains neither "
                    f"the year {year} nor {_PLACEHOLDER}"
                )
            coll.pattern_str = widened
            coll.pattern = re.compile(widened)
        per_collection.append((coll, _years_on_disk(coll)))

    all_years = set().union(*(y for _, y in per_collection))
    if not all_years:
        raise FileNotFoundError(f"decadal_gate: no input files for rule {rule.get('name')!r}")
    first = min(all_years)
    position = (year - first) % 10
    decade_start = year - position
    if year < first or position != 9:
        logger.info(
            f"decadal_gate: {rule.get('name')}: {year} is year {position + 1} of the decade "
            f"{decade_start}-{decade_start + 9} (run starts {first}), nothing to write"
        )
        return RuleSkipped(f"{year} does not close a decade counted from {first}")

    decade = set(range(decade_start, year + 1))
    for coll, years in per_collection:
        missing = sorted(decade - years)
        if missing:
            raise FileNotFoundError(
                f"decadal_gate: {rule.get('name')}: decade {decade_start}-{year} is incomplete "
                f"for {coll.pattern_str!r} in {coll.path}, missing {missing}"
            )
    rule.year_start = decade_start
    rule.year_end = year
    logger.info(f"decadal_gate: {rule.get('name')}: writing decade {decade_start}-{year} (run starts {first})")
    return data
