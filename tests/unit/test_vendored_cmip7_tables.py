"""Guards on the vendored CMIP7 CMOR tables in ``src/pycmor/data/cmip7/``.

These are upstream artifacts copied verbatim from WCRP-CMIP/cmip7-cmor-tables.
They are refreshed by hand, and a refresh that pulls an older table has
already cost us once: the 2026-07-09 ``CMIP7_coordinate.json`` collapsed the
four leaf-type tree axes onto ``value = "trees"``, which makes
``treeFracBdlDcd`` and friends indistinguishable on disk. Nothing failed, the
files were simply wrong. These tests read the JSON directly, no fixtures, so
they stay cheap and keep working when the surrounding test doubles rot.
"""

import json
from pathlib import Path

import pytest

DATA = Path(__file__).parent.parent.parent / "src" / "pycmor" / "data" / "cmip7"

#: Written on disk as the ``type`` coordinate of the four treeFrac variables.
#: The CF area-type table has carried the specific terms for years.
LEAF_TYPES = {
    "typetreebd": "broadleaf_deciduous_trees",
    "typetreebe": "broadleaf_evergreen_trees",
    "typetreend": "needleleaf_deciduous_trees",
    "typetreene": "needleleaf_evergreen_trees",
}


@pytest.fixture(scope="module")
def coordinate_table():
    return json.loads((DATA / "CMIP7_coordinate.json").read_text())


@pytest.mark.parametrize("axis,expected", sorted(LEAF_TYPES.items()))
def test_leaf_type_axes_are_specific(coordinate_table, axis, expected):
    """The four tree axes must name their leaf type, not just say "trees"."""
    entry = coordinate_table["axis_entry"][axis]
    assert entry["value"] == expected, (
        f"{axis} carries value={entry['value']!r}. A table older than "
        f"2026-07-21 collapses all four treeFrac variables onto 'trees'."
    )


def test_leaf_type_axes_are_mutually_distinct(coordinate_table):
    values = {a: coordinate_table["axis_entry"][a]["value"] for a in LEAF_TYPES}
    assert len(set(values.values())) == len(values), f"leaf types collide: {values}"


@pytest.mark.parametrize("table", ["CMIP7_coordinate", "CMIP7_grids", "CMIP7_formula_terms"])
def test_vendored_tables_parse_and_carry_a_date(table):
    header = json.loads((DATA / f"{table}.json").read_text())["Header"]
    assert header["table_date"], f"{table}.json has no table_date"
    assert header["cmor_version"], f"{table}.json has no cmor_version"
