#!/usr/bin/env python3
"""XIOS XML Linter — validates XIOS configuration files for structural correctness.

Checks performed:
  1. Well-formed XML (with Jinja2 template support)
  2. Known XIOS element names
  3. Known attributes per element type
  4. Reference integrity (*_ref attributes resolve to existing ids)
  5. Required attributes per element type
  6. Enum value validation (operation, type, format, etc.)
  7. Duplicate id detection

Usage:
  python lint_xios_xml.py file1.xml [file2.xml ...] [directory/]
  python lint_xios_xml.py --all  # lint all .xml/.xml.j2 in current directory tree

  # Declare alternative file sets (only one used at runtime):
  python lint_xios_xml.py core_atm/ \\
      --alternatives "field_def.xml,field_def_cmip6.xml,field_def_cmip7.xml,field_def_lpjg_safe.xml" \\
      --alternatives "file_def.xml.j2,file_def_lpjg_spinup.xml.j2,file_def_oifs_cmip7_spinup.xml.j2"

  Files in the same --alternatives group are each fully linted individually,
  but duplicate ids between them are suppressed (since only one is active
  at runtime).

Jinja2 templates (.xml.j2):
  {{ expressions }} are replaced with placeholder values before parsing.
  {% %} blocks are stripped. This allows structural validation of templates.
"""

import argparse
import fnmatch
import re
import sys
from pathlib import Path

from lxml import etree


# ============================================================================
# XIOS element and attribute definitions (from XIOS User Guide + Reference)
# ============================================================================

VALID_ELEMENTS = {
    # Top-level
    "simulation",
    "context",
    # Definitions
    "field_definition",
    "file_definition",
    "grid_definition",
    "domain_definition",
    "axis_definition",
    "variable_definition",
    # Groups
    "field_group",
    "file_group",
    "grid_group",
    "domain_group",
    "axis_group",
    "variable_group",
    # Core elements
    "field",
    "file",
    "grid",
    "domain",
    "axis",
    "variable",
    "calendar",
    # Grid sub-elements
    "scalar",
    # Transformations
    "zoom_domain",
    "zoom_axis",
    "interpolate_domain",
    "interpolate_axis",
    "generate_rectilinear_domain",
    "inverse_axis",
    "reduce_domain_to_axis",
    "extract_domain_to_axis",
    "reduce_axis_to_scalar",
    "reduce_axis_to_axis",
    "temporal_splitting",
    "duplicate_scalar_to_axis",
    "extract_axis_to_scalar",
    "redistribute_domain",
    "redistribute_axis",
    "reorder_domain",
}

# Common attributes that can appear on most elements
COMMON_ATTRS = {"id", "name", "enabled", "src", "description", "comment"}

# Attributes valid per element type
ELEMENT_ATTRS = {
    "simulation": COMMON_ATTRS,
    "context": COMMON_ATTRS | {"type", "calendar_type"},
    "calendar": COMMON_ATTRS | {
        "type", "timestep", "start_date", "time_origin",
        "day_length", "month_lengths", "year_length",
        "leap_year_drift", "leap_year_month", "leap_year_drift_offset",
    },
    "field": COMMON_ATTRS | {
        "field_ref", "grid_ref", "domain_ref", "axis_ref", "scalar_ref",
        "operation", "freq_op", "freq_offset",
        "long_name", "standard_name", "unit",
        "prec", "level", "default_value",
        "compression_level", "indexed_output",
        "detect_missing_value", "read_access",
        "cell_methods", "cell_methods_mode",
        "ts_enabled", "ts_split_freq",
        "expr", "field_id",
        "check_if_active",
    },
    "field_definition": COMMON_ATTRS | {
        "level", "prec", "enabled", "operation", "freq_op",
        "ts_enabled", "default_value",
    },
    "field_group": COMMON_ATTRS | {
        "field_ref", "grid_ref", "domain_ref", "axis_ref", "scalar_ref",
        "operation", "freq_op", "freq_offset",
        "long_name", "standard_name", "unit",
        "prec", "level", "default_value",
        "compression_level", "detect_missing_value",
        "ts_enabled", "ts_split_freq",
        "cell_methods", "cell_methods_mode",
    },
    "file": COMMON_ATTRS | {
        "output_freq", "output_level", "split_freq", "split_freq_format",
        "sync_freq", "type", "format", "par_access",
        "mode", "append", "convention",
        "timeseries", "ts_prefix",
        "compression_level", "name_suffix",
        "min_digits", "record_offset",
        "cyclic",
    },
    "file_definition": COMMON_ATTRS,
    "file_group": COMMON_ATTRS | {
        "output_freq", "output_level", "split_freq", "split_freq_format",
        "sync_freq", "type", "format", "par_access",
        "mode", "append", "convention",
        "timeseries", "ts_prefix",
        "compression_level", "name_suffix",
        "min_digits",
    },
    "grid": COMMON_ATTRS | {"grid_ref"},
    "grid_definition": COMMON_ATTRS,
    "grid_group": COMMON_ATTRS,
    "domain": COMMON_ATTRS | {
        "domain_ref", "type", "long_name",
        "ni_glo", "nj_glo", "ibegin", "jbegin", "ni", "nj",
        "data_dim", "data_ni", "data_nj", "data_ibegin", "data_jbegin",
        "lonvalue_1d", "latvalue_1d", "lonvalue_2d", "latvalue_2d",
        "bounds_lon_1d", "bounds_lat_1d", "bounds_lon_2d", "bounds_lat_2d",
        "i_index", "j_index", "data_i_index", "data_j_index",
        "nvertex", "area",
    },
    "domain_definition": COMMON_ATTRS,
    "domain_group": COMMON_ATTRS | {"type", "long_name"},
    "axis": COMMON_ATTRS | {
        "axis_ref", "long_name", "standard_name", "unit",
        "positive", "n_glo", "value", "bounds", "label",
        "index", "begin", "n", "data_begin", "data_n", "data_index",
        "prec",
    },
    "axis_definition": COMMON_ATTRS,
    "axis_group": COMMON_ATTRS | {"unit", "positive", "long_name", "standard_name"},
    "variable": COMMON_ATTRS | {"type"},
    "variable_definition": COMMON_ATTRS,
    "variable_group": COMMON_ATTRS,
    "scalar": COMMON_ATTRS | {"scalar_ref", "long_name", "standard_name", "unit", "value", "prec"},
    # Transformations
    "zoom_domain": COMMON_ATTRS | {"zoom_ibegin", "zoom_ni", "zoom_jbegin", "zoom_nj"},
    "zoom_axis": COMMON_ATTRS | {"begin", "n", "index"},
    "interpolate_domain": COMMON_ATTRS | {
        "order", "type", "weight_filename", "write_weight",
        "renormalize", "quantity", "mode",
        "detect_missing_value",
    },
    "interpolate_axis": COMMON_ATTRS | {"order", "type"},
    "generate_rectilinear_domain": COMMON_ATTRS | {"lat_start", "lat_end", "lon_start", "lon_end", "bounds_lat_start", "bounds_lat_end", "bounds_lon_start", "bounds_lon_end"},
    "inverse_axis": COMMON_ATTRS,
    "reduce_domain_to_axis": COMMON_ATTRS | {"direction", "operation"},
    "extract_domain_to_axis": COMMON_ATTRS | {"position"},
    "reduce_axis_to_scalar": COMMON_ATTRS | {"operation"},
    "reduce_axis_to_axis": COMMON_ATTRS | {"operation"},
    "temporal_splitting": COMMON_ATTRS,
    "duplicate_scalar_to_axis": COMMON_ATTRS,
    "extract_axis_to_scalar": COMMON_ATTRS | {"position"},
    "redistribute_domain": COMMON_ATTRS,
    "redistribute_axis": COMMON_ATTRS,
    "reorder_domain": COMMON_ATTRS | {"invert_lat"},
}

# Enum constraints
VALID_OPERATIONS = {"instant", "average", "accumulate", "minimum", "maximum", "once"}
VALID_FILE_TYPES = {"one_file", "multiple_file"}
VALID_FILE_FORMATS = {"netcdf4", "netcdf4_classic"}
VALID_FILE_MODES = {"write", "read"}
VALID_PAR_ACCESS = {"collective", "independent"}
VALID_CALENDAR_TYPES = {
    "Gregorian", "Julian", "NoLeap", "AllLeap", "D360",
    "user_defined",
    # case-insensitive aliases
    "gregorian", "julian", "noleap", "allleap", "d360",
}
VALID_TIMESERIES = {"none", "only", "both", "exclusive"}
VALID_CONVENTIONS = {"CF", "UGRID"}
VALID_DOMAIN_TYPES = {"rectilinear", "curvilinear", "unstructured", "gaussian", "gaussian_reduced"}
VALID_POSITIVE = {"up", "down"}


def preprocess_jinja(text):
    """Replace Jinja2 template expressions with XML-safe placeholders."""
    # Replace {{ expression }} with a placeholder string
    text = re.sub(r"\{\{[^}]*\}\}", "JINJA_PLACEHOLDER", text)
    # Remove {% block %} statements (entire line if alone)
    text = re.sub(r"\{%[^%]*%\}", "", text)
    # Remove {# comments #}
    text = re.sub(r"\{#[^#]*#\}", "", text)
    return text


class XiosLinter:
    def __init__(self, alternative_groups=None):
        self.errors = []
        self.warnings = []
        # id -> list of (file, element_tag, line) — multiple entries for alternatives
        self.ids = {}
        self.refs = []  # (ref_attr, ref_value, file, line, element_tag)
        # Elements with src= attribute: their id is an inclusion pointer, not
        # a real definition. Track them to suppress false duplicate errors.
        self.src_ids = set()
        # Build a mapping: filename -> group_index for alternative file groups
        self.alt_groups = {}  # normalized filename -> group_index
        if alternative_groups:
            for group_idx, group in enumerate(alternative_groups):
                for pattern in group:
                    self.alt_groups[pattern.strip()] = group_idx

    def _get_alt_group(self, filepath):
        """Return the alternative group index for a file, or None."""
        name = Path(filepath).name
        for pattern, group_idx in self.alt_groups.items():
            if fnmatch.fnmatch(name, pattern) or name == pattern:
                return group_idx
        return None

    def _same_alt_group(self, file_a, file_b):
        """True if both files belong to the same alternative group."""
        ga = self._get_alt_group(file_a)
        gb = self._get_alt_group(file_b)
        return ga is not None and ga == gb

    def error(self, filepath, line, msg):
        self.errors.append(f"{filepath}:{line}: ERROR: {msg}")

    def warn(self, filepath, line, msg):
        self.warnings.append(f"{filepath}:{line}: WARNING: {msg}")

    def lint_file(self, filepath):
        """Lint a single XIOS XML or XML.j2 file."""
        path = Path(filepath)
        is_jinja = path.suffix == ".j2" or ".j2" in path.suffixes

        try:
            text = path.read_text(encoding="utf-8")
        except Exception as e:
            self.error(filepath, 0, f"Cannot read file: {e}")
            return

        if is_jinja:
            text = preprocess_jinja(text)

        # Parse XML
        try:
            tree = etree.fromstring(text.encode("utf-8"))
        except etree.XMLSyntaxError as e:
            self.error(filepath, e.lineno or 0, f"XML syntax error: {e}")
            return

        self._walk(tree, filepath)

    def _walk(self, element, filepath):
        """Recursively validate an element and its children."""
        tag = element.tag
        line = element.sourceline or 0

        # Check element name
        if tag not in VALID_ELEMENTS:
            self.warn(filepath, line, f"Unknown element <{tag}>")

        # Check attributes
        known_attrs = ELEMENT_ATTRS.get(tag, COMMON_ATTRS)
        for attr in element.attrib:
            if attr not in known_attrs:
                self.warn(filepath, line, f"Unknown attribute '{attr}' on <{tag}>")

        # Track elements with src= (inclusion pointers — not real definitions)
        has_src = "src" in element.attrib

        # Collect ids
        elem_id = element.get("id")
        if elem_id:
            if has_src:
                # This is an inclusion pointer (e.g. <context id="oifs" src="./context_ifs.xml"/>)
                # Don't register as a real definition — the included file has the real one.
                self.src_ids.add(elem_id)
            elif elem_id in self.ids:
                prev_file, prev_tag, prev_line = self.ids[elem_id]
                # Suppress if both files are in the same alternative group
                if self._same_alt_group(filepath, prev_file):
                    pass  # expected duplicate between alternatives
                elif elem_id in self.src_ids:
                    pass  # previous occurrence was a src= inclusion pointer
                else:
                    self.error(
                        filepath, line,
                        f"Duplicate id='{elem_id}' "
                        f"(first defined in {prev_file}:{prev_line} on <{prev_tag}>)"
                    )
            else:
                self.ids[elem_id] = (filepath, tag, line)

        # Collect refs for later resolution
        for attr in element.attrib:
            if attr.endswith("_ref"):
                ref_val = element.get(attr)
                if ref_val and not ref_val.startswith("JINJA_PLACEHOLDER"):
                    self.refs.append((attr, ref_val, filepath, line, tag))

        # Validate enum attributes
        self._check_enum(element, "operation", VALID_OPERATIONS, filepath, line, tag)
        if tag in ("file", "file_group"):
            self._check_enum(element, "type", VALID_FILE_TYPES, filepath, line, tag)
            self._check_enum(element, "format", VALID_FILE_FORMATS, filepath, line, tag)
            self._check_enum(element, "mode", VALID_FILE_MODES, filepath, line, tag)
            self._check_enum(element, "par_access", VALID_PAR_ACCESS, filepath, line, tag)
            self._check_enum(element, "timeseries", VALID_TIMESERIES, filepath, line, tag)
            self._check_enum(element, "convention", VALID_CONVENTIONS, filepath, line, tag)
        if tag == "calendar":
            self._check_enum(element, "type", VALID_CALENDAR_TYPES, filepath, line, tag)
        if tag in ("domain", "domain_group"):
            self._check_enum(element, "type", VALID_DOMAIN_TYPES, filepath, line, tag)
        if tag in ("axis", "axis_group"):
            self._check_enum(element, "positive", VALID_POSITIVE, filepath, line, tag)

        # Check required attributes for fields inside files
        if tag == "field":
            has_field_ref = "field_ref" in element.attrib
            has_id = "id" in element.attrib

            in_field_def = False
            parent = element.getparent()
            while parent is not None:
                if parent.tag == "field_definition":
                    in_field_def = True
                    break
                parent = parent.getparent()

            if in_field_def and not has_field_ref and not has_id:
                self.warn(filepath, line, "<field> in field_definition should have 'id'")

        # Check file has output_freq (unless inherited from file_group)
        if tag == "file":
            if "output_freq" not in element.attrib:
                parent = element.getparent()
                parent_has_freq = False
                while parent is not None:
                    if parent.tag == "file_group" and "output_freq" in parent.attrib:
                        parent_has_freq = True
                        break
                    parent = parent.getparent()
                if not parent_has_freq:
                    self.warn(filepath, line, "<file> missing 'output_freq' (not inherited from parent)")

        # Recurse
        for child in element:
            if isinstance(child.tag, str):  # skip comments
                self._walk(child, filepath)

    def _check_enum(self, element, attr, valid_values, filepath, line, tag):
        """Check if an attribute value is in the allowed set."""
        val = element.get(attr)
        if val and not val.startswith("JINJA_PLACEHOLDER"):
            val_clean = val.strip().strip("'\"")
            if val_clean not in valid_values:
                self.error(
                    filepath, line,
                    f"Invalid {attr}='{val_clean}' on <{tag}> "
                    f"(expected one of: {', '.join(sorted(valid_values))})"
                )

    def check_refs(self):
        """After all files are parsed, check that all references resolve."""
        for attr, ref_val, filepath, line, tag in self.refs:
            if ref_val not in self.ids:
                # For src attributes, it's a file path, not an id reference
                if attr == "src":
                    continue
                self.warn(
                    filepath, line,
                    f"Unresolved {attr}='{ref_val}' on <{tag}> "
                    f"(no element with id='{ref_val}' found)"
                )

    def report(self):
        """Print results and return exit code."""
        for w in sorted(self.warnings):
            print(f"  {w}")
        for e in sorted(self.errors):
            print(f"  {e}")

        n_err = len(self.errors)
        n_warn = len(self.warnings)
        n_ids = len(self.ids)

        print(f"\n  {n_ids} ids collected, {len(self.refs)} references checked")
        if n_err == 0 and n_warn == 0:
            print("  All checks passed.")
        else:
            if n_warn:
                print(f"  {n_warn} warning(s)")
            if n_err:
                print(f"  {n_err} error(s)")

        return 1 if n_err > 0 else 0


def collect_files(paths):
    """Expand directories and globs into a list of XML/XML.j2 files."""
    result = []
    for p in paths:
        path = Path(p)
        if path.is_dir():
            result.extend(sorted(path.glob("**/*.xml")))
            result.extend(sorted(path.glob("**/*.xml.j2")))
        elif path.exists():
            result.append(path)
        else:
            print(f"WARNING: {p} not found, skipping", file=sys.stderr)
    return result


def main():
    parser = argparse.ArgumentParser(
        description="XIOS XML Linter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  %(prog)s core_atm/
  %(prog)s core_atm/ --alternatives "field_def.xml,field_def_cmip6.xml,field_def_cmip7.xml,field_def_lpjg_safe.xml"
  %(prog)s --all""",
    )
    parser.add_argument("paths", nargs="*", default=["."], help="Files or directories to lint")
    parser.add_argument("--all", action="store_true", help="Lint all .xml/.xml.j2 in current directory tree")
    parser.add_argument(
        "--alternatives", action="append", default=[],
        metavar="FILE1,FILE2,...",
        help="Comma-separated group of filenames that are alternatives "
             "(only one active at runtime). Duplicate ids between them "
             "are suppressed. Can be repeated for multiple groups.",
    )
    args = parser.parse_args()

    if args.all:
        files = collect_files(["."])
    else:
        files = collect_files(args.paths)

    if not files:
        print("No XML files found.")
        return 0

    # Parse alternative groups
    alt_groups = []
    for group_str in args.alternatives:
        alt_groups.append([f.strip() for f in group_str.split(",")])

    linter = XiosLinter(alternative_groups=alt_groups)
    print(f"Linting {len(files)} file(s)...")
    for f in files:
        print(f"  {f}")
        linter.lint_file(str(f))

    # Cross-file reference check
    linter.check_refs()

    print()
    return linter.report()


if __name__ == "__main__":
    sys.exit(main())
