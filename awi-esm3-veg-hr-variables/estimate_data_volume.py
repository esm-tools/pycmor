#!/usr/bin/env python3
"""
Estimate annual data volume for AWI-ESM3-VEG-HR CMIP7 CMORization.

Reads all YAML rule files and CSVs, classifies by realm/frequency/grid,
and computes estimated storage per year.

Grid sizes (float32 = 4 bytes per value):
  - atm/land (OIFS 0.25deg): 1440 x 720 = 1,036,800 gridpoints
  - ocean/seaice (FESOM DARS): 3,146,761 surface nodes
  - ocean 3D: 3,146,761 x 56 levels = 176,218,616 values
  - LPJ-GUESS: ~420,000 gridpoints (land-only)

Frequencies → timesteps per year:
  - fx:   1
  - yr:   1
  - mon:  12
  - day:  365
  - 6hr:  1460
  - 3hr:  2920
  - 1hr:  8760
"""

import glob
import os
import re
import csv
import sys

# ── Grid sizes (number of gridpoints) ──────────────────────────────
GRID_POINTS = {
    "atm_sfc": 1440 * 720,  # 1,036,800
    "atm_ml": 1440 * 720 * 91,  # 91 model levels
    "atm_pl": 1440 * 720 * 19,  # plev19
    "atm_pl3": 1440 * 720 * 3,  # plev3
    "atm_pl6": 1440 * 720 * 6,  # plev6
    "oce_sfc": 3_146_761,  # FESOM DARS surface
    "oce_3d": 3_146_761 * 56,  # FESOM DARS 3D
    "lpjg": 420_000,  # LPJ-GUESS land cells
}

# ── Timesteps per year ──────────────────────────────────────────────
TIMESTEPS = {
    "fx": 1,
    "yr": 1,
    "dec": 0.1,
    "mon": 12,
    "Amon": 12,
    "Lmon": 12,
    "Omon": 12,
    "SImon": 12,
    "AERmon": 12,
    "Emon": 12,
    "LImon": 12,
    "CFmon": 12,
    "day": 365,
    "Eday": 365,
    "SIday": 365,
    "Oday": 365,
    "CFday": 365,
    "6hr": 1460,
    "6hrPt": 1460,
    "3hr": 2920,
    "3hrPt": 2920,
    "CF3hr": 2920,
    "E3hrPt": 2920,
    "1hr": 8760,
    "E1hr": 8760,
    "AERhr": 8760,
}

BYTES_PER_VALUE = 4  # float32


def guess_grid(rule_name, compound_name, realm, model_variable, is_3d=False):
    """Guess grid size from rule metadata."""
    cn = compound_name.lower() if compound_name else ""
    rn = rule_name.lower()

    # Ocean/sea-ice realm
    if realm in ("ocean", "seaice", "seaIce", "landIce"):
        if is_3d or any(k in cn for k in ("-al-", "-ol-", "3d", "mlev")):
            return "oce_3d"
        return "oce_sfc"

    # Atmosphere model levels
    if "-al-" in cn or "ml" in rn or "pfull" in rn:
        return "atm_ml"

    # Atmosphere pressure levels
    if "plev19" in cn or "-p19-" in cn or "_pl_" in rn:
        return "atm_pl"
    if "plev3" in cn or "-p3-" in cn or "_pl3" in rn:
        return "atm_pl3"
    if "plev6" in cn or "-p6-" in cn or "_pl6" in rn:
        return "atm_pl6"

    # LPJ-GUESS
    if "lpjg" in rn or "lpj" in rn or "Lut" in rn:
        return "lpjg"

    # Default: atmosphere surface
    return "atm_sfc"


def guess_frequency(rule_name, compound_name):
    """Guess output frequency from compound_name or rule_name."""
    cn = compound_name if compound_name else ""

    # From compound name: ...freq.region
    parts = cn.split(".")
    if len(parts) >= 4:
        freq = parts[-2]
        if freq in TIMESTEPS:
            return freq

    # From rule name patterns
    rn = rule_name.lower()
    for freq_key in ["1hr", "3hr", "6hr", "day", "mon", "yr", "fx", "dec"]:
        if freq_key in rn:
            return freq_key

    return "mon"  # default


def is_3d_rule(rule_name, compound_name, model_variable):
    """Check if rule produces 3D output."""
    cn = (compound_name or "").lower()
    mv = (model_variable or "").lower()
    rn = rule_name.lower()
    return any(k in cn for k in ("-al-", "-ol-", "-p19-", "-p3-", "-p6-")) or any(
        k in rn for k in ("_ml", "_pl", "pfull", "plev")
    )


def parse_yaml_rules(yaml_path):
    """Parse rules from a pycmor YAML file (simple regex, no YAML lib needed)."""
    rules = []
    with open(yaml_path) as f:
        content = f.read()

    # Determine realm from directory name
    dirname = os.path.basename(os.path.dirname(yaml_path))
    if "ocean" in dirname:
        realm = "ocean"
    elif "seaice" in dirname:
        realm = "seaice"
    elif "land" in dirname:
        realm = "land"
    else:
        realm = "atmos"

    # Split into rule blocks
    rule_blocks = re.split(r"\n\s*- name:", content)
    for i, block in enumerate(rule_blocks):
        if i == 0:
            continue  # skip header before first rule
        lines = block.strip().split("\n")
        name = lines[0].strip()
        compound = ""
        model_var = ""
        has_lpjg = "lpjg" in block.lower() or "lpj_guess" in block.lower()

        for line in lines:
            line = line.strip()
            if line.startswith("compound_name:"):
                compound = line.split(":", 1)[1].strip().strip('"').strip("'")
            elif line.startswith("model_variable:"):
                model_var = line.split(":", 1)[1].strip().strip('"').strip("'")

        # Override realm from compound name
        if compound:
            cr = compound.split(".")[0].lower()
            if cr in ("ocean", "omon"):
                realm_r = "ocean"
            elif cr in ("seaice", "simon", "siday"):
                realm_r = "seaice"
            elif cr in ("landice",):
                realm_r = "land"
            elif cr in ("atmos", "aerosol", "atmoschem"):
                realm_r = "atmos"
            elif cr in ("land",):
                realm_r = "land"
            else:
                realm_r = realm
        else:
            realm_r = realm

        threed = is_3d_rule(name, compound, model_var)
        grid = guess_grid(name, compound, realm_r, model_var, threed)

        # Override for LPJ-GUESS rules
        if has_lpjg or "lpjg" in name.lower():
            grid = "lpjg"

        freq = guess_frequency(name, compound)

        rules.append(
            {
                "config": os.path.basename(yaml_path),
                "dir": dirname,
                "name": name,
                "compound": compound,
                "realm": realm_r,
                "grid": grid,
                "freq": freq,
                "model_var": model_var,
            }
        )
    return rules


def count_csv_rows(csv_dir):
    """Count total CSV rows (variables requested) in a directory."""
    total = 0
    for f in glob.glob(os.path.join(csv_dir, "*.csv")):
        with open(f) as fh:
            reader = csv.reader(fh)
            next(reader, None)  # skip header
            total += sum(1 for _ in reader)
    return total


def human_size(nbytes):
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} EB"


def main():
    base = os.path.dirname(os.path.abspath(__file__))

    # Collect all rules
    all_rules = []
    yaml_files = sorted(glob.glob(os.path.join(base, "*/cmip7_awiesm3-veg-hr_*.yaml")))
    for yf in yaml_files:
        all_rules.extend(parse_yaml_rules(yf))

    # Collect CSV counts per directory
    csv_counts = {}
    for d in sorted(glob.glob(os.path.join(base, "*/"))):
        dirname = os.path.basename(d.rstrip("/"))
        n = count_csv_rows(d)
        if n > 0:
            csv_counts[dirname] = n

    # ── Compute per-rule annual data volume ─────────────────────────
    realm_map = {"atmos": "Atmosphere", "land": "Land", "ocean": "Ocean", "seaice": "Sea Ice"}

    print("=" * 90)
    print(f"AWI-ESM3-VEG-HR CMIP7 — Annual Data Volume Estimate")
    print("=" * 90)
    print()

    # Per-realm aggregation
    realm_rules = {}
    realm_bytes = {}
    realm_csv = {"Atmosphere": 0, "Land": 0, "Ocean": 0, "Sea Ice": 0}
    freq_bytes = {}

    for r in all_rules:
        realm_label = realm_map.get(r["realm"], r["realm"])
        gp = GRID_POINTS.get(r["grid"], GRID_POINTS["atm_sfc"])
        ts = TIMESTEPS.get(r["freq"], 12)
        annual_bytes = gp * ts * BYTES_PER_VALUE

        realm_rules.setdefault(realm_label, 0)
        realm_rules[realm_label] += 1
        realm_bytes.setdefault(realm_label, 0)
        realm_bytes[realm_label] += annual_bytes

        freq_bytes.setdefault(r["freq"], 0)
        freq_bytes[r["freq"]] += annual_bytes

    # Map CSV dirs to realms
    dir_realm = {
        "core_atm": "Atmosphere",
        "veg_atm": "Atmosphere",
        "extra_atm": "Atmosphere",
        "cap7_atm": "Atmosphere",
        "core_land": "Land",
        "lrcs_land": "Land",
        "veg_land": "Land",
        "extra_land": "Land",
        "cap7_land": "Land",
        "cap7_aerosol": "Atmosphere",
        "core_ocean": "Ocean",
        "lrcs_ocean": "Ocean",
        "cap7_ocean": "Ocean",
        "core_seaice": "Sea Ice",
        "lrcs_seaice": "Sea Ice",
        "veg_seaice": "Sea Ice",
        "cap7_seaice": "Sea Ice",
    }
    for dirname, count in csv_counts.items():
        rlabel = dir_realm.get(dirname, "Other")
        realm_csv[rlabel] = realm_csv.get(rlabel, 0) + count

    # ── Print realm summary ─────────────────────────────────────────
    print(f"{'Realm':<15} {'Rules':>6} {'CSV vars':>9} {'Coverage':>9} {'Annual size':>14}")
    print("-" * 60)
    total_rules = 0
    total_csv = 0
    total_bytes = 0
    for realm_label in ["Atmosphere", "Land", "Ocean", "Sea Ice"]:
        nr = realm_rules.get(realm_label, 0)
        nc = realm_csv.get(realm_label, 0)
        nb = realm_bytes.get(realm_label, 0)
        cov = f"{nr/nc*100:.0f}%" if nc > 0 else "n/a"
        print(f"{realm_label:<15} {nr:>6} {nc:>9} {cov:>9} {human_size(nb):>14}")
        total_rules += nr
        total_csv += nc
        total_bytes += nb

    print("-" * 60)
    cov_total = f"{total_rules/total_csv*100:.0f}%" if total_csv > 0 else "n/a"
    print(f"{'TOTAL':<15} {total_rules:>6} {total_csv:>9} {cov_total:>9} {human_size(total_bytes):>14}")
    print()

    # ── Print frequency breakdown ───────────────────────────────────
    print(f"{'Frequency':<10} {'Rules':>6} {'Annual size':>14} {'Fraction':>9}")
    print("-" * 45)
    for freq in sorted(freq_bytes, key=lambda f: freq_bytes[f], reverse=True):
        nb = freq_bytes[freq]
        nrules = sum(1 for r in all_rules if r["freq"] == freq)
        frac = nb / total_bytes * 100 if total_bytes > 0 else 0
        print(f"{freq:<10} {nrules:>6} {human_size(nb):>14} {frac:>8.1f}%")
    print()

    # ── Top 20 largest rules ────────────────────────────────────────
    rule_sizes = []
    for r in all_rules:
        gp = GRID_POINTS.get(r["grid"], GRID_POINTS["atm_sfc"])
        ts = TIMESTEPS.get(r["freq"], 12)
        annual_bytes = gp * ts * BYTES_PER_VALUE
        rule_sizes.append((annual_bytes, r))
    rule_sizes.sort(key=lambda x: x[0], reverse=True)

    print(f"Top 50 largest rules (annual):")
    print(
        f"{'#':>3} {'Rule':<35} {'Freq':<6} {'Grid':<10} {'Tier':<10} {'Size':>12}"
    )
    print("-" * 82)
    for i, (nb, r) in enumerate(rule_sizes[:50]):
        tier = r["dir"].split("_")[0]  # core, lrcs, veg, extra
        print(
            f"{i+1:>3} {r['name']:<35} {r['freq']:<6} {r['grid']:<10} {tier:<10} {human_size(nb):>12}"
        )
    print()

    # ── Grid breakdown ──────────────────────────────────────────────
    grid_bytes = {}
    grid_rules = {}
    for r in all_rules:
        gp = GRID_POINTS.get(r["grid"], GRID_POINTS["atm_sfc"])
        ts = TIMESTEPS.get(r["freq"], 12)
        ab = gp * ts * BYTES_PER_VALUE
        grid_bytes.setdefault(r["grid"], 0)
        grid_bytes[r["grid"]] += ab
        grid_rules.setdefault(r["grid"], 0)
        grid_rules[r["grid"]] += 1

    print(f"{'Grid':<12} {'Rules':>6} {'Annual size':>14} {'Fraction':>9}")
    print("-" * 47)
    for g in sorted(grid_bytes, key=lambda g: grid_bytes[g], reverse=True):
        nb = grid_bytes[g]
        frac = nb / total_bytes * 100 if total_bytes > 0 else 0
        print(f"{g:<12} {grid_rules[g]:>6} {human_size(nb):>14} {frac:>8.1f}%")
    print()
    print(f"Total estimated annual volume: {human_size(total_bytes)}")
    print(f"  (uncompressed float32, before NetCDF compression)")
    print(f"  With typical 2-3x NetCDF4/zlib compression: ~{human_size(total_bytes/2.5)}")


if __name__ == "__main__":
    main()
