#!/usr/bin/env python
"""Measure compressed bytes per value in the cmorized AWI-ESM3-VEG-HR year.

Reads only HDF5 metadata (h5py storage sizes, shapes, attributes), no data.
For every file it records:
  - main variable storage size (compressed bytes on disk, from HDF5)
  - number of values of the main variable (product of its shape)
  - file size (stat) and the per-file overhead (file size minus main variable)
  - realm / frequency / branding / region from the global attributes / path
and assigns a rough size class (realm + rank + frequency; "1d" = one horizontal dimension, i.e. a 2D field on
an unstructured or reduced Gaussian grid). The rough classes in hr_bytes_per_value_classes.csv are an overview
only; estimate_lr_volume.py defines the classes it uses (CLASS_DEFS) directly from the per-file CSV.

Output: hr_bytes_per_value_measured.csv (one row per file) and
        hr_bytes_per_value_classes.csv  (aggregated per class) next to this script.

Usage:  python measure_hr_bytes_per_value.py [HR_ROOT]
"""
import csv
import os
import sys
from collections import defaultdict

import h5py
import numpy as np

HR_ROOT = "/scratch/a/a270092/pycmor_hr/cli123/cmorized/MIP-DRS7"
HERE = os.path.dirname(os.path.abspath(__file__))

COORD_NAMES = {
    "time", "time_bnds", "lat", "lon", "lat_bnds", "lon_bnds", "latitude", "longitude",
    "vertices_latitude", "vertices_longitude", "lev", "lev_bnds", "plev", "plev_bnds",
    "height", "depth", "quantization_info", "bnds", "nod2", "elem", "vertices",
}


def attr(obj, name, default=""):
    v = obj.attrs.get(name, default)
    if isinstance(v, bytes):
        v = v.decode()
    if isinstance(v, np.ndarray):
        v = v.tolist()
        if isinstance(v, list) and len(v) == 1:
            v = v[0]
        if isinstance(v, bytes):
            v = v.decode()
    return str(v)


def classify(realm, freq, shape, dims_hint):
    """Size class from realm, frequency and the main variable rank."""
    realm = realm.split()[0] if realm else "?"
    if realm in ("ocean", "ocnBgchem"):
        r = "ocean"
    elif realm in ("seaIce",):
        r = "seaIce"
    elif realm in ("atmos", "atmosChem", "aerosol"):
        r = "atmos"
    elif realm in ("land", "landIce"):
        r = "land"
    else:
        r = realm
    # spatial rank: drop the time dimension if present
    nd = len(shape) - (1 if freq != "fx" else 0)
    sp = "3d" if nd >= 3 or (nd == 2 and dims_hint == "unstr3d") else "2d"
    if nd <= 1 and dims_hint != "unstr":
        sp = "1d"
    return f"{r}{sp}_{freq}"


def main(root):
    rows = []
    for dirpath, _, files in os.walk(root):
        for f in files:
            if not f.endswith(".nc"):
                continue
            path = os.path.join(dirpath, f)
            var = f.split("_")[0]
            try:
                with h5py.File(path, "r") as h:
                    if var not in h:
                        continue
                    d = h[var]
                    shape = d.shape
                    nval = int(np.prod(shape))
                    store = d.id.get_storage_size()
                    realm = attr(h, "realm")
                    freq = attr(h, "frequency")
                    region = attr(h, "region")
                    branding = attr(h, "branding_suffix")
                    grid = attr(h, "grid_label")
                    dimnames = [dd.name.split("/")[-1] if dd.name else "" for dd in []]
                    # unstructured ocean: 'nod2' or 'elem' dimension present
                    unstr = ("nod2" in h) or ("elem" in h)
                    hint = ""
                    if unstr:
                        hint = "unstr3d" if (len(shape) - (1 if freq != "fx" else 0)) >= 2 else "unstr"
                    coord_store = 0
                    for k in h.keys():
                        if k != var and isinstance(h[k], h5py.Dataset):
                            coord_store += h[k].id.get_storage_size()
            except Exception as e:  # noqa
                print("skip", path, e, file=sys.stderr)
                continue
            fsize = os.stat(path).st_size
            cls = classify(realm, freq, shape, hint)
            rows.append(dict(
                file=os.path.relpath(path, root), var=var, realm=realm, frequency=freq, region=region,
                branding=branding, grid_label=grid, shape="x".join(map(str, shape)), n_values=nval,
                var_storage_bytes=store, bytes_per_value=store / nval if nval else 0,
                coord_storage_bytes=coord_store, file_bytes=fsize,
                overhead_bytes=fsize - store, file_bytes_per_value=fsize / nval if nval else 0,
                size_class=cls, unstructured=int(unstr),
            ))
    rows.sort(key=lambda r: (r["size_class"], r["var"]))
    out = os.path.join(HERE, "hr_bytes_per_value_measured.csv")
    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    # aggregate per class: value-weighted mean (sum storage / sum values) and median of per-file ratios
    agg = defaultdict(list)
    for r in rows:
        agg[r["size_class"]].append(r)
    out2 = os.path.join(HERE, "hr_bytes_per_value_classes.csv")
    with open(out2, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["size_class", "n_files", "sum_values", "sum_var_storage_bytes", "weighted_bytes_per_value",
                    "median_bytes_per_value", "p10_bytes_per_value", "p90_bytes_per_value",
                    "median_overhead_bytes", "unstructured_files", "example_vars"])
        for c, rs in sorted(agg.items()):
            sv = sum(r["n_values"] for r in rs)
            ss = sum(r["var_storage_bytes"] for r in rs)
            bpv = np.array([r["bytes_per_value"] for r in rs])
            ov = np.median([r["overhead_bytes"] for r in rs])
            w.writerow([c, len(rs), sv, ss, f"{ss / sv:.4f}", f"{np.median(bpv):.4f}",
                        f"{np.percentile(bpv, 10):.4f}", f"{np.percentile(bpv, 90):.4f}", int(ov),
                        sum(r["unstructured"] for r in rs), " ".join(sorted({r["var"] for r in rs})[:25])])
    print("wrote", out, out2, len(rows), "files")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else HR_ROOT)
