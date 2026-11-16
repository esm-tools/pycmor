"""Independent iceberg value audit; no pycmor/worker-transformation imports."""
from functools import lru_cache
import netCDF4
import numpy as np

# Node positions are compared on the sphere: XIOS float32 coordinates differ
# from mesh.nc by up to 0.003 deg near the pole, the minimum CORE3 node
# spacing is 0.069 deg. Kept independent of the planner and worker modules.
NODE_TOLERANCE_DEG = 0.01


def _same_nodes(lon, lat, reference_lon, reference_lat):
    lon, lat, rlon, rlat = (np.deg2rad(np.ma.filled(np.ma.asarray(x, dtype=float), np.nan))
                            for x in (lon, lat, reference_lon, reference_lat))
    if lon.shape != rlon.shape or lat.shape != rlat.shape:
        return False
    h = np.sin((lat - rlat) / 2) ** 2 + np.cos(lat) * np.cos(rlat) * np.sin((lon - rlon) / 2) ** 2
    offset = np.rad2deg(2 * np.arcsin(np.sqrt(np.clip(h, 0, 1))))
    return bool(np.isfinite(offset).all() and offset.max(initial=0) <= NODE_TOLERANCE_DEG)


@lru_cache(maxsize=4)
def _wet_layers(grid_file):
    """Index range [cav_nod_lev, depth_lev) of ocean layers at every node."""
    with netCDF4.Dataset(grid_file) as mesh:
        levels = len(mesh['depth'])
        top, bottom = (np.asarray(mesh[n][:]).astype(int) for n in ('cav_nod_lev', 'depth_lev'))
    if (top < 0).any() or (top >= bottom).any() or (bottom > levels).any():
        raise ValueError('ICEBERG_QC: invalid wet-layer range')
    return top, bottom, levels


@lru_cache(maxsize=4)
def _expected_month(grid_file, terms, year, month):
    with netCDF4.Dataset(grid_file) as mesh:
        lon = np.asarray(mesh['lon'][:])
        lat = np.asarray(mesh['lat'][:])
        cavity = np.asarray(mesh['cav_nod_mask'][:])
        if (lon.ndim != 1 or lat.shape != lon.shape or cavity.shape != lon.shape
                or not all(np.isfinite(x).all() for x in (lon, lat, cavity))
                or not np.isin(cavity, [0, 1]).all()):
            raise ValueError('ICEBERG_QC: invalid native coordinates/cavity mask')
        cavity = cavity == 1
    if sorted(t[0] for t in terms) != sorted(['ibfwb', 'ibfwbv', 'ibfwl', 'ibfwe']):
        raise ValueError('ICEBERG_QC: expected four distinct source terms')
    total = np.zeros(len(lon), dtype=np.float64)
    for variable, files in terms:
        matches = []
        for path in files:
            with netCDF4.Dataset(path) as ds:
                t = ds['time']
                dates = netCDF4.num2date(t[:], t.units, getattr(t, 'calendar', 'standard'))
                indices = [i for i, d in enumerate(dates) if (d.year, d.month) == (year, month)]
                for ti in indices:
                    field = ds[variable]
                    if field.ndim != 2 or field.shape[1] != len(lon) or getattr(field, 'units', '') != 'm s-1':
                        raise ValueError('ICEBERG_QC: wrong source dimensions/units')
                    if not _same_nodes(ds['lon'][:], ds['lat'][:], lon, lat):
                        raise ValueError('ICEBERG_QC: source/mask coordinates differ')
                    matches.append(np.ma.asarray(field[ti, :], dtype=float).filled(np.nan))
        if len(matches) != 1:
            raise ValueError('ICEBERG_QC: absent or duplicated source month')
        values = matches[0]
        if not np.isfinite(values[~cavity]).all():
            raise ValueError('ICEBERG_QC: exposed-ocean source missing')
        inside = values[cavity]
        if np.isinf(inside).any() or (np.isfinite(inside) & (inside != 0)).any():
            raise ValueError('ICEBERG_QC: source has nonzero/infinite cavity flux')
        # Distinct from worker implementation: accumulation only on exposed
        # nodes, never a generic fillna; signed freezing is retained.
        total[~cavity] += values[~cavity] * 1000.0
    return total, cavity, lon, lat


def expected_month(rule, year, month):
    terms = tuple((t['variable'], tuple(t['files'])) for t in rule['lr_terms'])
    return _expected_month(str(rule['grid_file']), terms, year, month)


def sample_freshwater(rule, year, month, node, level=0):
    total = expected_month(rule, year, month)[0][node]
    if not rule.get('lr_iceberg_layers'):
        return total
    top, bottom, _ = _wet_layers(str(rule['grid_file']))
    if not top[node] <= level < bottom[node]:
        return np.ma.masked
    return total if level == top[node] else 0.


def validate_output(rule, dataset, variable, decoded):
    """Check every native cell in first/last output months, including cavities.

    This is in addition to generic sampled-value checks, which might otherwise
    happen to select no iceberg-active or negative-freezing cells.
    """
    layered = bool(rule.get('lr_iceberg_layers'))
    if variable.ndim != 2 + layered or variable.dimensions[0] != 'time':
        raise ValueError('ICEBERG_QC: expected time by native-cell (by layer) output')
    if getattr(variable, 'units', '').replace(' ', '').replace('**', '').replace('^', '') != 'kgm-2s-1':
        raise ValueError('ICEBERG_QC: unexpected output units')
    checks = []
    for ti in sorted({0, len(decoded) - 1}):
        date = decoded[ti]
        expected, cavity, lon, lat = expected_month(rule, date.year, date.month)
        actual = np.ma.asarray(variable[ti], dtype=float)
        if layered:
            # Every wet layer present, every dry layer missing, the whole
            # column flux in the top wet layer and exactly zero below it.
            if len(dataset.dimensions[variable.dimensions[1]]) != len(expected):
                actual = actual.T
            top, bottom, levels = _wet_layers(str(rule['grid_file']))
            level = np.arange(levels)
            if actual.shape != (len(expected), levels):
                raise ValueError('ICEBERG_QC: unexpected node/layer output shape')
            wet = (level >= top[:, None]) & (level < bottom[:, None])
            surface = level == top[:, None]
            missing = np.ma.getmaskarray(actual) | ~np.isfinite(actual.filled(np.nan))
            if missing[wet].any() or not missing[~wet].all():
                raise ValueError('ICEBERG_QC: wet layers missing or dry layers filled')
            if np.any(actual.filled(0)[wet & ~surface] != 0):
                raise ValueError('ICEBERG_QC: nonzero flux below the top wet layer')
            actual = actual[surface]
        if actual.shape != expected.shape or np.ma.getmaskarray(actual).any() or not np.isfinite(actual).all():
            raise ValueError('ICEBERG_QC: finite native flux was masked or invalid')
        names = [next((n for n in choices if n in dataset.variables), None)
                 for choices in (('lon', 'longitude'), ('lat', 'latitude'))]
        if None in names:
            raise ValueError('ICEBERG_QC: output node coordinates missing')
        if not _same_nodes(dataset[names[0]][:], dataset[names[1]][:], lon, lat):
            raise ValueError('ICEBERG_QC: output node coordinates changed')
        if not np.allclose(actual, expected, rtol=3e-5, atol=1e-30):
            raise ValueError('ICEBERG_QC: output differs from independent four-term sum')
        if np.any(actual[cavity] != 0):
            raise ValueError('ICEBERG_QC: under-shelf iceberg source is not exactly zero')
        checks.append(dict(year=date.year, month=date.month, nodes=len(expected),
                           cavity_zero_nodes=int(cavity.sum()),
                           negative_freezing_nodes=int((expected < 0).sum()),
                           nonzero_nodes=int(np.count_nonzero(expected))))
    check = 'independent full-surface iceberg sum, cavity-zero and signed-freezing checks'
    if layered:
        check += '; full sum in the top wet layer, zero below, dry layers missing'
    return {'check': check, 'months': checks}
