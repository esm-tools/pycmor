"""Independent iceberg value audit; no pycmor/worker-transformation imports."""
from functools import lru_cache
import netCDF4
import numpy as np


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
                    dl = (np.asarray(ds['lon'][:]) - lon + 180) % 360 - 180
                    if not np.allclose(dl, 0, rtol=0, atol=1e-4) or not np.allclose(ds['lat'][:], lat, rtol=0, atol=1e-4):
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


def sample_freshwater(rule, year, month, node):
    return expected_month(rule, year, month)[0][node]


def validate_output(rule, dataset, variable, decoded):
    """Check every native cell in first/last output months, including cavities.

    This is in addition to generic sampled-value checks, which might otherwise
    happen to select no iceberg-active or negative-freezing cells.
    """
    if variable.ndim != 2 or variable.dimensions[0] != 'time':
        raise ValueError('ICEBERG_QC: expected time by native-cell output')
    if getattr(variable, 'units', '').replace(' ', '').replace('**', '').replace('^', '') != 'kgm-2s-1':
        raise ValueError('ICEBERG_QC: unexpected output units')
    checks = []
    for ti in sorted({0, len(decoded) - 1}):
        date = decoded[ti]
        expected, cavity, lon, lat = expected_month(rule, date.year, date.month)
        actual = np.ma.asarray(variable[ti, :], dtype=float)
        if actual.shape != expected.shape or np.ma.getmaskarray(actual).any() or not np.isfinite(actual).all():
            raise ValueError('ICEBERG_QC: finite native flux was masked or invalid')
        for choices, reference in ((('lon', 'longitude'), lon), (('lat', 'latitude'), lat)):
            name = next((n for n in choices if n in dataset.variables), None)
            if name is None:
                raise ValueError('ICEBERG_QC: output node coordinates missing')
            delta = np.asarray(dataset[name][:]) - reference
            if name in ('lon', 'longitude'):
                delta = (delta + 180) % 360 - 180
            if not np.allclose(delta, 0, rtol=0, atol=1e-4):
                raise ValueError('ICEBERG_QC: output node coordinates changed')
        if not np.allclose(actual, expected, rtol=3e-5, atol=1e-30):
            raise ValueError('ICEBERG_QC: output differs from independent four-term sum')
        if np.any(actual[cavity] != 0):
            raise ValueError('ICEBERG_QC: under-shelf iceberg source is not exactly zero')
        checks.append(dict(year=date.year, month=date.month, nodes=len(expected),
                           cavity_zero_nodes=int(cavity.sum()),
                           negative_freezing_nodes=int((expected < 0).sum()),
                           nonzero_nodes=int(np.count_nonzero(expected))))
    return {'check': 'independent full-surface iceberg sum, cavity-zero and signed-freezing checks', 'months': checks}
