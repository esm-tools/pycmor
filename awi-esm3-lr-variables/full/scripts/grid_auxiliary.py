"""Explicit, validated LR grid auxiliaries; no guessed HR mask/table reuse."""
import ast
from functools import lru_cache
from pathlib import Path
import re

import netCDF4
import numpy as np

from pycmor.std_lib.basins import basin_flag_mapping, canonical_basin_ids


def read_vtable(path):
    text = Path(path).read_text()
    arrays = []
    for name in ('DVALH', 'DVBH'):
        match = re.search(rf'{name}\(0:\)=(.*?)(?=\bDV[A-Z]+\(0:\)=|/\s*\Z)', text, re.S)
        if not match:
            raise ValueError(f'MISSING_COEFFICIENTS: {name} not found in {path}')
        values = [float(t.replace('D', 'e').replace('d', 'e'))
                  for t in re.split(r'[,\s]+', match.group(1)) if t]
        arrays.append(np.asarray(values))
    a, b = arrays
    if len(a) < 2 or len(a) != len(b) or not np.isfinite(a).all() or not np.isfinite(b).all():
        raise ValueError(f'INVALID_COEFFICIENTS: unequal, empty or nonfinite arrays in {path}')
    if np.any(np.diff(a / 100000.0 + b) <= 0):
        raise ValueError(f'INVALID_COEFFICIENTS: levels not ordered top-to-surface in {path}')
    return a, b


def _coordinate(ds, names):
    for name in names:
        if name in ds.variables:
            variable = ds[name]
            values = np.ma.filled(variable[:], np.nan).astype(float).reshape(-1)
            units = getattr(variable, 'units', '').lower()
            if units in ('radian', 'radians', 'rad'):
                values = np.rad2deg(values)
            if not np.isfinite(values).all():
                raise ValueError(f'INVALID_BASIN_GRID: nonfinite coordinate {name}')
            return values
    raise ValueError(f'UNVERIFIED_BASIN_GRID: missing coordinate among {names}')


@lru_cache(maxsize=8)
def _validate_basin_cached(path, mesh, expected_nodes, mask_mtime, mesh_mtime):
    with netCDF4.Dataset(path) as mask, netCDF4.Dataset(mesh) as grid:
        if 'basin' not in mask.variables:
            raise ValueError('INVALID_BASIN_MASK: missing basin variable')
        values = np.ma.asarray(mask['basin'][:]).reshape(-1)
        if values.size != expected_nodes or np.ma.getmaskarray(values).any():
            raise ValueError(f'INVALID_BASIN_MASK: expected {expected_nodes} unmasked node IDs')
        values = np.asarray(values)
        if not np.isfinite(values).all() or not np.equal(values, np.round(values)).all():
            raise ValueError('INVALID_BASIN_MASK: IDs must be finite integers')
        lat = _coordinate(mask, ('lat', 'latitude', 'clat'))
        lon = _coordinate(mask, ('lon', 'longitude', 'clon'))
        ref_lat = _coordinate(grid, ('lat', 'latitude'))
        ref_lon = _coordinate(grid, ('lon', 'longitude'))
        if any(v.size != expected_nodes for v in (lat, lon, ref_lat, ref_lon)):
            raise ValueError('INVALID_BASIN_GRID: coordinate/node count mismatch')
        if (not np.allclose(lat, ref_lat, atol=1e-6, rtol=0)
                or not np.allclose((lon - ref_lon + 180) % 360 - 180, 0, atol=1e-6, rtol=0)):
            raise ValueError('INVALID_BASIN_GRID: coordinates or node order differ from CORE3 mesh')
        flags = getattr(mask['basin'], 'flag_values', None)
        meanings = getattr(mask['basin'], 'flag_meanings', None)
        # Validate meanings and coverage, not an assumed numeric encoding.
        # Diagnostics use this SAME decoder before aggregating the basins.
        observed = basin_flag_mapping(flags, meanings)
        canonical_basin_ids(values, flags, meanings)
    return dict(path=path, mesh=mesh, nodes=expected_nodes, flags=observed)


def validate_basin_mask(path, mesh, expected_nodes=220509):
    path, mesh = Path(path), Path(mesh)
    if not path.is_file():
        raise ValueError(f'MISSING_BASIN_MASK: supply a validated CORE3 basin_mask_file; absent {path}')
    if not mesh.is_file():
        raise ValueError(f'MISSING_BASIN_REFERENCE: {mesh}')
    return _validate_basin_cached(str(path.resolve()), str(mesh.resolve()), expected_nodes,
                                  path.stat().st_mtime_ns, mesh.stat().st_mtime_ns)


@lru_cache(maxsize=8)
def _basin_readers(path, mtime):
    tree = ast.parse(Path(path).read_text())
    functions = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    readers = {name for name, node in functions.items() if any(
        isinstance(n, ast.Constant) and n.value == 'basin_mask_file' for n in ast.walk(node))}
    calls = {name: {n.func.id for n in ast.walk(node) if isinstance(n, ast.Call)
                   and isinstance(n.func, ast.Name)} for name, node in functions.items()}
    while True:
        expanded = readers | {name for name, children in calls.items() if children & readers}
        if expanded == readers:
            return readers
        readers = expanded


def configure_grid_auxiliaries(doc, config):
    """Configure ONLY the auxiliary files consumed by this one-rule recipe.

    Config: model_level_table (path), basin_mask_file (path), mesh_dir (path).
    No site-specific table discovery is hidden here. The caller may choose a
    site's vtable_L91 explicitly; actual input levels are checked on execution.
    A *_file alias makes the coefficient table part of dependency fingerprinting.
    """
    rule = doc['rules'][0]
    selected = set(rule.get('pipelines', []))
    steps = [step for pipeline in doc.get('pipelines', []) if pipeline['name'] in selected
             for step in pipeline.get('steps', [])]
    hybrid = any(step.endswith(':add_hybrid_sigma_coordinate') for step in steps)
    if hybrid:
        effective = dict(doc.get('inherit', {}), **rule)
        table = config.get('model_level_table') or effective.get('oifs_vtable')
        if not table or not Path(table).is_file():
            raise ValueError('MISSING_MODEL_LEVEL_TABLE: configure model_level_table for the actual LR vertical grid')
        a, b = read_vtable(table)
        rule['oifs_vtable'] = str(Path(table).resolve())
        rule['model_level_table_file'] = rule['oifs_vtable']
        rule['configured_model_levels'] = len(a) - 1
    needs_basin = False
    loads_basin = False
    density_total = False
    for step in steps:
        if step.startswith('script://') and ':' in step[9:]:
            path, symbol = step[9:].rsplit(':', 1)
            p = Path(path)
            if p.name == 'custom_steps.py' and p.is_file():
                needs_basin |= symbol in _basin_readers(str(p), p.stat().st_mtime_ns)
                loads_basin |= symbol == 'load_basin_mask'
                density_total |= symbol == 'compute_msftm_density'
    if needs_basin:
        effective = dict(doc.get('inherit', {}), **rule)
        mask = config.get('basin_mask_file') or effective.get('basin_mask_file')
        if not mask:
            raise ValueError('MISSING_BASIN_MASK: configure basin_mask_file for CORE3')
        mesh = Path(config['mesh_dir']) / 'mesh.nc'
        validate_basin_mask(mask, mesh)
        rule['basin_mask_file'] = str(Path(mask).resolve())
        rule['basin_reference_file'] = str(mesh.resolve())
        if loads_basin:
            # The fx basin reader consumes the mask directly, but its recipe
            # also lists it as a primary input for discovery/fingerprinting.
            # Keep both paths identical when a mask lives outside mesh_dir.
            # Other basin-using diagnostics must retain their ocean inputs.
            mask_path = Path(rule['basin_mask_file'])
            rule['inputs'] = [dict(path=str(mask_path.parent), pattern=re.escape(mask_path.name))]
    if density_total:
        # The LR density-space total includes GM bolus transport. Pin that
        # secondary input just like the resolved divergence: runtime discovery
        # must not silently add/remove a contribution after the plan is frozen.
        effective = dict(doc.get('inherit', {}), **rule)
        year = effective.get('year_start', effective.get('year'))
        last = effective.get('year_end', year)
        if year is None or int(last) != int(year) or not effective.get('data_path'):
            raise ValueError('UNRESOLVED_DENSITY_BOLUS: expected one year and data_path')
        bolus = Path(effective['data_path']) / f'std_dens_DIVbolus.fesom.{int(year):04d}.nc'
        if not bolus.is_file():
            raise ValueError(f'MISSING_DENSITY_BOLUS: LR total overturning requires {bolus}; '
                             'do not silently substitute resolved-only overturning')
        rule['msftm_bolus_file'] = str(bolus.resolve())
    # Annual XIOS instantaneous files may end exactly at next Jan 1 00:00.
    # Keep that genuine point, but no later/interval-midpoint spill. The full
    # planner separately checks exact sample count, spacing and pinned files.
    brand = rule['compound_name'].split('.')[2]
    if brand.startswith('tpt-'):
        rule['retain_year_end_point'] = True
    return doc
