#!/usr/bin/env python3
"""Full-catalog LR/CORE3 conversion: completed leg -> all supported recipes.

Includes the complete cmip7xml catalog, raw-output fallbacks and audited
REcoM/tracer rules. Missing rules/data are recorded, never declared success.
No model output, restart, runscript or upstream pycmor source is modified.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import copy
import csv
from datetime import datetime, timezone
import fcntl
from functools import lru_cache
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import traceback

import netCDF4
import numpy as np
import yaml

ROOT = Path(__file__).resolve().parent.parent
REPO = ROOT.parents[1]
RULES = ROOT / 'rules'
PERIOD = re.compile(r'\d{8}-\d{8}')
SERIAL = dict(enable_output_subdirs=True, warn_on_no_rule=True, parallel=False,
              pipeline_workflow_orchestrator='native', enable_dask=False)


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    sys.modules[name] = result
    spec.loader.exec_module(result)
    return result


@lru_cache(maxsize=16)
def support_module(name):
    """Load helpers from the checkout, or the campaign's exact frozen copy."""
    paths = (ROOT/'scripts'/f'{name}.py',
             Path(__file__).parent/'full_workflow/scripts'/f'{name}.py',
             Path(__file__).parent.parent/'reproduce/full_workflow/scripts'/f'{name}.py')
    for path in paths:
        if path.is_file():
            return module(path, f'full_support_{name}')
    raise ValueError(f'Missing full-workflow support module: {name}')


def read(path):
    return json.loads(Path(path).read_text())


def load_config(path):
    """Resolve site settings outside the source checkout; never borrow a user's paths."""
    if path is None:
        raise ValueError('Pass --config FILE or set PYCMOR_LR_CONFIG; see config/example.json')
    path = Path(path).expanduser().resolve(strict=True)
    config = read(path)
    defaults = dict(repo=str(REPO), python=sys.executable, commit=None,
        allow_dirty=False, vector_frame='auto', known_rotated_fesom_sha256=[],
        walltime='12:00:00', memory='48G', shards=24, concurrency=2,
        product_timeout_seconds=7200, validation_timeout_seconds=1800,
        metadata_overrides={}, publication_ready=False, leg='latest')
    for key, value in defaults.items():
        config.setdefault(key, value)
    required = {'metadata', 'cv_dir', 'mesh_dir', 'ghg_dir', 'results', 'esgvoc_home'}
    if missing := required - config.keys():
        raise ValueError(f'Missing configuration settings: {sorted(missing)}')
    for key in ('repo', 'python', *sorted(required), 'experiment',
                'model_level_table', 'basin_mask_file', 'hr_reference_manifest',
                'cmip7_tables_path', 'aicc_grid_config', 'source_audit_report'):
        if config.get(key):
            value = Path(os.path.expandvars(config[key])).expanduser()
            # Resolving a venv's python symlink would bypass that environment.
            config[key] = (os.path.abspath(path.parent / value) if key == 'python'
                           else str((path.parent / value).resolve()))
    if Path(config['repo']) != REPO:
        raise ValueError('repo must identify the checkout containing this launcher')
    output = Path(config['results'])
    if output == REPO or REPO in output.parents or output in REPO.parents:
        raise ValueError('results must be outside the source repository and not its ancestor')
    if config['vector_frame'] not in ('auto', 'rotated', 'geographic'):
        raise ValueError('vector_frame must be auto, rotated or geographic')
    for key in ('shards', 'concurrency', 'product_timeout_seconds', 'validation_timeout_seconds'):
        if not isinstance(config[key], int) or isinstance(config[key], bool) or config[key] < 1:
            raise ValueError(f'{key} must be a positive integer')
    if not isinstance(config['allow_dirty'], bool):
        raise ValueError('allow_dirty must be a JSON boolean')
    if 'aicc' in config.get('qc_tests', []):
        tables = Path(config.get('cmip7_tables_path', ''))
        if not tables.is_dir() or not list(tables.glob('CMIP7_*.json')):
            raise ValueError('AICC requires a pinned cmip7_tables_path with CMIP7_*.json')
        if not Path(config.get('aicc_grid_config', '')).is_file():
            raise ValueError('AICC requires aicc_grid_config')
    window = config.get('decadal_window')
    if window is not None and (not isinstance(window, list) or len(window) != 2
            or any(type(y) is not int for y in window) or window[1]-window[0] != 9):
        raise ValueError('decadal_window must be null or [first_year, last_year] covering exactly ten years')
    return config


def code_manifest(repo):
    """Hash reusable source, including new uncommitted files, not generated data."""
    repo = Path(repo)
    files = []
    suffixes = {'.py', '.yaml', '.yml', '.json', '.sh', '.csv', '.txt', '.rst', '.lock'}
    for directory in ('src/pycmor', 'examples', 'awi-esm3-lr-variables', 'awi-esm3-veg-hr-variables'):
        for path in (repo / directory).rglob('*'):
            if path.is_file() and '__pycache__' not in path.parts and (path.suffix in suffixes or path.name in ('cmorize', 'run-tests')):
                files.append(path)
    files.append(repo / 'pyproject.toml')
    return {str(p.relative_to(repo)): digest(p) for p in sorted(set(files))}


def verify_campaign(campaign):
    """Allow reviewed development snapshots, but never mix revisions within one run."""
    signature = read(Path(campaign) / 'plan.json')['signature']
    config = signature['config']
    head = subprocess.check_output(['git', '-C', config['repo'], 'rev-parse', 'HEAD'], text=True).strip()
    if head != config['commit'] or code_manifest(config['repo']) != signature['source_files']:
        raise ValueError('Pinned pycmor/workflow changed since planning; use a new output root after review')
    for path, expected in signature['metadata_files'].items():
        if digest(path) != expected:
            raise ValueError(f'Pinned metadata changed: {path}')
    for relative, expected in signature.get('snapshot_files', {}).items():
        if digest(Path(campaign) / relative) != expected:
            raise ValueError(f'Frozen reproduction code changed: {relative}')
    return config


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f'.{os.getpid()}.tmp')
    temporary.write_text(json.dumps(value, indent=2, default=str) + '\n')
    temporary.replace(path)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for data in iter(lambda: stream.read(4*1024*1024), b''):
            h.update(data)
    return h.hexdigest()


def stamp():
    return datetime.now(timezone.utc).isoformat()


def file_state(paths):
    return {str(p): [Path(p).stat().st_size, Path(p).stat().st_mtime_ns]
            for p in sorted(set(map(str, paths)))}


def select_leg(experiment, requested):
    selector = module(ROOT/'scripts/select_latest_completed.py', 'full_selector')
    if requested == 'latest':
        result = selector.discover_latest(experiment)
    else:
        if not PERIOD.fullmatch(requested):
            raise ValueError('--leg must be YYYYMMDD-YYYYMMDD or latest')
        log = selector.last_attempt(experiment, requested, 'compute')
        if log is None or not selector.successful(state := selector.job_status(log)):
            raise ValueError(f'{requested}: latest compute attempt is not verified successful')
        date = f'{requested[:4]}-{requested[4:6]}-{requested[6:8]}'
        progress = '\n'.join(p.read_text(errors='replace') for p in (experiment/'log').glob('*_awiesm3.log'))
        if not re.search(r'\|\s*observe_compute\s+\d+\s+'+date+r'T\S+\s+\d+\s+-\s+done\s*$', progress, re.M):
            raise ValueError('No completed compute observer for selected leg')
        result = dict(period=requested, compute=state)
    period = result['period']
    if period[4:8] != '0101' or period[13:] != '1231':
        raise ValueError('Only complete calendar-year legs are supported')
    log = selector.last_attempt(experiment, period, 'Combine_LPJG')
    if log is None or not selector.successful(merge := selector.job_status(log)):
        raise ValueError(f'{period}: LPJG merge has not completed; wait, do not use an older leg')
    if 'Successfully converted LPJ-GUESS output to nc' not in selector.tail(log):
        raise ValueError('LPJG merge completion marker missing')
    result['merge'] = merge
    result['selected_at_utc'] = stamp()
    return result


def identify(experiment, period, config):
    """Reject a different grid, not just a similar experiment directory name."""
    work = experiment/f'run_{period}'/'work'
    gridlist = work/'ece_gridlist_TCO95.txt'
    if not gridlist.is_file():
        raise ValueError(f'Missing staged TCO95 gridlist: {gridlist}; no guessed grid')
    with netCDF4.Dataset(work/'grids.nc') as ds:
        if 'TCO95-land.lon' not in ds.variables:
            raise ValueError('This command supports TCO95-land / CORE3, not HR or another mask')
    frame = config['vector_frame']
    binary = experiment/'bin/fesom/lib/fesom/libfesom.so'
    sha = digest(binary) if binary.is_file() else None
    if frame == 'auto':
        frame = 'rotated' if sha in config['known_rotated_fesom_sha256'] else 'unknown'
    if frame not in ('rotated', 'geographic', 'unknown'):
        raise ValueError('vector_frame must be auto, rotated or geographic')
    # CORE3 dimensions are checked against actual archived output.
    year = period[:4]
    files = list((experiment/'outdata/fesom').glob(f'temp.fesom.{year}.nc'))
    if not files:
        raise ValueError('No FESOM temp file to verify CORE3 grid')
    with netCDF4.Dataset(files[0]) as ds:
        if 220509 not in ds['temp'].shape:
            raise ValueError('FESOM data is not on the audited 220509-node CORE3 mesh')
    return dict(grid='TCO95-land / CORE3', vector_frame=frame, fesom_sha256=sha)


def remap_strings(value, replacements):
    if isinstance(value, dict):
        return {k: remap_strings(v, replacements) for k, v in value.items()}
    if isinstance(value, list):
        return [remap_strings(v, replacements) for v in value]
    if isinstance(value, str):
        for a, b in replacements:
            value = value.replace(a, b)
    return value


@lru_cache(maxsize=128)
def directory_files(base):
    return tuple(Path(base).iterdir())


@lru_cache(maxsize=8192)
def matching(base, pattern):
    base = Path(base)
    if not base.is_dir():
        return ()
    if '.out' in pattern:
        return tuple(sorted(base.glob(pattern)))
    regex = re.compile(pattern)
    return tuple(sorted(p for p in directory_files(str(base)) if regex.fullmatch(p.name) and p.is_file()))


@lru_cache(maxsize=4096)
def nc_header(path):
    with netCDF4.Dataset(path) as ds:
        time = next((ds[k] for k in ('time','time_counter') if k in ds.variables), None)
        result = dict(variables=list(ds.variables), dimensions={k: len(v) for k, v in ds.dimensions.items()},
                      units={k:getattr(v,'units',None) for k,v in ds.variables.items()},
                      shapes={k:list(v.shape) for k,v in ds.variables.items()})
        if time is not None:
            dates = netCDF4.num2date(time[:], time.units, getattr(time, 'calendar', 'standard'))
            date_tuple = support_module('input_streams').date_tuple
            result['dates'] = [date_tuple(d) for d in dates]
            result['calendar'] = getattr(time, 'calendar', 'standard')
            bounds = getattr(time, 'bounds', None)
            result['bounds'] = None
            if bounds and bounds in ds.variables:
                b = netCDF4.num2date(ds[bounds][:], time.units, result['calendar'])
                result['bounds'] = [(date_tuple(a), date_tuple(z)) for a,z in b]
        return result


def cover_dates(dates, year, frequency, calendar='standard'):
    if not dates:
        raise ValueError('Missing time records')
    if len(set(dates)) != len(dates) or sorted(dates) != dates:
        raise ValueError('Duplicate or non-monotonic timestamps')
    if frequency == 'fx':
        return
    if frequency == 'mon':
        if [(d[0], d[1]) for d in dates] != [(year,m) for m in range(1,13)]:
            raise ValueError('Expected exactly January-December, without gaps')
    elif frequency == 'yr':
        if len(dates) != 1 or dates[0][0] != year:
            raise ValueError('Expected one annual record in the selected year')
    elif frequency in ('day','1hr','3hr','6hr'):
        import cftime
        hours = {'day':24, '1hr':1, '3hr':3, '6hr':6}[frequency]
        units = f'hours since {year}-01-01'
        end = cftime.date2num(cftime.datetime(year+1,1,1,calendar=calendar),units,calendar)
        nums = cftime.date2num([cftime.datetime(*d,calendar=calendar) for d in dates],units,calendar)
        if len(nums) != round(end/hours) or not np.allclose(np.diff(nums),hours):
            raise ValueError(f'Incomplete {frequency} coverage')
        if not (-1e-8 <= nums[0] <= hours+1e-8 and end-hours-1e-8 <= nums[-1] <= end+1e-8):
            raise ValueError('Time range is not the requested calendar year')
    else:
        raise ValueError(f'No verified coverage checker for {frequency}')


@lru_cache(maxsize=16)
def custom_steps_reading_option(path, option):
    """Conservative transitive call graph for the pinned custom_steps module."""
    tree = ast.parse(Path(path).read_text())
    functions = {n.name:n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    needed = {name for name,node in functions.items() if any(
        isinstance(n, ast.Constant) and n.value == option for n in ast.walk(node))}
    calls = {name:{n.func.id for n in ast.walk(node) if isinstance(n,ast.Call)
                  and isinstance(n.func,ast.Name)} for name,node in functions.items()}
    while True:
        expanded = needed | {name for name,children in calls.items() if children & needed}
        if expanded == needed:
            return needed, set(functions)
        needed = expanded


def prune_unused_inherited_basin_mask(doc):
    # LRCS puts basin_mask_file in inherit for ALL rules, although only a few
    # pipelines use it. Do not block temperature/volume/salinity on that file.
    # Explicit per-rule masks and unknown/custom pipelines remain mandatory.
    r = doc['rules'][0]
    if 'basin_mask_file' in r or 'basin_mask_file' not in doc['inherit']:
        return
    selected = set(r.get('pipelines', []))
    pipelines = [p for p in doc.get('pipelines', []) if p['name'] in selected]
    if len(pipelines) != len(selected):
        return
    for pipeline in pipelines:
        if pipeline.get('uses') not in (None, 'pycmor.core.pipeline.DefaultPipeline'):
            return
        for step in pipeline.get('steps', []):
            if step.startswith('pycmor.'):
                continue
            if not step.startswith('script://') or ':' not in step[9:]:
                return
            path, name = step[9:].rsplit(':', 1)
            if Path(path).name != 'custom_steps.py':
                return
            needed, known = custom_steps_reading_option(path, 'basin_mask_file')
            if name not in known or name in needed:
                return
    doc['inherit'].pop('basin_mask_file')


def dependencies(doc, year, repointer):
    """Pin primary AND secondary inputs; no wildcard may read another leg."""
    from input_streams import validate_input_streams, validate_previous_day
    prune_unused_inherited_basin_mask(doc)
    r = doc['rules'][0]
    if (r['compound_name'].split('.')[1] == 'dgw'
            and r.get('model_variable') == 'swvl4'):
        raise ValueError('UNRESOLVED_GROUNDWATER_MAPPING: swvl4 is volumetric soil water '
                         'in layer 4, not a diagnosed groundwater reservoir; '
                         'a genuine archived groundwater diagnostic is required')
    effective = dict(doc['inherit'], **r)
    paths = set()
    primary_files = set()
    target_frequency = r['compound_name'].split('.')[-2]
    for inp in r.get('inputs', []):
        inp['pattern'] = repointer.anchor_pattern(repointer.add_year_to_pattern(inp['pattern'], str(year)))
        files = matching(inp['path'], inp['pattern'])
        if not files:
            raise ValueError('MISSING_INPUT: '+str(Path(inp['path'])/inp['pattern']))
        paths.update(files)
        primary_files.update(files)
        # Derived pipelines may use a placeholder model_variable: check in execution.
        if '/outdata/' in inp['path'] and all(p.suffix == '.nc' for p in files) and target_frequency != 'fx':
            streams = validate_input_streams(files, [year], target_frequency)
            steps=[s for p in doc.get('pipelines',[]) if p['name'] in r.get('pipelines',[]) for s in p.get('steps',[])]
            if any('compute_square' in s for s in steps) and any(
                    stream['frequency'] in ('yr','mon') for stream in streams.values()):
                raise ValueError('MISSING_FREQUENCY: mean of squares cannot be recovered by squaring monthly means')
    name = r['compound_name'].split('.')[1]
    if name in ('dslw', 'dsw', 'mrtws', 'mrsow', 'dcw', 'dsn'):
        if name in ('dcw', 'dsn'):
            required = ['src'] if name == 'dcw' else ['sd']
        else:
            required = [f'swvl{i}' for i in range(1, 5)]
            if name in ('dsw', 'mrtws'):
                required += ['sd', 'src']
        streams = validate_input_streams(sorted(primary_files), [year], target_frequency)
        if missing := set(required) - streams.keys():
            raise ValueError('MISSING_STORAGE_VARIABLE: ' + ', '.join(sorted(missing)))
        if name == 'mrsow':
            r['comment'] = (r.get('comment', '') + ' Soil wetness uses the inherited HR/LR '
                f'uniform porosity approximation ({float(r.get("porosity", 0.472)):g}) '
                'and clips the ratio to 0–1; it is not soil-type-specific saturation.').strip()
        if name in ('dslw', 'dsw', 'dcw', 'dsn'):
            previous = []
            for current in sorted(primary_files):
                suffix = f'_{year}-{year}.nc'
                if not current.name.endswith(suffix):
                    raise ValueError(f'UNRESOLVED_BOUNDARY_FILENAME: {current.name}')
                prior = current.with_name(current.name[:-len(suffix)] + f'_{year-1}-{year-1}.nc')
                if not prior.is_file():
                    raise ValueError(f'MISSING_PREVIOUS_DAY: December 31 {year-1}: {prior}')
                previous.append(prior)
            r['land_boundary_records'] = validate_previous_day(
                sorted(primary_files), previous, year, required)
            paths.update(previous)
            changed = 0
            # Copy pipeline objects: other rules in the same tier must not inherit
            # this rule's boundary step through a shallow template reference.
            doc['pipelines'] = copy.deepcopy(doc.get('pipelines', []))
            for pipe in doc['pipelines']:
                if pipe['name'] not in r.get('pipelines', []):
                    continue
                for i, step in enumerate(pipe.get('steps', [])):
                    if step.endswith(':compute_temporal_diff'):
                        pipe['steps'][i] = f'script://{RULES / "land_steps.py"}:compute_temporal_diff_with_boundary'
                        changed += 1
            if changed != 1:
                raise ValueError('UNRESOLVED_STORAGE_PIPELINE: expected one temporal-difference step')
    for key, val in list(effective.items()):
        if key.endswith('_pattern'):
            base = key[:-8]+'_path'
            if base not in effective:
                continue
            pattern = repointer.anchor_pattern(repointer.add_year_to_pattern(val, str(year)))
            r[key] = pattern
            files = matching(effective[base], pattern)
            if not files:
                raise ValueError('MISSING_SECONDARY: '+str(Path(effective[base])/pattern))
            paths.update(files)
        if key.endswith('_file') and isinstance(val,str) and val.startswith('/'):
            files = tuple(Path(val).parent.glob(Path(val).name)) if '*' in val else (Path(val),)
            if not files or any(not p.is_file() for p in files):
                raise ValueError('MISSING_AUXILIARY: '+val)
            paths.update(files)
    for p in paths:
        if p.stat().st_size == 0:
            raise ValueError('EMPTY_INPUT: '+str(p))
    if name in ('hfx','hfy') and target_frequency=='day' and r.get('vertical_sum'):
        secondary=matching(effective['hnode_path'],r['hnode_pattern'])
        if len(primary_files)!=1 or len(secondary)!=1:
            raise ValueError('Daily heat integration requires one frozen annual source and thickness file')
        r.update(heat_source_file=str(next(iter(primary_files))),
                 heat_hnode_file=str(secondary[0]),heat_spatial_block=32768)
        doc['pipelines']=copy.deepcopy(doc.get('pipelines',[]))
        changed=0
        for pipeline in doc['pipelines']:
            if pipeline['name'] not in r.get('pipelines',[]):continue
            for i,step in enumerate(pipeline.get('steps',[])):
                if step.endswith(':compute_heat_transport'):
                    pipeline['steps'][i]=f'script://{RULES / "heat_steps.py"}:compute_daily_heat_transport'
                    changed+=1
        if changed!=1:raise ValueError('Daily heat integration requires exactly one heat-transport step')
    return sorted(paths)


def new_doc(doc, config, year, compound):
    doc = copy.deepcopy(doc)
    doc.pop('jobqueue', None)
    doc.pop('distributed', None)
    doc['pycmor'] = dict(SERIAL)
    doc['general']['CMIP7_DReq_metadata'] = config['metadata']
    doc['general']['CV_Dir'] = config['cv_dir']
    inherit = doc.setdefault('inherit', {})
    for k in list(inherit):
        if k.startswith('parent_') or k.startswith('branch_'):
            inherit.pop(k)
    inherit.update(source_id='LR-SPINUP-TEST', experiment_id='piControl-spinup',
                   institution_id='AWI', institution='Alfred Wegener Institute for Polar and Marine Research',
                   contact='cmip7@awi.de',
                   activity_id='CMIP', variant_label='r1i1p1f1',
                   year=year, year_start=year, year_end=year,
                   time_units=config.get('time_units', 'days since 1850-01-01'),
                   lazy_write=True,
                   qc_enabled=True, qc_tests=['cf','wcrp_cmip7'], qc_ignore_codes=[],
                   qc_checker_options=[], qc_fail_on_mandatory=False,
                   netcdf_write_scheduler='synchronous',
                   comment='Test CMORization; actual simulation years retained. Not publication approved.')
    inherit.update(config.get('metadata_overrides', {}))
    inherit['qc_tests'] = config.get('qc_tests', ['cf', 'wcrp_cmip7'])
    if 'aicc' in inherit['qc_tests']:
        inherit['qc_checker_options'] = ['aicc:grid_config:' + config['aicc_grid_config']]
    if config.get('drs_version'):
        inherit['drs_version'] = config['drs_version']
    for rule in doc['rules']:
        if not rule.get('pipelines'):
            # Rule's implicit DefaultPipeline() otherwise starts Prefect despite
            # native global configuration. Keep the same steps, explicit backend.
            rule['pipelines']=['full_native_default']
            if not any(p['name']=='full_native_default' for p in doc.get('pipelines',[])):
                doc.setdefault('pipelines',[]).append(dict(name='full_native_default',
                    uses='pycmor.core.pipeline.DefaultPipeline',workflow_backend='native'))
    # Execution cannot be made parallel by inherited upstream worker settings.
    for p in doc.get('pipelines', []):
        p['workflow_backend'] = 'native'
    return doc


def build_catalog(config, experiment, period, identity, campaign):
    """All 17 full tiers + all raw fallbacks + REcoM/tracer + unimplemented lists."""
    repo = Path(config['repo'])
    rep = module(repo/'examples/repoint_hr_year.py', 'full_repointer')
    raw = module(RULES/'raw_adapter.py', 'full_raw_rules')
    raw.configure(campaign, repo, experiment, period, config)
    # Index each archive once, rather than stat'ing decades of files per rule.
    source_index={c:sorted((experiment/'outdata'/c).glob('*.nc')) for c in ('oifs','fesom')}
    source_index['lpj_guess']=sorted((experiment/'outdata/lpj_guess'/period).glob('*.out'))
    def raw_inputs(component, pattern):
        return [p for p in source_index[component] if re.fullmatch(pattern,p.name)
                and (component=='lpj_guess' or any(str(y) in p.name for y in raw.YEARS))]
    def raw_months(paths,variable,expected_units=None):
        dates=[];shape=None
        for path in paths:
            header=nc_header(str(path))
            if variable not in header['variables']:
                raise ValueError(f'{path}: missing {variable}')
            if expected_units and header['units'][variable]!=expected_units:
                raise ValueError(f'{path}: unexpected units for {variable}')
            spatial=header['shapes'][variable][1:]
            if shape is not None and spatial!=shape:
                raise ValueError('Spatial shape changed between files')
            shape=spatial
            dates.extend((d[0],d[1]) for d in header.get('dates',[]) if d[0] in raw.YEARS)
        if dates!=[(y,m) for y in raw.YEARS for m in range(1,13)]:
            raise ValueError('Incomplete/duplicate monthly raw data')
    raw.inputs=raw_inputs
    raw.check_months=raw_months
    grid = raw.grids()
    # Native land outputs are a subset of the atmospheric grid. Give the
    # existing pycmor expansion/bounds code the ACTUAL OASIS geometry, not
    # an absent XIOS orography file or a fabricated regular grid.
    full_grid=campaign/'grids/full_TCO95_grid.nc'
    if not full_grid.exists():
        import xarray as xr
        with netCDF4.Dataset(experiment/f'run_{period}'/'work/grids.nc') as g:
            lon=np.asarray(g['TCO95-land.lon'][:]).ravel()
            lat=np.asarray(g['TCO95-land.lat'][:]).ravel()
            if lon.size!=40320 or lat.size!=40320:
                raise ValueError('Unexpected TCO95 full-grid dimensions')
            ds=xr.Dataset({'lon':('ncells',lon),'lat':('ncells',lat),
                'lon_bnds':(('ncells','vertices'),np.asarray(g['TCO95-land.clo'][:]).reshape(4,-1).T),
                'lat_bnds':(('ncells','vertices'),np.asarray(g['TCO95-land.cla'][:]).reshape(4,-1).T)})
            ds.lon.attrs.update(units='degrees_east',bounds='lon_bnds')
            ds.lat.attrs.update(units='degrees_north',bounds='lat_bnds')
            ds.attrs['provenance']=str(experiment/f'run_{period}'/'work/grids.nc')
            ds.to_netcdf(full_grid)
    catalog = sorted((repo/'awi-esm3-lr-variables').glob('*/cmip7xml*.yaml'))
    if len(catalog) != 17:
        raise ValueError('Full LR catalog changed: expected 17 tiers; review before running')
    metadata = read(config['metadata'])['Compound Name']
    full_compounds = {r['compound_name'] for p in catalog for r in yaml.safe_load(p.read_text())['rules']}
    raw_rules = []
    for file in raw.PROFILE['tier_files']:
        raw_rules += yaml.safe_load((repo/'awi-esm3-lr-variables'/file).read_text())['rules']
    raw_rules += list(raw.recom_rules()) + list(raw.co2_rules())
    iceberg = support_module('iceberg_support')
    raw_rules += list(iceberg.rules(RULES))
    candidates = defaultdict(list)
    years = range(int(period[:4]),int(period[9:13])+1)
    scope = set(full_compounds) | {r['compound_name'] for r in raw_rules}
    # Retain the requested groundwater product after removing its physically
    # different deep-soil proxy from the executable XML catalogue.
    groundwater='land.dgw.tavg-u-hxy-lnd.day.glb'
    declaration=yaml.safe_load((repo/'awi-esm3-lr-variables/veg_land/cmip7_awiesm3-lr_veg_land.yaml').read_text())
    missing_groundwater=next(r for r in declaration['not_enabled'] if r['compound_name']==groundwater)
    scope.add(groundwater)
    for year in years:
        candidates[groundwater,year].append(dict(route='unmapped',tier='veg_land',
            compound=groundwater,year=year,status='BLOCKED',reason=missing_groundwater['reason']))
    if config.get('hr_reference_manifest'):
        scope.update(support_module('hr_reference').load_reference(
            config['hr_reference_manifest'], {'Compound Name': metadata}))
    for category in ('obgc','co2','icebergs_cavities'):
        for file in (repo/'awi-esm3-lr-variables'/category).glob('*.csv'):
            for row in csv.DictReader(file.open()):
                compound = row.get('cmip7_compound_name')
                if compound:
                    scope.add(compound)
    # HR rotation-aware pipelines are the audited equivalents of the LR rules.
    vectors = {}
    for p in (repo/'awi-esm3-veg-hr-variables').glob('*/cmip7_awiesm3-veg-hr*.yaml'):
        d = yaml.safe_load(p.read_text())
        for r in d['rules']:
            if r.get('vector_partner_pattern'):
                vectors[r['compound_name']] = (r, d)
    replacements = [
        ('/work/ab0246/a270092/input/fesom2/core3', config['mesh_dir']),
        ('/work/ab0246/a270092/input/oifs-48r1/cmip7-data/ghg', config['ghg_dir']),
        ('/work/ab0246/a270092/software/pycmor', str(repo)),
        ('$PYCMOR_HOME', str(repo)),
        (str(experiment/'outdata/lpj_guess'), str(experiment/'outdata/lpj_guess'/period)),
    ]
    # Audited REcoM additions need the same two surface pipelines as before.
    for original, name, step in [('lr_monthly','recom_surface','transform_and_surface'),
                                 ('lr_linear','recom_surface_linear','surface_only')]:
        pipe = copy.deepcopy(next(p for p in raw.PROFILE['pipelines'] if p['name']==original))
        pipe['name'] = name
        transform = 'pycmor.awiesm3_lr.transform'
        target = f'script://{campaign}/reproduce/extra_steps.py:{step}'
        if transform in pipe['steps']:
            pipe['steps'][pipe['steps'].index(transform)] = target
        else:
            pipe['steps'].insert(2,target)
        raw.PROFILE['pipelines'].append(pipe)
    vector = copy.deepcopy(next(p for p in raw.PROFILE['pipelines'] if p['name']=='lr_monthly'))
    vector['name'] = 'lr_rotated_vector'
    vector['steps'].insert(1, f'script://{repo}/examples/custom_steps.py:rotate_vector_to_geographic')
    raw.PROFILE['pipelines'].append(vector)
    raw.register_daily_pipelines(raw.PROFILE, campaign)
    iceberg.install_pipeline(raw.PROFILE, campaign/'reproduce')
    for year in years:
        print(f'Planning full catalog: {year}', flush=True)
        for path in catalog:
            doc = yaml.safe_load(rep.repoint_yaml(path,str(experiment),str(year),model='lr'))
            doc = remap_strings(doc, replacements)
            if 'bounds_reference_file' in doc.get('inherit',{}):
                doc['inherit']['bounds_reference_file']=str(full_grid)
            for rule in doc['rules']:
                compound = rule['compound_name']
                frequency = compound.split('.')[-2]
                if compound.split('.')[2].startswith('tclm-') and year != years.stop-1:
                    continue
                if frequency == 'fx' and year != years.start:
                    continue
                if frequency == 'dec' and year != years.stop-1:
                    continue
                item = dict(route='cmip7xml', tier=path.parent.name, source=str(path),
                            compound=compound, year=year)
                try:
                    if compound not in metadata:
                        raise ValueError('UNRESOLVED_DREQ: not present in pinned metadata')
                    if compound.split('.')[1]=='co2mass':
                        representation = config.get('carbon_context', {}).get('co2_representation')
                        if representation == 'nonuniform_diagnostic_tracer':
                            raise ValueError('NOT_APPLICABLE_NONUNIFORM_CO2: CMIP7 processing note requests mole fraction instead; prescribed-CO2 mass must not replace diagnostic tracer mass')
                        if representation != 'prescribed_uniform':
                            raise ValueError('UNRESOLVED_MAPPING: review co2_representation; prescribed-CO2 mass is not the prognostic tracer mass')
                    if compound in {r['compound_name'] for r in raw_rules if r.get('lr_group')=='co2'}:
                        raise ValueError('SUPERSEDED: use audited atmosphere-wide CO2 rules, not land-only flux')
                    one = dict(doc, rules=[copy.deepcopy(rule)])
                    if compound in vectors:
                        if identity['vector_frame']=='unknown':
                            raise ValueError('UNRESOLVED_VECTOR_FRAME: verify installed FESOM binary or set vector_frame')
                        if identity['vector_frame']=='rotated':
                            vr, vd = vectors[compound]
                            r = one['rules'][0]
                            for k,v in vr.items():
                                if k.startswith('vector_') or k in ('alpha_euler','beta_euler','gamma_euler','pipelines'):
                                    r[k] = v
                            r['vector_partner_path'] = str(experiment/'outdata/fesom')
                            r['vector_partner_pattern'] = rep.add_year_to_pattern(r['vector_partner_pattern'],str(year))
                            r['vector_rotation'] = 'always'
                            hp = {p['name']:p for p in vd.get('pipelines',[])}
                            lp = {p['name']:p for p in one.get('pipelines',[])}
                            lp.update({n:hp[n] for n in r['pipelines'] if n in hp})
                            one['pipelines'] = remap_strings(list(lp.values()),replacements)
                    one = new_doc(one,config,year,compound)
                    support_module('grid_auxiliary').configure_grid_auxiliaries(one, config)
                    # No template's Levante experiment/grid path may survive.
                    text = yaml.safe_dump(one)
                    if '/work/ab' in text or '/home/a/a270' in text:
                        raise ValueError('UNRESOLVED_PATH: remaining Levante path in recipe')
                    if compound.split('.')[2].startswith('tclm-'):
                        prune_unused_inherited_basin_mask(one)
                        sources = support_module('climatology_support').configure(one, years.start, years.stop-1)
                        item.update(window_start=years.start, window_end=years.stop-1)
                    elif frequency == 'dec':
                        decadal = support_module('decadal_support')
                        start, end = decadal.window_for_period(period, config.get('decadal_window'))
                        prune_unused_inherited_basin_mask(one)
                        sources = decadal.configure_decadal_doc(one, start, end, rep)
                        item.update(window_start=start, window_end=end)
                    else:
                        sources = dependencies(one,year,rep)
                    item.update(doc=one, inputs=list(map(str,sources)), status='READY')
                except (ValueError,KeyError,OSError) as e:
                    item.update(status='BLOCKED',reason=str(e))
                candidates[compound,year].append(item)
        raw.YEARS = [year]
        for original in raw_rules:
            compound = original['compound_name']
            if compound.split('.')[-2]=='fx' and year!=years.start:
                continue
            item = dict(route='raw',tier=original.get('lr_group','recom'),source=str(RULES/'raw_adapter.py'),
                        compound=compound,year=year)
            try:
                if original.get('lr_group') in ('recom', 'co2'):
                    if config.get('carbon_profile') != 'awiesm3-v3.5.1-cc-raw-v1':
                        raise ValueError('UNREVIEWED_CARBON_PROFILE: select the documented carbon_profile after checking source conventions')
                    if (original.get('lr_group') == 'co2' and compound.split('.')[1] != 'co2'
                            and config.get('carbon_context', {}).get('external_emissions_reader') is not False):
                        raise ValueError('UNRESOLVED_CARBON_FLUX: external emissions must be explicitly absent; other pathways need additional terms')
                if compound not in metadata:
                    raise ValueError('UNRESOLVED_DREQ: not present in pinned metadata')
                group, r = raw.materialize(original,grid)
                if group == 'icebergs':
                    iceberg.validate_rule(r)
                if r.get('vector_partner_pattern'):
                    if identity['vector_frame']=='unknown':
                        raise ValueError('UNRESOLVED_VECTOR_FRAME')
                    r['vector_rotation'] = 'always' if identity['vector_frame']=='rotated' else 'never'
                doc = {'general': {'name': 'full-lr', 'cmor_version':'CMIP7','mip':'CMIP'},
                       'inherit':dict(raw.PROFILE['defaults']), 'rules':[r],
                       'pipelines': [copy.deepcopy(p) for p in raw.PROFILE['pipelines'] if p['name'] in r['pipelines']]}
                doc = new_doc(doc,config,year,compound)
                # Raw adapters use the same physical grids as the XML routes.
                # A global LPJG land subset must be expanded onto full TCO95.
                if r.get('grid_label', doc['inherit'].get('grid_label')) == 'gn':
                    realm = compound.split('.')[0]
                    doc['rules'][0]['grid_label'] = (
                        'g236' if realm in ('ocean','seaIce','ocnBgchem') else
                        'g235' if realm in ('land','landIce') else 'g271')
                is_lpj = any(str(p).endswith('.out') for p in r.get('lr_source_files', []))
                if is_lpj:
                    doc['inherit']['bounds_reference_file'] = str(full_grid)
                sources = set(r.get('lr_source_files',[])+r.get('vector_source_files',[])
                              +r.get('lr_auxiliary_files',[]))
                if is_lpj:
                    sources.add(str(full_grid))
                for term in r.get('lr_terms',[]):
                    sources.update(term['files'])
                if r.get('grid_file'):
                    sources.add(r['grid_file'])
                item.update(tier=group,doc=doc,inputs=sorted(sources),status='READY')
            except (ValueError,KeyError,OSError) as e:
                item.update(status='BLOCKED',reason=str(e))
            candidates[compound,year].append(item)
    rows, tasks = [], []
    for compound in sorted(scope):
        frequency = compound.split('.')[-2]
        task_years = ([years.stop-1] if compound.split('.')[2].startswith('tclm-') else
                      [years.start] if frequency=='fx' else
                      [years.stop-1] if frequency=='dec' else years)
        for year in task_years:
            routes = candidates.get((compound,year),[])
            good = next((r for r in routes if r['status']=='READY'),None)
            row = dict(compound=compound,year=year, status='READY' if good else 'BLOCKED',
                       reason='' if good else ' | '.join(dict.fromkeys(r['reason'] for r in routes)) or 'UNMAPPED: no implemented rule',
                       alternatives=[{k:v for k,v in r.items() if k not in ('doc','inputs')} for r in routes])
            if not good:
                equivalent = support_module('coverage_policy').regional_global_alternative(
                    compound, year, metadata, candidates)
                if equivalent:
                    row.update(equivalent_compound=equivalent, reason=(
                        'GLOBAL_EQUIVALENT_PLANNED: pinned DReq says to omit this regional '
                        f'duplicate when {equivalent} is produced; no separate product counted'))
            if good:
                key = f'{year}__{compound}'
                good['doc']['inherit']['output_directory'] = str(campaign/'NOT_FOR_PUBLICATION'/key)
                target = campaign/'recipes'/f'{key}.yaml'
                target.write_text(yaml.safe_dump(good['doc'],sort_keys=False))
                task = {k:v for k,v in good.items() if k not in ('doc','status')}
                task.update(key=key,recipe=str(target),recipe_sha256=digest(target),
                            input_state=file_state(good['inputs']))
                tasks.append(task)
                row.update(route=good['route'],tier=good['tier'],key=key)
                if 'window_start' in good:
                    row.update(window_start=good['window_start'], window_end=good['window_end'])
            rows.append(row)
    return tasks, rows, len(full_compounds)


def plan(args, config):
    config = copy.deepcopy(config)
    repo = Path(config['repo'])
    head = subprocess.check_output(['git','-C',str(repo),'rev-parse','HEAD'],text=True).strip()
    if config.get('commit') and head != config['commit']:
        raise ValueError('pycmor revision changed; review rules before updating the configured commit')
    config['commit'] = head
    git_status = subprocess.check_output(['git','-C',str(repo),'status','--porcelain'],text=True)
    if git_status.strip() and not config.get('allow_dirty', False):
        raise ValueError('Checkout contains changes; review/commit them, or explicitly set allow_dirty=true for a fingerprinted development test')
    experiment = args.experiment.resolve(strict=True)
    result_root = Path(config['results']).resolve()
    if result_root == repo or repo in result_root.parents or result_root in repo.parents:
        raise ValueError('Results must be outside the source repository and not its ancestor')
    if result_root == experiment or experiment in result_root.parents or result_root in experiment.parents:
        raise ValueError('Results must not overlap the source experiment')
    selection = select_leg(experiment,args.leg)
    period = selection['period']
    identity = identify(experiment,period,config)
    expkey = re.sub('[^a-zA-Z0-9_.-]','_',experiment.name)+'__'+hashlib.sha256(str(experiment).encode()).hexdigest()[:8]
    campaign = Path(config['results'])/expkey/period
    campaign.mkdir(parents=True,exist_ok=True)
    with (campaign/'.plan.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        metadata_paths = [Path(config['metadata']), *sorted(Path(config['cv_dir']).rglob('*.json'))]
        if config.get('hr_reference_manifest'):
            metadata_paths.append(Path(config['hr_reference_manifest']))
        if config.get('source_audit_report'):
            audit = read(config['source_audit_report'])
            if audit.get('leg') != period or Path(audit.get('experiment', '')) != experiment:
                raise ValueError('Source scientific audit does not describe this experiment and leg')
            if any(r['status'] in ('FAIL', 'ERROR') for r in audit['checks']):
                raise ValueError('Source scientific audit has failures/errors; review before planning')
            metadata_paths.append(Path(config['source_audit_report']))
        if config.get('cmip7_tables_path'):
            metadata_paths.extend(sorted(Path(config['cmip7_tables_path']).rglob('*.json')))
        if config.get('esgvoc_home'):
            metadata_paths.extend(sorted(Path(config['esgvoc_home']).rglob('*.db')))
            metadata_paths.extend(sorted(Path(config['esgvoc_home']).rglob('*.active.json')))
        for path in config.get('native_io', {}).get('libraries', []):
            metadata_paths.append(Path(path))
        signature = dict(config=config, experiment=str(experiment), period=period,
                         source_files=code_manifest(repo),
                         metadata_files={str(p):digest(p) for p in metadata_paths},
                         driver_sha256=digest(__file__),identity=identity,
                         raw_rules_sha256=digest(RULES/'raw_adapter.py'),
                         helper_hashes={p.name:digest(p) for p in
                             (RULES/'extra_steps.py',RULES/'native_cli.py',RULES/'validate_product.py',
                              RULES/'iceberg_steps.py', RULES/'iceberg_validation.py')})
        if (campaign/'plan.json').exists():
            old = read(campaign/'plan.json')
            comparable = dict(old['signature']); comparable.pop('snapshot_files', None)
            if comparable != signature:
                raise ValueError('Existing campaign has different code/config; use --output-root for a separate result')
            verify_campaign(campaign)
            print(f'Reusing immutable plan: {campaign}',flush=True)
            return campaign
        for name in ('recipes','logs','status','report','reproduce','NOT_FOR_PUBLICATION','grids'):
            (campaign/name).mkdir(exist_ok=True)
        # Test metadata only unless an explicit reviewed metadata profile is supplied.
        if 'spinup' not in experiment.name.lower():
            required={'source_id','experiment_id','forcing_year'}
            if not required <= config.get('metadata_overrides',{}).keys():
                raise ValueError('For another experiment type, metadata_overrides must explicitly set source_id, experiment_id and forcing_year (null for historical)')
        tasks, coverage, full_count = build_catalog(config,experiment,period,identity,campaign)
        write(campaign/'tasks.json',tasks)
        write(campaign/'coverage.json',coverage)
        for p in (Path(__file__), Path(args.config), ROOT/'scripts/select_latest_completed.py', RULES/'native_cli.py',RULES/'extra_steps.py',RULES/'validate_product.py', RULES/'raw_adapter.py', RULES/'recom.yaml', RULES/'co2.yaml', RULES/'iceberg.yaml', RULES/'iceberg_steps.py', RULES/'iceberg_validation.py'):
            shutil.copy2(p,campaign/'reproduce'/p.name)
        if config.get('hr_reference_manifest'):
            shutil.copy2(config['hr_reference_manifest'], campaign/'reproduce/hr_reference_manifest.txt')
        shutil.copytree(ROOT, campaign/'reproduce/full_workflow',
                        ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '*.pdf'))
        (campaign/'reproduce/git_status.txt').write_text(git_status)
        (campaign/'reproduce/git_diff.patch').write_text(subprocess.check_output(
            ['git','-C',str(repo),'diff','HEAD','--'], text=True))
        write(campaign/'reproduce/config_used.json',config)
        write(campaign/'reproduce/input_files.json',{p:s for t in tasks for p,s in t['input_state'].items()})
        deps = subprocess.check_output([config['python'],'-m','pip','freeze'],text=True)
        (campaign/'reproduce/dependencies.txt').write_text(deps)
        signature['snapshot_files'] = {str(p.relative_to(campaign)):digest(p)
            for p in sorted((campaign/'reproduce').rglob('*')) if p.is_file()}
        write(campaign/'plan.json',dict(signature=signature,selection=selection,created=stamp(),
              full_catalog_products=full_count,scope_products=len({r['compound'] for r in coverage}),
              tasks=len(tasks),publication_ready=False,command=sys.argv))
        report(campaign)
    return campaign


def validate_output(task, recipe, output):
    from cf_units import Unit

    files = sorted(Path(output).rglob('*.nc'))
    if not files:
        raise ValueError('Converter produced no NetCDF files')
    cfg = yaml.safe_load(Path(recipe).read_text())
    expected = cfg['inherit']
    compound = task['compound']; name = compound.split('.')[1]; freq=compound.split('.')[-2]
    metadata = read(cfg['general']['CMIP7_DReq_metadata'])['Compound Name'][compound]
    dates=[];calendar='standard';records=0
    for p in files:
        with netCDF4.Dataset(p) as ds:
            if name not in ds.variables:
                raise ValueError(f'{p}: requested variable {name} absent')
            if not getattr(ds[name],'units',None):
                raise ValueError(f'{p}: missing units')
            if Unit(ds[name].units) != Unit(metadata['units']):
                raise ValueError(f'{p}: units differ from pinned DReq')
            if metadata.get('standard_name') and getattr(ds[name], 'standard_name', '') != metadata['standard_name']:
                raise ValueError(f'{p}: standard_name differs from pinned DReq')
            rule = cfg.get('rules', [{}])[0]
            label = rule.get('grid_label', expected.get('grid_label'))
            if compound.split('.')[-1] == 'glb' and '-hxy-' in compound:
                lat = next((ds[n] for n in ('latitude', 'lat') if n in ds.variables), None)
                lon = next((ds[n] for n in ('longitude', 'lon') if n in ds.variables), None)
                required = {'g235':40320,'g236':220509,'g238':426403,'g271':76800}.get(label)
                if required:
                    cells = (lat.size * lon.size if label == 'g271' and lat is not None and lon is not None
                             and lat.dimensions != lon.dimensions else lat.size if lat is not None else 0)
                    if cells != required:
                        raise ValueError(f'{p}: {label} requires {required} cells; got {cells}')
            if freq == 'fx' and 'time' in ds.dimensions:
                raise ValueError(f'{p}: fixed field has a time dimension')
            dimensions = metadata.get('dimensions', '').split()
            for axis, aliases in [('longitude', ('longitude', 'lon')), ('latitude', ('latitude', 'lat'))]:
                if axis in dimensions and not any(n in ds.variables for n in aliases):
                    raise ValueError(f'{p}: missing {axis} coordinate')
            if 'olevel' in dimensions and not any(
                    getattr(c, 'standard_name', '') == 'depth' and c.ndim == 1
                    and c.dimensions[0] in ds[name].dimensions for c in ds.variables.values()):
                raise ValueError(f'{p}: missing ocean depth axis')
            for key in ('source_id','experiment_id'):
                if getattr(ds,key,None)!=expected[key]:
                    raise ValueError(f'{p}: incorrect {key}')
            if freq != 'fx':
                if 'time' not in ds.variables:
                    raise ValueError(f'{p}: missing time coordinate')
                t=ds['time'];calendar=getattr(t,'calendar','standard')
                decoded=netCDF4.num2date(t[:],t.units,calendar)
                dates += [support_module('input_streams').date_tuple(d) for d in decoded]
                records+=len(decoded)
    if compound.split('.')[2].startswith('tclm-'):
        support_module('climatology_support').validate(files, task['window_start'], task['window_end'])
    elif freq == 'dec':
        support_module('decadal_support').validate_decadal_output(
            files, task['window_start'], task['window_end'])
    elif freq!='fx':
        cover_dates(dates,task['year'],freq,calendar)
    return dict(files=[str(p) for p in files], records=records, output_state=file_state(files),
                independent_scientific_value_check='not yet performed for full-catalog route',
                publication_ready=False)


def run_one(campaign, task, config):
    verify_campaign(campaign)
    if digest(task['recipe'])!=task['recipe_sha256']:
        raise ValueError('Recipe changed since planning')
    statusfile=campaign/'status'/f"{task['key']}.json"
    if statusfile.exists():
        old=read(statusfile)
        if old.get('status')=='PASS' and file_state(old['validation']['files'])==old['validation']['output_state']:
            if file_state(task['input_state'])!=task['input_state']:
                raise ValueError('Inputs changed since successful conversion')
            return
    if file_state(task['input_state'])!=task['input_state']:
        raise ValueError('Inputs changed since planning; no conversion of a live/changing archive')
    base=campaign/'NOT_FOR_PUBLICATION'/task['key']
    base.mkdir(parents=True,exist_ok=True)
    attempt=base/f'attempt_{len(list(base.glob("attempt_*")))+1:03d}'
    attempt.mkdir()
    recipe=yaml.safe_load(Path(task['recipe']).read_text())
    recipe['inherit']['output_directory']=str(attempt)
    rp=attempt/'recipe.yaml';rp.write_text(yaml.safe_dump(recipe,sort_keys=False))
    scratch=attempt/'scratch';scratch.mkdir()
    env=dict(os.environ,PYTHONPATH=str(Path(config['repo'])/'src'),PYCMOR_HOME=config['repo'],
             OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',
             NUMBA_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',
             PREFECT_HOME=str(scratch/'prefect'),XDG_CACHE_HOME=str(scratch/'cache'),
             ESGVOC_HOME=config['esgvoc_home'],
             TMPDIR=str(scratch),DASK_TEMPORARY_DIRECTORY=str(scratch),
             PYCMOR_WORKER_COMPUTE='off',
             PYCMOR_ESGVOC_PINNED='1',
             PREFECT_SERVER_ANALYTICS_ENABLED='false')
    if config.get('cmip7_tables_path'):
        env['CMIP7_TABLES_PATH'] = config['cmip7_tables_path']
    native = config.get('native_io', {})
    if native:
        env['HDF5_PLUGIN_PATH'] = native['plugin_path']
        env['LD_LIBRARY_PATH'] = ':'.join([
            str(Path(native['hdf5_prefix'])/'lib'),
            str(Path(native['netcdf_prefix'])/'lib'), env.get('LD_LIBRARY_PATH','')])
        env['PATH'] = str(Path(native['netcdf_prefix'])/'bin') + ':' + env.get('PATH','')
    env['PATH']=str(Path(config['python']).parent)+':'+env.get('PATH','')
    env.pop('PYTHONOPTIMIZE', None)
    result=dict(key=task['key'],started=stamp(),status='FAILED',output=str(attempt),publication_ready=False)
    try:
        with (attempt/'conversion.log').open('w') as log:
            cp=subprocess.run([config['python'],str(campaign/'reproduce/native_cli.py'),'process',str(rp)],
                              env=env,cwd=scratch,stdout=log,stderr=subprocess.STDOUT,
                              timeout=config['product_timeout_seconds'])
        result['exit_code']=cp.returncode
        if cp.returncode:
            raise ValueError(f'pycmor exit {cp.returncode}; see {attempt}/conversion.log')
        validation=validate_output(task,rp,attempt)
        checks = support_module('supervisor_output')
        validation['formal_qc_reports'] = checks.verify_qc(
            validation['files'], attempt, recipe['inherit']['qc_tests'])
        validation['supervisor_source_check'] = checks.source_check(
            task, recipe, validation['files'])
        if task['route']=='raw' and recipe['inherit']['source_id']=='LR-SPINUP-TEST':
            # Existing independent source-value audit, including the seven repairs.
            with (attempt/'source_validation.log').open('w') as log:
                cp=subprocess.run([config['python'],str(campaign/'reproduce/validate_product.py'),
                     str(rp),str(attempt/'source_validation.json')],env=env,stdout=log,stderr=subprocess.STDOUT,
                     timeout=config.get('validation_timeout_seconds',1800))
            if cp.returncode:
                raise ValueError('Independent raw-value validation failed')
            source_audit = read(attempt/'source_validation.json')
            validation['independent_source_check'] = source_audit
            validation['independent_scientific_value_check'] = (
                'passed sampled raw-value comparison' if source_audit.get('compared_samples',0) > 0
                else 'not performed: no independent source values compared')
        if file_state(task['input_state'])!=task['input_state']:
            raise ValueError('Source changed during conversion')
        result.update(status='PASS',validation=validation)
    except Exception as e:
        result['error']=str(e)
    result['finished']=stamp()
    write(statusfile,result)
    print(task['key'], result['status'], result.get('error',''), flush=True)


def work(campaign, shard, shards):
    config=verify_campaign(campaign)
    tasks=read(campaign/'tasks.json')[shard::shards]
    with (campaign/f'.worker_{shard}.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        for task in tasks:
            try:
                run_one(campaign,task,config)
            except Exception as e:
                write(campaign/'status'/f"{task['key']}.json",dict(status='FAILED',error=str(e),key=task['key']))
                print(task['key'],'FAILED',e,flush=True)
    return int(any(read(campaign/'status'/f"{t['key']}.json").get('status')!='PASS' for t in tasks))


def report(campaign):
    rows=read(campaign/'coverage.json')
    for r in rows:
        r.pop('alternatives',None)
        if r['status']=='READY':
            status=campaign/'status'/f"{r['key']}.json"
            info=read(status) if status.exists() else {}
            r['status']=info.get('status','PENDING')
            r['reason']=info.get('error','')
            r['output']=info.get('output','')
    target=campaign/'report';target.mkdir(exist_ok=True)
    fields=['compound','year','window_start','window_end','status','reason','route','tier','output',
            'equivalent_compound','equivalent_status']
    indexed = {(r['compound'], r['year']): r for r in rows}
    for r in rows:
        if r.get('equivalent_compound'):
            r['equivalent_status'] = indexed.get((r['equivalent_compound'], r['year']), {}).get('status', 'MISSING')
    with (target/'products.csv').open('w') as f:
        writer=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');writer.writeheader();writer.writerows(rows)
    counts=Counter(r['status'] for r in rows)
    inventory=defaultdict(list)
    for r in rows:
        inventory[r['compound']].append(r)
    with (target/'variable_inventory.csv').open('w') as stream:
        writer=csv.DictWriter(stream,fieldnames=['component','variable','compound',
            'frequency','first_year','last_year','pass','pending','failed','blocked'])
        writer.writeheader()
        for compound, entries in sorted(inventory.items()):
            parts=compound.split('.')
            states=Counter(r['status'] for r in entries)
            writer.writerow(dict(component=parts[0],variable=parts[1] if len(parts)>1 else compound,
                compound=compound,frequency=parts[-2] if len(parts)>2 else '',
                first_year=min(r.get('window_start', r['year']) for r in entries),
                last_year=max(r.get('window_end', r['year']) for r in entries),
                **{s.lower():states[s] for s in ('PASS','PENDING','FAILED','BLOCKED')}))
    state='COMPLETE' if counts and set(counts)=={'PASS'} else 'INCOMPLETE'
    realm_counts = {}
    for realm in sorted({c.split('.')[0] for c in inventory}):
        products = {c: entries for c, entries in inventory.items() if c.split('.')[0] == realm}
        realm_counts[realm] = dict(
            requested_products=len(products),
            variable_names=len({c.split('.')[1] if '.' in c else c for c in products}),
            products_with_ready_tasks=sum(any(r['status'] != 'BLOCKED' for r in rr) for rr in products.values()),
            wholly_blocked_products=sum(all(r['status'] == 'BLOCKED' for r in rr) for rr in products.values()),
            complete_products=sum(all(r['status'] == 'PASS' for r in rr) for rr in products.values()),
            products_with_failures=sum(any(r['status'] == 'FAILED' for r in rr) for rr in products.values()))
    validated_files = set()
    for path in (campaign/'status').glob('*.json'):
        record = read(path)
        if record.get('status') == 'PASS':
            validated_files.update(record.get('validation', {}).get('files', []))
    write(target/'summary.json',dict(status=state,counts=dict(counts),updated=stamp(),
          realms=realm_counts,validated_physical_netcdf_files=len(validated_files),
          distinct_products=len(inventory),
          distinct_variable_names=len({k.split('.')[1] if '.' in k else k for k in inventory}),
          counts_unit='annual/fixed/decadal task records, not variable names or NetCDF files',
          publication_ready=False,qc='Requested checker reports required per file; findings retained; metadata not approved'))
    (target/'STATUS.txt').write_text(f"{state}\n{dict(counts)}\nAll-catalog scope; no category excluded.\n"
        'BLOCKED is not success. See products.csv for every missing input, unmapped rule and deferred decade.\n'
        'PASS means conversion and stated checks, not publication QC. Full-catalog scientific value audits remain necessary.\n'
        'No raw model output may be deleted based on this status.\n')
    if (campaign/'plan.json').is_file():
        config = read(campaign/'plan.json')['signature']['config']
        support_module('hr_reference').write_report(campaign, config, rows)
    print(state,dict(counts),str(target/'products.csv'),flush=True)
    return 0 if state=='COMPLETE' else (1 if counts.get('FAILED') or counts.get('PENDING') else 3)


def batch(config, command, logfile, name, extra=()):
    account = os.environ.get('ESM_ACCOUNT') or config.get('account')
    if not account or not config.get('partition'):
        raise ValueError('Set account (or ESM_ACCOUNT) and partition in the site configuration before submission')
    argv=['sbatch','--parsable',f"--account={os.environ.get('ESM_ACCOUNT') or config['account']}",
          f"--partition={config['partition']}",
          '--nodes=1','--ntasks=1','--cpus-per-task=1',f"--mem={config['memory']}",
          f"--time={config['walltime']}",f'--job-name={name}',f'--output={logfile}',
          *([f"--qos={config['qos']}"] if config.get('qos') else []),
          *([f"--comment={config['slurm_comment']}"] if config.get('slurm_comment') else []),
          *extra,'--wrap',command]
    return subprocess.check_output(argv,text=True).strip().split(';')[0]


def submit_workers(campaign, config):
    marker=campaign/'submission.json'
    with (campaign/'.submit.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if marker.exists():
            raise ValueError(f'Already submitted: {marker}; use --run to resume serially after checking jobs have finished')
        shards=min(config['shards'],len(read(campaign/'tasks.json')))
        if not shards:
            report(campaign);raise ValueError('No supported products; no empty job submitted')
        cmd=shlex.join([config['python'],str(campaign/'reproduce/full_cmorization.py'),'--worker',str(campaign),'--shards',str(shards)])
        job=batch(config,cmd,campaign/'logs/worker-%A_%a.log','cmor-full',
                  [f'--array=0-{shards-1}%{config["concurrency"]}'])
        # Record the worker submission even if report-job submission fails.
        write(marker,dict(workers=job,report=None,submitted=stamp()))
        cmd=shlex.join([config['python'],str(campaign/'reproduce/full_cmorization.py'),'--report',str(campaign)])
        rep=batch(config,cmd,campaign/'logs/report-%j.log','cmor-report',[f'--dependency=afterany:{job}'])
        write(marker,dict(workers=job,report=rep,submitted=stamp()))
        print(f'Workers {job}; report {rep}; results {campaign}',flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('experiment',type=Path,nargs='?')
    p.add_argument('--leg',help='latest or YYYYMMDD-YYYYMMDD (defaults to config.leg, then latest)')
    p.add_argument('--config',type=Path,default=os.environ.get('PYCMOR_LR_CONFIG'))
    p.add_argument('--output-root',type=Path)
    mode=p.add_mutually_exclusive_group()
    mode.add_argument('--check',action='store_true',help='Prepare full inventory; no conversion and no submission')
    mode.add_argument('--run',action='store_true',help='Synchronous execution inside an allocated postprocessing job')
    mode.add_argument('--submit',action='store_true',help='Submit planning and conversion jobs (default)')
    p.add_argument('--dependency',help='Slurm dependency for planner, e.g. afterok:MERGE_JOB_ID')
    p.add_argument('--plan-and-submit',action='store_true',help=argparse.SUPPRESS)
    p.add_argument('--worker',type=Path,help=argparse.SUPPRESS)
    p.add_argument('--report',type=Path,help=argparse.SUPPRESS)
    p.add_argument('--shards',type=int,default=1,help=argparse.SUPPRESS)
    a=p.parse_args()
    if a.worker:
        return work(a.worker,int(os.environ.get('SLURM_ARRAY_TASK_ID',0)),a.shards)
    if a.report:
        return report(a.report)
    config=load_config(a.config)
    a.config=Path(a.config).expanduser().resolve()
    if not a.experiment:
        if config.get('experiment'):
            a.experiment=Path(config['experiment'])
        else:
            p.error('experiment directory is required as argument or config.experiment')
    a.leg = a.leg or config.get('leg','latest')
    if a.output_root:
        config['results']=str(a.output_root.resolve())
    output = Path(config['results'])
    if output == REPO or REPO in output.parents or output in REPO.parents:
        raise ValueError('Results must be outside the source repository and not its ancestor')
    if a.check or a.run or a.plan_and_submit:
        if a.run and not os.environ.get('SLURM_JOB_ID'):
            raise ValueError('--run requires a Slurm allocation; use default submission on a login node')
        campaign=plan(a,config)
        if a.run:
            work(campaign,0,1);return report(campaign)
        if a.plan_and_submit:
            submit_workers(campaign,config)
        print('Campaign:',campaign,flush=True)
        return 0
    logs=Path(config['results'])/'submission_logs';logs.mkdir(parents=True,exist_ok=True)
    cmd=[config['python'],str(Path(__file__).resolve()),str(a.experiment.resolve()),
         '--leg',a.leg,'--config',str(a.config.resolve()),'--plan-and-submit']
    if a.output_root:cmd += ['--output-root',str(a.output_root.resolve())]
    extra=[f'--dependency={a.dependency}'] if a.dependency else []
    job=batch(config,shlex.join(cmd),logs/'plan-%j.log','cmor-plan',extra)
    print(f'Planner job {job}; log {logs}/plan-{job}.log. Submission is not completion.')
    return 0


if __name__=='__main__':
    try:
        raise SystemExit(main())
    except (ValueError,RuntimeError,OSError,subprocess.SubprocessError) as error:
        print('ERROR:',error,file=sys.stderr)
        raise SystemExit(2)
