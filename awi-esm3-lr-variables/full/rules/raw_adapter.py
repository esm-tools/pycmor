#!/usr/bin/env python3
"""Audited native LR, REcoM and atmospheric CO2 rules.

Initialized explicitly by the full-catalog driver; no dated pilot or old
installation is required. Source model output is always read-only.
"""
import copy
import csv
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import netCDF4
import numpy as np
import pandas as pd
import xarray as xr
import yaml

CARBON_CONTEXT = {}
ARCHIVED_FILES = {}
GEOTHERMAL_CONTEXT = None
MODEL_LEVEL_TABLE = None


def configure(root, repo, experiment, period, config):
    global ROOT, REPO, EXP, PERIOD, YEARS, META, PROFILE, MESH, CARBON_CONTEXT, ARCHIVED_FILES, GEOTHERMAL_CONTEXT, VERIFIED_FESOM_SHA256
    global MODEL_LEVEL_TABLE
    ROOT, REPO, EXP, PERIOD = Path(root), Path(repo), Path(experiment), period
    MODEL_LEVEL_TABLE = config.get('model_level_table')
    YEARS = list(range(int(period[:4]), int(period[9:13]) + 1))
    META = json.loads(Path(config['metadata']).read_text())['Compound Name']
    PROFILE = yaml.safe_load((REPO/'awi-esm3-lr-variables/profile.yaml').read_text())
    MESH = Path(config['mesh_dir'])
    context = config.get('carbon_context', {})
    if not isinstance(context, dict):
        raise ValueError('carbon_context must be a mapping of reviewed run-specific evidence')
    CARBON_CONTEXT = copy.deepcopy(context)
    ARCHIVED_FILES = dict(config.get('_archived_files', {}))
    GEOTHERMAL_CONTEXT = copy.deepcopy(config.get('geothermal_context'))
    VERIFIED_FESOM_SHA256 = config.get('_verified_fesom_sha256')


def dump(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')


def grids():
    target = ROOT/'grids'
    target.mkdir(exist_ok=True)
    core = target/'core3_grid.nc'
    if not core.exists() and ARCHIVED_FILES.get('core3_grid.nc'):
        import shutil
        shutil.copy2(ARCHIVED_FILES['core3_grid.nc'], core)
    if not core.exists():
        # Pool mesh.nc contains an extra bottom centre. Use actual model layers
        # and interfaces from its diagnostic mesh, not extrapolated bounds.
        with xr.open_dataset(MESH/'mesh.nc') as src, xr.open_dataset(MESH/'fesom.mesh.diag.nc') as diag:
            mesh = src.drop_vars(['depth','depth_bnds']).load()
            mesh['depth'] = xr.DataArray(abs(diag.nz1.values), dims='nz1', attrs={'units':'m','positive':'down'})
            mesh['depth_bnds'] = xr.DataArray(abs(diag.nz.values), dims='nz', attrs={'units':'m','positive':'down'})
            mesh.attrs['provenance'] = 'CORE3 pool mesh geometry; exact 47 centres/48 interfaces from fesom.mesh.diag.nc'
            mesh.to_netcdf(core)
    land = target/'lpj_grid.nc'
    if not land.exists() and ARCHIVED_FILES.get('lpj_grid.nc'):
        import shutil
        shutil.copy2(ARCHIVED_FILES['lpj_grid.nc'], land)
    if not land.exists():
        work = EXP/f'run_{PERIOD}'/'work'
        # Geometry can be prepared while the archive merge is queued. The
        # staged gridlist is this leg's actual LPJG cell set, not another run.
        cells = pd.read_csv(work/'ece_gridlist_TCO95.txt',sep=r'\s+',header=None,usecols=[0,1],names=['Lon','Lat']).sort_values(['Lat','Lon'],ascending=[False,True])
        with netCDF4.Dataset(work/'grids.nc') as g, netCDF4.Dataset(work/'areas.nc') as a:
            lon = np.asarray(g['TCO95-land.lon'][:]).ravel()
            lat = np.asarray(g['TCO95-land.lat'][:]).ravel()
            from scipy.spatial import cKDTree
            def xyz(lon,lat):
                lon,lat=np.deg2rad(lon),np.deg2rad(lat)
                return np.column_stack([np.cos(lat)*np.cos(lon),np.cos(lat)*np.sin(lon),np.sin(lat)])
            distance, idx = cKDTree(xyz(lon,lat)).query(xyz(cells.Lon,cells.Lat))
            assert distance.max()<2e-5 and len(set(idx))==len(idx), 'LPJG coordinates do not match selected grid'
            ds=xr.Dataset({'lon':('ncells',cells.Lon.to_numpy()),'lat':('ncells',cells.Lat.to_numpy()),
                'cell_area':('ncells',np.asarray(a['TCO95-land.srf'][:]).ravel()[idx]),
                'lon_bnds':(('ncells','vertices'),np.asarray(g['TCO95-land.clo'][:]).reshape(4,-1).T[idx]),
                'lat_bnds':(('ncells','vertices'),np.asarray(g['TCO95-land.cla'][:]).reshape(4,-1).T[idx])})
            ds.cell_area.attrs['units']='m2'
            ds.attrs['provenance']=str(work/'grids.nc')+'; areas.nc; coordinates ordered exactly as native LPJG outputs'
            ds.to_netcdf(land)
    # Atmosphere fx area requires audited XIOS remapping geometry; do not
    # silently manufacture equal-area weights for the 400x192 grid.
    return {'core3':str(core),'lpj':str(land)}


def inputs(component, pattern):
    base=EXP/'outdata'/component
    if component=='lpj_guess':
        return sorted(p for p in (base/PERIOD).glob('*.out') if re.fullmatch(pattern,p.name))
    return sorted(p for p in base.glob('*.nc') if re.fullmatch(pattern,p.name)
                  and any(str(y) in p.name for y in YEARS))


def raw_input_pattern(component, pattern):
    """Accept both OpenIFS raw filename conventions, without widening streams.

    Older archives repeat the frequency (and ``pl`` for pressure levels)
    before the years; current archives omit that repetition. Only normalize
    the exact raw-selector grammar, not arbitrary regexes or other components.
    """
    if component != 'oifs':
        return pattern
    stream = re.match(r'\^?atm_remapped_([1-9]\d*[mhd](?:_pl)?)_', pattern)
    if stream is None:
        return pattern
    stream = stream[1]
    legacy = re.fullmatch(
        r'\^?(?P<prefix>atm_remapped_' + re.escape(stream) +
        r'_[A-Za-z0-9_]+)_' + re.escape(stream) + r'_(?P<suffix>.+)', pattern)
    if legacy is None or legacy['suffix'] not in (
            r'\d{4}-\d{4}\.nc$', r'\d{4}\-\d{4}\.nc$'):
        return pattern
    return '^' + legacy['prefix'] + '_(?:' + stream + '_)?' + legacy['suffix']


def check_months(paths, variable, expected_units=None):
    records=[]
    shape=None
    for path in paths:
        with netCDF4.Dataset(path) as ds:
            v=ds[variable]
            if expected_units and getattr(v,'units',None)!=expected_units:
                raise ValueError(f'{path.name}: {variable} units {getattr(v,"units",None)!r}, expected {expected_units!r}')
            t=next(ds[n] for n in ('time','time_counter') if n in ds.variables)
            dates=netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))
            records.extend((d.year,d.month) for d in dates if d.year in YEARS)
            if shape is not None and v.shape[1:]!=shape:
                raise ValueError('Spatial shape changed between files')
            shape=v.shape[1:]
    if records!=[(y,m) for y in YEARS for m in range(1,13)]:
        raise ValueError(f'Incomplete/duplicate monthly coverage: {len(records)} records')


def check_days(paths, variable, expected_units=None):
    """Header/time-only eligibility check for actual daily-mean native streams."""
    records, shape, calendar = [], None, None
    for path in paths:
        with netCDF4.Dataset(path) as ds:
            v = ds[variable]
            if expected_units and getattr(v, 'units', None) != expected_units:
                raise ValueError(f'{path.name}: {variable} units {getattr(v,"units",None)!r}, expected {expected_units!r}')
            if 'time: mean' not in getattr(v, 'cell_methods', ''):
                raise ValueError(f'{path.name}: expected an explicitly labelled daily mean')
            t = next(ds[n] for n in ('time', 'time_counter') if n in ds.variables)
            dates = netCDF4.num2date(t[:], t.units, getattr(t, 'calendar', 'standard'))
            this_calendar = dates[0].calendar if len(dates) else getattr(t, 'calendar', 'standard')
            if calendar is not None and this_calendar != calendar:
                raise ValueError('Calendar changed between daily files')
            calendar = this_calendar
            for d in dates:
                if d.year in YEARS:
                    if (d.hour, d.minute, d.second, d.microsecond) != (12, 0, 0, 0):
                        raise ValueError('Native daily means must have day-centred timestamps')
                    records.append((d.year, d.month, d.day))
            if shape is not None and v.shape[1:] != shape:
                raise ValueError('Spatial shape changed between daily files')
            shape = v.shape[1:]
    if not records:
        raise ValueError('Missing daily coverage')
    import cftime
    expected = []
    for year in YEARS:
        first = cftime.datetime(year, 1, 1, calendar=calendar)
        stop = cftime.datetime(year + 1, 1, 1, calendar=calendar)
        expected.extend((d.year, d.month, d.day) for d in
                        (first + timedelta(days=i) for i in range((stop-first).days)))
    if records != expected:
        raise ValueError(f'Incomplete/duplicate daily coverage: {len(records)} records, expected {len(expected)}')


def register_daily_pipelines(profile, campaign):
    """Extend the campaign profile without changing the established monthly routes."""
    variants = (
        ('lr_monthly', 'lr_daily', None),
        ('lr_linear', 'lr_daily_linear', None),
        ('lr_monthly', 'recom_daily_surface', 'transform_and_surface'),
        ('lr_linear', 'recom_daily_surface_linear', 'surface_only'),
    )
    existing = {p['name'] for p in profile['pipelines']}
    for base, name, surface_step in variants:
        if name in existing:
            continue
        pipe = copy.deepcopy(next(p for p in profile['pipelines'] if p['name'] == base))
        pipe['name'] = name
        replacements = {
            'pycmor.awiesm3_lr.load_monthly': 'pycmor.awiesm3_lr.load_daily',
            'pycmor.awiesm3_lr.load_linear': 'pycmor.awiesm3_lr.load_daily_linear',
            'pycmor.awiesm3_lr.monthly_time': 'pycmor.awiesm3_lr.daily_time',
        }
        pipe['steps'] = [replacements.get(step, step) for step in pipe['steps']]
        if surface_step:
            target = f'script://{campaign}/reproduce/extra_steps.py:{surface_step}'
            transform = 'pycmor.awiesm3_lr.transform'
            if transform in pipe['steps']:
                pipe['steps'][pipe['steps'].index(transform)] = target
            else:
                pipe['steps'].insert(2, target)
        profile['pipelines'].append(pipe)
    if 'recom_south30_linear' not in existing:
        pipe = copy.deepcopy(next(p for p in profile['pipelines'] if p['name'] == 'lr_linear'))
        pipe['name'] = 'recom_south30_linear'
        index = pipe['steps'].index('pycmor.std_lib.files.save_dataset')
        pipe['steps'].insert(index, f'script://{REPO}/examples/custom_steps.py:select_southern_hemisphere')
        profile['pipelines'].append(pipe)
    for base, name, surface_step in (
            ('lr_monthly','recom_south30_surface','transform_and_surface'),
            ('lr_linear','recom_south30_surface_linear','surface_only')):
        if name in existing:
            continue
        pipe = copy.deepcopy(next(p for p in profile['pipelines'] if p['name'] == base))
        pipe['name'] = name
        target = f'script://{campaign}/reproduce/extra_steps.py:{surface_step}'
        transform = 'pycmor.awiesm3_lr.transform'
        if transform in pipe['steps']:
            pipe['steps'][pipe['steps'].index(transform)] = target
        else:
            pipe['steps'].insert(2,target)
        index = pipe['steps'].index('pycmor.std_lib.files.save_dataset')
        pipe['steps'].insert(index, f'script://{REPO}/examples/custom_steps.py:select_southern_hemisphere')
        profile['pipelines'].append(pipe)


def register_derived_pipelines(profile, campaign):
    """Depth selection, column integrals, monthly means of daily streams and
    OpenIFS model levels; call after register_daily_pipelines."""
    existing = {p['name'] for p in profile['pipelines']}
    extra = f'script://{campaign}/reproduce/extra_steps.py:'
    transform = 'pycmor.awiesm3_lr.transform'
    save = 'pycmor.std_lib.files.save_dataset'
    def derive(base, name, edit):
        if name in existing:
            return
        pipe = copy.deepcopy(next(p for p in profile['pipelines'] if p['name'] == base))
        pipe['name'] = name
        edit(pipe['steps'])
        profile['pipelines'].append(pipe)
    def replace_or_insert(step):
        def edit(steps):
            if transform in steps:
                steps[steps.index(transform)] = extra + step
            else:
                steps.insert(2, extra + step)
        return edit
    derive('lr_monthly', 'lr_target_depth', replace_or_insert('transform_and_target_depth'))
    derive('lr_daily', 'lr_daily_target_depth', replace_or_insert('transform_and_target_depth'))
    derive('lr_daily_linear', 'lr_daily_target_depth_linear', replace_or_insert('target_depth_only'))
    def integral(steps):
        steps.insert(2, extra + 'vertical_integral')
        steps.insert(steps.index(save), f'script://{REPO}/examples/custom_steps.py:select_southern_hemisphere')
    derive('lr_linear', 'lr_south30_integral', integral)
    def daily_mean(steps):
        steps[steps.index('pycmor.awiesm3_lr.load_monthly')] = 'pycmor.awiesm3_lr.load_daily'
        steps.insert(1, extra + 'daily_to_monthly_mean')
    derive('lr_monthly', 'lr_daily_to_monthly', daily_mean)
    def model_levels(steps):
        steps.insert(steps.index('pycmor.std_lib.dimensions.map_dimensions') + 1,
                     f'script://{REPO}/examples/custom_steps.py:add_hybrid_sigma_coordinate')
    derive('lr_monthly', 'lr_model_levels', model_levels)
    derive('lr_monthly', 'lr_lowest_model_level', replace_or_insert('transform_and_lowest_level'))


def verify_recom_production(rule):
    """Bind net N/Fe production to this leg's actual namelist, not assumptions."""
    kind = rule.get('lr_recom_production')
    if not kind:
        return
    path = EXP / f'run_{PERIOD}' / 'config/fesom/namelist.recom'
    if not path.is_file() and ARCHIVED_FILES.get('namelist.recom'):
        path=Path(ARCHIVED_FILES['namelist.recom'])
    if not path.is_file():
        raise ValueError(f'Net REcoM production requires actual leg namelist: {path}')
    text = '\n'.join(line.split('!', 1)[0] for line in path.read_text().splitlines())
    def scalar(key):
        values = re.findall(r'^\s*' + re.escape(key) + r'\s*=\s*([^,\s/]+)', text, re.I | re.M)
        if len(values) != 1:
            raise ValueError(f'Require one explicit {key} in {path}')
        return values[0].lower()
    def number(key):
        value = float(scalar(key).replace('d','e'))
        if not np.isfinite(value):
            raise ValueError(f'Nonfinite REcoM parameter {key}')
        return value
    if kind == 'fe2n_ratio':
        # Particulate iron is not a tracer: organic Fe is fe2n * organic N
        # (micromol Fe per mmol N). Terms carry their own unit factor.
        fe2n = number('fe2n')
        if fe2n < 0:
            raise ValueError('REcoM fe2n must be nonnegative')
        for term in rule['lr_terms']:
            factor = term.pop('fe2n_factor', None)
            if factor is not None:
                term['coefficient'] = fe2n * factor
        evidence = {'fe2n_umolFe_per_mmolN': fe2n}
        rule['lr_recom_parameter_evidence'] = evidence
        rule['lr_auxiliary_files'] = list(rule.get('lr_auxiliary_files', [])) + [str(path)]
        rule['comment'] = (rule.get('comment','') + ' Actual leg namelist verified: ' +
                           json.dumps(evidence, sort_keys=True)).strip()
        return
    if scalar('enable_coccos') != '.false.':
        raise ValueError('NNAn+NNAd is complete only when enable_coccos=.false.')
    # The reviewed vertNNAd diagnostic uses lossN while biological update
    # uses lossN_d. These must coincide before reporting it as production.
    lossn, lossn_d = number('lossn'), number('lossn_d')
    if lossn != lossn_d:
        raise ValueError('NNAd diagnostic requires equal lossn and lossn_d in this reviewed source version')
    evidence = {'enable_coccos': False, 'lossn': lossn, 'lossn_d': lossn_d}
    if kind == 'iron':
        fe2n = number('fe2n')
        if fe2n < 0:
            raise ValueError('REcoM fe2n must be nonnegative')
        coefficient = fe2n * 1e-6 / 86400  # μmol Fe per mmol N, not mol/mol!
        for term in rule['lr_terms']:
            term['coefficient'] = coefficient
        evidence['fe2n_umolFe_per_mmolN'] = fe2n
    elif kind != 'nitrogen':
        raise ValueError(f'Unknown REcoM production kind {kind}')
    rule['lr_recom_parameter_evidence'] = evidence
    rule['lr_auxiliary_files'] = list(rule.get('lr_auxiliary_files', [])) + [str(path)]
    rule['comment'] = (rule.get('comment','') + ' Actual leg namelist verified: ' +
                       json.dumps(evidence, sort_keys=True)).strip()


def materialize(rule, gridpaths):
    rule=copy.deepcopy(rule)
    verify_recom_production(rule)
    # A monthly product may be averaged from a daily stream (lr_source_frequency).
    source_frequency = rule.get('lr_source_frequency', rule['compound_name'].split('.')[-2])
    check_time = check_days if source_frequency == 'day' else check_months
    group=rule.pop('lr_group','recom')
    key=rule.pop('lr_grid_key',None)
    if key:
        if key not in gridpaths:
            raise ValueError(f'No audited {key} fixed-grid geometry')
        rule['grid_file']=gridpaths[key]
    if rule['pipelines']==['lr_fx']:
        paths=[Path(rule['grid_file'])]
    else:
        selector=rule.pop('lr_input')
        # Normalize before lookup: the full-catalog driver replaces inputs()
        # with its indexed implementation. Keep all coverage/unit checks below.
        selector['pattern']=raw_input_pattern(selector['component'],selector['pattern'])
        paths=inputs(selector['component'],selector['pattern'])
        if not paths:
            raise ValueError(f'MISSING_INPUT: Missing source: {selector["component"]}/{selector["pattern"]}')
        if selector['component']!='lpj_guess':
            check_time(paths,rule['model_variable'],rule.get('expected_units'))
        for term in rule.get('lr_terms',[]):
            component=term.pop('component','oifs')
            pattern=raw_input_pattern(component,term.pop('pattern'))
            terms=inputs(component,pattern)
            if not terms:
                raise ValueError(f'MISSING_INPUT: {term["variable"]}: {component}/{pattern}')
            check_time(terms,term['variable'],term['units'])
            term['files']=list(map(str,terms))
        if rule.get('lr_thickness'):
            # Layer thickness of the same months for vertical integrals.
            thickness=rule.pop('lr_thickness')
            component,pattern=thickness['component'],thickness['pattern']
            files=inputs(component,pattern)
            if not files:
                raise ValueError(f'MISSING_INPUT: {thickness["variable"]}: {component}/{pattern}')
            check_months(files,thickness['variable'],thickness['units'])
            rule.update(lr_thickness_files=list(map(str,files)),lr_thickness_variable=thickness['variable'],
                        lr_thickness_units=thickness['units'])
            rule['lr_auxiliary_files']=list(rule.get('lr_auxiliary_files',[]))+list(map(str,files))
        if rule.get('lr_hybrid_levels'):
            # Same formula terms as the catalogue model-level rules: the
            # configured vtable and the 6-hourly surface pressure of this leg.
            if not MODEL_LEVEL_TABLE or not Path(MODEL_LEVEL_TABLE).is_file():
                raise ValueError('MISSING_MODEL_LEVEL_TABLE: configure model_level_table for the LR vertical grid')
            ps=rule.pop('lr_ps_input')
            files=inputs(ps['component'],ps['pattern'])
            if not files:
                raise ValueError(f'MISSING_INPUT: {ps["variable"]}: {ps["component"]}/{ps["pattern"]}')
            table=str(Path(MODEL_LEVEL_TABLE).resolve())
            rule.update(oifs_vtable=table,model_level_table_file=table,ps_variable=ps['variable'],
                        ps_path=str(files[0].parent),
                        ps_pattern='(?:'+'|'.join(re.escape(p.name) for p in files)+')')
            rule['lr_auxiliary_files']=list(rule.get('lr_auxiliary_files',[]))+[table]+list(map(str,files))
        rule['lr_source_files']=list(map(str,paths))
    if rule.get('lr_lpj_daily'):
        footprint=inputs('lpj_guess',rule['lr_lpj_footprint_pattern'])
        if not footprint:
            raise ValueError('Native daily LPJG requires the actual modeled-cell footprint')
        rule['lr_lpj_footprint_files']=list(map(str,footprint))
        rule['lr_auxiliary_files']=list(rule.get('lr_auxiliary_files',[]))+list(map(str,footprint))
        if rule.get('lr_soil_liquid_pattern'):
            liquid=inputs('lpj_guess',rule['lr_soil_liquid_pattern'])
            if not liquid:raise ValueError('Frozen soil water requires matching native liquid water')
            rule['lr_soil_liquid_source_files']=list(map(str,liquid))
            rule['lr_auxiliary_files']+=list(map(str,liquid))
    rule['inputs']=[{'path':str(p.parent),'pattern':re.escape(p.name)+'$'} for p in paths]
    if rule.get('lr_geothermal_zero'):
        if not GEOTHERMAL_CONTEXT or GEOTHERMAL_CONTEXT.get('binary_sha256')!=VERIFIED_FESOM_SHA256:
            raise ValueError('UNREVIEWED_GEOTHERMAL: reviewed binary does not match this archived run')
        import importlib.util
        path=Path(__file__).with_name('geothermal.py')
        # A directly frozen raw_adapter lives in reproduce/, while the complete
        # rules tree is retained in its full_workflow snapshot.
        if not path.is_file():path=Path(__file__).parent/'full_workflow/rules/geothermal.py'
        spec=importlib.util.spec_from_file_location('_verified_geothermal',path)
        geo=importlib.util.module_from_spec(spec);spec.loader.exec_module(geo)
        evidence=geo.verify_context(GEOTHERMAL_CONTEXT)
        rule.update(pipelines=['lr_geothermal_zero'],lr_geothermal_evidence_file=str(evidence),
                    lr_geothermal_binary_sha256=GEOTHERMAL_CONTEXT['binary_sha256'])
        rule['lr_auxiliary_files']=list(rule.get('lr_auxiliary_files',[]))+[str(evidence)]
    partners={'unod':('vnod','x'),'vnod':('unod','y'),'uice':('vice','x'),'vice':('uice','y')}
    if rule['model_variable'] in partners:
        partner,component=partners[rule['model_variable']]
        partner_paths=inputs('fesom',re.escape(partner)+r'\.fesom\.\d{4}\.nc$')
        check_months(partner_paths,partner,rule.get('expected_units'))
        rule.update(pipelines=['lr_rotated_vector'],vector_component=component,vector_rotation='always',
            vector_partner_path=str(EXP/'outdata/fesom'),
            vector_partner_pattern=re.escape(partner)+r'\.fesom\.(?:'+ '|'.join(map(str,YEARS))+r')\.nc$',
            vector_partner_variable=partner,alpha_euler=50.,beta_euler=15.,gamma_euler=-90.,
            vector_location='node',vector_source_files=list(map(str,partner_paths)))
    return group,rule


def _carbon_rules(filename):
    """Load canonical mappings, preserving unsupported candidates for reporting."""
    path = Path(__file__).resolve().parent / filename
    document = yaml.safe_load(path.read_text())
    if document.get('schema_version') != 1:
        raise ValueError(f'Unsupported carbon-rule schema: {path}')
    defaults = document.get('defaults', {})
    rules = document.get('rules')
    if not isinstance(defaults, dict) or not isinstance(rules, list):
        raise ValueError(f'Expected defaults mapping and rules list: {path}')
    names, compounds = set(), set()
    loaded = []
    for item in rules:
        if not isinstance(item, dict):
            raise ValueError(f'Expected a rule mapping: {path}')
        rule = copy.deepcopy(defaults)
        rule.update(copy.deepcopy(item))
        if not {'name', 'compound_name', 'model_variable', 'lr_input', 'pipelines'} <= rule.keys():
            raise ValueError(f'Incomplete carbon rule: {path}')
        if rule['name'] in names or rule['compound_name'] in compounds:
            raise ValueError(f'Duplicate carbon rule: {rule["name"]} in {path}')
        names.add(rule['name'])
        compounds.add(rule['compound_name'])
        if CARBON_CONTEXT:
            context = json.dumps(CARBON_CONTEXT, sort_keys=True, separators=(',', ':'))
            rule['comment'] = (rule.get('comment', '') +
                ' Operator-supplied carbon_context (not automatic scientific validation): ' +
                context).strip()
        loaded.append(rule)
    return document, loaded


def recom_rules():
    """Existing pool conversions plus metadata-supported surface-layer variants."""
    document, rules = _carbon_rules('recom.yaml')
    surface_options = document['surface_variants']
    if surface_options.get('policy') != 'metadata_supported_only':
        raise ValueError('REcoM surface variants require metadata_supported_only policy')
    for rule in rules:
        yield rule
        compound = rule['compound_name']
        if surface_options['source_brand'] not in compound or '.mon.' not in compound:
            continue
        surface_compound = compound.replace(
            surface_options['source_brand'], surface_options['target_brand'])
        if surface_compound in META:
            surface = copy.deepcopy(rule)
            surface.update(
                name=rule['name'] + surface_options['name_suffix'],
                compound_name=surface_compound, surface_layer=True,
                lr_exact_ocean_depth=False)
            surface['pipelines'] = [surface_options[
                'linear_pipeline' if rule.get('lr_terms') else 'monthly_pipeline']]
            yield surface
        else:
            # Some official requests contain only the Southern Ocean surface
            # product, not its global equivalent. Do not invent a global DReq
            # entry; use the reviewed first-layer conversion for that request.
            regional_compound = surface_compound.removesuffix('.glb') + '.30S-90S'
            if regional_compound in META:
                surface = copy.deepcopy(rule)
                surface.update(
                    name=rule['name'] + '_surface_south30',
                    compound_name=regional_compound, surface_layer=True,
                    lr_exact_ocean_depth=False, lr_region='30S-90S',
                    pipelines=['recom_south30_surface_linear' if rule.get('lr_terms')
                               else 'recom_south30_surface'])
                yield surface


def co2_rules():
    """Existing coupling/tracer conversions; experiment eligibility is driver-owned."""
    _, rules = _carbon_rules('co2.yaml')
    yield from rules


def daily_land_rules():
    """Native daily soil diagnostics; do not reuse remapped calendar mistakes."""
    _, rules = _carbon_rules('land_daily.yaml')
    yield from rules


def ocean_fixed_rules():
    _, rules = _carbon_rules('ocean_fixed.yaml')
    yield from rules
