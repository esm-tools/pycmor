"""Independent source checks for scientifically sensitive LR products.

Uses netCDF4/NumPy, never converter steps or their derived values. Checks are
bounded by an ocean surface slab. Unsupported science is explicitly recorded.
"""
from pathlib import Path
import re

import netCDF4
import numpy as np
from cf_units import Unit


SURFACE_MEANS={'ocean.tos.tavg-u-hm-sea.mon.glb': 'sst',
               'ocean.sos.tavg-u-hm-sea.mon.glb': 'sss'}


def verify_qc(paths, output, requested):
    import json
    reports={}
    for p in Path(output).glob('qc_*.json'):
        reports.update(json.loads(p.read_text()))
    checked=[]
    for path in paths:
        result=reports.get(str(path))
        if result is None:
            result=next((v for k,v in reports.items() if Path(k).resolve()==Path(path).resolve()),None)
        if not result:raise ValueError(f'Missing QC sidecar entry for {path}')
        present={k.split(':')[0] for k in result}
        if not set(requested)<=present:
            raise ValueError(f'Missing QC suites for {path}: {set(requested)-present}; got {present}')
        checked.append(str(path))
    return dict(status='REPORTS_PRESENT',files=checked,checkers=requested,
                limitations='Reports exist; all findings remain visible and require review.')


def source_check(task, cfg, paths):
    compound=task['compound']
    if compound in ('land.dcw.tavg-u-hxy-lnd.day.glb', 'land.dsn.tavg-u-hxy-lnd.day.glb'):
        return verify_storage_change(task, cfg, paths)
    if compound in ('seaIce.sivol.tavg-u-hm-u.day.nh', 'seaIce.sivol.tavg-u-hm-u.day.sh'):
        return verify_daily_ice_volume(task, cfg, paths)
    if compound == 'ocean.basin.ti-u-hxy-u.fx.glb':
        return verify_basin(cfg, paths)
    if compound.split('.')[1] in ('detoc','dissoc','zmeso','zmicro','zooc') and '.tavg-ol-' in compound and '.day.' in compound:
        return verify_full_daily(task, cfg, paths)
    if compound not in SURFACE_MEANS:
        return dict(status='NOT_IMPLEMENTED',compared_values=0,
                    limitation='Independent value audit is separate from conversion/basic checks.')
    rule=dict(cfg.get('inherit',{}),**cfg['rules'][0])
    inputs=rule['inputs'];var=SURFACE_MEANS[compound]
    source=[]
    for inp in inputs:
        source.extend(p for p in sorted(Path(inp['path']).iterdir()) if re.fullmatch(inp['pattern'],p.name))
    if rule.get('surface_area_file'):
        with netCDF4.Dataset(rule['surface_area_file']) as mesh:
            v=mesh['nod_area'];z=next(i for i,n in enumerate(v.dimensions) if n!='nod2')
            select=[slice(None)]*v.ndim;select[z]=0
            weights=np.ma.filled(v[tuple(select)],0.).astype('f8')
    else:
        with netCDF4.Dataset(rule['grid_file']) as mesh:
            weights=np.asarray(mesh['cell_area'][:],dtype='f8')
    monthly={};source_units=None
    for path in source:
        with netCDF4.Dataset(path) as ds:
            v=ds[var];source_units=v.units
            t=next(ds[n] for n in ('time','time_counter') if n in ds.variables)
            dates=netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))
            for ti,date in enumerate(dates):
                if date.year != task['year']:continue
                slab=np.ma.masked_invalid(np.ma.asarray(v[ti],dtype='f8')).reshape(-1)
                valid=~np.ma.getmaskarray(slab)
                if slab.size!=weights.size or weights[valid].sum()<=0:
                    raise ValueError('Invalid native surface mean geometry/coverage')
                mean=float(np.sum(np.ma.filled(slab,0)*weights)/weights[valid].sum())
                monthly.setdefault((date.year,date.month),[]).append(mean)
    expected={key:float(np.mean(values)) for key,values in monthly.items()}
    compared=0;max_error=0.
    for path in paths:
        with netCDF4.Dataset(path) as ds:
            v=ds[compound.split('.')[1]];t=ds['time']
            dates=netCDF4.num2date(t[:],t.units,t.calendar)
            for i,date in enumerate(dates):
                want=Unit(source_units).convert(expected[date.year,date.month],Unit(v.units))
                actual=float(v[i].squeeze());error=abs(want-actual)
                if not np.isclose(want,actual,rtol=2e-5,atol=2e-5):
                    raise ValueError(f'{compound}: area-weighted source mean {want} != {actual}')
                compared+=1;max_error=max(max_error,error)
    if compared!=12:raise ValueError('Surface mean check needs all twelve months')
    return dict(status='PASS',compared_values=compared,max_absolute_error=max_error,
                scope='Every monthly scalar, calculated independently from all exposed native surface nodes.')


def _primary_files(rule):
    return sorted({p for inp in rule['inputs'] for p in Path(inp['path']).iterdir()
                   if p.is_file() and re.fullmatch(inp['pattern'], p.name)})


def _compare_values(expected, actual, label):
    expected=np.ma.masked_invalid(np.ma.asarray(expected,dtype='f8'))
    actual=np.ma.masked_invalid(np.ma.asarray(actual,dtype='f8'))
    if expected.shape != actual.shape or not np.array_equal(
            np.ma.getmaskarray(expected),np.ma.getmaskarray(actual)):
        raise ValueError(label+': source/output shape or missing-value masks differ')
    if not np.allclose(expected.compressed(),actual.compressed(),rtol=2e-5,atol=2e-5):
        raise ValueError(label+': independent raw-value mismatch')
    return float(np.max(np.abs(expected.compressed()-actual.compressed()))) if expected.count() else 0.


def verify_storage_change(task, cfg, paths):
    """Independently check every daily water-storage difference, including Jan 1."""
    from contextlib import ExitStack
    from input_streams import date_tuple
    rule=dict(cfg.get('inherit',{}),**cfg['rules'][0])
    output_name=task['compound'].split('.')[1]
    source_name={'dcw':'src','dsn':'sd'}[output_name]
    files=_primary_files(rule)
    if len(files)!=1:raise ValueError('Storage audit requires one frozen annual primary file')
    boundary=rule['land_boundary_records'][source_name]
    compared=0;maximum=0.;seen=[]
    with ExitStack() as stack:
        source=stack.enter_context(netCDF4.Dataset(files[0]))
        prior=stack.enter_context(netCDF4.Dataset(boundary['file']))
        st=next(source[n] for n in ('time','time_counter') if n in source.variables)
        dates=[date_tuple(d) for d in netCDF4.num2date(st[:],st.units,getattr(st,'calendar','standard'))]
        indices={d:i for i,d in enumerate(dates)}
        previous=np.ma.asarray(prior[source_name][boundary['index']],dtype='f8')
        for path in sorted(paths):
            output=stack.enter_context(netCDF4.Dataset(path));t=output['time'];v=output[output_name]
            for ti,date in enumerate(netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))):
                key=date_tuple(date);si=indices[key]
                current=np.ma.asarray(source[source_name][si],dtype='f8')
                before=(previous if si==0 else np.ma.asarray(source[source_name][si-1],dtype='f8'))
                want=(current-before)*1000.0
                error=_compare_values(want,np.ma.asarray(v[ti]).reshape(want.shape),output_name)
                maximum=max(maximum,error);compared+=want.size;seen.append(key)
        if seen!=dates:raise ValueError('Storage audit does not cover every original daily record')
    return dict(status='PASS',compared_values=compared,max_absolute_error=maximum,
                scope='Every daily time and native cell; January 1 uses the pinned previous December 31 storage.')


def verify_daily_ice_volume(task, cfg, paths):
    """Recalculate every hemispheric daily integral from m_ice and surface area."""
    from contextlib import ExitStack
    from input_streams import date_tuple
    rule=dict(cfg.get('inherit',{}),**cfg['rules'][0])
    checked=0;maximum=0.;wanted={}
    with ExitStack() as stack:
        mesh=stack.enter_context(netCDF4.Dataset(rule['grid_file']))
        lat=next(mesh[n] for n in ('lat','latitude') if n in mesh.variables)[:]
        if rule.get('surface_area_file'):
            diag=stack.enter_context(netCDF4.Dataset(rule['surface_area_file']))
            area=diag['nod_area'];select=[slice(None)]*area.ndim
            select[next(i for i,n in enumerate(area.dimensions) if n!='nod2')]=0
            weights=np.ma.filled(area[tuple(select)],0.).astype('f8')
        else:
            weights=np.ma.filled(next(mesh[n] for n in ('cell_area','cluster_area') if n in mesh.variables)[:],0.).astype('f8')
        north=task['compound'].endswith('.nh')
        weights=weights*((lat>=0) if north else (lat<0))
        for filename in _primary_files(rule):
            source=stack.enter_context(netCDF4.Dataset(filename));v=source['m_ice']
            t=next(source[n] for n in ('time','time_counter') if n in source.variables)
            for ti,date in enumerate(netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))):
                key=date_tuple(date)
                if key in wanted:raise ValueError('Duplicate source ice-volume date')
                wanted[key]=float(np.sum(np.ma.filled(v[ti],0.)*weights))
        seen=[]
        for filename in sorted(paths):
            output=stack.enter_context(netCDF4.Dataset(filename));v=output['sivol'];t=output['time']
            for ti,date in enumerate(netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))):
                key=date_tuple(date)
                want=Unit('m3').convert(wanted[key],Unit(v.units))
                maximum=max(maximum,_compare_values(want,float(v[ti]),'daily sivol'))
                seen.append(key);checked+=1
        if seen!=sorted(wanted):raise ValueError('Ice-volume audit does not cover all daily records')
    return dict(status='PASS',compared_values=checked,max_absolute_error=maximum,
                scope='Every daily hemisphere integral, independently summed over native surface areas.')


def verify_full_daily(task, cfg, paths):
    """Compare every value of the five large daily REcoM products by slabs."""
    from contextlib import ExitStack
    rule=dict(cfg['inherit'],**cfg['rules'][0]);variable=task['compound'].split('.')[1]
    terms=rule.get('lr_terms') or [dict(files=rule['lr_source_files'],
        variable=rule['model_variable'],coefficient=rule.get('lr_scale',1.))]
    compared=0;maximum=0.
    with ExitStack() as stack:
        inputs=[]
        for term in terms:
            for filename in term['files']:
                ds=stack.enter_context(netCDF4.Dataset(filename));t=next(ds[n] for n in ('time','time_counter') if n in ds.variables)
                dates=netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))
                lookup={(d.year,d.month,d.day):i for i,d in enumerate(dates)}
                inputs.append((term,ds,lookup))
        for filename in paths:
            ds=stack.enter_context(netCDF4.Dataset(filename));v=ds[variable];t=ds['time']
            dates=netCDF4.num2date(t[:],t.units,t.calendar)
            # Locate output depth and horizontal dimensions independently.
            z=next(n for n,c in ds.variables.items() if c.ndim==1 and getattr(c,'standard_name','')=='depth')
            lat=next(ds[n] for n in ('latitude','lat') if n in ds.variables)
            output_order=[('vertical' if d==ds[z].dimensions[0] else 'horizontal'
                           if d==lat.dimensions[0] else d) for d in v.dimensions[1:]]
            for ti,date in enumerate(dates):
                expected=None
                for term,source,lookup in inputs:
                    key=(date.year,date.month,date.day)
                    if key not in lookup:continue
                    sv=source[term['variable']]
                    frame=np.ma.masked_invalid(np.ma.asarray(sv[lookup[key]],dtype='f8'))
                    order=['vertical' if d in ('nz','nz1','lev','depth','olevel') else
                           'horizontal' if d in ('nod2','cell','ncells') else d for d in sv.dimensions[1:]]
                    frame=frame.transpose([order.index(d) for d in output_order])*term['coefficient']
                    expected=frame if expected is None else expected+frame
                if expected is None:raise ValueError('Daily raw timestamp missing')
                if not rule.get('lr_terms'):expected+=rule.get('lr_offset',0.)
                actual=np.ma.masked_invalid(v[ti])
                if actual.shape!=expected.shape or not np.array_equal(np.ma.getmaskarray(actual),np.ma.getmaskarray(expected)):
                    raise ValueError(f'{variable}: daily shape/NaN mask changed at {date}')
                delta=np.ma.abs(actual-expected)
                invalid=np.ma.filled(delta>(3e-5*np.ma.abs(expected)+1e-30),False)
                if invalid.any():raise ValueError(f'{variable}: raw-value mismatch at {date}; {int(invalid.sum())} values')
                maximum=max(maximum,float(np.ma.max(delta)))
                compared+=int(actual.size)
    return dict(status='PASS',compared_values=compared,max_absolute_error=maximum,
                scope='All daily times, native nodes and depth layers, including identical missing-value masks; bounded one-day slabs.')


def verify_basin(cfg, paths):
    import json
    rule=dict(cfg['inherit'],**cfg['rules'][0]);meta=json.loads(Path(cfg['general']['CMIP7_DReq_metadata']).read_text())['Compound Name'][rule['compound_name']]
    norm=lambda s:s.lower().removesuffix('_ocean')
    want={norm(k):int(v) for k,v in zip(meta['flag_meanings'].split(),meta['flag_values'].split())}
    with netCDF4.Dataset(rule['basin_mask_file']) as ds:
        v=ds['basin'];original=np.asarray(v[:]);expected=np.full(original.shape,-1,dtype='i4')
        for meaning,code in zip(v.flag_meanings.split(),v.flag_values):
            expected[original==code]=want[norm(meaning)]
    if (expected<0).any():raise ValueError('Unclassified source basin flags')
    with netCDF4.Dataset(paths[0]) as ds:
        out=ds['basin']
        if not np.array_equal(out[:].reshape(-1),expected.reshape(-1)):raise ValueError('Basin values differ from DReq code mapping')
        if list(map(int,out.flag_values))!=list(map(int,meta['flag_values'].split())) or out.flag_meanings!=meta['flag_meanings']:
            raise ValueError('Basin flag metadata differs from DReq')
    return dict(status='PASS',compared_values=int(expected.size),scope='Every node recoded independently by CF meaning into DReq codes.')
