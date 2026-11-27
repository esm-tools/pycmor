#!/usr/bin/env python3
"""Read-only evidence for the November 2026 LR supervisor checklist.

Header/static checks run on login nodes. --science requires Slurm, reads
bounded source slabs, and never changes model data. Unchecked science stays
explicitly REVIEW_REQUIRED; approximate HR climatologies are not pass gates.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET

import netCDF4
import numpy as np
import yaml


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def number(value):
    return float(value) if np.isfinite(value) else None


def physical(ds):
    return [k for k,v in ds.variables.items() if k not in ds.dimensions and
            k not in ('time','time_counter','time_centered','time_bounds','time_counter_bounds',
                      'lat','lon','nav_lat','nav_lon') and
            any(d in v.dimensions for d in ('time','time_counter')) and
            getattr(v,'standard_name','') != 'time']


def time_axis(ds):
    t=next(ds[n] for n in ('time','time_counter') if n in ds.variables)
    return t, netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))


def xml_streams(work):
    rows=[]
    for filename in ('file_def_fesom.xml','file_def.xml'):
        path=work/filename
        if not path.exists(): continue
        def visit(el,inherit):
            attrs=dict(inherit,**el.attrib)
            if el.tag=='file' and attrs.get('enabled','true').lower() not in ('false','.false.'):
                for f in el.findall('field'):
                    rows.append(dict(component='fesom' if 'fesom' in filename else 'oifs',
                        file=attrs.get('name',attrs.get('id')),output_freq=attrs.get('output_freq'),
                        **{k:dict(attrs,**f.attrib).get(k) for k in ('field_ref','id','name','operation','freq_op')}))
            for child in el: visit(child,attrs)
        visit(ET.parse(path).getroot(),{})
    return rows


def static_audit(exp,period,config,repo):
    work=exp/f'run_{period}'/'work'
    xml=xml_streams(work)
    by_ref=defaultdict(set)
    for r in xml:
        if r['component']=='fesom': by_ref[r['field_ref']].add(r['freq_op'])
    cadences={k:sorted(map(str,v)) for k,v in by_ref.items() if len(v)>1}
    metadata=json.loads(Path(config['metadata']).read_text())
    catalog=[];singular=[];duplicates=defaultdict(list)
    for path in sorted((repo/'awi-esm3-lr-variables').glob('*/cmip7xml*.yaml')):
        doc=yaml.safe_load(path.read_text())
        for r in doc['rules']:
            catalog.append(r['compound_name'])
            duplicates[r['compound_name']].append(str(path.relative_to(repo)))
            if 'pipeline' in r:singular.append(dict(file=str(path),name=r['name']))
    env={}
    for name in ('pycmor','xarray','dask','distributed','netCDF4','h5py','prefect','esgvoc',
                 'compliance-checker','cc-plugin-wcrp','cc-plugin-aicc','tripyview','CMIP7_data_request_api'):
        try: env[name]=importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:env[name]='MISSING'
    linked={}
    try:
        import h5py
        linked['h5py_hdf5']=h5py.version.hdf5_version
        linked['netcdf4_hdf5']=netCDF4.__hdf5libversion__
        linked['netcdf_c']=netCDF4.__netcdf4libversion__
        maps=Path('/proc/self/maps').read_text()
        linked['loaded_hdf5_libraries']=sorted(set(line.split()[-1] for line in maps.splitlines() if 'libhdf5' in line))
    except Exception as e:linked['error']=str(e)
    checks=[dict(id='source_identity',status='REVIEW_REQUIRED',
                 detail='Actual v3.5.1 coupled spinup; retain LR-SPINUP-TEST until registered source/experiment/parent are reviewed.'),
            dict(id='dreq',status='PASS' if digest(config['metadata'])=='eccf9f9296488581939310a37c60849501f77c9c9dd27faf8585b2345c18b9e3' else 'REVIEW_REQUIRED',
                 detail={'path':config['metadata'],'sha256':digest(config['metadata']),
                         'missing_catalogue_compounds':sorted(set(catalog)-metadata['Compound Name'].keys())}),
            dict(id='singular_pipeline_key',status='FAIL' if singular else 'PASS',detail=singular),
            dict(id='duplicate_catalogue_routes',status='REVIEW_REQUIRED',
                 detail={k:v for k,v in duplicates.items() if len(v)>1}),
            dict(id='fesom_mixed_freq_op',status='REVIEW_REQUIRED' if cadences else 'PASS',detail=cadences),
            dict(id='hybrid_table',status='PASS' if Path(config['model_level_table']).name=='vtable_L91' else 'FAIL',
                 detail=config['model_level_table']),
            dict(id='formal_qc',status='PASS' if env['cc-plugin-aicc']!='MISSING' and 'aicc' in config.get('qc_tests',[]) else 'FAIL',
                 detail='Checker availability/configuration only. Findings must be reviewed per output.'),
            dict(id='hdf5_libraries',status='REVIEW_REQUIRED',detail=linked)]
    for ident,detail in [
        ('branch_metadata','Do not inherit HR parents; production branch identity still needs experiment provenance review.'),
        ('vector_build','Pin and review the binary that generated the selected archive; namelist vec_autorotate alone is insufficient.'),
        ('heat_transport_constants','rho0*cp differs across pycmor, FESOM and tripyview; requires scientific convention review.'),
        ('land_carbon_flux_signs','Validate coupled atmospheric and land/ocean terms; never substitute land-only net flux.'),
        ('tree_fraction','LAI-weighted seasonal treeFrac requires review against the actual variable definition.'),
        ('sweLut','Verify source and target dimensional units; HR reported a possible factor 1000.'),
        ('seaice_drag','Constant drag without no-ice masking is an open model/definition issue.'),
        ('pfull_climatology','Verify actual accumulator years, bounds and single-product policy when this product is generated.'),
        ('all_native_output_shapes','Check every generated file including declared South30 subsets.'),
        ('all_formal_qc_findings','Classify by exact checker/version/message; HR known findings are not blanket exemptions.'),
        ('reference_year','No accepted end-to-end LR year yet; preserve successful source-verified year as reference.'),
        ('regional_cmor_tables','AICC does not prove requested region is valid; compare exact compound with pinned DReq.')]:
        checks.append(dict(id=ident,status='REVIEW_REQUIRED',detail=detail))
    return dict(environment=env,checks=checks,xml_streams=xml,
                archived_config_files={str(p):digest(p) for p in work.iterdir()
                    if p.is_file() and (p.suffix in ('.xml','.ins') or p.name.startswith('namelist'))},
                caveat='PASS records describe the named check only, not the entire product or publication readiness.')


def cadence_check(base,monthly,daily,year):
    with netCDF4.Dataset(base/f'{monthly}.fesom.{year}.nc') as m, \
         netCDF4.Dataset(base/f'{daily}.fesom.{year}.nc') as d:
        mv=m[physical(m)[0]];dv=d[physical(d)[0]]
        mt,md=time_axis(m);dt,dd=time_axis(d)
        rows=[]
        for j,date in enumerate(md):
            idx=[i for i,x in enumerate(dd) if x.year==date.year and x.month==date.month]
            if not idx:raise ValueError('Missing daily coverage')
            total=np.zeros(dv.shape[1:],dtype='f8');count=np.zeros(dv.shape[1:],dtype='i4')
            last=None
            for i in idx:
                value=np.ma.masked_invalid(dv[i]);valid=~np.ma.getmaskarray(value)
                total+=np.ma.filled(value,0);count+=valid;last=value
            mean=np.ma.array(total/np.maximum(count,1),mask=count==0)
            actual=np.ma.masked_invalid(mv[j])
            valid=~np.ma.getmaskarray(actual)&~np.ma.getmaskarray(mean)
            delta=np.ma.abs(actual-mean)
            scale=max(float(np.ma.max(np.ma.abs(actual))),1e-12)
            err=float(np.ma.max(delta)) if valid.any() else np.nan
            rows.append(dict(month=date.month,max_abs=number(err),relative_max=number(err/scale),
                monthly_vs_last_day=number(float(np.ma.max(np.ma.abs(actual-last)))),
                nan_mask_equal=bool(np.array_equal(np.ma.getmaskarray(actual),np.ma.getmaskarray(mean)))))
        ok=all(r['relative_max'] is not None and r['relative_max']<2e-5 and r['nan_mask_equal'] for r in rows)
        return dict(id=f'cadence_{monthly}_{daily}',status='PASS' if ok else 'FAIL',detail=rows)


def bottom_check(base,mesh,year):
    with netCDF4.Dataset(mesh) as g:
        indices=np.asarray(g['depth_lev'][:],dtype=int)-1
    result=[]
    for variable in ('temp','salt'):
        with netCDF4.Dataset(base/f'{variable}.fesom.{year}.nc') as d:
            v=d[variable];axes=v.dimensions
            slab=np.ma.masked_invalid(v[0])
            n=axes[1:].index('nod2');slab=np.moveaxis(slab,n,0)
            finite=np.isfinite(np.ma.filled(slab,np.nan))
            last=np.where(finite,np.arange(slab.shape[1]),-1).max(axis=1)
            mismatch=int(np.count_nonzero((last>=0)&(indices!=last)))
            result.append(dict(id=f'bottom_{variable}',status='PASS' if mismatch==0 else 'FAIL',
                detail=dict(compared_nodes=int((last>=0).sum()),index_mismatches=mismatch,formula='depth_lev - 1')))
    return result


def pressure_check(base,year):
    ref=base/f'atmos_mon_pl_ta_{year}-{year}.nc'
    if not ref.exists():ref=base/f'atm_remapped_1m_pl_t_{year}-{year}.nc'
    rows=[]
    with netCDF4.Dataset(ref) as r:
        rn=next(n for n in physical(r) if n in ('ta','t'))
        rv=r[rn];rt,rd=time_axis(r)
        rp=next(v for n,v in r.variables.items() if v.ndim==1 and (
            getattr(v,'standard_name','')=='air_pressure' or n.startswith('pressure_levels')))
        reference_pressure=np.asarray(rp[:]);
        if getattr(rp,'units','').lower() in ('hpa','mbar'):reference_pressure*=100
        for p in sorted(base.glob(f'atmos_*pl*_ta_{year}-{year}.nc')):
            if p==ref:continue
            with netCDF4.Dataset(p) as d:
                sn=next(n for n in physical(d) if n in ('ta','t'));sv=d[sn]
                sp=next(v for n,v in d.variables.items() if v.ndim==1 and (
                    getattr(v,'standard_name','')=='air_pressure' or n.startswith('pressure_levels')))
                pressure=np.asarray(sp[:]);
                if getattr(sp,'units','').lower() in ('hpa','mbar'):pressure*=100
                st,sd=time_axis(d)
                za=sv.dimensions.index(sp.dimensions[0]);rza=rv.dimensions.index(rp.dimensions[0])
                # Use deterministic spatial samples at several latitudes, all
                # levels, and January/July. Memory is bounded by one slab.
                detail=[]
                for month in (1,7):
                    indices=[i for i,date in enumerate(sd) if date.month==month]
                    if not indices:continue
                    def sample(var,i,zaxis):
                        sel=[slice(None)]*var.ndim;sel[0]=i
                        for ax in range(1,var.ndim):
                            if ax!=zaxis:sel[ax]=slice(0,None,max(1,var.shape[ax]//8))
                        return np.ma.asarray(var[tuple(sel)])
                    mean=None
                    for i in indices:
                        x=sample(sv,i,za);mean=x.astype('f8') if mean is None else mean+x
                    mean/=len(indices)
                    ri=next(i for i,date in enumerate(rd) if date.month==month)
                    reference=sample(rv,ri,rza)
                    errors=[];first_errors=[]
                    for j,pressure_value in enumerate(pressure):
                        candidates=np.flatnonzero(np.isclose(reference_pressure,pressure_value,atol=1.,rtol=0))
                        if len(candidates)!=1:
                            errors.append(None);first_errors.append(None)
                            continue
                        a=np.take(mean,j,axis=za-1);b=np.take(reference,int(candidates[0]),axis=rza-1)
                        errors.append(number(float(np.ma.max(np.ma.abs(a-b)))))
                        first_errors.append(number(float(np.ma.max(np.ma.abs(a-np.take(reference,j,axis=rza-1))))))
                    detail.append(dict(month=month,same_pressure_max_abs_K=errors,first_n_max_abs_K=first_errors))
                errors=[e for row in detail for e in row['same_pressure_max_abs_K'] if e is not None]
                ok=bool(errors) and max(errors)<.05
                # Different instantaneous sampling can alter a monthly mean;
                # retain that uncertainty instead of declaring a model error.
                rows.append(dict(id='pressure_'+p.stem,status='PASS' if ok else 'REVIEW_REQUIRED',
                    detail=dict(pressures_Pa=pressure.tolist(),comparisons=detail,
                    scope='Sampled spatial locations; monthly means from daily/subdaily source versus monthly p19. Null means pressure absent from p19; it is not validated.')))
    return rows


def science_audit(exp,period,config):
    if not os.environ.get('SLURM_JOB_ID'):raise ValueError('--science requires Slurm allocation')
    year=int(period[:4]);base=exp/'outdata/fesom';out=[]
    for monthly,daily in [('m_ice','m_ice_day'),('temp','temp_day'),('salt','salt_day')]:
        if all((base/f'{name}.fesom.{year}.nc').exists() for name in (monthly,daily)):
            try:out.append(cadence_check(base,monthly,daily,year))
            except Exception as e:out.append(dict(id='cadence_'+monthly,status='ERROR',detail=str(e)))
    for func,args in [(bottom_check,(base,Path(config['mesh_dir'])/'mesh.nc',year)),
                      (pressure_check,(exp/'outdata/oifs',year))]:
        try:out.extend(func(*args))
        except Exception as e:out.append(dict(id=func.__name__,status='ERROR',detail=repr(e)))
    return out


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--experiment',type=Path,required=True);p.add_argument('--leg',required=True)
    p.add_argument('--config',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--science',action='store_true');a=p.parse_args()
    config=json.loads(a.config.read_text());repo=Path(config['repo'])
    report=static_audit(a.experiment,a.leg,config,repo)
    if a.science:report['checks'].extend(science_audit(a.experiment,a.leg,config))
    report.update(updated=datetime.now(timezone.utc).isoformat(),experiment=str(a.experiment),leg=a.leg,
                  publication_ready=False,counts=dict(Counter(x['status'] for x in report['checks'])))
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(dict(report=str(a.output),counts=report['counts']),indent=2),flush=True)
    return int(any(x['status'] in ('FAIL','ERROR') for x in report['checks']))


if __name__=='__main__':raise SystemExit(main())
