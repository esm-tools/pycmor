"""Read-only discovery of complete calendar years across arbitrary archive legs."""
import copy
import hashlib
import json
import re
import time
from pathlib import Path

import netCDF4
import numpy as np
import pandas as pd
import yaml

MONTHS='Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()


class CoverageError(ValueError):
    pass


def signature(path):
    stat=Path(path).stat()
    return [stat.st_size,stat.st_mtime_ns]


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()


def load_config(path):
    path=Path(path).resolve()
    cfg=yaml.safe_load(path.read_text())
    required={'name','run_root','state_dir','output_root','first_year','last_year',
              'venv','cache_root','metadata','grids','inherit'}
    missing=required-set(cfg)
    if missing:
        raise ValueError(f'Missing campaign settings: {sorted(missing)}')
    if not re.fullmatch(r'[A-Za-z0-9_-]+',cfg['name']):
        raise ValueError('Campaign name must contain only letters, digits, - or _')
    if not 1583<=int(cfg['first_year'])<=int(cfg['last_year'])<=9998:
        raise ValueError('This Gregorian profile requires 1583 <= first_year <= last_year <= 9998')
    cfg['first_year']=int(cfg['first_year']);cfg['last_year']=int(cfg['last_year'])
    for key in ['run_root','state_dir','output_root','venv','cache_root']:
        cfg[key]=str((path.parent/Path(cfg[key])).resolve())
    for section in ['metadata','grids']:
        cfg[section]={k:str((path.parent/Path(v)).resolve()) for k,v in cfg[section].items()}
    cfg['input_roots']={k:str((path.parent/Path(v)).resolve()) for k,v in cfg.get('input_roots',{}).items()}
    for key in ['source_id','institution_id','experiment_id','activity_id','variant_label']:
        if not cfg['inherit'].get(key):
            raise ValueError(f'Missing inherit.{key}; do not borrow HR experiment metadata')
    for key in ['year_start','year_end','output_directory']:
        if key in cfg['inherit']:
            raise ValueError(f'{key} is controlled by the campaign, not inherit')
    run=Path(cfg['run_root']);state=Path(cfg['state_dir']);output=Path(cfg['output_root'])
    if not run.is_dir():
        raise FileNotFoundError(run)
    if state==output or state in output.parents or output in state.parents:
        raise ValueError('state_dir and output_root must be separate directory trees')
    if any(p==run or run in p.parents or p in run.parents for p in (state,output)):
        raise ValueError('Campaign state/output must not overlap original experiment data')
    cfg.setdefault('groups',['atmos','ocean','seaice','land','fx_areacella_atmos','fx_areacella_lpj','fx_areacello'])
    cfg.setdefault('follow',False)
    cfg.setdefault('settle_seconds',300)
    cfg.setdefault('poll_seconds',300)
    cfg.setdefault('max_attempts',3)
    cfg.setdefault('profile','tco95-core3-raw-monthly-v1')
    cfg.setdefault('accumulation_seconds',3600)
    cfg.setdefault('slurm',{})
    for key,val in dict(account='ab0995',partition='shared',cpus=4,memory='32G',
                        time='04:00:00',concurrency=4,driver_time='3-00:00:00').items():
        cfg['slurm'].setdefault(key,val)
    for key in ['max_attempts','poll_seconds']:
        if int(cfg[key])<1:
            raise ValueError(f'{key} must be positive')
    if int(cfg['settle_seconds'])<0 or int(cfg['slurm']['concurrency'])<1:
        raise ValueError('Invalid settling interval/concurrency')
    return cfg


def check_profile(cfg,profile):
    if cfg['profile']!=profile['id'] or cfg['accumulation_seconds']!=profile['accumulation_seconds']:
        raise ValueError('Unsupported scientific profile/accumulation interval; audit mappings before using another')
    unknown=set(cfg['groups'])-set(profile['groups'])
    if unknown:
        raise ValueError(f'Unknown groups: {sorted(unknown)}')
    for path in [*cfg['grids'].values(),*cfg['metadata'].values()]:
        if not Path(path).exists():
            raise FileNotFoundError(path)
    root=Path(cfg['run_root'])
    forts=sorted((root/'config/oifs').glob('fort.4*'))
    if not forts:
        raise ValueError('No archived OpenIFS fort.4 found to verify accumulation settings')
    for path in {p.resolve() for p in forts}:
        text=path.read_text()
        for name,expected in [('TSTEP',3600.),('NFRPOS',1.),('NFRHIS',1.)]:
            match=re.search(rf'(?im)^\s*{name}\s*=\s*([0-9.eEdD+-]+)',text)
            if not match or float(match[1].replace('D','e').replace('d','e'))!=expected:
                raise ValueError(f'{path}: {name} does not match the audited accumulation profile')
        if not re.search(r'(?im)^\s*LRSACC\s*=\s*\.?true\.?',text):
            raise ValueError(f'{path}: LRSACC must be true')
    namelist=root/'config/fesom/namelist.io'
    if not re.search(r'(?im)^\s*vec_autorotate\s*=\s*\.?true\.?',namelist.read_text()):
        raise ValueError('This profile requires geographic (vec_autorotate=true) ocean vectors')


class SourceIndex:
    """Cache header/coverage checks by file stat and grid identity, never by leg size."""
    def __init__(self,cfg,cache=None):
        self.cfg=cfg
        self.cache={} if cache is None else cache
        self.files={}
        self.match_cache={}
        self.grids={}
        for key,path in cfg['grids'].items():
            with netCDF4.Dataset(path) as ds:
                self.grids[key]={name:np.asarray(ds[name][:]) for name in ['lat','lon','depth'] if name in ds.variables}
        self.grid_key=digest({k:signature(p) for k,p in cfg['grids'].items()})
        for component in ['oifs','fesom','lpj_guess']:
            base=Path(cfg.get('input_roots',{}).get(component,Path(cfg['run_root'])/'outdata'/component))
            # Only archived outdata is searched: never run_*/work directories.
            suffix='*.out' if component=='lpj_guess' else '*.nc'
            self.files[component]=sorted({p.resolve() for p in base.rglob(suffix) if p.is_file()})

    def matches(self,component,pattern):
        key=(component,pattern)
        if key in self.match_cache:
            return self.match_cache[key]
        regex=re.compile(pattern)
        self.match_cache[key]=[p for p in self.files[component] if regex.fullmatch(p.name)]
        return self.match_cache[key]

    def inspect(self,path,kind,variable,grid):
        before=signature(path)
        key=f'{path}|{kind}|{variable}|{grid}'
        cached=self.cache.get(key)
        if cached and cached['signature']==before and cached['grid_key']==self.grid_key:
            return cached
        if kind=='nc':
            summary=self._netcdf(path,variable,grid)
        else:
            summary=self._lpj(path,kind,variable,grid)
        if signature(path)!=before:
            raise CoverageError(f'File changed while inspecting: {path}')
        summary.update(signature=before,grid_key=self.grid_key)
        self.cache[key]=summary
        return summary

    def _netcdf(self,path,variable,grid):
        result={'years':{},'units':None}
        expected=self.grids[grid]
        with netCDF4.Dataset(path) as ds:
            if variable not in ds.variables:
                raise CoverageError(f'{path}: missing variable {variable}')
            var=ds[variable]
            result['units']=getattr(var,'units','')
            for name in ['lat','lon']:
                actual=np.asarray(ds[name][:])
                wanted=expected[name]
                if actual.shape!=wanted.shape:
                    raise CoverageError(f'{path}: incompatible {name} shape {actual.shape}')
                delta=actual-wanted
                if name=='lon':
                    delta=(delta+180)%360-180
                if not np.allclose(delta,0,atol=1e-4,rtol=0):
                    raise CoverageError(f'{path}: incompatible {name} coordinates')
            if grid=='core3' and var.ndim==3:
                dim=var.dimensions[-1]
                with netCDF4.Dataset(self.cfg['grids']['core3']) as mesh:
                    depths=mesh['depth_bnds'][:] if variable=='w' else mesh['depth'][:]
                    if ds[dim].shape!=depths.shape or not np.allclose(ds[dim][:],depths,atol=1e-4,rtol=0):
                        raise CoverageError(f'{path}: incompatible ocean vertical grid')
            name=next((n for n in ['time_counter','time'] if n in ds.variables),None)
            if name is None:
                raise CoverageError(f'{path}: no time coordinate')
            t=ds[name];cal=getattr(t,'calendar','standard')
            if cal not in ['gregorian','standard','proleptic_gregorian']:
                raise CoverageError(f'{path}: unsupported calendar {cal}')
            if var.dimensions[0] not in t.dimensions:
                raise CoverageError(f'{path}: time must be first data dimension')
            dates=netCDF4.num2date(t[:],t.units,cal)
            bounds=netCDF4.num2date(ds[t.bounds][:],t.units,cal)
            for date,(start,end) in zip(dates,bounds):
                year=str(date.year)
                result['years'].setdefault(year,{'months':[],'errors':[]})
                result['years'][year]['months'].append(date.month)
                next_year=date.year+(date.month==12);next_month=date.month%12+1
                if str(start)!=f'{date.year:04}-'+f'{date.month:02}-01 00:00:00' or str(end)!=f'{next_year:04}-{next_month:02}-01 00:00:00':
                    result['years'][year]['errors'].append('Non-calendar-month bounds')
        return result

    def _lpj(self,path,kind,variable,grid):
        columns=MONTHS if kind=='lr_lpj' else ([variable] if kind=='lr_lpj_yearly' else ['psl','pst','crp','urb'])
        required=['Year','Lon','Lat']+(['Mth'] if kind=='lr_lpj_lut' else [])+columns
        df=pd.read_csv(path,sep=r'\s+',usecols=required)
        result={'years':{}}
        wanted=self.grids[grid]
        for year,frame in df.groupby('Year'):
            record={'months':[],'errors':[]}
            groups=frame.groupby('Mth') if kind=='lr_lpj_lut' else [(0,frame)]
            for month,part in groups:
                part=part.sort_values(['Lat','Lon'],ascending=[False,True])
                if part.duplicated(['Lat','Lon']).any() or len(part)!=len(wanted['lat']):
                    record['errors'].append('Missing/duplicate land cells')
                    continue
                if not np.allclose(part.Lat,wanted['lat'],atol=1e-4,rtol=0) or not np.allclose((part.Lon.to_numpy()-wanted['lon']+180)%360-180,0,atol=1e-4,rtol=0):
                    record['errors'].append('Land grid differs from configured grid')
                if not np.isfinite(part[columns].to_numpy()).all():
                    record['errors'].append('Missing/non-finite LPJ values')
                record['months']+=([int(month)] if kind=='lr_lpj_lut' else ([0] if kind=='lr_lpj_yearly' else list(range(1,13))))
            result['years'][str(int(year))]=record
        return result

    def select(self,component,pattern,kind,variable,grid,year,units=None):
        paths=[];months=[]
        errors=[]
        for path in self.matches(component,pattern):
            try:
                info=self.inspect(path,kind,variable,grid)
            except (OSError,RuntimeError,ValueError,KeyError,AttributeError) as exc:
                # Never silently ignore an unreadable matching archive.
                errors.append(str(exc));continue
            record=info['years'].get(str(year))
            if record is None:
                continue
            if time.time()-path.stat().st_mtime<self.cfg['settle_seconds']:
                errors.append(f'Not settled yet: {path}');continue
            if units is not None and info['units']!=units:
                errors.append(f'{path}: expected units {units}, found {info["units"]}')
            errors.extend(f'{path}: {e}' for e in record['errors'])
            paths.append(str(path));months+=record['months']
        expected=[0] if kind=='lr_lpj_yearly' else list(range(1,13))
        if sorted(months)!=expected:
            errors.append(f'{variable} {year}: expected records {expected}, found {sorted(months)} (missing or overlapping archives)')
        if errors:
            raise CoverageError('; '.join(errors[:4]))
        return paths


def recipe_for(cfg,profile,index,group,year):
    rules=copy.deepcopy(profile['groups'][group])
    errors=[]
    for r in rules:
        key=r.pop('lr_grid_key',None)
        if key:
            r['grid_file']=cfg['grids'][key]
        if r['pipelines']==['lr_fx']:
            p=Path(r['grid_file'])
            r['inputs']=[{'path':str(p.parent),'pattern':re.escape(p.name)+'$'}]
            continue
        selector=r.pop('lr_input')
        selector.update(cfg.get('input_patterns',{}).get(r['name'],{}))
        component=selector['component'];grid=key or 'atmos'
        kind=r['pipelines'][0] if component=='lpj_guess' else 'nc'
        try:
            paths=index.select(component,selector['pattern'],kind,r['model_variable'],grid,year,r.get('expected_units'))
            r['lr_source_files']=paths
            # InputFileCollection remains well-formed; custom readers use the exact list.
            r['inputs']=[{'path':str(Path(p).parent),'pattern':re.escape(Path(p).name)+'$'} for p in paths]
            for term in r.get('lr_terms',[]):
                pattern=term.pop('pattern')
                term['files']=index.select('oifs',pattern,'nc',term['variable'],'atmos',year,term['units'])
        except CoverageError as exc:
            errors.append(f'{r["name"]}: {exc}')
    if errors:
        raise CoverageError('\n'.join(errors))
    inherit=copy.deepcopy(profile['defaults'])
    inherit.update(cfg['inherit'])
    inherit.update(year_start=year,year_end=year,time_units=cfg.get('time_units',f'days since {cfg["first_year"]}-01-01'),
                   output_directory=str(Path(cfg['output_root'])/f'{group}_{year}'))
    if group.startswith('fx_'):
        inherit['output_directory']=str(Path(cfg['output_root'])/group)
    return {'general':{'name':f'{cfg["name"]}-{group}-{year}','cmor_version':'CMIP7','mip':'CMIP',
                       'CMIP7_DReq_metadata':cfg['metadata']['dreq'],'CV_Dir':cfg['metadata']['cv']},
            'pycmor':copy.deepcopy(profile['pycmor']),'inherit':inherit,
            'pipelines':copy.deepcopy(profile['pipelines']),'rules':rules}


def source_paths(recipe):
    paths=set()
    for r in recipe['rules']:
        paths.update(r.get('lr_source_files',[]))
        for term in r.get('lr_terms',[]):
            paths.update(term['files'])
        if r.get('grid_file'):
            paths.add(r['grid_file'])
    return sorted(paths)


def plan(cfg,profile,index):
    tasks=[];years=[]
    for year in range(cfg['first_year'],cfg['last_year']+1):
        row={'year':year,'ready':True,'errors':{}}
        candidates=[]
        for group in cfg['groups']:
            if group.startswith('fx_'):
                continue
            try:
                recipe=recipe_for(cfg,profile,index,group,year)
                candidates.append({'id':f'{group}_{year}','group':group,'year':year,'recipe':recipe})
            except CoverageError as exc:
                row['ready']=False;row['errors'][group]=str(exc)
        # A year is released only when all requested realms are ready.
        if row['ready']:
            tasks+=candidates
        years.append(row)
    ready=next((r['year'] for r in years if r['ready']),None)
    if ready is not None:
        for group in cfg['groups']:
            if group.startswith('fx_'):
                tasks.append({'id':group,'group':group,'year':None,'recipe':recipe_for(cfg,profile,index,group,ready)})
    return {'years':years,'tasks':tasks,'requested_years':len(years),
            'ready_years':sum(r['ready'] for r in years),
            'expected_products':sum(len(profile['groups'][g])*(1 if g.startswith('fx_') else len(years)) for g in cfg['groups'])}
