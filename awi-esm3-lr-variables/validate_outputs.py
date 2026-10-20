"""Independent source-to-output numerical, mask, calendar and coverage checks."""
import argparse
from contextlib import ExitStack
import itertools
import json
import os
from pathlib import Path
import re
import netCDF4 as nc
import numpy as np
import pandas as pd
import yaml

ROOT=Path(os.environ.get('PYCMOR_LR_ROOT',Path(__file__).resolve().parents[2]))

def dates(variable,values=None):
    return [str(x) for x in np.asarray(nc.num2date(variable[:] if values is None else values,variable.units,getattr(variable,'calendar','standard'))).ravel()]

def compare_arrays(expected,actual):
    expected=np.ma.masked_invalid(expected);actual=np.ma.masked_invalid(actual)
    assert expected.shape==actual.shape,(expected.shape,actual.shape)
    assert np.array_equal(np.ma.getmaskarray(expected),np.ma.getmaskarray(actual)),'missing masks differ'
    a=expected.compressed().astype('float64');b=actual.compressed().astype('float64')
    # Five significant decimal digit BitGroom: deliberately looser than its binary ULP.
    assert np.allclose(a,b,rtol=2e-5,atol=1e-15),'numerical values differ beyond quantization tolerance'
    return float(np.max(np.abs(a-b),initial=0)),len(a)

def compare_coord(expected,actual,name):
    difference=np.asarray(expected)-np.asarray(actual)
    if 'lon' in name:
        difference=(difference+180)%360-180
    assert np.allclose(difference,0,rtol=0,atol=1e-4),f'{name} mismatch'


def source_records(stack,paths,variable,year,units):
    """Independent selection: an input file can span any number of years."""
    records={}
    for path in paths:
        src=stack.enter_context(nc.Dataset(path))
        assert src[variable].units==units
        time=src['time_counter'] if 'time_counter' in src.variables else src['time']
        for index,date in enumerate(nc.num2date(time[:],time.units,getattr(time,'calendar','standard'))):
            if date.year!=year:
                continue
            assert date.month not in records, f'Duplicate source month {year}-{date.month}'
            records[date.month]=(src,index,time)
    assert set(records)==set(range(1,13)),f'Incomplete source year {year}'
    return [records[m] for m in range(1,13)]


def check_record_geometry(records,dst,tb):
    for src,index,time in records:
        for name in ['lat','lon']:
            dname=name if name in dst.variables else ('latitude' if name=='lat' else 'longitude')
            compare_coord(src[name][:],dst[dname][:],name)
    actual=[d for src,index,time in records for d in dates(time,src[time.bounds][index])]
    assert actual==dates(dst['time'],tb),'source time bounds differ'


def main():
    ap=argparse.ArgumentParser();ap.add_argument('recipe');ap.add_argument('--snapshot');args=ap.parse_args()
    cfg=yaml.safe_load(Path(args.snapshot or args.recipe).read_text());year=cfg['inherit']['year_start'];out=Path(cfg['inherit']['output_directory'])
    files=list(out.rglob('*.nc'));reports=[]
    for r in cfg['rules']:
        record={'rule':r['name'],'compound_name':r['compound_name']}
        try:
            var=r['compound_name'].split('.')[1]
            _,_,branding,frequency,region=r['compound_name'].split('.')
            grid=r.get('grid_label',cfg['inherit']['grid_label'])
            prefix=f'{var}_{branding}_{frequency}_{region}_{grid}_'
            matches=[p for p in files if p.name.startswith(prefix)]
            assert len(matches)==1,f'Expected one annual file, found {len(matches)}'
            p=matches[0];record['output']=str(p)
            with nc.Dataset(p) as dst:
                if r['pipelines']==['lr_fx']:
                    assert 'time' not in dst.dimensions
                    with nc.Dataset(r['grid_file']) as src:
                        err,count=compare_arrays(src[r['lr_mesh_variable']][:],dst[var][:])
                    record.update(status='PASS',max_absolute_error=err,valid_values_compared=count)
                    reports.append(record)
                    continue
                records=1 if frequency=='yr' else 12
                dv=dst[var];assert len(dst.dimensions['time'])==records
                times=nc.num2date(dst['time'][:],dst['time'].units,dst['time'].calendar)
                expected_months=[7] if frequency=='yr' else list(range(1,13))
                assert [(d.year,d.month) for d in times]==[(year,m) for m in expected_months]
                tb=dst[dst['time'].bounds][:];assert np.all(tb[:-1,1]==tb[1:,0])
                bounds=nc.num2date(tb,dst['time'].units,dst['time'].calendar)
                assert str(bounds[0,0])==f'{year}-01-01 00:00:00' and str(bounds[-1,1])==f'{year+1}-01-01 00:00:00'
                # pycmor canonicalizes Gregorian to proleptic Gregorian. For this
                # post-1582 experiment these calendars have identical dates and
                # durations; verify the actual bounds below, including leap years.
                assert year>=1583 and dst['time'].calendar in ['gregorian','standard','proleptic_gregorian']
                assert np.allclose(dst['time'][:],tb.mean(1)), 'times are not interval midpoints'
                units=str(dv.units)
                record.update(units=units,calendar=dst['time'].calendar,records=records)
                max_error=0.;valid=0
                if r['pipelines'] in (['lr_lpj'],['lr_lpj_yearly'],['lr_lpj_lut']):
                    inp=r['inputs'][0]
                    paths=r.get('lr_source_files') or [Path(inp['path'])/inp['pattern']]
                    df=pd.concat([pd.read_csv(p,sep=r'\s+') for p in paths],ignore_index=True)
                    df=df[df.Year==year].sort_values(['Lat','Lon'],ascending=[False,True])
                    if r['pipelines']==['lr_lpj_lut']:
                        assert not df.duplicated(['Lat','Lon','Mth']).any()
                        expected=np.asarray([df[df.Mth==month][['psl','pst','crp','urb']].to_numpy().T for month in range(1,13)])*r.get('lr_scale',1)
                        df=df[df.Mth==1]
                        labels=dst['sector'][:]
                        if labels.dtype.kind=='S' and labels.ndim==2:labels=nc.chartostring(labels)
                        labels=[v.decode() if isinstance(v,bytes) else str(v) for v in labels]
                        assert labels==['primary_and_secondary_land','pastures','crops','urban']
                    else:
                        assert not df.duplicated(['Lat','Lon']).any()
                        columns=[r['model_variable']] if frequency=='yr' else 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()
                        expected=df[columns].to_numpy().T
                    actual=dv[:]
                    max_error,valid=compare_arrays(expected,actual)
                    for k,col in [('latitude','Lat'),('longitude','Lon')]:
                        name=k if k in dst.variables else k[:3]
                        compare_coord(df[col],dst[name][:],name)
                elif r['pipelines']==['lr_monthly']:
                    inp=r['inputs'][0]
                    sources=r.get('lr_source_files') or [p for p in Path(inp['path']).iterdir() if re.fullmatch(inp['pattern'],p.name)]
                    with ExitStack() as stack:
                        records=source_records(stack,sources,r['model_variable'],year,r['expected_units'])
                        src=records[0][0]
                        sv=src[r['model_variable']]
                        assert sv.units==r['expected_units']
                        sdims=sv.dimensions[1:];ddims=dv.dimensions[1:]
                        shapes=sv.shape[1:];target=dv.shape[1:]
                        if r.get('lr_interface_midpoints'):shapes=shapes[:-1]+(shapes[-1]-1,)
                        bottom=None
                        if r.get('lr_bottom'):
                            with nc.Dataset(r['grid_file']) as m:bottom=m['depth_lev'][:].astype(int)-1
                            shapes=shapes[:-1]
                        permutations=[order for order in itertools.permutations(range(len(shapes))) if tuple(shapes[i] for i in order)==target]
                        assert len(permutations)==1,(shapes,target)
                        order=permutations[0]
                        cavity=None
                        if r.get('lr_ice_free_zero'):
                            with nc.Dataset(r['grid_file']) as m:cavity=m['cav_nod_mask'][:]>0
                        for month,(part,index,_) in enumerate(records):
                            expected=part[r['model_variable']][index].astype('float64')
                            if r.get('lr_interface_midpoints'):expected=(expected[...,:-1]+expected[...,1:])*0.5
                            if bottom is not None:expected=expected[np.arange(len(bottom)),bottom]
                            if cavity is not None:expected=np.ma.array(expected.filled(0),mask=cavity)
                            expected=expected*r.get('lr_scale',1)+r.get('lr_offset',0)
                            err,count=compare_arrays(expected.transpose(order),dv[month])
                            max_error=max(max_error,err);valid+=count
                        check_record_geometry(records,dst,tb)
                    if r.get('lr_exact_ocean_depth'):
                        with nc.Dataset(r['grid_file']) as mesh:
                            depth=next(v for n,v in dst.variables.items() if v.ndim==1 and v.shape==mesh['depth'].shape and getattr(v,'standard_name','')=='depth')
                            exact=np.column_stack([mesh['depth_bnds'][:-1],mesh['depth_bnds'][1:]])
                            assert np.allclose(dst[depth.bounds][:],exact,rtol=0,atol=1e-8),'ocean layer bounds differ from mesh'
                elif r['pipelines']==['lr_linear']:
                    with ExitStack() as stack:
                        sources=[]
                        for term in r['lr_terms']:
                            paths=term.get('files') or [Path(r['data_path'])/term['file'].format(year=year)]
                            records=source_records(stack,paths,term['variable'],year,term['units'])
                            sources.append((records,term))
                            check_record_geometry(records,dst,tb)
                        for month in range(12):
                            expected=sum(records[month][0][t['variable']][records[month][1]].astype('float64')*t['coefficient'] for records,t in sources)
                            err,count=compare_arrays(expected,dv[month]);max_error=max(max_error,err);valid+=count
                else:
                    raise AssertionError('Independent validator missing for this pipeline')
                record.update(status='PASS',max_absolute_error=max_error,valid_values_compared=valid)
        except Exception as e:
            record.update(status='FAIL',error=f'{type(e).__name__}: {e}')
        reports.append(record)
    result={'recipe':str(Path(args.recipe).resolve()),'pass':len(files)==len(reports) and all(r['status']=='PASS' for r in reports),
            'expected_products':len(reports),'netcdf_files':len(files),'products':reports,'publication_ready':False}
    dest=ROOT/'inventory'/f'validation_{Path(args.recipe).stem}.json';dest.write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2));raise SystemExit(0 if result['pass'] else 1)

if __name__=='__main__':main()
