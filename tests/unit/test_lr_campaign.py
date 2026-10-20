"""Offline regressions for leg-independent discovery and safe campaign recovery."""
import copy
import importlib.util
import json
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace

import cftime
import netCDF4
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pycmor.awiesm3_lr import load_monthly,load_lpj,load_lpj_yearly,monthly_time

SCRIPTS=Path(__file__).resolve().parents[2]/'awi-esm3-lr-variables'
sys.path.insert(0,str(SCRIPTS))
import discovery as disc
import experiment_campaign as campaign
import validate_outputs as independent


class Rule(SimpleNamespace):
    def get(self,key,default=None):
        return getattr(self,key,default)


def write_nc(path,years,omit=None):
    path.parent.mkdir(parents=True,exist_ok=True)
    intervals=[]
    for year in years:
        for month in range(1,13):
            if (year,month)==omit:
                continue
            a=cftime.DatetimeGregorian(year,month,1)
            b=cftime.DatetimeGregorian(year+(month==12),month%12+1,1)
            intervals.append((a,b))
    units='days since 2000-01-01'
    with netCDF4.Dataset(path,'w') as ds:
        for dim,length in [('time_counter',len(intervals)),('lat',1),('lon',2),('bnds',2)]:
            ds.createDimension(dim,length)
        t=ds.createVariable('time_counter','f8',('time_counter',))
        t.units=units;t.calendar='gregorian';t.bounds='time_counter_bounds'
        bounds=ds.createVariable(t.bounds,'f8',('time_counter','bnds'))
        bounds[:]=netCDF4.date2num(np.asarray(intervals),units,'gregorian')
        t[:]=bounds[:].mean(axis=1)
        ds.createVariable('lat','f8',('lat',))[:]=[0.]
        ds.createVariable('lon','f8',('lon',))[:]=[1.,2.]
        v=ds.createVariable('2t','f8',('time_counter','lat','lon'));v.units='K'
        v[:]=np.asarray([a.year+a.month/100 for a,b in intervals])[:,None,None]


def write_lpj(folder,years):
    folder.mkdir(parents=True,exist_ok=True)
    rows=[[lon,0.,year,*([float(year)]*12)] for year in years for lon in [1.,2.]]
    pd.DataFrame(rows,columns=['Lon','Lat','Year',*disc.MONTHS]).to_csv(folder/'cVeg_monthly.out',sep=' ',index=False)
    pd.DataFrame([[lon,0.,year,float(year)] for year in years for lon in [1.,2.]],
                 columns=['Lon','Lat','Year','Total']).to_csv(folder/'baresoilFrac_yearly.out',sep=' ',index=False)


@pytest.fixture
def miniature(tmp_path):
    grid=tmp_path/'grids';grid.mkdir()
    xr.Dataset({'lat':('lat',[0.]),'lon':('lon',[1.,2.])}).to_netcdf(grid/'atmos.nc')
    xr.Dataset({'lat':('ncells',[0.,0.]),'lon':('ncells',[1.,2.])}).to_netcdf(grid/'lpj.nc')
    cfg={'name':'test','run_root':str(tmp_path/'run'),'state_dir':str(tmp_path/'state'),
         'output_root':str(tmp_path/'output'),'first_year':2000,'last_year':2019,
         'groups':['atmos','land'],'settle_seconds':0,'max_attempts':3,
         'metadata':{'dreq':'dreq.json','cv':'cv'},'inherit':{'source_id':'TEST'},
         'grids':{'atmos':str(grid/'atmos.nc'),'lpj':str(grid/'lpj.nc')},
         'venv':str(tmp_path/'venv'),'slurm':{'account':'test','partition':'test','concurrency':4,
         'cpus':1,'memory':'1G','time':'00:10:00'}}
    prof=copy.deepcopy(campaign.profile())
    prof['groups']={'atmos':[r for r in prof['groups']['atmos'] if r['name']=='tas'],
                    'land':[r for r in prof['groups']['land'] if r['name'] in ['cVeg_mon','baresoilFrac_yr']]}
    assert len(prof['groups']['land'])==2
    return cfg,prof


def archive(cfg,lengths,nested=True):
    year=cfg['first_year'];root=Path(cfg['run_root'])/'outdata'
    for length in lengths:
        years=range(year,year+length)
        leg=f'{year}0101-{year+length-1}1231'
        ncdir=root/'oifs'/leg if nested else root/'oifs'
        write_nc(ncdir/f'atm_remapped_1m_2t_1m_{year}-{year+length-1}.nc',years)
        write_lpj(root/'lpj_guess'/leg,years)
        year+=length


@pytest.mark.parametrize('length',[1,5,10,20])
def test_leg_length_does_not_change_year_tasks(miniature,length):
    cfg,prof=miniature
    archive(cfg,[length]*(20//length))
    result=disc.plan(cfg,prof,disc.SourceIndex(cfg))
    assert result['ready_years']==20
    assert len(result['tasks'])==40
    assert result['expected_products']==60
    for year in [2000,2009,2019]:
        for group in ['atmos','land']:
            item=next(t for t in result['tasks'] if t['id']==f'{group}_{year}')
            for spec in item['recipe']['rules']:
                rule=Rule(**spec,year_start=year,year_end=year)
                if group=='atmos':
                    data=monthly_time(load_monthly(None,rule),rule)
                    assert len(data.time)==12
                    assert set(data.time.dt.year.values)=={year}
                    np.testing.assert_allclose(data[:,0,0],year+np.arange(1,13)/100)
                elif spec['pipelines']==['lr_lpj_yearly']:
                    data=load_lpj_yearly(None,rule)
                    assert len(data.time)==1
                    np.testing.assert_allclose(data['Total'],year)
                else:
                    data=load_lpj(None,rule)
                    np.testing.assert_allclose(data,year)


def test_mixed_leg_lengths_and_flat_netcdf(miniature):
    cfg,prof=miniature
    archive(cfg,[1,5,10,4],nested=False)
    result=disc.plan(cfg,prof,disc.SourceIndex(cfg))
    assert result['ready_years']==20
    assert {t['year'] for t in result['tasks']}==set(range(2000,2020))


def test_overlap_is_rejected_not_deduplicated_silently(miniature):
    cfg,prof=miniature;archive(cfg,[20])
    root=Path(cfg['run_root'])/'outdata'
    src=next((root/'oifs').rglob('*.nc'))
    duplicate=root/'oifs/duplicate'/src.name;duplicate.parent.mkdir();shutil.copyfile(src,duplicate)
    result=disc.plan(cfg,prof,disc.SourceIndex(cfg))
    assert result['ready_years']==0
    assert 'overlapping' in result['years'][0]['errors']['atmos']


def test_overlapping_lpj_tables_rejected(miniature):
    cfg,prof=miniature;archive(cfg,[20])
    write_lpj(Path(cfg['run_root'])/'outdata/lpj_guess/duplicate',[2007])
    result=disc.plan(cfg,prof,disc.SourceIndex(cfg))
    assert result['ready_years']==19
    assert not result['years'][7]['ready']


def test_missing_month_and_future_years_never_count_as_complete(miniature):
    cfg,prof=miniature;archive(cfg,[5])
    source=next((Path(cfg['run_root'])/'outdata/oifs').rglob('*.nc'))
    write_nc(source,range(2000,2005),omit=(2002,8))
    result=disc.plan(cfg,prof,disc.SourceIndex(cfg))
    assert result['ready_years']==4
    assert not result['years'][2]['ready']
    assert all(not r['ready'] for r in result['years'][5:])


def test_rescan_discovers_new_legs_without_old_recipe_changes(miniature):
    cfg,prof=miniature;archive(cfg,[5])
    cache={}
    first=disc.plan(cfg,prof,disc.SourceIndex(cfg,cache))
    nextcfg={**cfg,'first_year':2005};archive(nextcfg,[5])
    second=disc.plan(cfg,prof,disc.SourceIndex(cfg,cache))
    assert first['ready_years']==5 and second['ready_years']==10
    for before in first['tasks']:
        assert before==next(t for t in second['tasks'] if t['id']==before['id'])


def test_recent_files_wait_and_wrong_units_fail(miniature):
    cfg,prof=miniature;archive(cfg,[20])
    assert disc.plan({**cfg,'settle_seconds':3600},prof,disc.SourceIndex({**cfg,'settle_seconds':3600}))['ready_years']==0
    path=next((Path(cfg['run_root'])/'outdata/oifs').rglob('*.nc'))
    with netCDF4.Dataset(path,'a') as ds:
        ds['2t'].units='unexpected'
    result=disc.plan(cfg,prof,disc.SourceIndex(cfg))
    assert result['ready_years']==0
    assert 'expected units K' in result['years'][0]['errors']['atmos']


def test_independent_validator_selects_correct_year_in_twenty_year_file(miniature):
    cfg,prof=miniature;archive(cfg,[20])
    path=next((Path(cfg['run_root'])/'outdata/oifs').rglob('*.nc'))
    from contextlib import ExitStack
    with ExitStack() as stack:
        records=independent.source_records(stack,[path],'2t',2010,'K')
        assert len(records)==12
        assert [index for _,index,_ in records]==list(range(120,132))
        assert float(records[0][0]['2t'][records[0][1],0,0])==pytest.approx(2010.01)


def test_success_marker_checks_actual_files_and_input_changes(tmp_path):
    cfg={'state_dir':str(tmp_path/'state')}
    source=tmp_path/'input';source.write_text('source')
    out=tmp_path/'out';out.mkdir();file=out/'tas.nc';file.write_bytes(b'netcdf')
    marker={'sources':campaign.file_records([source]),'files':campaign.file_records([file]),'output_directory':str(out)}
    campaign.atomic_json(tmp_path/'state/done/atmos_2000.json',marker)
    assert campaign.done_ok(cfg,'atmos_2000')
    source.write_text('modified source')
    with pytest.raises(RuntimeError,match='changed'):
        campaign.done_ok(cfg,'atmos_2000')


def test_publish_recovers_after_rename_without_overwriting(tmp_path):
    cfg={'state_dir':str(tmp_path/'state')}
    attempt=tmp_path/'state/attempts/test/a001';out=attempt/'output';out.mkdir(parents=True)
    (out/'tas.nc').write_bytes(b'validated data')
    source=tmp_path/'source';source.write_text('original')
    target=tmp_path/'final'
    item={'id':'test','sources':campaign.file_records([source]),
          'recipe':{'inherit':{'output_directory':str(target)},'rules':[{}]}}
    campaign.publish(cfg,item,attempt,out,'validation-status.json')
    assert target.is_dir() and not out.exists()
    (tmp_path/'state/done/test.json').unlink()  # simulate crash before final marker
    campaign.publish(cfg,item,attempt,out,None)
    assert campaign.done_ok(cfg,'test')
    (target/'tas.nc').write_bytes(b'conflict')
    with pytest.raises(RuntimeError,match='conflict'):
        campaign.publish(cfg,item,attempt,out,None)


def test_uncertain_submission_prevents_duplicate_jobs(tmp_path):
    cfg={'state_dir':str(tmp_path)}
    campaign.atomic_json(tmp_path/'submissions/uncertain.json',{'state':'submitting'})
    with pytest.raises(RuntimeError,match='Uncertain'):
        campaign.active_submissions(cfg)


def test_submit_skips_active_tasks_and_reserves_new_attempt_on_retry(miniature,monkeypatch):
    cfg,prof=miniature
    item={'id':'atmos_2000'}
    monkeypatch.setattr(campaign,'prepare',lambda c:{'tasks':[item]})
    monkeypatch.setattr(campaign,'done_ok',lambda c,t:False)
    monkeypatch.setattr(campaign,'active_submissions',lambda c:set())
    submitted=[]
    monkeypatch.setattr(campaign.subprocess,'check_output',lambda command,**kw:submitted.append(command) or '12345\n')
    assert campaign.submit(cfg)=='12345'
    manifest=next((Path(cfg['state_dir'])/'inventory').glob('submission_*.json'))
    assert campaign.read_json(manifest)==[{'task':'atmos_2000','attempt':1}]
    monkeypatch.setattr(campaign,'active_submissions',lambda c:{'atmos_2000'})
    assert campaign.submit(cfg) is None
    assert len(submitted)==1
    monkeypatch.setattr(campaign,'active_submissions',lambda c:set())
    assert campaign.submit(cfg)=='12345'
    manifests=list((Path(cfg['state_dir'])/'inventory').glob('submission_*.json'))
    assert sorted(campaign.read_json(p)[0]['attempt'] for p in manifests)==[1,2]


def test_pinned_configuration_change_rejected(tmp_path):
    cfg={'state_dir':str(tmp_path),'first_year':2000}
    campaign.atomic_json(tmp_path/'pin.json',{'config_digest':disc.digest(cfg)})
    with pytest.raises(RuntimeError,match='settings differ'):
        campaign.verify({**cfg,'first_year':2001})


@pytest.mark.parametrize('value,seconds',[('3-00:00:00',258600),('01:00:00',3000),('30',1200)])
def test_controller_renews_before_configured_time_limit(value,seconds):
    assert campaign.controller_renewal_seconds(value)==seconds


def test_full_campaign_status_does_not_hide_missing_years(miniature):
    cfg,_=miniature
    result=campaign.report(cfg)
    assert result['expected_tasks']==40
    assert result['completed_tasks']==0
    assert result['complete'] is False


def test_tier_catalog_is_complete_and_has_no_experiment_paths():
    prof=campaign.profile()
    assert len(prof['tier_files'])==17
    assert sum(len(rr) for rr in prof['groups'].values())==198
    text=json.dumps(prof)
    assert '/work/' not in text
    assert '18500101-18541231' not in text
    for filename in prof['tier_files']:
        assert (SCRIPTS/filename).is_file()


def test_tier_migration_preserves_the_validated_scientific_rules():
    # Canonical scientific-field fingerprints were checked against all 198
    # validated mappings before removing the development recipes (2026-10-20).
    # This regression must not depend on archived experiment-specific files.
    expected={
        'atmos':'8d7917282f77275d4deaa0a0c8783b8211646194663b7b67b0d32f494420cd88',
        'ocean':'14c96c061c661a7e557aa92d5994ad8de5dd12b860baffebe867e60207cb3ba4',
        'seaice':'3bc28c075ced372e0d7f7797729703655864f626641fb7b991d0cfe8c27ebf35',
        'land':'7f03e9e19f6a6da8c50ce63ba5b1e3c76ad0b27c9ef6ad0fbaf6193701f53fd2',
        'fx_areacella_atmos':'6ebd1dce0d79f51aea4a671893b6142eb144ec5254c2c88049f73c6ca0de7c97',
        'fx_areacella_lpj':'a5182eb75e58d27a7fcce2a7a6530f7366715b795412d1e856e689431563a0a9',
        'fx_areacello':'7a471e09aa1663ff31ee4f4f5830228dc1383735a608688cf9fbc447a7cde07f',
    }
    fields=['compound_name','model_variable','lr_scale','lr_offset','lr_units',
            'expected_units','source_units','lr_bottom','lr_interface_midpoints',
            'lr_exact_ocean_depth','lr_ice_free_zero','pipelines']
    groups=campaign.profile()['groups']
    assert set(groups)==set(expected)
    for group,rules in groups.items():
        science={
            r['name']:{
                **{key:r.get(key) for key in fields},
                'lr_terms':[{key:term[key] for key in ['variable','units','coefficient']}
                            for term in r.get('lr_terms',[])],
            }
            for r in rules
        }
        assert disc.digest(science)==expected[group],group


def test_runtime_directory_has_only_reusable_scripts():
    expected={
        'campaign.py','experiment_campaign.py','discovery.py',
        'run_recipe.py','validate_outputs.py','start_campaign.sh',
        'run_experiment_driver.sh','run_experiment_task.sh','run_recipe.sh',
    }
    actual={p.name for p in SCRIPTS.iterdir() if p.is_file() and p.suffix in ['.py','.sh']}
    assert actual==expected
    assert not (SCRIPTS/'first_leg').exists()
    assert not (SCRIPTS/'recipes').exists()


def test_entrypoint_requires_an_explicit_campaign_config():
    spec=importlib.util.spec_from_file_location('lr_campaign_cli',SCRIPTS/'campaign.py')
    cli=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    assert cli.main is campaign.main
    with pytest.raises(SystemExit) as exc:
        cli.main(['report'])
    assert exc.value.code==2
