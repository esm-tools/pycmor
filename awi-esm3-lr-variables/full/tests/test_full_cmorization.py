"""Small non-submitting regression tests for the all-catalog entry point."""
import os
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import netCDF4
import yaml

import full_cmorization as f

REPO = Path(__file__).resolve().parents[3]


def configured_metadata(test):
    path = os.environ.get('PYCMOR_LR_CONFIG')
    if not path:
        test.skipTest('Set PYCMOR_LR_CONFIG for the metadata-dependent rule inventory test')
    config = f.load_config(Path(path))
    if not Path(config['metadata']).is_file():
        test.skipTest('Configured DReq metadata is not installed')
    return config


class Tests(unittest.TestCase):
    def test_missing_groundwater_is_reported_for_every_year_without_a_proxy(self):
        with tempfile.TemporaryDirectory() as directory:
            base=Path(directory);period='26100101-26141231'
            for component in ('oifs','fesom',f'lpj_guess/{period}'):
                (base/'outdata'/component).mkdir(parents=True)
            campaign=base/'campaign'
            (campaign/'grids').mkdir(parents=True)
            (campaign/'reproduce').mkdir()
            # No geometry is required when no requested field is available.
            (campaign/'grids/full_TCO95_grid.nc').touch()
            metadata=base/'metadata.json';f.write(metadata,{'Compound Name':{}})
            config=dict(repo=str(REPO),metadata=str(metadata),mesh_dir=str(base),ghg_dir=str(base))
            load_module=f.module
            def module(path,name):
                result=load_module(path,name)
                if name=='full_raw_rules':
                    result.grids=lambda: {}
                return result
            with patch.object(f,'module',side_effect=module):
                tasks,coverage,_=f.build_catalog(config,base,period,{},campaign)
            groundwater=[row for row in coverage if row['compound']=='land.dgw.tavg-u-hxy-lnd.day.glb']
            self.assertEqual([row['year'] for row in groundwater],list(range(2610,2615)))
            self.assertTrue(all(row['status']=='BLOCKED' and 'GROUNDWATER_MAPPING' in row['reason']
                                for row in groundwater))
            self.assertEqual(tasks,[])

    def test_full_catalog_not_profile_subset(self):
        files=list((REPO/'awi-esm3-lr-variables').glob('*/cmip7xml*.yaml'))
        compounds={r['compound_name'] for p in files for r in yaml.safe_load(p.read_text())['rules']}
        self.assertEqual(len(files),17)
        self.assertEqual(len(compounds),555)  # Groundwater cannot use the removed deep-soil proxy.

    def test_recom_seven_rules_retained(self):
        raw=f.module(f.RULES/'raw_adapter.py','test_raw_seven')
        config=configured_metadata(self)
        raw.configure(f.ROOT, REPO, '/unused', '25950101-25991231', config)
        names={r['name'] for r in raw.recom_rules()}
        self.assertTrue({'calc','chl','chl_surface','detoc','phyc','pp','zooc'} <= names)

    def test_serial_runtime_fix(self):
        self.assertFalse(f.SERIAL['parallel'])
        self.assertFalse(f.SERIAL['enable_dask'])
        self.assertIn("scheduler='synchronous'",(f.RULES/'native_cli.py').read_text())

    def test_monthly_gap_and_duplicate_rejected(self):
        dates=[(2000,m,15,0,0,0) for m in range(1,13)]
        f.cover_dates(dates,2000,'mon')
        for bad in (dates[:-1],dates+[dates[-1]],dates[::-1]):
            with self.assertRaises(ValueError): f.cover_dates(bad,2000,'mon')

    def test_daily_leap_year(self):
        import cftime
        dates=cftime.num2date(list(range(366)), 'days since 2000-01-01',calendar='standard')
        values=[(d.year,d.month,d.day,d.hour,d.minute,d.second) for d in dates]
        f.cover_dates(values,2000,'day')
        with self.assertRaises(ValueError):f.cover_dates(values[:-1],2000,'day')

    def test_hourly_360day_calendar(self):
        import cftime
        dates=cftime.num2date(list(range(0,360*24,3)), 'hours since 2001-01-01',calendar='360_day')
        values=[(d.year,d.month,d.day,d.hour,d.minute,d.second) for d in dates]
        f.cover_dates(values,2001,'3hr','360_day')

    def test_old_parent_metadata_removed(self):
        d={'general':{},'inherit':{'parent_time_units':'days since 1350-01-01',
             'branch_time_in_parent':120522.},'rules':[{}],
             'pycmor':{'dask_cluster':'slurm'},'jobqueue':{'slurm':{}},
             'pipelines':[{'name':'x','workflow_backend':'prefect'}]}
        config={'metadata':'/a','cv_dir':'/b'}
        result=f.new_doc(d,config,2595,'x')
        self.assertNotIn('parent_time_units',result['inherit'])
        self.assertNotIn('jobqueue',result)
        self.assertEqual(result['inherit']['year_start'],2595)
        self.assertEqual(result['inherit']['variant_label'],'r1i1p1f1')
        self.assertEqual(result['inherit']['activity_id'],'CMIP')
        self.assertEqual(result['rules'][0]['pipelines'],['full_native_default'])
        self.assertEqual(result['pipelines'][0]['workflow_backend'],'native')
        self.assertIn('parent_time_units',d['inherit'])

    def test_missing_secondary_not_ready(self):
        rep=f.module(REPO/'examples/repoint_hr_year.py','test_repointer')
        with tempfile.TemporaryDirectory() as directory:
            base=Path(directory);(base/'temp.fesom.2595.nc').touch()
            doc={'inherit':{},'rules':[{'compound_name':'ocean.tos.x.fx.glb',
                 'inputs':[{'path':directory,'pattern':r'temp\.fesom\.\d{4}\.nc'}],
                 'second_input_path':directory,'second_input_pattern':r'salt\.fesom\.\d{4}\.nc'}]}
            with self.assertRaisesRegex(ValueError,'MISSING_SECONDARY'):
                f.dependencies(doc,2595,rep)

    def test_unused_inherited_basin_mask_is_not_a_dependency(self):
        custom=str(REPO/'examples/custom_steps.py')
        d={'inherit':{'basin_mask_file':'/missing.nc'},
           'rules':[{'pipelines':['p']}], 'pipelines':[{'name':'p','steps':[
                f'script://{custom}:compute_volcello_fx']}]}
        f.prune_unused_inherited_basin_mask(d)
        self.assertNotIn('basin_mask_file',d['inherit'])

    def test_required_basin_mask_stays_a_dependency(self):
        custom=str(REPO/'examples/custom_steps.py')
        for function in ('load_basin_mask','compute_msftm_density'):
            d={'inherit':{'basin_mask_file':'/missing.nc'},
               'rules':[{'pipelines':['p']}], 'pipelines':[{'name':'p','steps':[
                    f'script://{custom}:{function}']}]}
            f.prune_unused_inherited_basin_mask(d)
            self.assertIn('basin_mask_file',d['inherit'])

    def test_pins_secondary_year(self):
        rep=f.module(REPO/'examples/repoint_hr_year.py','test_repointer2')
        with tempfile.TemporaryDirectory() as directory:
            base=Path(directory)
            for name in ('temp.fesom.2595.nc','salt.fesom.2595.nc','salt.fesom.2596.nc'):
                (base/name).write_bytes(b'not-empty')
            doc={'inherit':{},'rules':[{'compound_name':'ocean.tos.x.fx.glb',
                 'inputs':[{'path':directory,'pattern':r'temp\.fesom\.\d{4}\.nc'}],
                 'salt_path':directory,'salt_pattern':r'salt\.fesom\.\d{4}\.nc'}]}
            files=f.dependencies(doc,2595,rep)
            self.assertEqual({p.name for p in files},{'temp.fesom.2595.nc','salt.fesom.2595.nc'})

    def test_report_not_silent_success_for_missing(self):
        with tempfile.TemporaryDirectory() as d:
            base=Path(d)
            f.write(base/'coverage.json',[dict(compound='x',year=2595,status='BLOCKED',reason='missing')])
            self.assertEqual(f.report(base),3)
            self.assertEqual(f.read(base/'report/summary.json')['status'],'INCOMPLETE')

    def test_output_root_unique_by_experiment(self):
        import hashlib
        a=hashlib.sha256(b'/one/experiment').hexdigest()[:8]
        b=hashlib.sha256(b'/two/experiment').hexdigest()[:8]
        self.assertNotEqual(a,b)

    def test_production_validation_requires_depth_metadata(self):
        compound = 'ocean.thetao.tavg-ol-hxy-sea.fx.glb'
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            output = base / 'output'
            output.mkdir()
            metadata = base / 'metadata.json'
            f.write(metadata, {'Compound Name': {compound: dict(
                units='degC', standard_name='sea_water_potential_temperature',
                dimensions='olevel')}})
            recipe = base / 'recipe.yaml'
            recipe.write_text(yaml.safe_dump(dict(
                general={'CMIP7_DReq_metadata': str(metadata)},
                inherit=dict(source_id='TEST', experiment_id='test'))))
            with netCDF4.Dataset(output / 'thetao.nc', 'w') as ds:
                ds.source_id = 'TEST'
                ds.experiment_id = 'test'
                ds.createDimension('nz', 2)
                z = ds.createVariable('nz', 'f8', ('nz',))
                z[:] = [2.5, 10.0]
                v = ds.createVariable('thetao', 'f4', ('nz',))
                v.units = 'degC'
                v.standard_name = 'sea_water_potential_temperature'
                v[:] = [5., 4.]
            task = dict(compound=compound, year=2000)
            with self.assertRaisesRegex(ValueError, 'missing ocean depth axis'):
                f.validate_output(task, recipe, output)
            with netCDF4.Dataset(output / 'thetao.nc', 'a') as ds:
                ds['nz'].standard_name = 'depth'
                ds['nz'].units = 'm'
                ds['nz'].positive = 'down'
            self.assertEqual(len(f.validate_output(task, recipe, output)['files']), 1)

    def test_submit_helper_is_only_submission_interface(self):
        config=dict(account='test-account', partition='test-partition', qos='boost',
                    memory='1G', walltime='00:01:00')
        with patch.object(f.subprocess,'check_output',return_value='123;cluster\n') as mock:
            self.assertEqual(f.batch(config,'true','/tmp/test.log','cmor-test',['--dependency=afterok:999']),'123')
            cmd=mock.call_args.args[0]
            self.assertIn('--dependency=afterok:999',cmd)
            self.assertIn('--qos=boost',cmd)
            self.assertEqual(cmd[-2:],['--wrap','true'])


class PortableConfigTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)
        self.config_file = self.base / 'site.json'
        self.config = dict(metadata='metadata.json', cv_dir='cv', mesh_dir='mesh',
                           ghg_dir='ghg', results='results', esgvoc_home='cache')

    def test_config_uses_checkout_and_config_relative_paths(self):
        f.write(self.config_file, self.config)
        loaded = f.load_config(self.config_file)
        self.assertEqual(loaded['repo'], str(REPO))
        for key, value in self.config.items():
            self.assertEqual(loaded[key], str(self.base / value))
        self.assertIsNone(loaded['commit'])
        self.assertFalse(loaded['allow_dirty'])

    def test_config_preserves_virtual_environment_python_symlink(self):
        interpreter = self.base / 'venv/bin/python'
        interpreter.parent.mkdir(parents=True)
        interpreter.symlink_to(sys.executable)
        f.write(self.config_file, dict(self.config, python='venv/bin/python'))
        self.assertEqual(f.load_config(self.config_file)['python'], str(interpreter))

    def test_config_refuses_results_in_source_repository(self):
        f.write(self.config_file, dict(self.config, results=str(REPO / 'test-results')))
        with self.assertRaisesRegex(ValueError, 'outside the source repository'):
            f.load_config(self.config_file)

    def test_config_requires_explicit_site_settings(self):
        with self.assertRaisesRegex(ValueError, 'Pass --config'):
            f.load_config(None)

    def test_snapshot_includes_new_sources_and_verifies_from_frozen_location(self):
        repository = self.base / 'repo'
        helper = repository / 'awi-esm3-lr-variables/full/scripts/helper.py'
        helper.parent.mkdir(parents=True)
        helper.write_text('value = 1\n')
        (repository / 'pyproject.toml').write_text('[project]\nname = "fixture"\n')
        manifest = f.code_manifest(repository)
        self.assertIn(str(helper.relative_to(repository)), manifest)
        campaign = self.base / 'campaign'
        config = dict(repo=str(repository), commit='fixture-head')
        f.write(campaign / 'plan.json', {'signature': dict(config=config,
                source_files=manifest, metadata_files={}, snapshot_files={})})
        with patch.object(f.subprocess, 'check_output', return_value='fixture-head\n'), \
             patch.object(f, 'REPO', self.base / 'relocated-frozen-driver'):
            self.assertEqual(f.verify_campaign(campaign), config)
            helper.write_text('value = 2\n')
            with self.assertRaisesRegex(ValueError, 'changed since planning'):
                f.verify_campaign(campaign)


if __name__=='__main__':unittest.main()
