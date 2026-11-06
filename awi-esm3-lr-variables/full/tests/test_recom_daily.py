"""Daily REcoM conversions: dates, leap days, native values and lazy loading."""
import copy
from datetime import timedelta
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import cftime
import netCDF4
import numpy as np
import xarray as xr
import yaml

from pycmor.awiesm3_lr import daily_time, load_daily, load_daily_linear

RULES = Path(__file__).resolve().parents[1] / 'rules'


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, RULES / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


RAW = module('recom_daily_adapter', 'raw_adapter.py')
EXTRA = module('recom_daily_surface', 'extra_steps.py')
VALIDATOR = module('recom_daily_validator', 'validate_product.py')


class Rule(SimpleNamespace):
    def get(self, key, default=None):
        return getattr(self, key, default)


class DailyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        RAW.YEARS = [2000]
        RAW.META = {}
        RAW.CARBON_CONTEXT = {}
        RAW.REPO = RULES.parents[2]

    def source(self, stem='O2_sfc', variable='O2', units='[mmol/m3]', year=2000,
               calendar='gregorian', indices=None, levels=1, surface=True):
        first = cftime.datetime(year, 1, 1, 12, calendar=calendar)
        stop = cftime.datetime(year+1, 1, 1, 12, calendar=calendar)
        times = [first + timedelta(days=i) for i in range((stop-first).days)]
        if indices is not None:
            times = [times[i] for i in indices]
        path = self.base / f'{stem}.fesom.{year}.nc'
        with netCDF4.Dataset(path, 'w') as ds:
            ds.createDimension('time', len(times)); ds.createDimension('nod2', 2)
            depth = 'nz_sfc' if surface else 'nz'
            ds.createDimension(depth, levels)
            t = ds.createVariable('time', 'f8', ('time',))
            t.units = f'days since {year}-01-01'; t.calendar = calendar
            t[:] = netCDF4.date2num(times, t.units, calendar)
            z = ds.createVariable(depth, 'f4', (depth,)); z[:] = np.arange(levels)+2.5
            v = ds.createVariable(variable, 'f4', ('time','nod2',depth), fill_value=-9999.)
            v.units = units; v.cell_methods = 'time: mean'
            values = np.arange(len(times)*2*levels,dtype='float32').reshape(len(times),2,levels)+1
            values[:,1,-1] = -9999.
            v[:] = values
        return path

    def test_daily_recipe_catalogue_is_exactly_twelve_audited_products(self):
        rules = [r for r in RAW.recom_rules() if '.day.' in r['compound_name']]
        expected = {
            'intpp': ('NPPn', ['NPPn','NPPd'], 1e-3/86400),
            'chl': ('PhyChl', ['PhyChl','DiaChl'], 1e-6),
            'o2': ('O2', [], 1e-3), 'co3': ('CO3', [], 1.), 'ph': ('pH', [], 1.),
            'phyc': ('PhyC', ['PhyC','DiaC'], 1e-3), 'phydiat': ('DiaC', [], 1e-3),
            'detoc': ('DetC', ['DetC','idetz2c'], 1e-3), 'dissoc': ('DOC', [], 1e-3),
            'zmeso': ('HetC', [], 1e-3), 'zmicro': ('Zoo3C', [], 1e-3),
            'zooc': ('HetC', ['HetC','Zoo2C','Zoo3C'], 1e-3),
        }
        self.assertEqual(len(rules), 12)
        self.assertEqual({r['compound_name'].split('.')[1] for r in rules}, set(expected))
        for r in rules:
            variable, terms, scale = expected[r['compound_name'].split('.')[1]]
            self.assertEqual(r['model_variable'], variable)
            self.assertEqual([t['variable'] for t in r.get('lr_terms',[])], terms)
            for coefficient in ([t['coefficient'] for t in r.get('lr_terms',[])] or [r['lr_scale']]):
                self.assertEqual(coefficient, scale)
            self.assertTrue('daily' in r['pipelines'][0])

    def test_pipeline_registration_leaves_monthly_unchanged_and_is_idempotent(self):
        profile = yaml.safe_load((RULES.parents[1] / 'profile.yaml').read_text())
        before = copy.deepcopy(profile['pipelines'])
        RAW.register_daily_pipelines(profile, self.base)
        RAW.register_daily_pipelines(profile, self.base)
        self.assertEqual(profile['pipelines'][:len(before)], before)
        self.assertEqual(len(profile['pipelines']), len(before)+7)
        for p in profile['pipelines'][len(before):len(before)+4]:
            self.assertIn('pycmor.awiesm3_lr.daily_time', p['steps'])
            self.assertNotIn('pycmor.awiesm3_lr.monthly_time', p['steps'])

    def test_daily_headers_and_lazy_loading_preserve_leap_and_masks(self):
        path = self.source(surface=False, levels=3)
        RAW.check_days([path], 'O2', '[mmol/m3]')
        rule = Rule(year_start=2000, year_end=2000, model_variable='O2', lr_source_files=[str(path)])
        result = daily_time(load_daily(None, rule), rule)
        self.assertEqual(result.sizes['time'],366)
        self.assertEqual(max(result.chunksizes['time']),1)
        self.assertEqual(rule.frequency_str, 'D')
        self.assertEqual(int(result.time.dt.month.values[59]),2)
        self.assertEqual(int(result.time.dt.day.values[59]),29)
        self.assertTrue(np.isnan(result.isel(time=59,nod2=1,nz=2).compute().item()))

    def test_missing_duplicate_unordered_and_monthly_records_rejected(self):
        for indices in (range(365), [0]+list(range(365)), list(reversed(range(366))), range(0,366,30)):
            path = self.source(indices=indices)
            with self.assertRaisesRegex(ValueError,'daily coverage'):
                RAW.check_days([path], 'O2', '[mmol/m3]')
            with xr.open_dataset(path,use_cftime=True) as data:
                with self.assertRaisesRegex(ValueError,'daily records'):
                    daily_time(data.O2,Rule(year_start=2000,year_end=2000))

    def test_daily_mean_metadata_and_noon_required(self):
        path = self.source()
        with netCDF4.Dataset(path,'r+') as ds:
            ds['O2'].cell_methods='time: point'
        with self.assertRaisesRegex(ValueError,'daily mean'):
            RAW.check_days([path], 'O2')
        with netCDF4.Dataset(path,'r+') as ds:
            ds['O2'].cell_methods='time: mean';ds['time'][:]=ds['time'][:]-0.5
        with self.assertRaisesRegex(ValueError,'day-centred'):
            RAW.check_days([path], 'O2')

    def test_linear_daily_sum_preserves_day_and_surface_mask(self):
        paths=[self.source('PhyChl_sfc','PhyChl','[mg/m3]'),
               self.source('DiaChl_sfc','DiaChl','[mg/m3]')]
        rule=Rule(year_start=2000,year_end=2000,model_variable='PhyChl',lr_units='kg m-3',
                  surface_layer=True,lr_terms=[{'files':[str(p)],'variable':v,'units':'[mg/m3]',
                  'coefficient':1e-6} for p,v in zip(paths,['PhyChl','DiaChl'])])
        result=EXTRA.surface_only(load_daily_linear(None,rule),rule)
        self.assertEqual(result.dims,('time','nod2'))
        self.assertEqual(max(result.chunksizes['time']),1)
        self.assertAlmostEqual(float(result.isel(time=59,nod2=0).compute()),238e-6)
        self.assertTrue(np.isnan(result.isel(time=59,nod2=1).compute().item()))

    def test_independent_source_sampler_uses_day_not_first_day_of_month(self):
        path=self.source()
        rule={'lr_source_files':[str(path)],'model_variable':'O2','lr_scale':.001}
        a=VALIDATOR.sample_nc(rule,2000,2,{'nod2':0},0,day=1)
        b=VALIDATOR.sample_nc(rule,2000,2,{'nod2':0},0,day=29)
        self.assertAlmostEqual(float(a),.063)
        self.assertAlmostEqual(float(b),.119)

    def test_independent_output_validator_covers_leapday_and_rejects_wrong_values(self):
        source=self.source()
        out=self.base/'result';out.mkdir()
        product=out/'o2_daily.nc'
        with netCDF4.Dataset(source) as src, netCDF4.Dataset(product,'w') as ds:
            ds.createDimension('time',366);ds.createDimension('nod2',2);ds.createDimension('bnds',2)
            t=ds.createVariable('time','f8',('time',)); t.setncatts({
                'units':src['time'].units,'calendar':'gregorian','bounds':'time_bnds'})
            t[:]=src['time'][:]
            bounds=ds.createVariable('time_bnds','f8',('time','bnds'))
            bounds[:]=np.column_stack([t[:]-0.5,t[:]+0.5])
            v=ds.createVariable('o2','f4',('time','nod2'),fill_value=-9999.)
            v.units='mol m-3';v[:]=src['O2'][:,:,0]*.001
            ds.source_id='LR-SPINUP-TEST';ds.tracking_id='test-only'
        recipe=self.base/'recipe.yaml'
        recipe.write_text(yaml.safe_dump({'inherit':{'year_start':2000,'year_end':2000,
            'output_directory':str(out)},'rules':[{
                'compound_name':'ocnBgchem.o2.tavg-ols-hxy-sea.day.glb','model_variable':'O2',
                'lr_source_files':[str(source)],'expected_units':'[mmol/m3]',
                'lr_units':'mol m-3','lr_scale':.001,'surface_layer':True}]}))
        result=VALIDATOR.validate(recipe)
        self.assertTrue(result['raw_sample_comparison'])
        with netCDF4.Dataset(product,'r+') as ds:
            ds['o2'][59,0]=7.0
        with self.assertRaises(AssertionError):
            VALIDATOR.validate(recipe)

    def test_360_day_calendar_does_not_invent_gregorian_dates(self):
        path=self.source(calendar='360_day')
        RAW.check_days([path],'O2')
        with xr.open_dataset(path,use_cftime=True) as data:
            result=daily_time(data.O2,Rule(year_start=2000,year_end=2000))
            self.assertEqual(result.sizes['time'],360)

    def test_daily_materializer_does_not_use_monthly_validator(self):
        self.source()
        RAW.EXP=self.base;RAW.PERIOD='20000101-20001231'
        original=next(r for r in RAW.recom_rules() if r['name']=='o2_daily_surface')
        with patch.object(RAW,'inputs',return_value=[self.base/'O2_sfc.fesom.2000.nc']), \
             patch.object(RAW,'check_months',side_effect=AssertionError('monthly used')):
            _,rule=RAW.materialize(original,{'core3':'/fixture/mesh.nc'})
        self.assertEqual(len(rule['lr_source_files']),1)

    def test_production_rules_read_actual_namelist_and_mixed_fe_units(self):
        RAW.EXP=self.base;RAW.PERIOD='20000101-20001231'
        namelist=self.base/f'run_{RAW.PERIOD}'/'config/fesom/namelist.recom'
        namelist.parent.mkdir(parents=True)
        text='&param\n enable_coccos=.false.\n lossn=.05\n lossn_d=.05\n fe2n=.033\n/\n'
        namelist.write_text(text)
        original=next(r for r in RAW.recom_rules() if r['name']=='intpbfe_south30')
        rule=copy.deepcopy(original);RAW.verify_recom_production(rule)
        self.assertEqual(rule['lr_auxiliary_files'],[str(namelist)])
        for term in rule['lr_terms']:
            self.assertEqual(term['coefficient'],.033e-6/86400)
        self.assertIsNone(original['lr_terms'][0]['coefficient'])
        namelist.write_text(text.replace('.033','.05'))
        changed=copy.deepcopy(original);RAW.verify_recom_production(changed)
        self.assertEqual(changed['lr_terms'][0]['coefficient'],.05e-6/86400)
        nitrogen=next(r for r in RAW.recom_rules() if r['name']=='intpbn_south30')
        RAW.verify_recom_production(nitrogen)
        self.assertEqual(nitrogen['lr_terms'][0]['coefficient'],1e-3/86400)

    def test_regional_surface_only_when_official_global_surface_target_absent(self):
        names={'chldiat','dissic','ph','phydiat','zmeso','zmicro','zmisc','zooc'}
        RAW.META={f'ocnBgchem.{name}.tavg-ols-hxy-sea.mon.30S-90S':{} for name in names}
        regional=[r for r in RAW.recom_rules() if r['name'].endswith('_surface_south30')]
        self.assertEqual({r['compound_name'].split('.')[1] for r in regional},names)
        for r in regional:
            self.assertTrue(r['surface_layer'])
            self.assertFalse(r['lr_exact_ocean_depth'])
            self.assertEqual(r['lr_region'],'30S-90S')
            self.assertEqual(r['pipelines'],['recom_south30_surface_linear'
                                            if r.get('lr_terms') else 'recom_south30_surface'])
        RAW.META['ocnBgchem.ph.tavg-ols-hxy-sea.mon.glb']={}
        rules=list(RAW.recom_rules())
        self.assertNotIn('ph_surface_south30',{r['name'] for r in rules})
        self.assertIn('ph_surface',{r['name'] for r in rules})

    def test_production_rejects_missing_namelist_extra_groups_and_unequal_loss(self):
        RAW.EXP=self.base;RAW.PERIOD='20000101-20001231'
        rule=next(r for r in RAW.recom_rules() if r['name']=='intpbn_south30')
        with self.assertRaisesRegex(ValueError,'actual leg namelist'):
            RAW.verify_recom_production(copy.deepcopy(rule))
        namelist=self.base/f'run_{RAW.PERIOD}'/'config/fesom/namelist.recom'
        namelist.parent.mkdir(parents=True)
        for text,pattern in (
                ('enable_coccos=.true.\nlossn=.05\nlossn_d=.05','enable_coccos'),
                ('enable_coccos=.false.\nlossn=.05\nlossn_d=.04','equal lossn'),
                ('enable_coccos=.false.\nlossn=.05','explicit lossn_d')):
            namelist.write_text(text)
            with self.assertRaisesRegex(ValueError,pattern):
                RAW.verify_recom_production(copy.deepcopy(rule))

    def test_regional_validator_maps_native_coordinates_not_subset_indices(self):
        source=self.base/'native.nc';output=self.base/'regional.nc'
        with netCDF4.Dataset(source,'w') as ds:
            ds.createDimension('nod2',4)
            ds.createVariable('lon','f8',('nod2',))[:]=[10.,20.,30.,40.]
            ds.createVariable('lat','f8',('nod2',))[:]=[50.,-40.,10.,-60.]
        with netCDF4.Dataset(output,'w') as ds:
            ds.createDimension('nod2',2)
            ds.createVariable('longitude','f8',('nod2',))[:]=[20.,40.]
            ds.createVariable('latitude','f8',('nod2',))[:]=[-40.,-60.]
        rule={'lr_region':'30S-90S','lr_source_files':[str(source)]}
        with netCDF4.Dataset(output) as ds:
            np.testing.assert_array_equal(VALIDATOR.regional_native_indices(rule,ds),[1,3])
        with netCDF4.Dataset(output,'r+') as ds:
            ds['latitude'][1]=-40.;ds['longitude'][1]=20.
        with netCDF4.Dataset(output) as ds:
            with self.assertRaisesRegex(AssertionError,'Duplicated'):
                VALIDATOR.regional_native_indices(rule,ds)

    def test_regional_float32_coordinates_match_exact_promoted_values(self):
        source=self.base/'native_float32.nc';output=self.base/'regional_float64.nc'
        # Actual CORE3 coordinates plus values close to decimal-rounding
        # boundaries. The output copies their represented values exactly.
        lon=np.array([10.,181.55618,-178.44383,42.123455],dtype=np.float32)
        lat=np.array([50.,-84.4691,-30.000005,-60.123455],dtype=np.float32)
        selected=np.flatnonzero(lat <= -30.)
        with netCDF4.Dataset(source,'w') as ds:
            ds.createDimension('nod2',len(lon))
            ds.createVariable('lon','f4',('nod2',))[:]=lon
            ds.createVariable('lat','f4',('nod2',))[:]=lat
        with netCDF4.Dataset(output,'w') as ds:
            ds.createDimension('nod2',len(selected))
            ds.createVariable('longitude','f8',('nod2',))[:]=lon[selected].astype(np.float64)
            ds.createVariable('latitude','f8',('nod2',))[:]=lat[selected].astype(np.float64)
        rule={'lr_region':'30S-90S','lr_source_files':[str(source)]}
        with netCDF4.Dataset(output) as ds:
            np.testing.assert_array_equal(VALIDATOR.regional_native_indices(rule,ds),selected)
        # A genuinely changed coordinate must not be accepted by a nearest
        # neighbour or a tolerance fallback.
        with netCDF4.Dataset(output,'r+') as ds:
            ds['longitude'][0]=ds['longitude'][0]+0.001
        with netCDF4.Dataset(output) as ds:
            with self.assertRaises(KeyError):
                VALIDATOR.regional_native_indices(rule,ds)


if __name__ == '__main__':
    unittest.main()
