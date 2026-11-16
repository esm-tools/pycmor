"""Small synthetic tests; no Slurm jobs or full model arrays are read."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

import cftime
import netCDF4
import numpy as np
import xarray as xr

import full_cmorization as f
from input_streams import date_tuple, validate_input_streams, validate_previous_day
from pycmor.core.gather_inputs import _check_compatible_schemas


def write(path, name='swvl1', year=2001, days=None, calendar='standard',
          lats=(30., 40.), value=0.2, time_dim='time_counter', aux_time=False):
    if days is None:
        n = int(cftime.date2num(cftime.datetime(year+1, 1, 1, calendar=calendar),
                                f'days since {year}-01-01', calendar))
        days = np.arange(n) + .5
    with netCDF4.Dataset(path, 'w') as ds:
        ds.createDimension(time_dim, len(days)); ds.createDimension('cell', 2)
        ds.createDimension('bounds', 2)
        time = ds.createVariable(time_dim, 'f8', (time_dim,))
        time.units=f'days since {year}-01-01'; time.calendar=calendar
        time.standard_name='time'; time.bounds='time_bounds'; time[:] = days
        bounds = ds.createVariable('time_bounds', 'f8', (time_dim, 'bounds'))
        bounds[:] = np.array([np.asarray(days)-.5, np.asarray(days)+.5]).T
        if aux_time:
            centered = ds.createVariable('time_centered', 'f8', (time_dim,))
            centered.units=time.units; centered.calendar=calendar
            centered.standard_name='time'; centered[:] = days
        lat = ds.createVariable('lat', 'f8', ('cell',))
        lat.standard_name='latitude'; lat[:] = lats
        v = ds.createVariable(name, 'f8', (time_dim, 'cell'))
        v.units = 'm' if name in ('sd', 'src') else 'm3 m-3'
        v.coordinates='time_centered lat' if aux_time else 'lat'; v[:] = value
    return Path(path)


class StreamTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def files(self, year=2001, names=None, **kwargs):
        return [write(self.base/f'atmos_day_land_{n}_{year}-{year}.nc',
                      name=n, year=year, **kwargs) for n in
                (names or ['swvl1','swvl2','swvl3','swvl4','sd','src'])]

    def test_six_simultaneous_variables_are_daily_not_2190_steps(self):
        result = validate_input_streams(self.files(), [2001], 'day')
        self.assertEqual(set(result), {'swvl1','swvl2','swvl3','swvl4','sd','src'})
        self.assertEqual({v['frequency'] for v in result.values()}, {'day'})
        self.assertEqual({len(v['dates']) for v in result.values()}, {365})

    def test_leap_year(self):
        result = validate_input_streams(self.files(year=2000), [2000], 'day')
        self.assertEqual({len(v['dates']) for v in result.values()}, {366})

    def test_schema_allows_leap_time_counter_but_rejects_spatial_change(self):
        paths = self.files(year=2000, names=['swvl1']) + self.files(year=2001, names=['swvl1'])
        _check_compatible_schemas(paths, {'time_dimname': 'time_counter'})
        # CF discovery must also work without the explicit rule attribute.
        _check_compatible_schemas(paths, {})
        incompatible = self.base/'other.nc'
        xr.Dataset({'swvl1': (('time_counter','cell'), np.ones((365,3)))},
                   coords={'time_counter': np.arange(365)}).to_netcdf(incompatible)
        with self.assertRaisesRegex(ValueError, "dim 'cell'"):
            _check_compatible_schemas([paths[0], incompatible], {'time_dimname': 'time_counter'})

    def test_hourly_midpoints_at_distant_epoch_keep_exact_cadence(self):
        numeric = cftime.date2num(cftime.datetime(2610,1,1), 'days since 1850-01-01')
        numeric = numeric + (np.arange(365*24)+.5)/24
        dates = [date_tuple(d) for d in cftime.num2date(numeric, 'days since 1850-01-01')]
        f.cover_dates(dates,2610,'1hr')
        self.assertEqual(dates[0],(2610,1,1,0,30,0))
        self.assertEqual(date_tuple(cftime.datetime(2610,12,31,23,59,59,999998)),
                         (2611,1,1,0,0,0))
        with self.assertRaisesRegex(ValueError,'Fractional-second'):
            date_tuple(cftime.datetime(2610,1,1,microsecond=20000))

    def test_chunks_of_same_variable_are_concatenated(self):
        a=write(self.base/'a.nc', days=np.arange(180)+.5)
        b=write(self.base/'b.nc', days=np.arange(180,365)+.5)
        self.assertEqual(len(validate_input_streams([b,a],[2001],'day')['swvl1']['dates']),365)

    def test_duplicate_chunks_not_hidden_by_deduplication(self):
        a=write(self.base/'a.nc'); b=write(self.base/'b.nc')
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            validate_input_streams([a,b],[2001],'day')

    def test_reverse_dates_rejected(self):
        a=write(self.base/'a.nc', days=np.arange(365)[::-1]+.5)
        with self.assertRaisesRegex(ValueError,'non-monotonic'):
            validate_input_streams([a],[2001],'day')

    def test_gap_rejected(self):
        a=write(self.base/'a.nc', days=np.delete(np.arange(365),50)+.5)
        with self.assertRaisesRegex(ValueError,'INCOMPLETE'):
            validate_input_streams([a],[2001],'day')

    def test_shifted_variables_rejected(self):
        a=write(self.base/'a.nc', name='swvl1')
        b=write(self.base/'b.nc', name='swvl2', days=np.arange(365)+.6)
        with self.assertRaisesRegex(ValueError,'UNSYNCHRONIZED'):
            validate_input_streams([a,b],[2001],'day')

    def test_coordinates_must_match(self):
        a=write(self.base/'a.nc', name='swvl1')
        b=write(self.base/'b.nc', name='swvl2', lats=(31.,40.))
        with self.assertRaisesRegex(ValueError,'INCOMPATIBLE_GRID'):
            validate_input_streams([a,b],[2001],'day')

    def test_calendar_mismatch_rejected(self):
        a=write(self.base/'a.nc', name='swvl1')
        b=write(self.base/'b.nc', name='swvl2', calendar='noleap')
        with self.assertRaisesRegex(ValueError,'INCOMPATIBLE_CALENDAR'):
            validate_input_streams([a,b],[2001],'day')

    def test_previous_day_verified(self):
        current=self.files(); prior=self.files(year=2000,value=.1)
        records=validate_previous_day(current,prior,2001,['swvl1','swvl2','swvl3','swvl4','sd','src'])
        self.assertEqual(records['swvl1']['date'],[2000,12,31,12,0,0])
        self.assertEqual(records['swvl1']['index'],365)

    def test_previous_day_360day_calendar(self):
        current=self.files(calendar='360_day'); prior=self.files(year=2000,calendar='360_day')
        records=validate_previous_day(current,prior,2001,['swvl1'])
        self.assertEqual(records['swvl1']['date'],[2000,12,30,12,0,0])

    def test_missing_boundary_not_fabricated(self):
        with self.assertRaisesRegex(ValueError,'MISSING_PREVIOUS_DAY'):
            validate_previous_day(self.files(),[],2001,['swvl1'])

    def test_boundary_from_monthly_mean_not_allowed(self):
        prior=self.files(year=2000, days=np.array([15.,45.,75.,105.,135.,165.,195.,225.,255.,285.,315.,345.]))
        with self.assertRaisesRegex(ValueError,'MISSING_PREVIOUS_DAY'):
            validate_previous_day(self.files(),prior,2001,['swvl1'])

    def test_difference_preserves_first_day_and_all_dates(self):
        current=self.files(); prior=self.files(year=2000,value=.1)
        records=validate_previous_day(current,prior,2001,['swvl1','swvl2','swvl3','swvl4','sd','src'])
        step=f.module(f.RULES/'land_steps.py','test_land_boundary')
        with xr.open_mfdataset(current) as source:
            data=source.rename(time_counter='time')
            output=step.compute_temporal_diff_with_boundary(data,dict(
                model_variable='total_water',land_boundary_records=records,year_start=2001))
            self.assertEqual(output.sizes['time'],365)
            np.testing.assert_array_equal(output.time.values,data.time.values)
            np.testing.assert_allclose(output['total_water'].isel(time=0),489.)
            np.testing.assert_allclose(output['total_water'].isel(time=slice(1,None)),0.)
            self.assertEqual(output['total_water'].attrs['units'],'kg m-2')

    def test_dependencies_records_boundary_and_included_files(self):
        names=['swvl1','swvl2','swvl3','swvl4']
        self.files(names=names); self.files(year=2000,names=names)
        rep=f.module(f.REPO/'examples/repoint_hr_year.py','test_boundary_repoint')
        doc={'inherit':{},'rules':[dict(compound_name='land.dslw.tavg-u-hxy-lnd.day.glb',
            model_variable='soil_moisture',pipelines=['diff'],inputs=[dict(path=str(self.base),
            pattern=r'atmos_day_land_swvl[1-4]_.*\.nc')])],
            'pipelines':[dict(name='diff',steps=['script:///unused/custom_steps.py:compute_temporal_diff'])]}
        paths=f.dependencies(doc,2001,rep)
        self.assertEqual(len(paths),8)
        self.assertEqual(len(doc['rules'][0]['land_boundary_records']),4)
        self.assertTrue(doc['pipelines'][0]['steps'][0].endswith(':compute_temporal_diff_with_boundary'))

    def test_canopy_and_snow_keep_january_first_in_all_calendars(self):
        rep=f.module(f.REPO/'examples/repoint_hr_year.py','test_single_store_repoint')
        step=f.module(f.RULES/'land_steps.py','test_single_store_boundary')
        for calendar,year,days in (('standard',2001,365),('standard',2000,366),
                                   ('noleap',2000,365),('360_day',2000,360)):
            for name,source,variable in (('dcw','src','skin_reservoir'),('dsn','sd','sd')):
                with self.subTest(calendar=calendar,year=year,product=name):
                    current=self.files(year=year,names=[source],calendar=calendar,value=.2)
                    prior=self.files(year=year-1,names=[source],calendar=calendar,value=.1)
                    f.directory_files.cache_clear()
                    f.matching.cache_clear()
                    rule=dict(compound_name=f'land.{name}.tavg-u-hxy-lnd.day.glb',
                              model_variable=variable,pipelines=['diff'],year_start=year,
                              inputs=[dict(path=str(self.base),pattern=rf'atmos_day_land_{source}_.*\.nc')])
                    doc=dict(inherit={},rules=[rule],pipelines=[dict(name='diff',steps=[
                        'script:///unused/custom_steps.py:compute_temporal_diff'])])
                    paths=f.dependencies(doc,year,rep)
                    self.assertEqual(set(paths),set(current+prior))
                    self.assertEqual(set(rule['land_boundary_records']),{source})
                    with xr.open_dataset(current[0]) as data:
                        data=data.rename(time_counter='time')
                        result=step.compute_temporal_diff_with_boundary(data,rule)
                        self.assertEqual(result.sizes['time'],days)
                        np.testing.assert_array_equal(result.time.values,data.time.values)
                        np.testing.assert_allclose(result[variable].isel(time=0),100.)
                        np.testing.assert_allclose(result[variable].isel(time=slice(1,None)),0.)

    def test_deep_soil_moisture_is_not_published_as_groundwater(self):
        doc=dict(inherit={},rules=[dict(compound_name='land.dgw.tavg-u-hxy-lnd.day.glb',
                                      model_variable='swvl4',inputs=[])])
        with self.assertRaisesRegex(ValueError,'UNRESOLVED_GROUNDWATER_MAPPING'):
            f.dependencies(doc,2001,None)

    def test_cftime_auxiliary_time_is_not_auto_rechunked(self):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', xr.SerializationWarning)
            current=self.files(year=2606,aux_time=True)
            prior=self.files(year=2605,value=.1,aux_time=True)
            names=['swvl1','swvl2','swvl3','swvl4','sd','src']
            records=validate_previous_day(current,prior,2606,names)
            step=f.module(f.RULES/'land_steps.py','test_land_cftime')
            with xr.open_mfdataset(current) as source:
                data=source.rename(time_counter='time')
                output=step.compute_temporal_diff_with_boundary(data,dict(
                    model_variable='total_water',land_boundary_records=records,year_start=2606))
                self.assertEqual(output.sizes['time'],365)
                np.testing.assert_allclose(output.total_water.isel(time=0).compute(),489.)


def write_points(path, name, year, day=1):
    """XIOS monthly instants: 0Z on the first of Feb Y .. Jan Y+1, value = sample year*100+month."""
    dates = [cftime.DatetimeGregorian(year + (m == 12), m % 12 + 1, day) for m in range(1, 13)]
    units = 'seconds since 2840-01-01 00:00:00'
    with netCDF4.Dataset(path, 'w') as ds:
        ds.createDimension('time', 12); ds.createDimension('elem', 2)
        time = ds.createVariable('time', 'f8', ('time',))
        time.units = units; time.calendar = 'gregorian'; time.standard_name = 'time'; time.axis = 'T'
        time[:] = cftime.date2num(dates, units, 'gregorian')
        lat = ds.createVariable('lat', 'f4', ('elem',)); lat.standard_name = 'latitude'; lat[:] = [70., 80.]
        v = ds.createVariable(name, 'f4', ('time', 'elem'))
        v.units = 'N m-1'; v.cell_methods = 'time: point'; v.coordinates = 'lat'
        v[:] = np.repeat([[d.year * 100 + d.month] for d in dates], 2, axis=1)
    return Path(path)


class MonthlyPointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)/'outdata/fesom'
        self.base.mkdir(parents=True)
        f.directory_files.cache_clear()
        f.matching.cache_clear()

    def tearDown(self):
        self.temp.cleanup()

    def points(self, name, years, **kwargs):
        return [write_points(self.base/f'{name}_inst.fesom.{y}.nc', name, y, **kwargs) for y in years]

    def test_january_comes_from_previous_year_file(self):
        files = self.points('sgm11', (2844, 2845))
        result = validate_input_streams(files, [2845], 'mon', monthly_points=True)
        self.assertEqual(result['sgm11']['frequency'], 'mon')

    def test_current_file_alone_lacks_january(self):
        files = self.points('sgm11', (2845,))
        with self.assertRaisesRegex(ValueError, 'INCOMPLETE_INPUT'):
            validate_input_streams(files, [2845], 'mon', monthly_points=True)
        with self.assertRaisesRegex(ValueError, 'January through December'):
            validate_input_streams(files, [2845], 'mon')

    def test_monthly_means_are_not_point_samples(self):
        files = self.points('sgm11', (2844, 2845), day=15)
        with self.assertRaisesRegex(ValueError, '0Z on the first'):
            validate_input_streams(files, [2845], 'mon', monthly_points=True)

    def doc(self):
        rule = dict(compound_name='seaIce.sistressmax.tpt-u-hxy-si.mon.glb', model_variable='sgm11',
                    previous_year_input=True,
                    inputs=[dict(path=str(self.base), pattern=r'sgm11_inst\.fesom\.\d{4}\.nc')],
                    sgm22_path=str(self.base), sgm22_pattern=r'sgm22_inst\.fesom\.\d{4}\.nc')
        return dict(inherit={}, rules=[rule])

    def test_planner_pins_previous_and_current_files_only(self):
        for name in ('sgm11', 'sgm22'):
            self.points(name, (2843, 2844, 2845, 2846))
        rep = f.module(f.REPO/'examples/repoint_hr_year.py', 'test_point_repoint')
        doc = self.doc()
        paths = f.dependencies(doc, 2845, rep)
        self.assertEqual(sorted(p.name for p in paths),
                         [f'{n}_inst.fesom.{y}.nc' for n in ('sgm11', 'sgm22') for y in (2844, 2845)])
        rule = doc['rules'][0]
        self.assertTrue(rule['skip_input_year_filter'])
        self.assertIn('(?:2844|2845)', rule['sgm22_pattern'])

    def test_first_year_without_previous_file_is_blocked(self):
        for name in ('sgm11', 'sgm22'):
            self.points(name, (2845,))
        rep = f.module(f.REPO/'examples/repoint_hr_year.py', 'test_point_repoint_first')
        with self.assertRaisesRegex(ValueError, 'INCOMPLETE_INPUT'):
            f.dependencies(self.doc(), 2845, rep)

    def test_only_monthly_point_products_may_read_the_previous_year(self):
        doc = self.doc()
        doc['rules'][0]['compound_name'] = 'seaIce.sistressmax.tavg-u-hxy-si.mon.glb'
        with self.assertRaisesRegex(ValueError, 'UNSUPPORTED_PREVIOUS_YEAR_INPUT'):
            f.dependencies(doc, 2845, None)

    def test_pipeline_keeps_january_to_december_of_the_year(self):
        spec = importlib.util.spec_from_file_location('point_custom_steps', f.REPO/'examples/custom_steps.py')
        steps = importlib.util.module_from_spec(spec); spec.loader.exec_module(steps)
        files = self.points('sgm11', (2844, 2845))
        with xr.open_mfdataset(files, decode_times=xr.coders.CFDatetimeCoder(use_cftime=True)) as ds:
            result = steps.select_monthly_point_samples(ds['sgm11'], dict(year_start=2845, year_end=2845))
            self.assertEqual([(d.year, d.month, d.day, d.hour) for d in result.time.values],
                             [(2845, m, 1, 0) for m in range(1, 13)])
            np.testing.assert_array_equal(result.isel(elem=0).values, [284500 + m for m in range(1, 13)])
            with self.assertRaisesRegex(ValueError, 'INCOMPLETE_INPUT'):
                steps.select_monthly_point_samples(ds['sgm11'].isel(time=slice(12, None)),
                                                   dict(year_start=2845, year_end=2845))

    def test_catalogue_rules_use_the_previous_year_layout(self):
        import yaml
        catalogue = f.REPO/'awi-esm3-lr-variables/lrcs_seaice/cmip7xml_awiesm3-lr_lrcs_seaice.yaml'
        doc = yaml.safe_load(catalogue.read_text())
        pipelines = {p['name']: p['steps'] for p in doc['pipelines']}
        for name in ('sistressave', 'sistressmax'):
            rule = next(r for r in doc['rules'] if r['name'] == name)
            self.assertTrue(rule['previous_year_input'])
            self.assertEqual(rule['model_variable'], 'sgm11')
            steps = pipelines[rule['pipelines'][0]]
            select = steps.index('script://$PYCMOR_HOME/examples/custom_steps.py:select_monthly_point_samples')
            self.assertEqual(steps[select - 1], f'script://$PYCMOR_HOME/examples/custom_steps.py:compute_{name}')


if __name__=='__main__':
    unittest.main()
