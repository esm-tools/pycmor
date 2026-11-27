"""Small synthetic decade tests; never reads model-sized three-dimensional data."""
from pathlib import Path
import re
import tempfile
from types import SimpleNamespace
import unittest

import cftime
import netCDF4
import numpy as np
import xarray as xr

import decadal_support as d
from pycmor.core.gather_inputs import InputFileCollection
from pycmor.std_lib.decadal import decadal_gate, explicit_decadal_mean
from pycmor.std_lib.dimension_mapping import DimensionMapper


class Rule(SimpleNamespace):
    def get(self, name, default=None):
        return getattr(self, name, default)


def create_year(base, year, variable='temp', calendar='gregorian', missing_month=None,
                shift_grid=False, bad_bounds=False, reverse=False):
    path = base / f'{variable}.fesom.{year}.nc'
    dates, bounds = [], []
    for month in range(1, 13):
        if month == missing_month:
            continue
        lo = cftime.datetime(year, month, 1, calendar=calendar)
        hi = cftime.datetime(year + (month == 12), 1 if month == 12 else month + 1,
                             1, calendar=calendar)
        dates.append(cftime.datetime(year, month, 15, calendar=calendar))
        bounds.append((lo, hi))
    if reverse:
        dates.reverse(); bounds.reverse()
    units = f'days since {year}-01-01'
    with netCDF4.Dataset(path, 'w') as ds:
        ds.createDimension('time', len(dates))
        ds.createDimension('node', 2)
        ds.createDimension('bounds', 2)
        t = ds.createVariable('time', 'f8', ('time',))
        t.units = units; t.calendar = calendar; t.bounds = 'time_bounds'
        t[:] = cftime.date2num(dates, units, calendar)
        b = ds.createVariable('time_bounds', 'f8', ('time', 'bounds'))
        b[:] = cftime.date2num(bounds, units, calendar)
        if bad_bounds:
            b[0, 1] -= 1
        x = ds.createVariable('lon', 'f8', ('node',))
        x.units = 'degrees_east'; x[:] = [1 + shift_grid, 2]
        v = ds.createVariable(variable, 'f8', ('time', 'node'))
        v.units = 'K' if variable == 'temp' else '1'
        v[:] = np.arange(len(dates))[:, None] + np.zeros((len(dates), 2))
    return path


def document(base, variable='temp', end=2009):
    return {'inherit': {'year': end, 'year_start': end, 'year_end': end},
            'rules': [{'compound_name': 'ocean.thetao.tavg-ol-hxy-sea.dec.glb',
                       'inputs': [{'path': str(base), 'pattern': rf'{variable}\.fesom\.{end}\.nc$'}],
                       'model_variable': variable, 'pipelines': ['dec']}],
            'pipelines': [{'name': 'dec', 'steps': [
                'pycmor.std_lib.decadal.decadal_gate',
                'pycmor.std_lib.timeaverage.timeavg']} ]}


class Tests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)

    def decade(self, variable='temp', calendar='gregorian'):
        return [create_year(self.base, y, variable=variable, calendar=calendar)
                for y in range(2000, 2010)]

    def test_period_window_and_invalid_configuration(self):
        self.assertEqual(d.window_for_period('26050101-26091231'), (2600, 2609))
        self.assertEqual(d.window_for_period('26100101-26141231', [2605, 2614]), (2605, 2614))
        for invalid in ([2605, 2609], [2605, 2614], ['2600', 2609], [True, 2610]):
            with self.assertRaises(ValueError):
                d.window_for_period('26050101-26091231', invalid)

    def test_freezes_exact_ten_years_and_replaces_averaging(self):
        files = self.decade()
        create_year(self.base, 1999); create_year(self.base, 2010)
        doc = document(self.base)
        selected = d.configure_decadal_doc(doc, 2000, 2009)
        self.assertEqual(selected, files)
        rule = doc['rules'][0]
        pattern = re.compile(rule['inputs'][0]['pattern'])
        self.assertFalse(pattern.fullmatch('temp.fesom.1999.nc'))
        self.assertFalse(pattern.fullmatch('temp.fesom.2010.nc'))
        self.assertEqual((rule['year_start'], rule['year_end']), (2000, 2009))
        self.assertEqual(sum(rule['decadal_month_weights']), 3653)
        self.assertIn('pycmor.std_lib.decadal.explicit_decadal_mean', doc['pipelines'][0]['steps'])

    def test_secondary_vector_partner_included_all_years(self):
        primary = self.decade('tx_sur')
        secondary = self.decade('ty_sur')
        doc = document(self.base, 'tx_sur')
        r = doc['rules'][0]
        r.update(vector_partner_path=str(self.base),
                 vector_partner_pattern=r'ty_sur\.fesom\.2009\.nc$',
                 vector_partner_variable='ty_sur')
        selected = d.configure_decadal_doc(doc, 2000, 2009)
        self.assertEqual(set(selected), set(primary + secondary))
        self.assertEqual(len(r['decadal_input_files']), 20)
        self.assertEqual(len([p for p in self.base.iterdir()
                              if re.fullmatch(r['vector_partner_pattern'], p.name)]), 10)

    def test_missing_secondary_year_not_ready(self):
        self.decade('tx_sur'); self.decade('ty_sur')
        (self.base / 'ty_sur.fesom.2004.nc').unlink()
        doc = document(self.base, 'tx_sur')
        doc['rules'][0].update(vector_partner_path=str(self.base),
            vector_partner_pattern=r'ty_sur\.fesom\.\d{4}\.nc')
        with self.assertRaisesRegex(ValueError, 'INCOMPLETE_MONTHS'):
            d.configure_decadal_doc(doc, 2000, 2009)

    def test_vector_partner_must_share_geometry(self):
        self.decade('tx_sur')
        for year in range(2000, 2010):
            create_year(self.base, year, variable='ty_sur', shift_grid=True)
        doc = document(self.base, 'tx_sur')
        doc['rules'][0].update(vector_partner_path=str(self.base),
            vector_partner_pattern=r'ty_sur\.fesom\.\d{4}\.nc')
        with self.assertRaisesRegex(ValueError, 'VECTOR_GRID_MISMATCH'):
            d.configure_decadal_doc(doc, 2000, 2009)

    def test_missing_month_duplicate_year_and_nonmonotonic_rejected(self):
        files = self.decade()
        create_year(self.base, 2003, missing_month=4)
        with self.assertRaisesRegex(ValueError, 'INCOMPLETE_MONTHS'):
            d.validate_monthly_streams(files, 2000, 2009)
        create_year(self.base, 2003, reverse=True)
        with self.assertRaisesRegex(ValueError, 'NONMONOTONIC'):
            d.validate_monthly_streams(files, 2000, 2009)
        create_year(self.base, 2003)
        clone = self.base / 'duplicate.nc'
        clone.write_bytes(files[0].read_bytes())
        with self.assertRaisesRegex(ValueError, 'INCOMPLETE_MONTHS'):
            d.validate_monthly_streams(files + [clone], 2000, 2009)

    def test_bad_month_bounds_and_grid_changes_rejected(self):
        files = self.decade()
        create_year(self.base, 2007, bad_bounds=True)
        with self.assertRaisesRegex(ValueError, 'INVALID_MONTH_BOUNDS'):
            d.validate_monthly_streams(files, 2000, 2009)
        create_year(self.base, 2007, shift_grid=True)
        with self.assertRaisesRegex(ValueError, 'CHANGED_COORDINATES'):
            d.validate_monthly_streams(files, 2000, 2009)

    def test_calendar_change_rejected(self):
        files = self.decade()
        create_year(self.base, 2003, calendar='360_day')
        with self.assertRaisesRegex(ValueError, 'CHANGED_CALENDAR'):
            d.validate_monthly_streams(files, 2000, 2009)

    def test_360day_and_noleap_weights(self):
        for calendar, total in [('360_day', 3600), ('noleap', 3650)]:
            files = self.decade(calendar=calendar)
            info = d.validate_monthly_streams(files, 2000, 2009)
            self.assertEqual(sum(info['weights']), total)

    def test_explicit_gate_preserves_exact_pattern(self):
        self.decade(); doc = document(self.base)
        d.configure_decadal_doc(doc, 2000, 2009)
        r = doc['rules'][0]
        rule = Rule(**{**r, 'inputs': [InputFileCollection.from_dict(v) for v in r['inputs']]})
        pattern = rule.inputs[0].pattern_str
        create_year(self.base, 2010)
        self.assertEqual(decadal_gate('payload', rule), 'payload')
        self.assertEqual(rule.inputs[0].pattern_str, pattern)
        self.assertEqual(len(rule.inputs[0].files), 10)

    def test_weighted_mean_and_midpoint_are_exact(self):
        files = self.decade(); info = d.validate_monthly_streams(files, 2000, 2009)
        dates = [cftime.datetime(*t, calendar=info['calendar']) for t in info['dates']]
        values = np.arange(120, dtype=float)
        data = xr.DataArray(values, dims='time', coords={'time': dates}, name='temp', attrs={'units': 'K'})
        rule = Rule(explicit_decadal_window=True, year_start=2000, year_end=2009,
                    decadal_dates=info['dates'], decadal_calendar=info['calendar'],
                    decadal_month_weights=info['weights'])
        result = explicit_decadal_mean(data, rule)
        self.assertAlmostEqual(float(result[0]), np.average(values, weights=info['weights']))
        self.assertNotAlmostEqual(float(result[0]), float(values.mean()))
        midpoint = cftime.date2num(result.time.values[0], 'days since 2000-01-01', info['calendar'])
        self.assertEqual(midpoint, 3653 / 2)
        self.assertEqual(result.attrs['units'], 'K')
        data[0] = np.nan
        self.assertTrue(np.isnan(float(explicit_decadal_mean(data, rule)[0])))

    def test_runtime_wrong_dates_and_weights_rejected(self):
        info = d.validate_monthly_streams(self.decade(), 2000, 2009)
        dates = [cftime.datetime(*t, calendar=info['calendar']) for t in info['dates']]
        data = xr.DataArray(np.ones(120), dims='time', coords={'time': dates})
        rule = Rule(explicit_decadal_window=True, year_start=2000, year_end=2009,
                    decadal_dates=info['dates'], decadal_calendar=info['calendar'],
                    decadal_month_weights=[1] * 120)
        with self.assertRaisesRegex(ValueError, 'weights do not span'):
            explicit_decadal_mean(data, rule)
        rule.decadal_month_weights = info['weights']
        with self.assertRaisesRegex(ValueError, 'time coordinates differ'):
            explicit_decadal_mean(data.isel(time=slice(None, None, -1)), rule)

    def test_weighted_mean_preserves_depth_and_horizontal_metadata_lazily(self):
        info = d.validate_monthly_streams(self.decade(), 2000, 2009)
        dates = [cftime.datetime(*t, calendar=info['calendar']) for t in info['dates']]
        values = np.arange(120 * 2 * 3, dtype=float).reshape(120, 2, 3)
        data = xr.DataArray(values, dims=('time', 'node', 'nz'),
                            coords={'time': dates, 'nz': [2.5, 10., 100.],
                                    'lon': ('node', [1., 2.]), 'lat': ('node', [3., 4.])},
                            name='temp', attrs={'units': 'degC'}).chunk({'time': 12})
        data.nz.attrs = {'standard_name': 'depth', 'long_name': 'depth below sea',
                         'axis': 'Z', 'positive': 'down', 'units': 'm'}
        data.nz.encoding = {'dtype': 'float32', '_FillValue': None}
        data.lon.attrs = {'standard_name': 'longitude', 'units': 'degrees_east'}
        data.lat.attrs = {'standard_name': 'latitude', 'units': 'degrees_north'}
        metadata = {name: (coord.attrs.copy(), coord.encoding.copy())
                    for name, coord in data.coords.items() if name != 'time'}
        rule = Rule(explicit_decadal_window=True, year_start=2000, year_end=2009,
                    decadal_dates=info['dates'], decadal_calendar=info['calendar'],
                    decadal_month_weights=info['weights'])
        # Even an explicit caller policy of False must be restored after this
        # function, without stripping depth information from its own output.
        with xr.set_options(keep_attrs=False):
            result = explicit_decadal_mean(data, rule)
            self.assertFalse(xr.get_options()['keep_attrs'])
        self.assertIsNotNone(result.chunks)
        np.testing.assert_allclose(result.compute()[0],
                                   np.average(values, weights=info['weights'], axis=0))
        for name, (attrs, encoding) in metadata.items():
            self.assertEqual(result[name].attrs, attrs)
            self.assertEqual(result[name].encoding, encoding)
            self.assertEqual(data[name].attrs, attrs)
            self.assertEqual(data[name].encoding, encoding)
            np.testing.assert_array_equal(result[name], data[name])
        # This is the downstream semantic detection that failed for actual
        # decadal so/thetao files: nz is not a CMOR depth-axis name by itself.
        mapper = DimensionMapper()
        self.assertEqual(mapper.detect_dimension_type(result, 'nz'), 'depth')
        mapping = mapper.create_mapping_flexible(result, target_dimensions=['time', 'olevel', 'node'])
        self.assertEqual(mapping['nz'], 'olevel')
        output = self.base / 'mean_depth.nc'
        result.to_netcdf(output)
        with netCDF4.Dataset(output) as ds:
            self.assertEqual(ds['nz'].standard_name, 'depth')
            self.assertEqual(ds['nz'].axis, 'Z')
            self.assertEqual(ds['nz'].positive, 'down')
            self.assertEqual(ds['nz'].units, 'm')
            self.assertEqual(ds['nz'].dtype, np.dtype('float32'))

    def test_output_bounds_validate_full_decade(self):
        output = self.base / 'output.nc'
        with netCDF4.Dataset(output, 'w') as ds:
            ds.createDimension('time', 1); ds.createDimension('bnds', 2)
            t = ds.createVariable('time', 'f8', ('time',)); t.units = 'days since 2000-01-01'
            t.calendar = 'gregorian'; t.bounds = 'time_bnds'; t[:] = 3653 / 2
            b = ds.createVariable('time_bnds', 'f8', ('time', 'bnds')); b[:] = [[0, 3653]]
        self.assertEqual(d.validate_decadal_output([output], 2000, 2009), 1)
        with netCDF4.Dataset(output, 'a') as ds:
            ds['time_bnds'][:] = [[0, 366]]
        with self.assertRaisesRegex(ValueError, 'OUTPUT_WINDOW'):
            d.validate_decadal_output([output], 2000, 2009)


if __name__ == '__main__':
    unittest.main()
