"""Regression checks for native-grid regional output and vertical/mask auxiliaries."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cftime
import numpy as np
import xarray as xr

import grid_auxiliary as auxiliary

REPO = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('_grid_aux_custom_steps', REPO / 'examples/custom_steps.py')
custom = importlib.util.module_from_spec(spec)
spec.loader.exec_module(custom)


class Rule(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None


def table(path):
    path.write_text('&NAMVV1\n DVALH(0:)=0.,1000.,5000.,0.\n DVBH(0:)=0.,0.,0.5,1.\n/\n')


class RegionalTests(unittest.TestCase):
    def test_native_latitude_is_not_an_index(self):
        data = xr.DataArray([1, 2, 3, 4], dims='cell', coords={
            'lat': ('cell', [-60., -30., 0., 40.]), 'lon': ('cell', [10., 20., 30., 40.])})
        got = custom.select_southern_hemisphere(data, Rule())
        np.testing.assert_array_equal(got, [1, 2])
        np.testing.assert_array_equal(got.lon, [10., 20.])

    def test_regular_latitude_keeps_longitude(self):
        data = xr.DataArray(np.arange(8).reshape(4, 2), dims=('lat', 'lon'),
                            coords={'lat': [40, 0, -30, -60], 'lon': [10, 20]})
        got = custom.select_southern_hemisphere(data, Rule())
        self.assertEqual(got.shape, (2, 2))
        np.testing.assert_array_equal(got, data.values[2:])


class HybridTests(unittest.TestCase):
    def test_non_hr_levels_never_default_to_l137(self):
        with self.assertRaisesRegex(ValueError, 'No OIFS coefficient table'):
            custom._oifs_vtable_for_levels(Rule(), 91)
        self.assertTrue(custom._oifs_vtable_for_levels(Rule(), 137).endswith('L137'))

    def test_explicit_table_and_dimension_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'vtable_L3'; table(path)
            t = [cftime.DatetimeGregorian(2001, 1, 1)]
            data = xr.DataArray(np.arange(3).reshape(1, 3, 1), dims=('time', 'model_levels', 'cell'),
                coords={'time': t, 'model_levels': [1, 2, 3]}, name='hus')
            ps = xr.DataArray([[100000.]], dims=('time', 'cell'), coords={'time': t})
            with patch.object(custom, '_load_secondary_mf', return_value=ps):
                result = custom.add_hybrid_sigma_coordinate(data.to_dataset(), Rule(oifs_vtable=str(path)))
            np.testing.assert_array_equal(result.hus.values.ravel(), [2, 1, 0])
            self.assertIn('ps', result)
            self.assertTrue(np.all(np.diff(result.lev.values) < 0))
            with self.assertRaisesRegex(ValueError, 'does not bound'):
                custom.add_hybrid_sigma_coordinate(data.isel(model_levels=slice(0, 2)), Rule(oifs_vtable=str(path)))

    def test_configure_only_selected_hybrid_pipeline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'vtable_L3'; table(path)
            doc = {'inherit': {}, 'rules': [{'compound_name': 'atmos.hus.tpt-al-hxy-u.6hr.glb',
                    'pipelines': ['hybrid']}], 'pipelines': [{'name': 'hybrid', 'steps': [
                    f'script://{REPO}/examples/custom_steps.py:add_hybrid_sigma_coordinate']}]}
            auxiliary.configure_grid_auxiliaries(doc, {'model_level_table': str(path)})
            self.assertEqual(doc['rules'][0]['configured_model_levels'], 3)
            self.assertEqual(doc['rules'][0]['model_level_table_file'], str(path))
            self.assertTrue(doc['rules'][0]['retain_year_end_point'])
            doc['rules'][0]['pipelines'] = []
            auxiliary.configure_grid_auxiliaries(doc, {})  # unused auxiliary is not required


class BasinTests(unittest.TestCase):
    def test_lr_density_total_pins_bolus_as_fingerprinted_dependency(self):
        import full_cmorization
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            mask, mesh = base / 'basin.nc', base / 'mesh.nc'
            bolus = base / 'std_dens_DIVbolus.fesom.2605.nc'
            for path in (mask, mesh, bolus):
                path.write_text('nonempty fixture')
            doc = {'inherit': {'data_path': str(base), 'year': 2605}, 'rules': [{
                'compound_name': 'ocean.msftm.tavg-rho-hyb-sea.mon.glb',
                'pipelines': ['moc'], 'inputs': []}],
                'pipelines': [{'name': 'moc', 'steps': [
                    f'script://{REPO}/examples/custom_steps.py:compute_msftm_density']}]}
            with patch.object(auxiliary, 'validate_basin_mask'):
                auxiliary.configure_grid_auxiliaries(doc, dict(mesh_dir=directory, basin_mask_file=str(mask)))
            self.assertEqual(doc['rules'][0]['msftm_bolus_file'], str(bolus))
            self.assertIn(bolus, full_cmorization.dependencies(doc, 2605, None))
            bolus.unlink()
            with patch.object(auxiliary, 'validate_basin_mask'), self.assertRaisesRegex(
                    ValueError, 'MISSING_DENSITY_BOLUS'):
                auxiliary.configure_grid_auxiliaries(doc, dict(mesh_dir=directory, basin_mask_file=str(mask)))

    def test_external_mask_repoints_only_basin_primary(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            mask = base / 'external' / 'basin_mask_CORE3.nc'
            original_inputs = [dict(path=str(base / 'mesh'), pattern=r'basin_mask\.nc')]
            doc = {'inherit': {}, 'rules': [{
                'compound_name': 'ocean.basin.ti-u-hxy-u.fx.glb',
                'pipelines': ['basin'], 'inputs': original_inputs}],
                'pipelines': [{'name': 'basin', 'steps': [
                    f'script://{REPO}/examples/custom_steps.py:load_basin_mask']}]}
            config = dict(mesh_dir=str(base / 'mesh'), basin_mask_file=str(mask))
            with patch.object(auxiliary, 'validate_basin_mask') as validate:
                auxiliary.configure_grid_auxiliaries(doc, config)
                validate.assert_called_once_with(str(mask), base / 'mesh' / 'mesh.nc')
            self.assertEqual(doc['rules'][0]['inputs'], [dict(
                path=str(mask.parent), pattern=r'basin_mask_CORE3\.nc')])
            self.assertEqual(doc['rules'][0]['basin_mask_file'], str(mask))

            # A transport helper consumes a basin auxiliary, not its primary
            # input. The ocean field must not be replaced by the mask.
            ocean_inputs = [dict(path=str(base / 'ocean'), pattern=r'w\.fesom\.2605\.nc')]
            doc['rules'][0]['inputs'] = ocean_inputs
            doc['pipelines'][0]['steps'] = [f'script://{REPO}/examples/custom_steps.py:_load_basin_nodes']
            with patch.object(auxiliary, 'validate_basin_mask') as validate:
                auxiliary.configure_grid_auxiliaries(doc, config)
                validate.assert_called_once()
            self.assertEqual(doc['rules'][0]['inputs'], ocean_inputs)

    def test_order_flags_and_longitude_convention(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); mesh = base / 'mesh.nc'; mask = base / 'basin.nc'
            lat = np.array([-60, -20, 10, 40, 70.]); lon = np.arange(5.)
            xr.Dataset({'lat': ('node', lat), 'lon': ('node', lon)}).to_netcdf(mesh)
            def write(order=None, flags=None, values=None):
                order = np.arange(5) if order is None else order
                values = [10, 3, 2, 1, 11] if values is None else values
                ds = xr.Dataset({'basin': ('node', values),
                                 'lat': ('node', lat[order]), 'lon': ('node', lon[order] + 360)})
                ds.basin.attrs.update(flag_values=np.array([1, 2, 3, 10, 11]),
                    flag_meanings=flags or 'atlantic_ocean pacific_ocean indian_ocean southern_ocean arctic_ocean')
                ds.to_netcdf(mask)
            write()
            self.assertEqual(auxiliary.validate_basin_mask(mask, mesh, 5)['nodes'], 5)
            write(order=np.arange(5)[::-1])
            with self.assertRaisesRegex(ValueError, 'node order'):
                auxiliary.validate_basin_mask(mask, mesh, 5)
            write(flags='atlantic_ocean pacific_ocean indian_ocean arctic_ocean southern_ocean',
                  values=[11, 3, 2, 1, 10])
            self.assertEqual(auxiliary.validate_basin_mask(mask, mesh, 5)['flags'][10], 'arctic')
            write(flags='atlantic_ocean pacific_ocean indian_ocean southern_ocean southern_ocean')
            with self.assertRaises(ValueError):
                auxiliary.validate_basin_mask(mask, mesh, 5)

    def test_semantic_decoder_reaches_density_diagnostics_without_changing_mask(self):
        import hashlib
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); mask = base / 'basin.nc'; mesh = base / 'mesh.nc'
            lat = np.array([10., 0., -10., -70., 75.])
            xr.Dataset({'lat': ('nod2', lat)}).to_netcdf(mesh)
            ds = xr.Dataset({'basin': ('ncells', [1, 2, 3, 10, 11])})
            ds.basin.attrs.update(flag_values=[1, 2, 3, 10, 11],
                flag_meanings='atlantic_ocean pacific_ocean indian_ocean southern_ocean arctic_ocean')
            ds.to_netcdf(mask)
            before = hashlib.sha256(mask.read_bytes()).hexdigest()
            rule = Rule(basin_mask_file=str(mask), mesh_path=str(mesh), data_path=directory,
                        model_variable='msftm', std_dens=[30.])
            np.testing.assert_array_equal(custom._load_basin_nodes(rule), [1, 2, 3, 11, 10])
            data = xr.DataArray([[[1., 2., 3., 100., 7.]]], dims=('time','ndens','nod2'),
                               coords={'time': [cftime.DatetimeGregorian(2605,1,15)], 'ndens': [30.]})
            for function, variable in ((custom.compute_msftm_density, 'std_dens_DIV'),
                                       (custom.compute_msftmmpa_density, 'std_dens_DIVbolus')):
                with patch.object(custom, '_open_fesom_year_files', return_value=None):
                    result = function(data.to_dataset(name=variable), rule)
                # North of 60N, Atlantic-Arctic includes the Arctic node's 7,
                # not the Southern node's 100. The old numeric assumption
                # incorrectly yielded zero for this Atlantic-Arctic bin.
                actual = result.msftm.isel(time=0,rho=0,basin=0).sel(lat=60.5).item()
                self.assertEqual(actual, -7 * custom._RHO0)
            original = custom.load_basin_mask(None, rule)
            try:
                np.testing.assert_array_equal(original.basin.values, [1, 2, 3, 10, 11])
                self.assertEqual(original.basin.attrs['flag_meanings'], ds.basin.attrs['flag_meanings'])
            finally:
                original.close()
            self.assertEqual(hashlib.sha256(mask.read_bytes()).hexdigest(), before)

    def test_pinned_bolus_is_required_added_and_exactly_aligned(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); mask = base / 'basin.nc'; mesh = base / 'mesh.nc'
            bolus_path = base / 'std_dens_DIVbolus.fesom.2605.nc'
            xr.Dataset({'lat': ('nod2', [10., 0., -10., -70., 75.])}).to_netcdf(mesh)
            ds = xr.Dataset({'basin': ('ncells', [1, 2, 3, 10, 11])})
            ds.basin.attrs.update(flag_values=[1, 2, 3, 10, 11],
                flag_meanings='atlantic_ocean pacific_ocean indian_ocean southern_ocean arctic_ocean')
            ds.to_netcdf(mask)
            data = xr.DataArray([[[1., 2., 3., 100., 7.]]], dims=('time', 'ndens', 'nod2'),
                coords={'time': [cftime.DatetimeGregorian(2605, 1, 15)], 'ndens': [30.]})
            bolus = xr.ones_like(data).to_dataset(name='std_dens_DIVbolus')
            bolus.to_netcdf(bolus_path)
            rule = Rule(basin_mask_file=str(mask), mesh_path=str(mesh), data_path=directory,
                model_variable='msftm', std_dens=[30.], msftm_bolus_file=str(bolus_path))
            with patch.object(custom, '_open_fesom_year_files') as discover:
                result = custom.compute_msftm_density(data.to_dataset(name='std_dens_DIV'), rule)
                discover.assert_not_called()
            actual = result.msftm.isel(time=0, rho=0, basin=0).sel(lat=60.5).item()
            self.assertEqual(actual, -8 * custom._RHO0)
            bolus.assign_coords(time=[cftime.DatetimeGregorian(2605, 1, 16)]).to_netcdf(bolus_path)
            with self.assertRaisesRegex(ValueError, 'align|equal'):
                custom.compute_msftm_density(data.to_dataset(name='std_dens_DIV'), rule)
            bolus_path.unlink()
            with self.assertRaises(FileNotFoundError):
                custom.compute_msftm_density(data.to_dataset(name='std_dens_DIV'), rule)


class BoundaryTests(unittest.TestCase):
    def test_exact_endpoint_opt_in_does_not_keep_later_spill(self):
        from pycmor.core.gather_inputs import _year_selection_mask
        dates = [cftime.DatetimeGregorian(2605, 1, 1, 6),
                 cftime.DatetimeGregorian(2605, 12, 31, 18),
                 cftime.DatetimeGregorian(2606, 1, 1),
                 cftime.DatetimeGregorian(2606, 1, 1, 1, 30)]
        np.testing.assert_array_equal(_year_selection_mask(dates, 2605, 2605), [1, 1, 0, 0])
        np.testing.assert_array_equal(_year_selection_mask(dates, 2605, 2605, True), [1, 1, 1, 0])

    def test_numpy_dates_and_360_day_calendar(self):
        from pycmor.core.gather_inputs import _year_selection_mask
        dates = np.array(['2000-12-31T18:00', '2001-01-01T00:00', '2001-01-01T00:00:00.000001'], dtype='datetime64[us]')
        np.testing.assert_array_equal(_year_selection_mask(dates, 2000, 2000, True), [1, 1, 0])
        dates = [cftime.Datetime360Day(2000, 12, 30, 18), cftime.Datetime360Day(2001, 1, 1)]
        np.testing.assert_array_equal(_year_selection_mask(dates, 2000, 2000, True), [1, 1])

    def test_end_inclusive_points_stay_in_one_file_chunk(self):
        from pycmor.std_lib.files import split_data_timespan
        dates = [cftime.DatetimeGregorian(2605, 12, 31, 18), cftime.DatetimeGregorian(2606, 1, 1)]
        data = xr.Dataset({'hus': ('time', [1., 2.])}, coords={'time': dates})
        with patch('pycmor.std_lib.files.file_timespan_tail', return_value=[dates[-1]]):
            got = split_data_timespan(data, Rule(retain_year_end_point=True))
            self.assertEqual(len(got), 1)
            self.assertEqual(got[0].sizes['time'], 2)


if __name__ == '__main__':
    unittest.main()
