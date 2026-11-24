"""Native sources for ocean fluxes, weighted tracer columns, and their independent QC."""
from pathlib import Path
import sys
import tempfile
import unittest

import numpy as np
import xarray as xr
import yaml

from pycmor.awiesm3_lr import exact_vertical_bounds

RULES = Path(__file__).resolve().parents[1] / 'rules'
sys.path.insert(0, str(RULES))
import ocean_steps
import ocean_validation


REVIEWED_BINARY = '94377dfcbf82fc6b739413054f63b464af1f510fbb00c5f9eaa06f4d7fb5cc04'
EQUIVALENT_BINARY = 'fec1cda3169516ee3f96274e9365ea63d6e04ccf68d69debb36267c5432c0928'


class OceanPhysicsTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.geo = xr.Dataset({
            'nz': ('nz', [0., 100., 250., 500.]), 'nz1': ('nz1', [50.,175.,375.]),
            'ulevels_nod2D': ('nod2',[1,2,1]), 'nlevels_nod2D': ('nod2',[4,4,2]),
            'zbar_n_surface': ('nod2',[0.,150.,0.]),
            'lat': ('nod2',[40.,-75.,20.]), 'lon': ('nod2',[-5.,60.,30.])})
        self.geom = self.root/'fesom.mesh.diag.nc'
        self.geo.to_netcdf(self.geom)
        self.tracer = xr.DataArray([[[1.,2.,4.],[np.nan,6.,9.],[12.,np.nan,np.nan]]],
                                   dims=('time','nod2','nz'),name='temp')
        self.h = xr.DataArray([[[100.,150.,250.],[np.nan,100.,250.],[50.,np.nan,np.nan]]],
                              dims=self.tracer.dims,name='hnode')

    def test_exact_layer_edges_survive_horizontal_grid_choice(self):
        rule = {'lr_exact_ocean_depth': True, 'ocean_geometry_file': str(self.geom)}
        for horizontal in [('cell',), ('lat', 'lon')]:
            dimensions = ('lev',) + horizontal
            data = xr.DataArray(np.ones((3,) + (1,) * len(horizontal)), dims=dimensions,
                                coords={'lev': self.geo.nz1.values}, name='so')
            result = exact_vertical_bounds(data, rule)
            self.assertEqual(result.lev.attrs['bounds'], 'lev_bnds')
            np.testing.assert_array_equal(result.lev_bnds, [[0., 100.], [100., 250.], [250., 500.]])
            with self.assertRaisesRegex(ValueError, 'tracer centers'):
                exact_vertical_bounds(data.assign_coords(lev=[50., 170., 375.]), rule)

    def test_netcdf_masked_scalar_in_water_column(self):
        from netCDF4 import Dataset
        path = self.root/'masked-ocean.nc'
        with Dataset(path, 'w') as ds:
            ds.createDimension('cell', 3)
            variable = ds.createVariable('thetao', 'f4', ('cell',), fill_value=1e20)
            variable[0] = 5.
            variable[1] = np.nan
        with Dataset(path) as ds:
            variable = ds['thetao']
            self.assertEqual(ocean_validation.field_at(variable, 0, 0), 5.)
            self.assertTrue(np.ma.is_masked(ocean_validation.field_at(variable, 0, 1)))
            self.assertTrue(np.ma.is_masked(ocean_validation.field_at(variable, 0, 2)))

    def test_partial_layer_weight_masks_and_cavity_roof(self):
        x = ocean_steps.column_statistic(self.tracer,self.h,self.geo,300.)
        np.testing.assert_allclose(x, [[2.,7.,12.]])
        x = ocean_steps.column_statistic(self.tracer,self.h,self.geo,100.)
        np.testing.assert_allclose(x, [[1.,np.nan,12.]],equal_nan=True)
        broken = self.tracer.copy()
        broken[0,0,1] = np.nan
        self.assertTrue(np.isnan(ocean_steps.column_statistic(broken,self.h,self.geo)[0,0]))
        x = ocean_steps.column_statistic(self.tracer,self.h,self.geo,inventory=True)
        np.testing.assert_allclose(x, [[1030.*1400.,1030.*2850.,1030.*600.]])

    def test_daily_vertical_velocity_and_fluxes(self):
        v = xr.DataArray([[[0.,2.,4.,8.],[np.nan,1.,5.,7.],[0.,8.,np.nan,np.nan]]],
                         dims=('time','nod2','nz1'),name='w')
        def transform(x, op):
            return ocean_steps.compute(x, {'ocean_physics_operation':op,'ocean_geometry_file':self.geom,
                                           'ocean_fresh_river_binary_sha256':REVIEWED_BINARY})
        got = transform(v,'vertical_velocity')
        np.testing.assert_allclose(got,[[[1.,3.,6.],[np.nan,3.,6.],[4.,np.nan,np.nan]]],equal_nan=True)
        np.testing.assert_equal(got.nz.values,self.geo.nz1.values)
        field=xr.DataArray([[1.,np.nan,np.nan]],dims=('time','nod2'),name='runoff')
        np.testing.assert_array_equal(transform(field,'river_salt'),[[0.,0.,0.]])
        np.testing.assert_allclose(transform(field,'water_flux'),[[-1025.,np.nan,np.nan]],equal_nan=True)
        np.testing.assert_allclose(transform(field,'heat_flux'),[[-1.,np.nan,np.nan]],equal_nan=True)

    def test_annual_budget_independent_source_qc(self):
        months=np.array([31,29,31,30,31,30,31,31,30,31,30,31])
        edges=np.r_[0,months.cumsum()].astype(float)
        bounds=np.column_stack([edges[:-1],edges[1:]])
        def native(name, data):
            path = self.root/f'{name}.fesom.2000.nc'
            array = xr.DataArray(data,dims=('time','nod2','nz'),attrs={
                'units':{'temp':'degC','hnode':'m'}[name], 'cell_methods':'time: mean'})
            ds=xr.Dataset({name:array, 'time_bounds':(('time','bnds'),bounds),
                           'lat':self.geo.lat,'lon':self.geo.lon},
                          coords={'time':('time',bounds.mean(axis=1),{'units':'days since 2000-01-01',
                          'calendar':'gregorian','bounds':'time_bounds'}),
                                  'nz':('nz',self.geo.nz1.values)})
            ds.to_netcdf(path)
            return path
        source=native('temp',self.tracer.values+np.arange(12)[:,None,None])
        h=native('hnode',np.broadcast_to(self.h.values,(12,3,3)))
        work=self.root/'run_20000101-20001231/work'
        work.mkdir(parents=True)
        (work/'namelist.config').write_text("which_ale = 'linfs'\n")
        rule={'ocean_physics_operation':'inventory','model_variable':'temp','ocean_geometry_file':str(self.geom),
              'compound_name':'ocean.opottempmint.tavg-u-hxy-sea.yr.glb'}
        paths={source,h,self.geom}
        ocean_validation.prepare(rule,{source},paths,{'experiment':self.root,'leg':'20000101-20001231'})
        self.assertIn(work/'namelist.config',paths)
        average=np.average(np.arange(12),weights=months)
        annual = 1030.*(np.array([1400.,2850.,600.])+average*np.array([500.,350.,50.]))
        def emit(a):
            f=self.root/'result.nc'
            ds=xr.Dataset({'opottempmint':(('time','cell'),a[None,:]),
                           'time_bounds':(('time','bnds'),[[0.,366.]]),
                           'lat':('cell',self.geo.lat.values),'lon':('cell',self.geo.lon.values)},
                          coords={'time':('time',[183.],{'units':'days since 2000-01-01','bounds':'time_bounds'})})
            ds.to_netcdf(f)
            return f
        self.assertGreater(ocean_validation.validate({'rules':[rule]},[emit(annual)])['compared_samples'],0)
        with self.assertRaisesRegex(ValueError,'sample mismatch'):
            ocean_validation.validate({'rules':[rule]},[emit(annual*1027/1030)])

    def test_freshwater_river_requires_matching_reviewed_build_and_profile(self):
        runoff = xr.DataArray([[1.]], dims=('time', 'nod2'), name='runoff')
        rule = {'ocean_physics_operation': 'river_salt'}
        for digest in (REVIEWED_BINARY, EQUIVALENT_BINARY):
            config = {'scientific_profile': 'awiesm3-v3.5.2-cc-' + digest[:8], '_verified_fesom_sha256': digest}
            ocean_validation.require_fresh_rivers(config)
            result = ocean_steps.compute(runoff, {**rule, 'ocean_fresh_river_binary_sha256': digest})
            np.testing.assert_array_equal(result, [[0.]])
            other_digest = EQUIVALENT_BINARY if digest == REVIEWED_BINARY else REVIEWED_BINARY
            for override in ({'scientific_profile': 'arbitrary'}, {'_verified_fesom_sha256': other_digest},
                             {'_verified_fesom_sha256': 'unknown'}):
                with self.assertRaisesRegex(ValueError, 'verified freshwater'):
                    ocean_validation.require_fresh_rivers({**config, **override})
        for digest in (None, 'unknown'):
            with self.assertRaisesRegex(ValueError, 'verified freshwater'):
                ocean_steps.compute(runoff, {**rule, 'ocean_fresh_river_binary_sha256': digest})

    def test_catalogue_registrations(self):
        found=[]
        for path in RULES.parents[1].glob('*ocean/cmip7xml*.yaml'):
            document=yaml.safe_load(path.read_text())
            found.extend(r for r in document['rules'] if r.get('ocean_physics_operation'))
        self.assertEqual(len({r['compound_name'] for r in found}),11)
        self.assertNotIn('ocean.friver.tavg-u-hxy-sea.3hr.glb', {r['compound_name'] for r in found})
        self.assertEqual({r['model_variable'] for r in found if r.get('lr_exact_ocean_depth')}, {'w', 'salt'})
        limits=sorted(r['ocean_depth_limit'] for r in found if 'ocean_depth_limit' in r)
        self.assertEqual(limits,[300,700,2000])
        self.assertEqual({r['model_variable'] for r in found if r.get('scientific_repairs')}, {'temp','salt'})
