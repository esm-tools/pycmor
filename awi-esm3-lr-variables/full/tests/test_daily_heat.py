"""Daily heat transport must preserve the formula with a bounded graph."""
from pathlib import Path
import copy
import tempfile
from unittest import TestCase
from unittest.mock import patch

import dask.array as da
import netCDF4
import numpy as np
import xarray as xr

import full_cmorization as f


class DailyHeatTests(TestCase):
    def test_planner_pins_daily_files_and_changes_only_the_selected_pipeline(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for name,n,horizontal,size in (('utemp_day',365,'elem',2),('hnode',12,'nod2',4)):
                with netCDF4.Dataset(root/f'{name}.fesom.2001.nc','w') as ds:
                    ds.createDimension('time',n);ds.createDimension(horizontal,size);ds.createDimension('nz',3)
                    t=ds.createVariable('time','f8',('time',));t.units='days since 2001-01-01'
                    t[:]=np.arange(n)+.5 if n==365 else np.arange(12)*30+15
                    v=ds.createVariable('utemp' if name=='utemp_day' else 'hnode','f4',('time',horizontal,'nz'));v[:]=1.
            rule=dict(compound_name='ocean.hfx.tavg-u-hxy-sea.day.glb',model_variable='utemp',
                      vertical_sum=True,hnode_path=str(root),hnode_pattern=r'hnode\.fesom\.\d{4}\.nc',
                      inputs=[dict(path=str(root),pattern=r'utemp_day\.fesom\.\d{4}\.nc')],pipelines=['heat'])
            steps=['script:///unused/custom_steps.py:compute_heat_transport']
            doc=dict(inherit={},rules=[rule],pipelines=[dict(name='heat',steps=steps)])
            monthly=copy.deepcopy(doc);monthly['rules'][0]['compound_name']='ocean.hfx.tavg-u-hxy-sea.mon.glb'
            rep=f.module(f.REPO/'examples/repoint_hr_year.py','test_heat_repointer')
            paths=f.dependencies(doc,2001,rep)
            self.assertEqual(set(paths),{root/'utemp_day.fesom.2001.nc',root/'hnode.fesom.2001.nc'})
            self.assertEqual(rule['heat_source_file'],str(root/'utemp_day.fesom.2001.nc'))
            self.assertEqual(rule['heat_hnode_file'],str(root/'hnode.fesom.2001.nc'))
            self.assertTrue(doc['pipelines'][0]['steps'][0].endswith(':compute_daily_heat_transport'))
            self.assertEqual(steps,['script:///unused/custom_steps.py:compute_heat_transport'])
            f.dependencies(monthly,2001,rep)
            self.assertTrue(monthly['pipelines'][0]['steps'][0].endswith(':compute_heat_transport'))

    def test_lazy_daily_slabs_monthly_thickness_masks_and_spatial_blocks(self):
        helper=f.module(f.RULES/'heat_steps.py','test_daily_heat_values')
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'utemp_day.nc';hnode=root/'hnode.nc'
            times=[.5,30.5,31.5,32.5];units='days since 2001-01-01'
            with netCDF4.Dataset(source,'w') as ds:
                for name,size in [('time',4),('elem',2),('nz',3)]:ds.createDimension(name,size)
                t=ds.createVariable('time','f8',('time',));t.units=units;t[:]=times
                v=ds.createVariable('utemp','f4',('time','elem','nz'),fill_value=-999.)
                values=np.ma.array(np.tile([1.,2.,3.],(4,2,1)),mask=False)
                values.mask[2,0,2]=True;v[:]=values
            with netCDF4.Dataset(hnode,'w') as ds:
                for name,size in [('time',2),('nod2',4),('nz',3)]:ds.createDimension(name,size)
                t=ds.createVariable('time','f8',('time',));t.units=units;t[:]=[14.5,45.5]
                ds.createVariable('hnode','f4',('time','nod2','nz'))[:]=np.array([np.ones((4,3)),2*np.ones((4,3))])
            dates=netCDF4.num2date(times,units)
            data=xr.DataArray(da.zeros((4,2,3),chunks=(1,1,3)),dims=('time','elem','nz'),
                coords={'time':dates,'nz':[1,2,3],'lon':('elem',[10.,20.])},name='utemp')
            rule=dict(heat_source_file=str(source),heat_hnode_file=str(hnode),grid_file='fixture',
                      model_variable='utemp',vertical_sum=True,scale_factor=2.,heat_spatial_block=1)
            geometry=(np.array([10.,20.]),np.array([[0,1],[1,2],[2,3]]))
            with patch.object(helper,'_geometry',return_value=geometry), \
                 patch.object(da.Array,'compute',side_effect=AssertionError('No eager annual computation')):
                result=helper.compute_daily_heat_transport(data,rule)
            self.assertEqual(result.dims,('time','elem'))
            self.assertEqual(result.chunks[0],(1,1,1,1))
            self.assertLess(len(result.data.__dask_graph__()),100)
            self.assertNotIn('nz',result.coords)
            np.testing.assert_array_equal(result.lon,data.lon)
            with patch.object(helper,'_geometry',return_value=geometry):
                np.testing.assert_allclose(result.compute(scheduler='synchronous'),
                    [[120.,240.],[120.,240.],[120.,480.],[240.,480.]])
            with self.assertRaisesRegex(ValueError,'shape differs'):
                helper.compute_daily_heat_transport(data.isel(time=slice(1,None)),rule)

    def test_geometry_preserves_existing_triangle_area_proxy(self):
        helper=f.module(f.RULES/'heat_steps.py','test_daily_heat_geometry')
        custom=f.module(f.REPO/'examples/custom_steps.py','test_existing_heat_geometry')
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'mesh.nc'
            xr.Dataset({'lon':('nod2',[0.,1.,0.,1.]),'lat':('nod2',[0.,0.,1.,1.]),
                        'triag_nodes':(('three','elem'),[[1,2],[2,4],[3,3]])}).to_netcdf(path)
            edge,tri=helper._geometry(str(path));old_edge,old_tri=custom._elem_geometry(str(path))
            np.testing.assert_allclose(edge,old_edge,rtol=1e-14)
            np.testing.assert_array_equal(tri,old_tri)
