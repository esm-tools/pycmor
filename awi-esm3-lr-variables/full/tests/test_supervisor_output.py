"""Missing checker evidence must fail, rather than look clean."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from supervisor_output import verify_qc


class QCRequiredTests(TestCase):
    def test_missing_sidecar_or_checker_is_rejected(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp);nc=root/'data.nc';nc.touch()
            with self.assertRaisesRegex(ValueError,'Missing QC sidecar'):
                verify_qc([nc],root,['cf','wcrp_cmip7','aicc'])
            side=root/'qc_test.json'
            side.write_text(json.dumps({str(nc):{'cf':{},'wcrp_cmip7':{}}}))
            with self.assertRaisesRegex(ValueError,'Missing QC suites'):
                verify_qc([nc],root,['cf','wcrp_cmip7','aicc'])
            side.write_text(json.dumps({str(nc):{'cf':{},'wcrp_cmip7':{},'aicc':{}}}))
            self.assertEqual(verify_qc([nc],root,['cf','wcrp_cmip7','aicc'])['status'],'REPORTS_PRESENT')


class RepairedDailyValuesTests(TestCase):
    def test_every_storage_change_including_january_first_is_checked(self):
        import netCDF4
        import numpy as np
        from supervisor_output import source_check
        with TemporaryDirectory() as tmp:
            root=Path(tmp)
            for product,source_name in (('dcw','src'),('dsn','sd')):
                current=root/f'{source_name}.nc';previous=root/f'{source_name}_prior.nc'
                target=root/f'{product}.nc'
                for path,name,values,times in (
                        (current,source_name,[[.001,.002],[.0015,.0025],[0.,.001]],[.5,1.5,2.5]),
                        (previous,source_name,[[0.,0.]],[-.5]),
                        (target,product,[[1.,2.],[.5,.5],[-1.5,-1.5]],[.5,1.5,2.5])):
                    with netCDF4.Dataset(path,'w') as ds:
                        ds.createDimension('time',len(times));ds.createDimension('cell',2)
                        t=ds.createVariable('time','f8',('time',));t.units='days since 2001-01-01';t.calendar='standard';t[:]=times
                        ds.createVariable(name,'f8',('time','cell'))[:]=values
                cfg=dict(inherit={},rules=[dict(inputs=[dict(path=str(root),pattern=current.name)],
                     land_boundary_records={source_name:dict(file=str(previous),index=0)})])
                task=dict(compound=f'land.{product}.tavg-u-hxy-lnd.day.glb')
                result=source_check(task,cfg,[target])
                self.assertEqual(result['compared_values'],6)
                with netCDF4.Dataset(target,'a') as ds:ds[product][0,:]=0.
                with self.assertRaisesRegex(ValueError,'raw-value mismatch'):
                    source_check(task,cfg,[target])

    def test_daily_ice_volume_uses_surface_areas_and_correct_hemisphere(self):
        import netCDF4
        import numpy as np
        from supervisor_output import source_check
        with TemporaryDirectory() as tmp:
            root=Path(tmp);mesh=root/'mesh.nc';diag=root/'diag.nc';source=root/'m_ice_day.nc';target=root/'out.nc'
            with netCDF4.Dataset(mesh,'w') as ds:
                ds.createDimension('nod2',2)
                ds.createVariable('lat','f8',('nod2',))[:]=[10.,-10.]
                ds.createVariable('cell_area','f8',('nod2',))[:]=[1e12,1e12]
            with netCDF4.Dataset(diag,'w') as ds:
                ds.createDimension('nz',1);ds.createDimension('nod2',2)
                ds.createVariable('nod_area','f8',('nz','nod2'))[:]=[[1e12,3e12]]
            with netCDF4.Dataset(source,'w') as ds:
                ds.createDimension('time',1);ds.createDimension('nod2',2)
                t=ds.createVariable('time','f8',('time',));t.units='days since 2001-01-01';t[:]=.5
                ds.createVariable('m_ice','f8',('time','nod2'))[:]=[[2.,4.]]
            cfg=dict(inherit={},rules=[dict(grid_file=str(mesh),surface_area_file=str(diag),
                                           inputs=[dict(path=str(root),pattern=source.name)])])
            for hemisphere,want in (('nh',2.),('sh',12.)):
                with netCDF4.Dataset(target,'w') as ds:
                    ds.createDimension('time',1);t=ds.createVariable('time','f8',('time',));t.units='days since 2001-01-01';t[:]=.5
                    v=ds.createVariable('sivol','f8',('time',));v.units='1e3 km3';v[:]=want
                task=dict(compound=f'seaIce.sivol.tavg-u-hm-u.day.{hemisphere}')
                self.assertEqual(source_check(task,cfg,[target])['compared_values'],1)
                with netCDF4.Dataset(target,'a') as ds:ds['sivol'][:]=want+1
                with self.assertRaisesRegex(ValueError,'raw-value mismatch'):
                    source_check(task,cfg,[target])

class FullDailyValuesTests(TestCase):
    def test_every_value_masks_and_axis_order_and_perturbation(self):
        import netCDF4
        import numpy as np
        from supervisor_output import verify_full_daily
        with TemporaryDirectory() as tmp:
            root=Path(tmp);sources=[]
            for name,offset in [('DetC',0.),('idetz2c',10.)]:
                p=root/(name+'.nc');sources.append(str(p))
                with netCDF4.Dataset(p,'w') as ds:
                    ds.createDimension('time',1);ds.createDimension('nod2',3);ds.createDimension('nz1',2)
                    t=ds.createVariable('time','f8',('time',));t.units='days since 2001-01-01';t.calendar='proleptic_gregorian';t[:]=.5
                    v=ds.createVariable(name,'f8',('time','nod2','nz1'),fill_value=-999.)
                    a=np.ma.array(np.arange(6.).reshape(1,3,2)+offset,mask=False);a.mask[0,2,1]=True;v[:]=a
            target=root/'output.nc'
            with netCDF4.Dataset(target,'w') as ds:
                ds.createDimension('time',1);ds.createDimension('depth',2);ds.createDimension('cell',3)
                t=ds.createVariable('time','f8',('time',));t.units='days since 2001-01-01';t.calendar='proleptic_gregorian';t[:]=.5
                z=ds.createVariable('depth','f8',('depth',));z.standard_name='depth';z[:]=[2.5,10.]
                ds.createVariable('lat','f8',('cell',))[:]=[1.,2.,3.]
                v=ds.createVariable('detoc','f4',('time','depth','cell'),fill_value=1e20)
                values=np.ma.array((np.arange(6.).reshape(1,3,2)*2+10)*.001,mask=False);values.mask[0,2,1]=True
                v[:]=values.transpose(0,2,1)
            compound='ocnBgchem.detoc.tavg-ol-hxy-sea.day.glb'
            cfg={'inherit':{},'rules':[{'lr_terms':[dict(variable=n,files=[p],coefficient=.001) for n,p in zip(('DetC','idetz2c'),sources)]}]}
            result=verify_full_daily({'compound':compound},cfg,[target])
            self.assertEqual(result['compared_values'],6)
            with netCDF4.Dataset(target,'a') as ds:ds['detoc'][0,1,1]=.5
            with self.assertRaisesRegex(ValueError,'raw-value mismatch'):
                verify_full_daily({'compound':compound},cfg,[target])

class UpstreamSurfaceMeanTests(TestCase):
    def test_diagnostic_surface_areas_and_all_daily_records_are_used(self):
        import netCDF4
        import numpy as np
        from supervisor_output import source_check
        with TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'sst.nc';diag=root/'diag.nc';mesh=root/'mesh.nc';target=root/'tos.nc'
            with netCDF4.Dataset(mesh,'w') as ds:
                ds.createDimension('nod2',2);ds.createVariable('cell_area','f8',('nod2',))[:]=[100,100]
            with netCDF4.Dataset(diag,'w') as ds:
                ds.createDimension('nz',2);ds.createDimension('nod2',2)
                ds.createVariable('nod_area','f8',('nz','nod2'))[:]=[[1,3],[100,100]]
            with netCDF4.Dataset(source,'w') as ds:
                ds.createDimension('time',24);ds.createDimension('nod2',2)
                t=ds.createVariable('time','f8',('time',));t.units='days since 2001-01-01';t.calendar='360_day';t[:]=[30*m+i+.5 for m in range(12) for i in (0,1)]
                v=ds.createVariable('sst','f8',('time','nod2'));v.units='degC';v[:]=[[0,10],[5,15]]*12
            with netCDF4.Dataset(target,'w') as ds:
                ds.createDimension('time',12);t=ds.createVariable('time','f8',('time',));t.units='days since 2001-01-01';t.calendar='360_day';t[:]=np.arange(12)*30+15
                v=ds.createVariable('tos','f4',('time',));v.units='degC';v[:]=10.
            cfg={'inherit':{},'rules':[dict(inputs=[dict(path=str(root),pattern=r'sst\.nc')],grid_file=str(mesh),surface_area_file=str(diag))]}
            result=source_check(dict(compound='ocean.tos.tavg-u-hm-sea.mon.glb',year=2001),cfg,[target])
            self.assertEqual(result['compared_values'],12)
            with netCDF4.Dataset(target,'a') as ds:ds['tos'][0]=12.5
            with self.assertRaisesRegex(ValueError,'area-weighted source mean'):
                source_check(dict(compound='ocean.tos.tavg-u-hm-sea.mon.glb',year=2001),cfg,[target])
