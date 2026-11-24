import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

import netCDF4
import numpy as np
import pandas as pd
import xarray as xr

RULES = Path(__file__).resolve().parents[1]/'rules'
spec = importlib.util.spec_from_file_location('land_requested_module',RULES/'requested_land.py')
land = importlib.util.module_from_spec(spec)
spec.loader.exec_module(land)


class Rule(SimpleNamespace):
    def get(self, key, default=None):
        return getattr(self,key,default)


class TestRequestedLand(unittest.TestCase):
    def test_model_lakes_at_land_and_water_points(self):
        cov,domain=land.lake_geometry(np.array([.49,.49,.50,.51,1.]),np.array([.2,.6,.5,.2,.0]))
        np.testing.assert_equal(cov,[0,1,0,.2,0])
        np.testing.assert_equal(domain,[False,True,False,True,True])

    def test_lake_fraction_preserves_exact_endpoints_in_written_file(self):
        from pycmor.std_lib.files import _encoding_from_dask_chunks
        options = next(r for r in land.rules() if r['model_variable']=='sftlaf')
        rule = Rule(**options, _pycmor_cfg=lambda key: 1)
        percentages = np.array([0,100,100,0,.125], dtype='float32')
        ds = xr.Dataset({'sftlaf': ('cell', percentages)})
        with TemporaryDirectory() as tmp:
            path = Path(tmp)/'sftlaf.nc'
            ds.to_netcdf(path, encoding=_encoding_from_dask_chunks(ds, rule), engine='netcdf4')
            with netCDF4.Dataset(path) as written:
                np.testing.assert_array_equal(written['sftlaf'][:], percentages)

    def test_native_flux_identity_and_missing_land(self):
        with TemporaryDirectory() as tmp:
            p=Path(tmp)
            grid=p/'grid.nc'
            xr.Dataset({'lat':('ncells',[-75.,30.,45.,0.]),'lon':('ncells',[10.,20.,30.,40.])}).to_netcdf(grid)
            cells=p/'gridlist.txt'
            cells.write_text('10 -75 1 1\n20 30 2 2\n30 45 0 3\n')
            files=[]
            for name,value in [('evspsbl',.003),('tran',.001),('ec',.0005)]:
                path=p/f'{name}.out'
                pd.DataFrame(dict(Lon=20.,Lat=30.,Year=2000,Day=np.arange(1,367),Total=value)).to_csv(path,sep=' ',index=False)
                files.append(str(path))
            base=dict(land_gridlist_file=str(cells),grid_file=str(grid),lr_land_year=2000)
            item=next(r for r in land.rules() if r['model_variable']=='evspsblsoi')
            ds=land.load(None,Rule(**base,**item,lr_source_files=files))
            np.testing.assert_allclose(ds.evspsblsoi[0],[np.nan,.0015,np.nan,0],equal_nan=True)
            self.assertEqual(ds.time.size,366)
            self.assertEqual((ds.time_bnds[59,0].item().month,ds.time_bnds[59,0].item().day),(2,29))
            item=next(r for r in land.rules() if r['model_variable']=='evspsblveg')
            ds=land.load(None,Rule(**base,**item,lr_source_files=[files[-1]]))
            np.testing.assert_allclose(ds.evspsblveg[-1],[0,.0005,0,0])
            bad=pd.read_csv(files[0],sep=' ').iloc[:-1]
            bad.to_csv(files[0],sep=' ',index=False)
            with self.assertRaisesRegex(ValueError,'Missing native LPJG cells/time'):
                land.load(None,Rule(**base,**item,lr_source_files=[files[0]]))

    def test_moss_cover_is_retained_and_unclosed_total_detected(self):
        with TemporaryDirectory() as tmp:
            p=Path(tmp)
            keys=dict(Lon=[0.,0.],Lat=[50.,50.],Year=[2000,2001])
            def table(name,**kwargs):
                path=p/name
                pd.DataFrame(dict(**keys,**kwargs)).to_csv(path,sep=' ',index=False)
                return str(path)
            total=table('vegFrac',Total=[70.,85.])
            groups=tuple(table(g,Total=[v,v]) for g,v in zip(land.GROUP_COVER,[20.,5.,25.,5.,5.]))
            moss=table('fpc',pmoss=[.2,.2],PEAT_STANDFRAC=[.5,.5])
            d=land._annual_closure(total,groups,moss)
            self.assertAlmostEqual(d[2000],0)
            self.assertAlmostEqual(d[2001],15)

    def test_incomplete_hourly_source_rejected(self):
        with TemporaryDirectory() as tmp:
            path=Path(tmp)/'sample.nc'
            with netCDF4.Dataset(path,'w') as ds:
                for dim,n in [('time_counter',2),('cell',1),('bnds',2)]:
                    ds.createDimension(dim,n)
                t=ds.createVariable('time_counter','f8','time_counter')
                t.units='hours since 2001-01-01'
                t[:]=[.5,1.5];t.bounds='time_bnds'
                ds.createVariable('time_bnds','f8',('time_counter','bnds'))[:]=[[0.,1.],[1.,2.]]
                for key in ('lat','lon'):
                    ds.createVariable(key,'f8','cell')[:]=[0.]
                v=ds.createVariable('rsds','f4',('time_counter','cell'));v.units='W m-2';v.online_operation='average'
            with netCDF4.Dataset(path) as ds,self.assertRaisesRegex(ValueError,'Incomplete or uncentred'):
                land._check_oifs_stream(ds,'rsds',2001,np.array([0.]),np.array([0.]))

    def test_staged_source_missing_and_ice_condition(self):
        with TemporaryDirectory() as tmp:
            p=Path(tmp);grid=p/'grid.nc';grid.write_text('grid')
            raw=Rule(EXP=p,PERIOD='20000101-20041231',YEARS=[2000])
            rule=next(r for r in land.rules() if r['model_variable']=='lithk')
            log=p/'log/oifs'/f'NODE.001_01_{raw.PERIOD}'; log.parent.mkdir(parents=True);log.write_text(' ECE_LANDICE = F,\n')
            with self.assertRaisesRegex(ValueError,'UNREVIEWED_ICE_SHEET'):
                land.materialize(rule,raw,{},grid,{})
            _, frozen=land.materialize(rule,raw,{},grid,{'land_ice_representation':'orography_and_mask'})
            self.assertEqual(frozen['lr_group'],'requested_land')
            self.assertIn(str(log),frozen['lr_auxiliary_files'])
            rule=next(r for r in land.rules() if r['model_variable']=='evspsblveg')
            with self.assertRaisesRegex(ValueError,'MISSING_INPUT'):
                land.materialize(rule,raw,{},grid,{})


if __name__=='__main__':
    unittest.main()
