"""Steps and rules added to close the LR atmosphere and land gaps."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

import numpy as np
import xarray as xr


REPO=Path(__file__).resolve().parents[3]
SPEC=importlib.util.spec_from_file_location('gap_custom_steps',REPO/'examples/custom_steps.py')
CUSTOM=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CUSTOM)


class Rule(SimpleNamespace):
    def get(self,key,default=None):return getattr(self,key,default)


class RootDepthTests(unittest.TestCase):
    def test_deepest_rooted_layer_of_any_vegetated_tile(self):
        # cells: forest, desert type on the low tile only, bare land, ocean,
        # grass that appears in the second month, low tile with cover but no type
        months=lambda *v:xr.DataArray(np.array(v,dtype=float),dims=('time_counter','cell'))
        data=xr.Dataset(dict(
            tvl=months([2,8,0,0,0,0],[2,8,0,0,2,0]),cvl=months([.3,.5,0,0,0,.4],[.3,.5,0,0,.6,.4]),
            tvh=months([3,0,0,0,0,0],[3,0,0,0,0,0]),cvh=months([.6,0,0,0,0,0],[.6,0,0,0,0,0]),
            slt=months([2,1,1,0,3,2],[2,1,1,0,3,2])))
        out=CUSTOM.compute_rootd(data,Rule(model_variable='rootd')).rootd
        self.assertEqual(out.dims,('time_counter','cell'))
        np.testing.assert_allclose(out.isel(time_counter=0),[2.89,.07,.07,np.nan,2.89,.07])
        np.testing.assert_allclose(out.isel(time_counter=1),out.isel(time_counter=0))
        self.assertEqual(out.attrs['units'],'m')

    def test_type_change_within_a_month_is_not_mistaken_for_a_top_layer_type(self):
        data=xr.Dataset({k:('cell',np.array(v,dtype=float)) for k,v in dict(
            tvl=[8.72,8.0],cvl=[.5,.5],tvh=[0,0],cvh=[0,0]).items()})
        out=CUSTOM.compute_rootd(data,Rule(model_variable='rootd')).rootd
        np.testing.assert_allclose(out,[2.89,.07])


if __name__=='__main__':unittest.main()


class AtmosphereStepTests(unittest.TestCase):
    def test_pextra_increments_become_tendencies(self):
        data=xr.Dataset({f'pextra_{n}':('cell',[3600.*n,0.]) for n in (95,98,109)})
        rule=Rule(model_variable='tntscpbl',pextra_terms=[98,109],pextra_step_seconds=3600,scaled_units='K s-1')
        out=CUSTOM.oifs_pextra_tendency(data,rule)
        np.testing.assert_allclose(out,[98+109,0.])
        self.assertEqual((out.name,out.attrs['units']),('tntscpbl','K s-1'))
        with self.assertRaisesRegex(ValueError,'pextra_102'):
            CUSTOM.oifs_pextra_tendency(data,Rule(model_variable='x',pextra_terms=[102],pextra_step_seconds=3600))

    def test_sum_variables_refuses_missing_or_mixed_terms(self):
        data=xr.Dataset({'aod550':('cell',[.1,.2],{'units':'1'}),'macv2sp_taod550':('cell',[.01,0.],{'units':'1'})})
        out=CUSTOM.sum_variables(data,Rule(model_variable='od550aer',summed_variables=['aod550','macv2sp_taod550']))
        np.testing.assert_allclose(out,[.11,.2])
        with self.assertRaisesRegex(ValueError,'not in input'):
            CUSTOM.sum_variables(data,Rule(model_variable='x',summed_variables=['aod550','aod550bc']))
        data['macv2sp_taod550'].attrs['units']='m'
        with self.assertRaisesRegex(ValueError,'different units'):
            CUSTOM.sum_variables(data,Rule(model_variable='x',summed_variables=['aod550','macv2sp_taod550']))

    def test_lowest_model_level_is_the_last_xios_level(self):
        data=xr.DataArray(np.arange(6.).reshape(3,2),dims=('model_levels','cell'))
        np.testing.assert_allclose(CUSTOM.select_lowest_model_level(data,Rule()),[4.,5.])
        self.assertEqual(CUSTOM.select_lowest_model_level(data.isel(model_levels=[2]),Rule()).dims,('cell',))

    def test_rule_comment_is_appended_to_the_request_comment(self):
        data=xr.Dataset({'cls':('cell',[1.],{'comment':'Request text.'})})
        CUSTOM.apply_variable_attributes(data,Rule(cmor_variable='cls',model_variable='cl',
                                                   variable_attributes={'comment':'Same as cl.'}))
        self.assertEqual(data.cls.attrs['comment'],'Request text. Same as cl.')

    def test_twelve_monthly_values_become_a_climatology_of_the_window(self):
        import cftime
        times=[cftime.DatetimeProlepticGregorian(2849,m,16) for m in range(1,13)]
        data=xr.DataArray(np.full(12,8e-7),dims='time',coords={'time':times})
        rule=Rule(climatology_window=[2845,2849])
        out=CUSTOM.as_monthly_climatology(data,rule)
        self.assertEqual(rule.climatology_years,(2845,2849))
        nextyear=xr.DataArray([1.,2.],dims='time',coords={'time':[times[-1],cftime.DatetimeProlepticGregorian(2850,1,1)]})
        self.assertEqual(CUSTOM.select_climatology_window(nextyear,rule).sizes['time'],1)
        self.assertEqual(str(out.time.values[0])[:10],'2845-01-01')
        with self.assertRaisesRegex(ValueError,'twelve months'):
            CUSTOM.as_monthly_climatology(data.isel(time=slice(0,11)),Rule(climatology_window=[2845,2849]))


VTABLE='''  &NAMVV1
   DVALH(0:)=0.,
     5000.,
     2000.,
     0.,
   DVBH(0:)=    0.0000000000,
     0.0,
     0.5,
     1.0,
 /
'''


class HalfLevelTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.vtable=Path(self.directory.name)/'vtable_L3';self.vtable.write_text(VTABLE)

    def test_half_level_pressure_and_its_coordinate(self):
        from unittest.mock import patch
        ps=xr.DataArray([[100000.,80000.]],dims=('time','cell'),coords={'time':[np.datetime64('2845-01-01')]},name='sp')
        rule=Rule(model_variable='sp',oifs_vtable=str(self.vtable))
        phalf=CUSTOM.oifs_half_level_pressure(ps,rule)
        self.assertEqual(phalf.dims,('time','model_levels','cell'))
        np.testing.assert_allclose(phalf.isel(time=0),[[0,0],[5000,5000],[52000,42000],[100000,80000]])
        with patch.object(CUSTOM,'_load_secondary_mf',return_value=ps):
            out=CUSTOM.add_hybrid_sigma_coordinate(phalf.to_dataset(),rule)
        self.assertNotIn('lev_bnds',out)
        self.assertNotIn('bounds',out.lev.attrs)
        # stored surface first: the level with b=1 comes first
        np.testing.assert_allclose(out.b,[1.,.5,0.,0.])
        np.testing.assert_allclose(out.ap+out.b*out.ps.isel(time=0,cell=1),out.sp.isel(time=0,cell=1))


class GhgClimatologyTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        import cftime
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        base=Path(self.directory.name)
        lat=np.array([-60.,0.,60.]);pressure=np.array([10000.,50000.,90000.])
        # vmr rises with pressure and is twice as large in the north, constant in time
        profile=np.array([1.,2.,4.])[:,None]*np.array([1.,1.5,2.])[None,:]*1e-6
        clim=xr.Dataset({'ch4_vmr':(('month','level','latitude'),np.repeat(profile[None],12,0)),
                         'pressure':('level',pressure)},coords={'month':np.arange(1,13),'latitude':lat})
        self.clim=base/'clim.nc';clim.to_netcdf(self.clim)
        years=[cftime.DatetimeProlepticGregorian(y,7,2,12) for y in range(1849,1852)]
        gm=xr.Dataset({'ch4':('time',[790.,800.,810.])},coords={'time':years})
        gm.time.encoding.update(units='days since 1850-01-01',calendar='proleptic_gregorian')
        self.gm=base/'gm.nc';gm.to_netcdf(self.gm)
        self.vtable=base/'vtable_L3';self.vtable.write_text(VTABLE)
        weight=np.cos(np.deg2rad(lat))
        self.surface_mean=float((profile[-1]*weight).sum()/weight.sum())
        self.scale=800e-9/self.surface_mean

    def rule(self,**fields):
        return Rule(model_variable='sp',ghg_gas='ch4',ghg_climatology_file=str(self.clim),
                    ghg_forcing_file=str(self.gm),forcing_year=1850,year=2845,**fields)

    def months(self,values,dims,coords):
        import cftime
        times=[cftime.DatetimeGregorian(2845,m,1,6) for m in range(1,13) for _ in range(2)]
        return xr.DataArray(np.broadcast_to(values,(24,)+np.shape(values)).copy(),dims=('time',)+dims,
                            coords=dict(time=times,**coords))

    def test_time_weights_are_the_model_interpolation(self):
        w=CUSTOM._ghgclim_time_weights(2845)
        np.testing.assert_allclose(w.sum(1),1.)
        self.assertGreater(w[6,6],.7)
        self.assertGreater(w[0,11],.1)

    def test_pressure_levels_take_the_containing_layer_scaled_to_the_global_mean(self):
        ps=self.months(np.full((2,1),1e5),('lat','lon'),dict(lat=[0.,60.],lon=[0.]))
        out=CUSTOM.oifs_ghg_climatology_field(ps,self.rule(ghg_vertical='pressure_levels',plevels=[100000.,50000.,1000.]))
        self.assertEqual(out.dims,('time','pressure_levels','lat','lon'))
        self.assertEqual(out.sizes['time'],12)
        np.testing.assert_allclose(out.isel(time=0,lon=0),
                                   np.array([[6.,8.],[3.,4.],[1.5,2.]])*1e-6*self.scale,rtol=1e-6)

    def test_model_levels_conserve_the_column_mass(self):
        ps=self.months(np.array([1e5,1e5]),('cell',),dict(lat=('cell',[0.,60.])))
        out=CUSTOM.oifs_ghg_climatology_field(ps,self.rule(ghg_vertical='model_levels',oifs_vtable=str(self.vtable)))
        self.assertEqual(out.dims,('time','model_levels','cell'))
        a,b=np.array([0.,5000.,2000.,0.]),np.array([0.,0.,.5,1.])
        dp=np.diff(a+b*1e5)
        column=(out.isel(time=3).values*dp[:,None]).sum(0)
        # climatology layers 0-30000, 30000-70000, 70000-110000 Pa, the bottom one
        # only down to the surface at 100000 Pa
        expected=np.array([1.5*30000+3*40000+6*30000,2*30000+4*40000+8*30000])*1e-6*self.scale
        np.testing.assert_allclose(column,expected,rtol=1e-5)


FORT4='''&NAMGFL
    YQ_NL%LGP = .true.
    YO3_NL%LGP = .true.
/

&NAMRIP
    TSTEP = 3600
/

&NAMCLDP
    RVICE = 0.16
{cldp}/
'''


class PlannerTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.base=Path(self.directory.name)

    def test_namelist_decides_ozone_and_the_pextra_budget(self):
        import oifs_namelist
        (self.base/'fort.4').write_text(FORT4.format(cldp=''))
        run=oifs_namelist.identity(self.base)
        self.assertEqual(run,dict(ozone='prognostic',pextra_budget='run_accumulated',oifs_tstep=3600.))
        rule={'pextra_terms':[93],'pextra_step_seconds':3600}
        with self.assertRaisesRegex(ValueError,'UNRESOLVED_PEXTRA_ACCUMULATION'):
            oifs_namelist.check_pextra(rule,run)
        (self.base/'fort.4').write_text(FORT4.format(cldp='    LCLDBUD_TIMEINT = .false.\n'))
        run=oifs_namelist.identity(self.base)
        oifs_namelist.check_pextra(rule,run)
        with self.assertRaisesRegex(ValueError,'UNRESOLVED_PEXTRA_STEP'):
            oifs_namelist.check_pextra(dict(rule,pextra_step_seconds=900),run)
        oifs_namelist.check_pextra({},{})
        self.assertEqual(oifs_namelist.identity(self.base/'absent')['ozone'],'unknown')

    def test_prognostic_ozone_climatology_is_not_applicable(self):
        from data_request_coverage import conditional_exclusion
        compound='atmosChem.o3.tclm-p19-hxy-air.mon.glb'
        metadata={compound:{'processing_note':'If the O3 varies from one year to the next,  then report instead '
                                               'the field described in the previous table entry.'}}
        self.assertIn('NOT_APPLICABLE',conditional_exclusion(compound,metadata,{'ozone':'prognostic'},{}))
        self.assertIsNone(conditional_exclusion(compound,metadata,{'ozone':'unknown'},{}))

    def test_static_forcing_climatology_pins_the_file_without_yearly_streams(self):
        from climatology_support import configure
        (self.base/'ch4_gm_1750-2022.nc').write_bytes(b'x')
        doc={'inherit':{},'rules':[{'climatology_static_inputs':True,
             'inputs':[{'path':str(self.base),'pattern':r'ch4_gm_1750-2022\.nc'}]}]}
        self.assertEqual([p.name for p in configure(doc,2845,2849)],['ch4_gm_1750-2022.nc'])
        self.assertEqual(doc['rules'][0]['climatology_window'],[2845,2849])

    def test_point_stream_climatology_accepts_the_next_january_sample(self):
        from climatology_support import configure
        for year in (2845,2846):
            write_stream(self.base/f'atmos_6h_pt_sp_{year}-{year}.nc','sp',
                         np.arange(6.,8761.,6.)+24*365*(year-2845))
        rule={'inputs':[{'path':str(self.base),'pattern':r'atmos_6h_pt_sp_2846-2846\.nc'}]}
        with self.assertRaisesRegex(ValueError,'INCOMPLETE_INPUT'):
            configure({'inherit':{},'rules':[dict(rule,inputs=[dict(rule['inputs'][0])])]},2845,2846)
        doc={'inherit':{},'rules':[dict(rule,climatology_point_stream=True)]}
        self.assertEqual(len(configure(doc,2845,2846)),2)


def write_stream(path,variable,hours,cells=2,extra_dims=()):
    import netCDF4
    with netCDF4.Dataset(path,'w') as ds:
        ds.createDimension('time_counter',len(hours));ds.createDimension('cell',cells)
        for name,size in extra_dims:ds.createDimension(name,size)
        t=ds.createVariable('time_counter','f8',('time_counter',))
        t.units='hours since 2845-01-01 00:00:00';t.calendar='standard';t.standard_name='time';t[:]=hours
        for name,value in (('lat',0.),('lon',10.)):
            c=ds.createVariable(name,'f4',('cell',));c.standard_name={'lat':'latitude','lon':'longitude'}[name]
            c[:]=np.full(cells,value)
        dims=('time_counter',)+tuple(n for n,_ in extra_dims)+('cell',)
        v=ds.createVariable(variable,'f4',dims);v.units='1';v[:]=np.ones([len(hours)]+[s for _,s in extra_dims]+[cells])


class CatalogueRuleTests(unittest.TestCase):
    """The new rules materialise from synthetic files and report missing streams."""

    PRODUCTS=['atmos.ua.tpt-h100m-hxy-u.1hr.glb','atmos.wsg.tmax-h10m-hxy-u.1hr.glb',
              'atmos.bldep.tpt-u-hxy-u.3hr.glb','atmos.tnta.tavg-al-hxy-u.mon.glb',
              'aerosol.od550aer.tavg-u-hxy-u.mon.glb','atmosChem.o3.tavg-al-hxy-u.mon.glb']

    def plan(self,identity):
        import full_cmorization as f
        from unittest.mock import patch
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            base=Path(directory);period='28450101-28451231'
            for component in ('oifs','fesom',f'lpj_guess/{period}'):
                (base/'outdata'/component).mkdir(parents=True)
            oifs=base/'outdata/oifs'
            write_stream(oifs/'atmos_1h_pt_100u_2845-2845.nc','100u',np.arange(8760.))
            write_stream(oifs/'atmos_mon_ml_pextra_93_2845-2845.nc','pextra_93',
                         [24.*(sum([31,28,31,30,31,30,31,31,30,31,30,31][:m])+14) for m in range(12)],
                         extra_dims=(('model_levels',3),))
            write_stream(oifs/'atmos_6h_pt_sp_2845-2845.nc','sp',np.arange(6.,8761.,6.))
            write_stream(oifs/'atmos_mon_aod_aod550_2845-2845.nc','aod550',
                         [24.*(sum([31,28,31,30,31,30,31,31,30,31,30,31][:m])+14) for m in range(12)])
            campaign=base/'campaign';(campaign/'grids').mkdir(parents=True);(campaign/'reproduce').mkdir();(campaign/'recipes').mkdir()
            (campaign/'grids/full_TCO95_grid.nc').touch()
            vtable=base/'vtable_L3';vtable.write_text(VTABLE)
            metadata=base/'metadata.json';f.write(metadata,{'Compound Name':{c:{} for c in self.PRODUCTS}})
            config=dict(repo=str(REPO),metadata=str(metadata),mesh_dir=str(base),ghg_dir=str(base),
                        cv_dir=str(base),model_level_table=str(vtable))
            load_module=f.module
            def module(path,name):
                result=load_module(path,name)
                if name=='full_raw_rules':
                    result.grids=lambda:{}
                return result
            with patch.object(f,'module',side_effect=module):
                tasks,coverage,_=f.build_catalog(config,base,period,identity,campaign)
        return {r['compound']:r for r in coverage if r['compound'] in self.PRODUCTS}

    def test_rules_are_ready_or_name_the_missing_stream(self):
        rows=self.plan({})
        self.assertEqual(rows['atmos.ua.tpt-h100m-hxy-u.1hr.glb']['status'],'READY')
        for compound,stream in (('atmos.wsg.tmax-h10m-hxy-u.1hr.glb','atmos_1h_gust_wsg10_2845-2845'),
                                ('atmos.bldep.tpt-u-hxy-u.3hr.glb','atmos_3h_pt_blh_2845-2845'),
                                ('aerosol.od550aer.tavg-u-hxy-u.mon.glb','atmos_mon_aod_macv2sp_taod550_2845-2845'),
                                ('atmosChem.o3.tavg-al-hxy-u.mon.glb','atmos_mon_ml_o3_2845-2845')):
            with self.subTest(compound=compound):
                self.assertEqual(rows[compound]['status'],'BLOCKED')
                self.assertIn('MISSING_INPUT',rows[compound]['reason'])
                self.assertIn(stream,rows[compound]['reason'].replace('\\',''))
        self.assertIn('UNRESOLVED_PEXTRA_ACCUMULATION',rows['atmos.tnta.tavg-al-hxy-u.mon.glb']['reason'])

    def test_tendency_is_ready_once_the_budget_is_per_step(self):
        rows=self.plan(dict(pextra_budget='per_step',oifs_tstep=3600.))
        self.assertEqual(rows['atmos.tnta.tavg-al-hxy-u.mon.glb']['status'],'READY',
                         rows['atmos.tnta.tavg-al-hxy-u.mon.glb'].get('reason'))


class HalfLevelSaveTests(unittest.TestCase):
    def test_save_adds_no_midpoint_bounds_to_a_half_level_hybrid_axis(self):
        from pycmor.std_lib.files import _ensure_vertical_bounds
        attrs={'standard_name':'atmosphere_hybrid_sigma_pressure_coordinate','units':'1'}
        ds=xr.Dataset({'phalf':(('lev',),[1.,2.,3.])},coords={'lev':('lev',[1.,.5,0.],attrs)})
        self.assertNotIn('lev_bnds',_ensure_vertical_bounds(ds))
        ds=xr.Dataset({'x':(('lev',),[1.,2.,3.])},coords={'lev':('lev',[1.,2.,3.])})
        self.assertIn('lev_bnds',_ensure_vertical_bounds(ds))
