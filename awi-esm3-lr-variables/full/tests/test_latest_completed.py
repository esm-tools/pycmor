import os
from pathlib import Path
import tempfile
import unittest

import netCDF4

from select_latest_completed import discover_latest, check_archive


class LatestLegTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cmor-latest-test-')
        self.addCleanup(self.temp.cleanup)
        self.exp = Path(self.temp.name)
        (self.exp/'log').mkdir()
        self.states = {}

    def state(self, log):
        return self.states[str(log)]

    def leg(self, period, job, state='COMPLETED', observer=True):
        (self.exp/f'run_{period}').mkdir(exist_ok=True)
        log = self.exp/'log'/f'test_awiesm3_compute_{period}_{job}.log'
        log.write_text('test\n')
        os.utime(log, ns=(int(job)*1000000000, int(job)*1000000000))
        self.states[str(log)] = {'job': job, 'state': state,
                                'exit_code': '0:0' if state == 'COMPLETED' else '1:0'}
        if observer:
            date = f'{period[:4]}-{period[4:6]}-{period[6:8]}'
            with (self.exp/'log/test_awiesm3.log').open('a') as stream:
                stream.write(f' | observe_compute 1 {date}T00:00:00 {job} - done\n')
        return log

    def archive(self, period, months=12, merge_state='COMPLETED'):
        year = int(period[:4])
        log = self.exp/'log'/f'test_awiesm3_Combine_LPJG_{period}_200.log'
        log.write_text('Successfully converted LPJ-GUESS output to nc\n')
        self.states[str(log)] = {'job': '200', 'state': merge_state, 'exit_code': '0:0'}
        native = self.exp/'outdata/lpj_guess'/period
        native.mkdir(parents=True)
        (native/'cVeg_monthly.out').write_text('Lon Lat Year Jan Feb\n0 0 2000 1 1\n')
        for component, name in [('oifs', f'atm_remapped_1m_2t_1m_{year}-{year}.nc'),
                                ('fesom', f'DIC.fesom.{year}.nc')]:
            path = self.exp/'outdata'/component/name
            path.parent.mkdir(exist_ok=True)
            with netCDF4.Dataset(path, 'w') as ds:
                ds.createDimension('time', months)
                t = ds.createVariable('time', 'f8', ('time',))
                t.units = f'days since {year}-01-01'
                t.calendar = '360_day'
                t[:] = [15+30*m for m in range(months)]
                v = ds.createVariable('test', 'f4', ('time',))
                v[:] = 1

    def test_pending_newer_directory_does_not_win(self):
        self.leg('20000101-20001231', '1')
        self.leg('20010101-20051231', '2', 'RUNNING')
        selected = discover_latest(self.exp, self.state)
        self.assertEqual(selected['period'], '20000101-20001231')

    def test_different_leg_lengths(self):
        self.leg('20000101-20001231', '1')
        self.leg('20010101-20051231', '2')
        self.assertEqual(discover_latest(self.exp, self.state)['period'], '20010101-20051231')

    def test_latest_failed_attempt_not_older_success(self):
        self.leg('20000101-20001231', '1')
        self.leg('20010101-20051231', '2')
        self.leg('20010101-20051231', '3', 'FAILED')
        self.assertEqual(discover_latest(self.exp, self.state)['period'], '20000101-20001231')

    def test_scheduler_success_alone_not_sufficient(self):
        self.leg('20000101-20001231', '1', observer=False)
        with self.assertRaisesRegex(RuntimeError, 'No completed compute leg'):
            discover_latest(self.exp, self.state)

    def test_complete_archive(self):
        self.leg('20000101-20001231', '1')
        self.archive('20000101-20001231')
        selected = discover_latest(self.exp, self.state)
        result = check_archive(self.exp, selected, self.state)
        self.assertEqual(result['last_year'], 2000)
        self.assertEqual(result['native_lpj_files'], 1)
        self.assertEqual(len(result['files']), 3)

    def test_missing_month_blocks(self):
        self.leg('20000101-20001231', '1')
        self.archive('20000101-20001231', months=11)
        with self.assertRaisesRegex(RuntimeError, 'monthly records'):
            check_archive(self.exp, discover_latest(self.exp, self.state), self.state)

    def test_wait_for_latest_merge_without_falling_back(self):
        self.leg('19990101-19991231', '1')
        self.leg('20000101-20001231', '2')
        self.archive('20000101-20001231', merge_state='RUNNING')
        selected = discover_latest(self.exp, self.state)
        self.assertEqual(selected['period'], '20000101-20001231')
        with self.assertRaisesRegex(RuntimeError, 'do not substitute an older leg'):
            check_archive(self.exp, selected, self.state)


if __name__ == '__main__':
    unittest.main()

class CurrentArchiveTests(LatestLegTests):
    def test_current_openifs_names_and_daily_fesom_are_not_monthly_gaps(self):
        period='20000101-20001231'
        self.leg(period,'1');self.archive(period)
        base=self.exp/'outdata/oifs'
        (base/'atm_remapped_1m_2t_1m_2000-2000.nc').rename(base/'atm_remapped_1m_2t_2000-2000.nc')
        path=self.exp/'outdata/fesom/sst.fesom.2000.nc'
        with netCDF4.Dataset(path,'w') as ds:
            ds.createDimension('time',360);ds.createDimension('bnds',2)
            t=ds.createVariable('time','f8',('time',));t.units='days since 2000-01-01'
            t.calendar='360_day';t.bounds='time_bounds';t[:]=[i+.5 for i in range(360)]
            b=ds.createVariable('time_bounds','f8',('time','bnds'));b[:]=[[i,i+1] for i in range(360)]
            ds.createVariable('sst','f4',('time',))[:]=1
        result=check_archive(self.exp,discover_latest(self.exp,self.state),self.state)
        self.assertEqual(result['component_counts']['fesom']['monthly_series'],1)
