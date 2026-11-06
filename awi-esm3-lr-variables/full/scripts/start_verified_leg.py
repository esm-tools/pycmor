#!/usr/bin/env python3
"""Audit and freeze one exact completed leg, then gate its full conversion.

Run in Slurm, after the selected leg's LPJ-GUESS merge. Every READY product
is converted for one representative year before remaining product-years are
released. A source-audit failure or failed representative blocks submission.
"""
import argparse
import fcntl
import os
from pathlib import Path
import subprocess

import full_cmorization as f
import retest_all_products as r


def start(experiment, leg, config_path):
    if not os.environ.get('SLURM_JOB_ID'):
        raise ValueError('Start the verified leg inside a Slurm allocation')
    experiment = experiment.resolve(strict=True)
    config = f.load_config(config_path)
    f.select_leg(experiment, leg)
    launch = Path(config['results'])/'launch'/leg
    launch.mkdir(parents=True, exist_ok=True)
    with (launch/'.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        marker = launch/'started.json'
        if marker.exists():
            raise ValueError(f'Leg already started; inspect {marker} before resuming')
        audit = launch/'source_audit.json'
        config['source_audit_report'] = str(audit)
        used = launch/'config.json'
        f.write(used, config)
        environment = dict(os.environ, OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1',
                           MKL_NUM_THREADS='1', NUMBA_NUM_THREADS='1', NUMEXPR_NUM_THREADS='1')
        native = config.get('native_io', {})
        if native:
            environment['HDF5_PLUGIN_PATH'] = native['plugin_path']
        os.environ.update(environment)
        subprocess.run([config['python'], str(Path(__file__).with_name('supervisor_audit.py')),
                        '--experiment', str(experiment), '--leg', leg, '--config', str(used),
                        '--output', str(audit), '--science'], env=environment, check=True)
        campaign = f.plan(argparse.Namespace(experiment=experiment, leg=leg, config=used), config)
        r.prepare(campaign)
        r.submit(campaign, full_after_pass=True)
        f.write(marker, dict(campaign=str(campaign), started=f.stamp(),
                            planner_job=os.environ['SLURM_JOB_ID'],
                            status='ALL_READY_REPRESENTATIVES_SUBMITTED',
                            publication_ready=False,
                            validation_submission=r.f.read(campaign/'representative_test/submission.json')))
        print(f'Full conversion queued behind all-product validation: {campaign}', flush=True)
        return campaign


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('experiment', type=Path)
    parser.add_argument('--leg', required=True, help='Exact YYYYMMDD-YYYYMMDD; never latest')
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    if args.leg == 'latest':
        parser.error('An exact leg must be frozen before submission')
    start(args.experiment, args.leg, args.config)


if __name__ == '__main__':
    main()
