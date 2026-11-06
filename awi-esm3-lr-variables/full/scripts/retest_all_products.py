#!/usr/bin/env python3
"""Bounded representative-year conversion tests; not a publication campaign.

Prepare from a NEW full-catalog plan. Test every distinct READY compound once,
retain blocked requests, and report execution, independent checks and formal
QC separately. Never change model data or the production conversion rules.
"""
import argparse
from collections import Counter
import csv
import fcntl
import json
import os
from pathlib import Path
import shutil
import shlex

import netCDF4
import numpy as np
from cf_units import Unit

import full_cmorization as f


# Exercise repaired boundary and daily-source routes early, while retaining
# every other READY representative in the same mandatory validation array.
REGRESSION_FIRST={
    f'land.{name}.tavg-u-hxy-lnd.day.glb' for name in ('dcw','dsn','dslw','dsw')
} | {f'ocean.{name}.tavg-u-hxy-sea.day.glb' for name in ('hfx','hfy')} | {
    f'seaIce.sivol.tavg-u-hm-u.day.{hemisphere}' for hemisphere in ('nh','sh')}


def representatives(tasks):
    selected = {}
    for task in sorted(tasks, key=lambda t: (t['year'], t['compound'])):
        selected.setdefault(task['compound'], task)
    return [selected[c] for c in sorted(selected,key=lambda c:(c not in REGRESSION_FIRST,c))]


def prepare(campaign):
    f.verify_campaign(campaign)
    dest = campaign / 'representative_test'
    if dest.exists():
        raise ValueError('Test directory exists; do not replace a frozen test manifest')
    tasks = representatives(f.read(campaign / 'tasks.json'))
    if not tasks:
        raise ValueError('No READY products to test')
    config = f.read(campaign / 'plan.json')['signature']['config']
    dest.mkdir()
    (dest / 'status').mkdir()
    (dest / 'logs').mkdir()
    for source in (Path(__file__), Path(f.__file__)):
        shutil.copy2(source, dest / source.name)
    paths = [campaign / 'plan.json', campaign / 'tasks.json', campaign / 'coverage.json',
             *[Path(t['recipe']) for t in tasks],
             *[p for p in (campaign / 'reproduce').iterdir() if p.is_file()],
             dest / Path(__file__).name, dest / Path(f.__file__).name,
             Path(config['metadata']),
             *sorted(Path(config['cv_dir']).rglob('*.json'))]
    f.write(dest / 'manifest.json', {
        'selected': tasks, 'hashes': {str(p): f.digest(p) for p in paths},
        'created': f.stamp(), 'publication_ready': False,
        'selection': 'Earliest READY year for EVERY distinct compound in the verified completed leg',
        'scope': 'Operational conversion test, additional data checks and retained QC; not a complete scientific audit',
        'config': config,
    })
    report(campaign)
    print(f'Prepared {len(tasks)} representative tests in {dest}', flush=True)


def verify(manifest, campaign):
    # Use the same source fingerprint policy as the full driver, including
    # explicitly permitted, frozen development checkouts.
    f.verify_campaign(campaign)
    for path, expected in manifest['hashes'].items():
        if f.digest(path) != expected:
            raise ValueError(f'Pinned test input/code/metadata changed: {path}')


def qc_summary(attempt):
    reports = []
    for path in sorted(attempt.glob('qc_*.json')):
        for product, checks in f.read(path).items():
            for checker, result in checks.items():
                reports.append(dict(file=product, checker=checker,
                    high=int(result.get('high_count', 0)),
                    medium=int(result.get('medium_count', 0)),
                    low=int(result.get('low_count', 0)),
                    scored=result.get('scored_points'), possible=result.get('possible_points')))
    return dict(status=('NOT_REPORTED' if not reports else
                         'FINDINGS' if any(r['high'] or r['medium'] or r['low'] or
                             r['scored'] != r['possible'] for r in reports) else 'PASS'),
                checkers=reports)


def check_data(task, paths, metadata):
    """Independent additional checks, streamed one output record at a time."""
    variable = task['compound'].split('.')[1]
    frequency = task['compound'].split('.')[-2]
    # The pinned DReq maps time-point to CMIP7's time1 axis, whose
    # must_have_bounds is "no". Keep interval/unknown metadata strict;
    # instantaneous data still need complete, ordered timestamps below.
    climatology = task['compound'].split('.')[2].startswith('tclm-')
    daily_statistic = metadata.get('temporal_shape') == 'monthly-mean-daily-stat'
    requires_bounds = not climatology and (metadata.get('temporal_shape') or '').strip() != 'time-point'
    details = []
    all_dates = []
    first_left = None
    previous_right = None
    selected_calendar = None
    for path in sorted(paths):
        with netCDF4.Dataset(path) as ds:
            v = ds[variable]
            if Unit(v.units) != Unit(metadata['units']):
                raise ValueError(f'Wrong units: {v.units!r}; DReq expects {metadata["units"]!r}')
            if metadata.get('standard_name') and getattr(v, 'standard_name', '') != metadata['standard_name']:
                raise ValueError('standard_name differs from pinned DReq')
            dims = metadata.get('dimensions', '').split()
            for axis, aliases in [('longitude', ('longitude', 'lon')), ('latitude', ('latitude', 'lat'))]:
                if axis in dims and not any(name in ds.variables for name in aliases):
                    raise ValueError(f'Missing {axis} coordinate')
            if 'olevel' in dims and not any(getattr(c, 'standard_name', '') == 'depth' and
                    c.ndim == 1 and c.dimensions[0] in v.dimensions for c in ds.variables.values()):
                raise ValueError('Missing ocean depth axis')
            finite, missing, invalid = 0, 0, 0
            block = max(1, 262144 // max(1, int(np.prod(v.shape[1:]))))
            for index in range(0, v.shape[0] if v.ndim else 1, block):
                values = np.ma.asarray(v[index:index + block] if v.ndim else v[...])
                compressed = values.compressed()
                finite += int(np.isfinite(compressed).sum())
                invalid += int((~np.isfinite(compressed)).sum())
                missing += int(np.ma.getmaskarray(values).sum())
            if invalid or finite == 0:
                raise ValueError(f'Invalid/all-missing variable: finite={finite}, invalid={invalid}, masked={missing}')
            if frequency != 'fx':
                if 'time' not in ds.variables:
                    raise ValueError('Missing time coordinate')
                t = ds['time']
                if t.ndim != 1 or t.dimensions[0] not in v.dimensions:
                    raise ValueError('Data variable does not use the time coordinate dimension')
                if np.ma.getmaskarray(t[:]).any():
                    raise ValueError('Missing time coordinate values')
                numeric = np.asarray(t[:])
                if not np.all(np.isfinite(numeric)):
                    raise ValueError('Nonfinite time coordinate values')
                if len(numeric) > 1 and np.any(np.diff(numeric) <= 0):
                    raise ValueError('Time records not strictly increasing in original order')
                calendar = getattr(t, 'calendar', 'standard')
                if selected_calendar is not None and calendar != selected_calendar:
                    raise ValueError('Calendar changed between output files')
                selected_calendar = calendar
                dates = netCDF4.num2date(numeric, t.units, calendar)
                all_dates.extend(f.support_module('input_streams').date_tuple(d) for d in dates)
                bounds_name = getattr(t, 'bounds', '')
                if daily_statistic:
                    bounds_name = getattr(t, 'climatology', '') or bounds_name
                if requires_bounds and not bounds_name:
                    raise ValueError('Missing time bounds')
                if bounds_name:
                    # Optional bounds, when declared, must also be valid.
                    if bounds_name not in ds.variables:
                        raise ValueError('Missing time bounds')
                    bounds_values = ds[bounds_name][:]
                    if np.ma.getmaskarray(bounds_values).any():
                        raise ValueError('Missing interval bounds values')
                    bounds = np.asarray(bounds_values)
                    if not np.all(np.isfinite(bounds)):
                        raise ValueError('Nonfinite interval bounds')
                    if bounds.shape != (len(numeric), 2) or np.any(bounds[:, 1] <= bounds[:, 0]):
                        raise ValueError('Invalid interval bounds')
                    if np.any(numeric < bounds[:, 0]) or np.any(numeric > bounds[:, 1]):
                        raise ValueError('Time outside its interval bounds')
                    decoded = netCDF4.num2date(bounds, t.units, calendar)
                    if requires_bounds and frequency in ('mon', 'yr'):
                        for left, right in decoded:
                            if first_left is None:
                                first_left = left
                            if left.day != 1 or left.hour or left.minute or left.second or right.day != 1 or right.hour or right.minute or right.second:
                                raise ValueError('Monthly/yearly bounds do not use calendar boundaries')
                            expected = ((left.year + (left.month == 12), left.month % 12 + 1)
                                        if frequency == 'mon' else (left.year + 1, 1))
                            if (right.year, right.month) != expected or (frequency == 'yr' and left.month != 1):
                                raise ValueError('Interval is not one calendar month/year')
                            if previous_right is not None and left != previous_right:
                                raise ValueError('Gap or overlap in output bounds')
                            previous_right = right
            details.append(dict(file=str(path), finite_values=finite, masked_values=missing,
                                dimensions=list(v.dimensions), units=v.units))
    if not details:
        raise ValueError('No output files to test')
    if climatology:
        f.support_module('climatology_support').validate(paths, task['window_start'], task['window_end'])
    elif frequency == 'dec':
        f.support_module('decadal_support').validate_decadal_output(
            paths, task['window_start'], task['window_end'])
    elif frequency != 'fx':
        f.cover_dates(all_dates, task['year'], frequency, calendar)
    if requires_bounds and frequency in ('mon', 'yr'):
        fields = lambda value: (value.year, value.month, value.day,
                                value.hour, value.minute, value.second)
        if (first_left is None or previous_right is None or
                fields(first_left) != (task['year'], 1, 1, 0, 0, 0) or
                fields(previous_right) != (task['year'] + 1, 1, 1, 0, 0, 0)):
            raise ValueError('Bounds do not cover exactly the requested calendar year')
    return details


def worker(campaign, index):
    if not os.environ.get('SLURM_JOB_ID'):
        raise ValueError('Conversion tests require a Slurm allocation')
    # Old raw validators use assert. Never silently disable their checks.
    os.environ.pop('PYTHONOPTIMIZE', None)
    dest = campaign / 'representative_test'
    manifest = f.read(dest / 'manifest.json')
    task = manifest['selected'][index]
    record = dict(compound=task['compound'], year=task['year'], key=task['key'],
                  job=os.environ['SLURM_JOB_ID'], started=f.stamp(), status='TEST_FAILED',
                  publication_ready=False)
    try:
        verify(manifest, campaign)
        if f.digest(task['recipe']) != task['recipe_sha256']:
            raise ValueError('Recipe changed')
        f.run_one(campaign, task, manifest['config'])
        result = f.read(campaign / 'status' / (task['key'] + '.json'))
        record['conversion'] = result
        if result['status'] != 'PASS':
            raise ValueError(result.get('error', 'Conversion failed'))
        output = Path(result['output'])
        record['qc'] = qc_summary(output)
        record['supervisor_source_check'] = result.get('validation', {}).get('supervisor_source_check', {})
        source_check = output / 'source_validation.json'
        record['independent_source_check'] = (f.read(source_check) if source_check.exists()
            else dict(raw_sample_comparison=False, compared_samples=0,
                      limitations=['No independent source-value audit for this full-catalog route']))
        metadata = f.read(manifest['config']['metadata'])['Compound Name'][task['compound']]
        record['additional_data_checks'] = check_data(task, result['validation']['files'], metadata)
        verify(manifest, campaign)
        record['status'] = 'CONVERSION_AND_BASIC_CHECKS_PASS'
    except Exception as exc:
        record['error'] = f'{type(exc).__name__}: {exc}'
    record['finished'] = f.stamp()
    f.write(dest / 'status' / (task['key'] + '.json'), record)
    print(json.dumps(record, indent=2), flush=True)
    return int(record['status'] != 'CONVERSION_AND_BASIC_CHECKS_PASS')


def report(campaign):
    dest = campaign / 'representative_test'
    manifest = f.read(dest / 'manifest.json')
    selected = {t['compound']: t for t in manifest['selected']}
    coverage = {}
    for row in f.read(campaign / 'coverage.json'):
        coverage.setdefault(row['compound'], []).append(row)
    rows = []
    for compound, records in sorted(coverage.items()):
        row = dict(compound=compound, variable=compound.split('.')[1],
                   status='BLOCKED', tested_year='', reason=' | '.join(sorted({r['reason'] for r in records if r['reason']})),
                   formal_qc='NOT_TESTED', independent_samples=0)
        if compound in selected:
            task = selected[compound]
            path = dest / 'status' / (task['key'] + '.json')
            result = f.read(path) if path.exists() else {}
            row.update(status=result.get('status', 'PENDING_OR_NO_RESULT'), tested_year=task['year'],
                       reason=result.get('error', ''), formal_qc=result.get('qc', {}).get('status', 'NOT_TESTED'),
                       independent_samples=result.get('independent_source_check', {}).get('compared_samples', 0))
        rows.append(row)
    with (dest / 'products.csv').open('w') as stream:
        writer = csv.DictWriter(stream, fieldnames=['compound', 'variable', 'status',
            'tested_year', 'reason', 'formal_qc', 'independent_samples'])
        writer.writeheader(); writer.writerows(rows)
    summary = dict(updated=f.stamp(), scope_products=len(rows), representative_tests=len(selected),
                   results=dict(Counter(r['status'] for r in rows)),
                   formal_qc=dict(Counter(r['formal_qc'] for r in rows)),
                   products_with_independent_samples=sum(r['independent_samples'] > 0 for r in rows),
                   all_requested_products_verified=False, publication_ready=False,
                   limitations=['One representative year per available product, not all years.',
                       'Blocked/absent/cancelled/timed-out tests are not passes.',
                       'Conversion and basic data checks do not replace a scientific audit or formal CMIP QC.',
                       'Documented disabled requests outside the executable catalog are listed in the separate catalogue audit.'])
    f.write(dest / 'summary.json', summary)
    print(json.dumps(summary, indent=2), flush=True)


def submit(campaign, compounds=None, full_after_pass=False):
    """Run selected real conversions; optionally gate full submission on checks."""
    dest = campaign / 'representative_test'
    manifest = f.read(dest / 'manifest.json')
    verify(manifest, campaign)
    requested = set(compounds or [t['compound'] for t in manifest['selected']])
    selected = [(i, t) for i, t in enumerate(manifest['selected']) if t['compound'] in requested]
    if not requested or {t['compound'] for _, t in selected} != requested:
        raise ValueError('Requested validation products are absent or blocked; no jobs submitted')
    with (dest / '.submit.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        marker = dest / 'submission.json'
        if marker.exists():
            raise ValueError('Validation already submitted; inspect existing jobs first')
        config = manifest['config']
        command = shlex.join([config['python'], str(dest / 'retest_all_products.py'),
                              'worker', str(campaign)])
        array = ','.join(str(i) for i, _ in selected) + f'%{config["concurrency"]}'
        job = f.batch(config, command, dest / 'logs/test-%A_%a.log', 'cmor-validate',
                      [f'--array={array}'])
        submission = dict(workers=job, gate=None, full_after_pass=full_after_pass,
                          required=[t['key'] for _, t in selected], submitted=f.stamp())
        f.write(marker, submission)
        command = shlex.join([config['python'], str(dest / 'retest_all_products.py'),
                              'gate', str(campaign)])
        gate_job = f.batch(config, command, dest / 'logs/gate-%j.log', 'cmor-validation-gate',
                           [f'--dependency=afterany:{job}'])
        submission['gate'] = gate_job
        f.write(marker, submission)
        print(json.dumps(submission, indent=2), flush=True)


def gate(campaign):
    """A failed, missing or stale test must never launch the full campaign."""
    dest = campaign / 'representative_test'
    manifest = f.read(dest / 'manifest.json')
    verify(manifest, campaign)
    submission = f.read(dest / 'submission.json')
    tasks = {t['key']: t for t in manifest['selected']}
    required = submission['required']
    if not required or any(key not in tasks for key in required):
        raise ValueError('Invalid validation gate selection')
    problems = []
    for key in required:
        path = dest / 'status' / (key + '.json')
        record = f.read(path) if path.exists() else {}
        conversion = record.get('conversion', {})
        if record.get('status') != 'CONVERSION_AND_BASIC_CHECKS_PASS' or conversion.get('status') != 'PASS':
            problems.append(dict(key=key, error=record.get('error', 'Missing successful test')))
            continue
        validation = conversion.get('validation', {})
        try:
            if f.file_state(validation['files']) != validation['output_state']:
                raise ValueError('Output changed after validation')
            if f.file_state(tasks[key]['input_state']) != tasks[key]['input_state']:
                raise ValueError('Input changed after validation')
        except (KeyError, OSError, ValueError) as exc:
            problems.append(dict(key=key, error=str(exc)))
    report(campaign)
    f.report(campaign)
    f.write(dest / 'gate_result.json', dict(
        status='BLOCKED' if problems else 'PASS', problems=problems,
        required=required, checked=f.stamp(), publication_ready=False,
        scope='Selected regression conversions and basic checks, not all-products or publication approval'))
    if problems:
        print('Full CMORization NOT submitted: validation gate failed', flush=True)
        return 1
    if submission['full_after_pass']:
        f.submit_workers(campaign, manifest['config'])
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'worker', 'report', 'submit', 'gate'])
    parser.add_argument('campaign', type=Path)
    parser.add_argument('--index', type=int)
    parser.add_argument('--compounds', nargs='+', help='Exact compounds for targeted validation (default: all READY products)')
    parser.add_argument('--full-after-pass', action='store_true', help='Submit all READY product-years only after every selected test passes')
    args = parser.parse_args()
    campaign = args.campaign.resolve(strict=True)
    if args.action == 'prepare':
        prepare(campaign)
    elif args.action == 'worker':
        index = args.index if args.index is not None else int(os.environ['SLURM_ARRAY_TASK_ID'])
        return worker(campaign, index)
    elif args.action == 'submit':
        submit(campaign, args.compounds, args.full_after_pass)
    elif args.action == 'gate':
        return gate(campaign)
    else:
        report(campaign)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
