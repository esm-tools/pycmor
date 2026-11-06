#!/usr/bin/env python3
"""Select the latest completed spinup leg, never the latest directory alone.

Read-only by default. --save-selection writes a dated evidence record and
settings for a NEW CMORization directory. No model output is modified, and
no conversion jobs are submitted. A selected period stays frozen for that
conversion; rerun this selector before starting another conversion.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess

import netCDF4

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]
PERIOD = re.compile(r"(?:run_)?(\d{8}-\d{8})$")


def last_attempt(experiment, period, task):
    logs = list((experiment / 'log').glob(f'*_{task}_{period}_*.log'))
    logs = [p for p in logs if re.search(r'_\d+\.log$', p.name)]
    if not logs:
        return None
    return max(logs, key=lambda p: (p.stat().st_mtime_ns, p.name))


def job_status(log):
    job = re.search(r'_(\d+)\.log$', log.name)[1]
    command = ['sacct', '-X', '-n', '-P', '-j', job,
               '-o', 'JobIDRaw,State,ExitCode,End']
    result = subprocess.run(command, text=True, capture_output=True,
                            check=True, timeout=30)
    rows = [line.split('|') for line in result.stdout.splitlines()]
    row = next((r for r in rows if r[0] == job), None)
    if row is None:
        raise RuntimeError(f'No accounting record for job {job}; cannot certify completion')
    return dict(job=job, state=row[1], exit_code=row[2], end=row[3], log=str(log))


def successful(status):
    return status['state'] == 'COMPLETED' and status['exit_code'] == '0:0'


def discover_latest(experiment, status_reader=job_status):
    periods = set()
    for parent in (experiment, experiment/'outdata/lpj_guess'):
        if parent.is_dir():
            for p in parent.iterdir():
                match = PERIOD.fullmatch(p.name)
                if match and p.is_dir():
                    periods.add(match[1])
    progress = '\n'.join(p.read_text(errors='replace')
                         for p in (experiment/'log').glob('*_awiesm3.log'))
    excluded = []
    for period in sorted(periods, key=lambda p: (p[9:], p[:8]), reverse=True):
        log = last_attempt(experiment, period, 'compute')
        if log is None:
            excluded.append({'period': period, 'reason': 'No compute attempt recorded'})
            continue
        state = status_reader(log)
        if not successful(state):
            excluded.append({'period': period, 'reason': 'Latest compute attempt not successful', **state})
            continue
        first = period[:8]
        date = f'{first[:4]}-{first[4:6]}-{first[6:8]}'
        finished = re.search(r'\|\s*observe_compute\s+\d+\s+' + re.escape(date)
                             + r'T\S+\s+\d+\s+-\s+done\s*$', progress, re.M)
        if not finished:
            excluded.append({'period': period, 'reason': 'No successful compute observer completion', **state})
            continue
        return {'period': period, 'compute': state, 'excluded_newer_legs': excluded}
    raise RuntimeError('No completed compute leg verified: ' + json.dumps(excluded))


def tail(path, size=65536):
    with path.open('rb') as stream:
        stream.seek(max(0, path.stat().st_size-size))
        return stream.read().decode(errors='replace')


def check_archive(experiment, selection, status_reader=job_status):
    period = selection['period']
    start, end = period.split('-')
    if start[4:] != '0101' or end[4:] != '1231':
        raise RuntimeError('Current CMOR recipes require whole-year legs; no partial-year substitution')
    years = list(range(int(start[:4]), int(end[:4])+1))
    expected = [(y, m) for y in years for m in range(1, 13)]
    merge_log = last_attempt(experiment, period, 'Combine_LPJG')
    if merge_log is None:
        raise RuntimeError(f'{period}: latest completed leg has no LPJG merge log; wait, do not substitute an older leg')
    merge = status_reader(merge_log)
    if not successful(merge) or 'Successfully converted LPJ-GUESS output to nc' not in tail(merge_log):
        raise RuntimeError(f'{period}: LPJG postprocessing not verified complete; wait, do not substitute an older leg')
    native = sorted((experiment/'outdata/lpj_guess'/period).glob('*.out'))
    if not native or any(p.stat().st_size == 0 for p in native):
        raise RuntimeError(f'{period}: native LPJG files missing/empty; current native recipes cannot run')
    records = []
    problems = []
    component_counts = {}
    for component in ('oifs', 'fesom'):
        files = []
        for path in (experiment/'outdata'/component).iterdir():
            if component == 'oifs':
                match = re.fullmatch(r'(atm_remapped_1m_.+_)(\d{4})-(\d{4})\.nc', path.name)
                # Keep raw monthly XIOS files only; other layouts need their
                # own route, not an unsupported inference about availability.
                if match and match[1] and int(match[2]) in years and match[2] == match[3]:
                    files.append((path, match[1]))
            else:
                match = re.fullmatch(r'(.+\.fesom\.)(\d{4})\.nc', path.name)
                # Daily/subdaily/instant streams share the FESOM suffix but
                # must not be counted as incomplete monthly series.
                if match and int(match[2]) in years and not re.search(
                        r'_(?:day|daily|sfc|3h|6h|inst)\.fesom\.', path.name):
                    files.append((path, match[1]))
        if not files:
            raise RuntimeError(f'{period}: no recognized monthly {component} archive; inspect output layout')
        groups = {}
        for path, group in sorted(files):
            before = path.stat()
            with netCDF4.Dataset(path) as ds:
                time = next((ds[n] for n in ('time', 'time_counter') if n in ds.variables), None)
                if time is None:
                    problems.append(f'{path}: missing time coordinate')
                    continue
                dates = netCDF4.num2date(time[:], time.units, getattr(time, 'calendar', 'standard'))
                bname = getattr(time, 'bounds', None)
                if component == 'fesom' and bname in ds.variables:
                    bounds = ds[bname]
                    decoded = netCDF4.num2date(bounds[:], getattr(bounds, 'units', time.units),
                                              getattr(time, 'calendar', 'standard'))
                    if len(decoded) and all((hi-lo).total_seconds() <= 86400
                                            for lo, hi in decoded):
                        continue
                months = [(d.year, d.month) for d in dates]
                groups.setdefault(group, []).extend(months)
                variables = {n: {'dimensions': list(v.dimensions), 'shape': list(v.shape),
                                  'units': getattr(v, 'units', None)}
                             for n, v in ds.variables.items() if time.dimensions[0] in v.dimensions and n != time.name}
            after = path.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                problems.append(f'{path}: changed while reading')
            records.append({'path': str(path), 'size': after.st_size, 'mtime_ns': after.st_mtime_ns,
                            'component': component, 'months': months, 'variables': variables})
        for group, months in groups.items():
            if months != expected:
                problems.append(f'{component}/{group}: missing, duplicate or out-of-period monthly records')
        component_counts[component] = {'files': len(files), 'monthly_series': len(groups)}
    if problems:
        raise RuntimeError('Latest completed leg archive needs review:\n' + '\n'.join(problems))
    for p in native:
        s = p.stat()
        records.append({'path': str(p), 'component': 'lpj_guess', 'size': s.st_size, 'mtime_ns': s.st_mtime_ns})
    return {'first_year': years[0], 'last_year': years[-1], 'merge': merge,
            'component_counts': component_counts, 'native_lpj_files': len(native),
            'files': records,
            'validation_scope': 'Compute/merge success and complete monthly NetCDF timestamps; native LPJG values and all CMOR products still require per-product validation.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('experiment', type=Path)
    parser.add_argument('--save-selection', action='store_true')
    parser.add_argument('--output-dir', type=Path, help='Required with --save-selection; outside the source checkout and experiment')
    args = parser.parse_args()
    experiment = args.experiment.resolve(strict=True)
    selected = discover_latest(experiment)
    archive = check_archive(experiment, selected)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    evidence = {'selected_at_utc': stamp, 'experiment': str(experiment), **selected, **archive,
                'policy': 'Latest completed compute leg, with finished postprocessing. Freeze selection for each conversion. Never silently fall back if its data are incomplete.'}
    destination = args.output_dir.resolve() if args.output_dir else None
    if args.save_selection:
        if destination is None:
            parser.error('--save-selection requires --output-dir outside the source repository')
        if any(destination == p or p in destination.parents or destination in p.parents for p in (REPO, experiment)):
            parser.error('--output-dir must not overlap the source repository or experiment')
        destination.mkdir(parents=True, exist_ok=True)
        (destination/'inventory').mkdir(exist_ok=True)
        record = destination/'inventory'/f'latest_leg_selection_{stamp}.json'
        record.write_text(json.dumps(evidence, indent=2)+'\n')
        config = {'experiment': str(experiment), 'period': selected['period'],
                  'first_year': archive['first_year'], 'last_year': archive['last_year'],
                  'pycmor_commit': subprocess.check_output(['git','-C',str(REPO),'rev-parse','HEAD'],text=True).strip(),
                  'merge_job': archive['merge']['job'], 'publication_ready': False,
                  'selection_evidence': str(record),
                  'comment': 'CMORization of a coupled spinup, NOT an official piControl submission. Preserve actual simulation years. Source, experiment, grid registration and branch metadata require team approval before publication.'}
        settings = destination/'settings.json'
        if settings.exists():
            previous = json.loads(settings.read_text())
            for key in ('experiment', 'period', 'first_year', 'last_year', 'pycmor_commit'):
                if previous[key] != config[key]:
                    raise RuntimeError(f'Refusing to alter existing conversion settings: {key}')
            if not (destination/'recipes').exists():
                settings.write_text(json.dumps(config, indent=2)+'\n')
        else:
            settings.write_text(json.dumps(config, indent=2)+'\n')
        evidence['saved_evidence'] = str(record)
    evidence.pop('files')
    evidence['conversion_directory'] = str(destination) if destination else None
    evidence['jobs_submitted'] = False
    print(json.dumps(evidence, indent=2))


if __name__ == '__main__':
    main()
