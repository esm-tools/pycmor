#!/usr/bin/env python3
"""Read-only source-catalogue audit; write reports only to --output-dir.

This does not plan conversions, open model data, create grids or submit jobs.
READY means a planner found a candidate route, not a successful conversion.
All default locations are relative to this script, never the working directory.
"""
from __future__ import annotations

import sys

sys.dont_write_bytecode = True

import argparse
import ast
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re

import yaml
import full_cmorization as f


WORKFLOW_ROOT = Path(__file__).resolve().parent.parent
EXTRA_NAMES = (
    'opottemptend', 'osalttend', 'opottemprmadvect',
    'opottempdiff', 'osaltrmadvect', 'osaltdiff',
)
EXTRA_COMPOUNDS = {
    f'ocean.{name}.tavg-ol-hxy-sea.yr.glb' for name in EXTRA_NAMES
}
CSV_CATEGORIES = ('obgc', 'co2', 'icebergs_cavities')
CSV_REASON_FIELDS = (
    'status', 'peer_precheck', 'peer_todo_section', 'note',
    'lr_source_hint', 'hr_writes', 'hr_cli123_file',
)
REPORT_NAMES = (
    'summary.json', 'catalogue_products.csv', 'unavailable_products.csv',
    'coverage_product_years.csv', 'findings.txt',
)


def read_json(path):
    return json.loads(Path(path).read_text())


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def absolute_argument(value):
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise argparse.ArgumentTypeError('Use an absolute path; cwd is not used.')
    return path.resolve()


def describe(compounds, rows=None):
    result = {
        'distinct_products': len(compounds),
        'distinct_variable_names': len({c.split('.')[1] for c in compounds}),
        'frequency_counts': dict(sorted(Counter(c.split('.')[-2] for c in compounds).items())),
    }
    if rows is not None:
        result['source_rows'] = rows
    return result


def co2mass_guard_evidence(driver, full_rows):
    """Inspect the guard without importing or running the conversion driver."""
    text = driver.read_text()
    guards = []
    for node in ast.walk(ast.parse(text, filename=str(driver))):
        if not isinstance(node, ast.If):
            continue
        for comparison in ast.walk(node.test):
            if not isinstance(comparison, ast.Compare):
                continue
            left = comparison.left
            if (isinstance(left, ast.Subscript)
                    and isinstance(left.value, ast.Name) and left.value.id == 'rule'
                    and isinstance(left.slice, ast.Constant) and left.slice.value == 'name'
                    and len(comparison.ops) == 1 and isinstance(comparison.ops[0], ast.Eq)
                    and any(isinstance(v, ast.Constant) and v.value == 'co2mass'
                            for v in comparison.comparators)):
                guards.append({'path': str(driver), 'line': comparison.lineno,
                               'test': ast.get_source_segment(text, comparison)})
    actual = [r for r in full_rows if r['compound'].split('.')[1] == 'co2mass']
    names = sorted({r['rule_name'] for r in actual})
    return {
        'name_guard_mismatch': bool(guards and actual and 'co2mass' not in names),
        'name_guard_tests': guards,
        'actual_rule_names': names,
        'actual_rules': actual,
        'explanation': (
            'A rule-name check for co2mass does not match co2mass_mon. '
            'This audit records evidence; it does not modify the driver or approve '
            'prescribed-CO2 mass for a prognostic tracer.'
        ),
    }


def load_shared_rules(workflow_root, repo, config):
    """Only configure/read rule generators; never call materialize or grids."""
    path = workflow_root / 'rules/raw_adapter.py'
    spec = importlib.util.spec_from_file_location('_catalogue_audit_raw_adapter', path)
    if spec is None or spec.loader is None:
        raise ValueError(f'Cannot load rule definitions: {path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.configure(workflow_root, repo, '/unused-read-only-catalogue-audit',
                     '20000101-20041231', config)
    return list(module.recom_rules()), list(module.co2_rules())


def campaign_coverage(campaign):
    if campaign is None:
        return [], {'supplied': False}
    source = campaign / 'coverage.json'
    rows = read_json(source)
    if not isinstance(rows, list):
        raise ValueError(f'Expected a list in {source}')
    seen = set()
    normalized = []
    for row in rows:
        identity = (row['compound'], row['year'])
        if identity in seen:
            raise ValueError(f'Duplicate campaign product/year: {identity}')
        seen.add(identity)
        status_path = None
        status_info = {}
        if row.get('key'):
            key = row['key']
            if Path(key).name != key:
                raise ValueError(f'Unsafe campaign task key: {key!r}')
            status_path = campaign / 'status' / f'{key}.json'
            if status_path.is_file():
                status_info = read_json(status_path)
        observed = status_info.get('status')
        if not observed:
            observed = 'PENDING' if row['status'] == 'READY' else row['status']
        normalized.append({
            'compound': row['compound'], 'year': row['year'],
            'coverage_status': row['status'], 'observed_status': observed,
            'reason': status_info.get('error') or row.get('reason', ''),
            'route': row.get('route', ''), 'tier': row.get('tier', ''),
            'status_file': str(status_path) if status_info else '',
            'alternatives': row.get('alternatives', []),
        })
    by_compound = defaultdict(list)
    for row in normalized:
        by_compound[row['compound']].append(row)
    ready = {c for c, entries in by_compound.items()
             if any(r['coverage_status'] == 'READY' for r in entries)}
    all_ready = {c for c, entries in by_compound.items()
                 if all(r['coverage_status'] == 'READY' for r in entries)}
    all_pass = {c for c, entries in by_compound.items()
                if all(r['observed_status'] == 'PASS' for r in entries)}
    mixed = {c for c, entries in by_compound.items()
             if len({r['coverage_status'] for r in entries}) > 1}
    info = {
        'supplied': True, 'path': str(campaign), 'coverage_file': str(source),
        'coverage_sha256': sha256(source), 'product_year_records': len(normalized),
        'distinct_products': len(by_compound),
        'coverage_status_counts': dict(Counter(r['coverage_status'] for r in normalized)),
        'observed_status_counts': dict(Counter(r['observed_status'] for r in normalized)),
        'products_with_any_ready_year': len(ready),
        'products_with_all_covered_years_ready': len(all_ready),
        'products_with_all_covered_years_pass': len(all_pass),
        'products_with_mixed_coverage_status': sorted(mixed),
        'interpretation': 'Existing coverage/status evidence only; no conversion is run.',
    }
    return normalized, info


def audit(config_file=None, campaign=None):
    configured = config_file or os.environ.get('PYCMOR_LR_CONFIG')
    config = f.load_config(configured)
    config_file = Path(configured).expanduser().resolve() if configured else None
    repo = Path(config['repo'])
    metadata_path = Path(config['metadata'])
    base = repo / 'awi-esm3-lr-variables'
    metadata = read_json(metadata_path)['Compound Name']
    if not isinstance(metadata, dict):
        raise ValueError('Expected a Compound Name dictionary in DReq metadata')
    sources = defaultdict(list)
    groups = defaultdict(set)
    counts = Counter()
    provenance = {Path(__file__).resolve(), metadata_path, WORKFLOW_ROOT / 'rules/raw_adapter.py',
                  WORKFLOW_ROOT / 'scripts/full_cmorization.py'}
    if config_file is not None:
        provenance.add(config_file)
    full_files = sorted(base.glob('*/cmip7xml*.yaml'))
    full_rows = []
    commented = defaultdict(list)

    def add_rule(rule, group, path, tier):
        compound = rule['compound_name']
        item = {'kind': group, 'path': str(path), 'tier': tier,
                'rule_name': rule.get('name', ''), 'compound': compound}
        sources[compound].append(item)
        groups[group].add(compound)
        counts[group] += 1
        if group == 'full_cmip7xml':
            full_rows.append(item)

    for path in full_files:
        provenance.add(path)
        text = path.read_text()
        for rule in yaml.safe_load(text)['rules']:
            add_rule(rule, 'full_cmip7xml', path, path.parent.name)
        for line_number, line in enumerate(text.splitlines(), 1):
            match = re.match(r'''\s*#+\s*compound_name:\s*['"]?([^#\s'"]+)''', line)
            if match and match[1] in EXTRA_COMPOUNDS:
                commented[match[1]].append({'path': str(path), 'line': line_number})

    profile_path = base / 'profile.yaml'
    provenance.add(profile_path)
    profile = yaml.safe_load(profile_path.read_text())
    for name in profile['tier_files']:
        path = (base / name).resolve()
        if not path.is_relative_to(base):
            raise ValueError(f'Raw tier outside catalogue: {path}')
        provenance.add(path)
        for rule in yaml.safe_load(path.read_text())['rules']:
            add_rule(rule, 'raw_profile', path, path.parent.name)
    recom, co2 = load_shared_rules(WORKFLOW_ROOT, repo, config)
    for group, rules in [('shared_recom', recom), ('shared_co2', co2)]:
        for rule in rules:
            add_rule(rule, group, WORKFLOW_ROOT / 'rules/raw_adapter.py', rule.get('lr_group', ''))
    implemented = set().union(*(groups[g] for g in
                               ('full_cmip7xml', 'raw_profile', 'shared_recom', 'shared_co2')))
    skipped_csvs = []
    for category in CSV_CATEGORIES:
        group = 'csv_' + category
        groups[group] = set()
        for path in sorted((base / category).glob('*.csv')):
            provenance.add(path)
            with path.open(newline='') as stream:
                reader = csv.DictReader(stream)
                if 'cmip7_compound_name' not in (reader.fieldnames or []):
                    skipped_csvs.append(str(path))
                    continue
                for row in reader:
                    compound = (row.get('cmip7_compound_name') or '').strip()
                    if not compound:
                        continue
                    sources[compound].append({
                        'kind': group, 'path': str(path), 'line_end': reader.line_num,
                        'csv_reasons': {k: row[k] for k in CSV_REASON_FIELDS if row.get(k)},
                    })
                    groups[group].add(compound)
                    counts[group] += 1
    scope = set(sources)
    extras = EXTRA_COMPOUNDS - scope
    notes = repo / 'notes/LR_reactivation_plan.md'
    if notes.exists():
        provenance.add(notes)
    for compound in EXTRA_COMPOUNDS:
        sources[compound].append({
            'kind': 'documented_commented_request',
            'commented_rule_evidence': commented.get(compound, []),
            'notes_path': str(notes),
            'limitation': 'Upstream documents missing 3D FESOM diagnostics; requires source changes.',
        })
    coverage, campaign_info = campaign_coverage(campaign)
    by_compound = defaultdict(list)
    for row in coverage:
        by_compound[row['compound']].append(row)
    records = []
    for compound in sorted(scope | EXTRA_COMPOUNDS):
        entries = by_compound.get(compound, [])
        kinds = sorted({r['kind'] for r in sources[compound]})
        gaps = []
        if compound in extras:
            gaps.append('EXTRA_NOT_COVERED: documented commented-out request outside workflow scope')
        if compound not in implemented:
            gaps.append('UNMAPPED: no implemented rule')
        if compound not in metadata:
            gaps.append('UNRESOLVED_DREQ: absent from pinned metadata')
        if compound.split('.')[-2] == 'dec':
            gaps.append('DEFERRED_WINDOW: workflow defers decadal products')
        if campaign is not None and not entries:
            gaps.append('NOT_IN_CAMPAIGN_COVERAGE')
        records.append({
            'compound': compound, 'variable': compound.split('.')[1],
            'frequency': compound.split('.')[-2], 'component': compound.split('.')[0],
            'workflow_scope': compound in scope, 'extra_not_covered': compound in extras,
            'has_implemented_rule': compound in implemented, 'metadata_supported': compound in metadata,
            'catalogue_groups': kinds, 'catalogue_gaps': gaps,
            'coverage_status_counts': dict(Counter(r['coverage_status'] for r in entries)),
            'observed_status_counts': dict(Counter(r['observed_status'] for r in entries)),
            'campaign_reasons': sorted({r['reason'] for r in entries if r['reason']}),
            'csv_reasons': [r for r in sources[compound] if r.get('csv_reasons')],
            'sources': sources[compound],
        })
    no_rule = sorted(scope - implemented)
    groups_summary = {g: describe(products, counts[g]) for g, products in sorted(groups.items())}
    for group, products in groups.items():
        groups_summary[group]['products_supported_by_metadata'] = len(products & set(metadata))
    for group in ('csv_' + c for c in CSV_CATEGORIES):
        products = groups[group]
        groups_summary[group].update(
            products_without_rule=len(products - implemented),
            products_missing_metadata=len(products - set(metadata)),
            products_with_any_ready_year=sum(any(r['coverage_status'] == 'READY'
                                                 for r in by_compound[c]) for c in products)
            if campaign is not None else None,
        )
    duplicates = Counter(r['compound'] for r in full_rows)
    summary = {
        'audit_kind': 'requested_catalogue', 'schema_version': 2,
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'workflow_root': str(WORKFLOW_ROOT), 'repo': str(repo),
        'config': str(config_file) if config_file is not None else None,
        'full_tier_files': len(full_files), 'expected_full_tier_files': 17,
        'groups': groups_summary, 'implemented_union': describe(implemented),
        'workflow_scope': describe(scope), 'documented_additional_scope': describe(extras),
        'combined_audited_scope': describe(scope | EXTRA_COMPOUNDS),
        'raw_products_outside_full': sorted(groups['raw_profile'] - groups['full_cmip7xml']),
        'full_duplicate_products': {c: n for c, n in duplicates.items() if n > 1},
        'workflow_products_without_rule': no_rule,
        'workflow_products_missing_metadata': sorted(scope - set(metadata)),
        'documented_extra_products': sorted(extras),
        'known_extra_products_missing_comment_evidence': sorted(EXTRA_COMPOUNDS - set(commented)),
        'co2mass_guard': co2mass_guard_evidence(WORKFLOW_ROOT / 'scripts/full_cmorization.py', full_rows),
        'campaign': campaign_info,
        'campaign_products_outside_workflow_scope': sorted(set(by_compound) - scope),
        'workflow_products_missing_from_campaign': sorted(scope - set(by_compound))
        if campaign is not None else [],
        'skipped_nonrequest_csvs': skipped_csvs,
        'source_sha256': {str(p): sha256(p) for p in sorted(provenance)},
        'limitations': [
            'Scope is active workflow catalogues plus six documented commented requests, not all CMIP7 DReq.',
            'Rules and metadata presence do not prove runtime convertibility or scientific correctness.',
            'No model outputs were opened; no fresh input inventory or conversion was performed.',
            'Campaign evidence is a read-time snapshot, not a lock against running workers.',
            'Full-route validator checks headers and dates, not independent scientific values.',
        ],
    }
    return summary, records, coverage


def write_csv(path, rows, fields):
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else v
                             for k, v in row.items() if k in fields})


def save_reports(output, summary, records, coverage):
    repo = Path(summary['repo'])
    protected = (repo, WORKFLOW_ROOT)
    if any(output.is_relative_to(p) for p in protected):
        raise ValueError('Report output must be outside the repository source tree')
    if any(part in ('outdata', 'restart', 'restarts') or part.startswith('run_') for part in output.parts):
        raise ValueError('Report output must not be inside model data, restart or run directories')
    if any((output / name).exists() or (output / name).is_symlink() for name in REPORT_NAMES):
        raise ValueError('Report files already exist; use a new dedicated output directory')
    output.mkdir(parents=True, exist_ok=True)
    fields = list(records[0]) if records else []
    (output / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
    write_csv(output / 'catalogue_products.csv', records, fields)
    unavailable = [r for r in records if r['catalogue_gaps'] or r['campaign_reasons']]
    write_csv(output / 'unavailable_products.csv', unavailable, fields)
    write_csv(output / 'coverage_product_years.csv', coverage, [
        'compound', 'year', 'coverage_status', 'observed_status', 'reason',
        'route', 'tier', 'status_file', 'alternatives',
    ])
    full = summary['groups']['full_cmip7xml']
    scope = summary['workflow_scope']
    lines = [
        'Requested-catalogue audit (read-only; not a conversion test)',
        f"Full tiers: {summary['full_tier_files']}; {full['source_rows']} rule rows; "
        f"{full['distinct_products']} products; {full['distinct_variable_names']} variable names.",
        f"Workflow scope: {scope['distinct_products']} products / {scope['distinct_variable_names']} names.",
        f"Implemented product union: {summary['implemented_union']['distinct_products']}.",
        f"Workflow products without a rule: {len(summary['workflow_products_without_rule'])}.",
        f"Workflow products absent from pinned metadata: {len(summary['workflow_products_missing_metadata'])}.",
        f"Documented extra requests outside workflow coverage: {len(summary['documented_extra_products'])}; "
        f"combined audit: {summary['combined_audited_scope']['distinct_products']} products.",
    ]
    if summary['co2mass_guard']['name_guard_mismatch']:
        lines.append('CO2MASS GUARD MISMATCH: driver checks co2mass; actual rule name is co2mass_mon.')
    if summary['campaign']['supplied']:
        info = summary['campaign']
        lines.extend([
            f"Campaign: {info['path']}",
            f"Planner product-year counts: {info['coverage_status_counts']}",
            f"Observed product-year counts (status files override READY): {info['observed_status_counts']}",
            f"Products with any READY year: {info['products_with_any_ready_year']}; "
            f"all covered years PASS: {info['products_with_all_covered_years_pass']}.",
        ])
    else:
        lines.append('No campaign supplied: input availability and runtime outcomes were not assessed.')
    lines.extend(['', 'Limitations:', *summary['limitations'], '',
                  'Detailed per-product evidence, CSV annotations and unavailable lists are in the CSV reports.'])
    (output / 'findings.txt').write_text('\n'.join(lines) + '\n')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=absolute_argument,
                        default=os.environ.get('PYCMOR_LR_CONFIG'),
                        help='Site configuration JSON; defaults to PYCMOR_LR_CONFIG')
    parser.add_argument('--campaign', type=absolute_argument,
                        help='Existing campaign containing coverage.json and optional status/*.json')
    parser.add_argument('--output-dir', '--output', type=absolute_argument, required=True,
                        help='Dedicated report directory; existing report files are never overwritten')
    args = parser.parse_args(argv)
    try:
        summary, records, coverage = audit(args.config, args.campaign)
        save_reports(args.output_dir, summary, records, coverage)
    except (ValueError, OSError, KeyError, yaml.YAMLError) as error:
        parser.exit(2, f'Catalogue audit error: {error}\n')
    print((args.output_dir / 'findings.txt').read_text(), end='')
    print(f'Reports: {args.output_dir}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
