"""Explicit experiment request scope and coverage of completed products.

Consumes an official export_dreq_lists_json export supplied outside Git. Never
counts a rule declaration or a planned global alternative as a written product.
"""
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path

PRIORITIES = ('Core', 'High', 'Medium', 'Low')


def conditional_exclusion(compound, metadata, identity, config):
    info = metadata.get(compound, {})
    comment = ' '.join(info.get('comment', '').lower().split())
    if (compound.split('.')[1] == 'bigthetao'
            and 'only for models using conservative temperature as prognostic field' in comment
            and identity.get('ocean_temperature') == 'potential'):
        return ('NOT_APPLICABLE: DReq requests this product only for prognostic conservative '
                'temperature; the archived temperature field identifies potential temperature.')
    if (compound == 'atmos.co2mass.tavg-u-hm-u.mon.glb'
            and config.get('carbon_context', {}).get('co2_representation') == 'nonuniform_diagnostic_tracer'):
        return ('NOT_APPLICABLE: nonuniform diagnostic CO2; the DReq processing note '
                'requires the corresponding mole-fraction diagnostic instead of prescribed-uniform CO2 mass.')
    return None


def load_request(path, experiment, metadata):
    document = json.loads(Path(path).read_text())
    entries = document.get('experiment', {}).get(experiment)
    if not isinstance(entries, dict) or not entries:
        raise ValueError(f'Data Request export contains no experiment {experiment!r}')
    requested = {}
    for priority in PRIORITIES:
        for compound in entries.get(priority, []):
            if compound not in metadata:
                raise ValueError(f'Requested product absent from pinned metadata: {compound}')
            if compound in requested:
                raise ValueError(f'Duplicate requested product across priorities: {compound}')
            requested[compound] = priority
    if not requested:
        raise ValueError('Empty Data Request experiment selection')
    return requested, document.get('Header', {})


def global_alternative(compound, metadata, completed):
    """A regional duplicate is covered only by an exact completed global field."""
    target = compound.rsplit('.', 1)[0] + '.glb'
    if target == compound or target not in completed:
        return None
    info, glob = metadata.get(compound, {}), metadata.get(target, {})
    note = ' '.join(info.get('processing_note', '').lower().split())
    if not all(s in note for s in ('regional subset', 'global equivalent', 'omit this regional')):
        return None
    for key in ('units', 'standard_name', 'dimensions', 'cell_methods', 'temporal_shape'):
        if not info.get(key) or info[key] != glob.get(key):
            return None
    return target


def write_report(campaign, config, product_rows):
    if not config.get('data_request_file'):
        return None
    metadata = json.loads(Path(config['metadata']).read_text())['Compound Name']
    requested, header = load_request(config['data_request_file'],
                                    config.get('data_request_experiment', 'piControl'), metadata)
    grouped = defaultdict(list)
    for row in product_rows:
        grouped[row['compound']].append(row)
    completed = {c for c, rows in grouped.items() if rows and all(r['status'] == 'PASS' for r in rows)}
    details = []
    for compound, priority in sorted(requested.items()):
        rows = grouped.get(compound, [])
        alt = global_alternative(compound, metadata, completed)
        if compound in completed:
            state = 'WRITTEN'
        elif alt:
            state = 'WRITTEN_GLOBAL_ALTERNATIVE'
        elif rows and all(r.get('status') == 'NOT_APPLICABLE' for r in rows):
            state = 'NOT_APPLICABLE'
        else:
            state = 'NOT_WRITTEN'
        details.append(dict(compound=compound, priority=priority, status=state,
            selection=any(r.get('selection') for r in rows),
            global_alternative=alt or '',
            reason=' | '.join(sorted({r.get('reason', '') for r in rows if r.get('reason')})),
            successful_tasks=sum(r.get('status') == 'PASS' for r in rows),
            required_tasks=len(rows),
            processing_note=metadata[compound].get('processing_note', '')))
    summary = dict(experiment=config.get('data_request_experiment', 'piControl'),
        request_header=header, requested_products=len(requested),
        priorities=dict(Counter(requested.values())),
        statuses=dict(Counter(r['status'] for r in details)),
        selected_statuses=dict(Counter(r['status'] for r in details if r['selection'])),
        total_written_products=len(completed),
        written_products_outside_request=len(completed - requested.keys()),
        count_definition='Distinct compound products. Global alternatives satisfy requests but are not extra written products. Rules and pending tasks are not completed output.',
        quality_limit='Availability does not establish scientific/QC approval. Applicability and selected opportunities still require experiment-specific review.')
    target = Path(campaign) / 'report'
    target.mkdir(exist_ok=True)
    with (target / 'data_request_coverage.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(details[0]))
        writer.writeheader()
        writer.writerows(details)
    (target / 'data_request_coverage.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary
