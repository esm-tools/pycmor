"""Optional, explicit HR archive baseline; counts never imply successful conversion."""
from collections import defaultdict
import csv
import json
from pathlib import Path


def load_reference(manifest, metadata):
    index = defaultdict(set)
    for compound in metadata['Compound Name']:
        realm, name, brand, frequency, region = compound.split('.')
        index[(region, frequency, name, brand)].add(compound)
    products = set()
    for line in Path(manifest).read_text().splitlines():
        if not line.strip():
            continue
        parts = line.split('/')
        if len(parts) != 8 or not parts[-1].endswith('.nc'):
            raise ValueError(f'HR_REFERENCE: expected relative DRS7 filename, got {line!r}')
        candidates = index[tuple(parts[1:5])]
        if len(candidates) != 1:
            raise ValueError(f'HR_REFERENCE: ambiguous/unknown product {line!r}')
        products.update(candidates)
    if not products:
        raise ValueError('HR_REFERENCE: manifest contains no products')
    return products


def summarize(reference, rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row['compound']].append(row)
    result = []
    for compound in sorted(reference):
        entries = grouped.get(compound, [])
        states = [r['status'] for r in entries]
        planned = any(s in ('READY', 'PENDING', 'PASS', 'FAILED') for s in states)
        # Every required task, not just one successful year, must have passed.
        complete = bool(states) and all(s == 'PASS' for s in states)
        result.append(dict(compound=compound, lr_planned=planned,
            lr_complete=complete, successful_tasks=states.count('PASS'),
            required_tasks=len(states), reason=' | '.join(sorted({r.get('reason', '')
                for r in entries if r.get('reason')})) if entries else 'NO_LR_RULE'))
    return result


def write_report(campaign, config, rows):
    if not config.get('hr_reference_manifest'):
        return
    frozen = Path(campaign)/'reproduce/hr_reference_manifest.txt'
    manifest = frozen if frozen.is_file() else Path(config['hr_reference_manifest'])
    metadata = json.loads(Path(config['metadata']).read_text())
    reference = load_reference(manifest, metadata)
    details = summarize(reference, rows)
    destination = Path(campaign)/'report'
    with (destination/'hr_reference_coverage.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(details[0]))
        writer.writeheader()
        writer.writerows(details)
    summary = dict(reference_products=len(reference),
        lr_planned_reference_products=sum(r['lr_planned'] for r in details),
        lr_complete_reference_products=sum(r['lr_complete'] for r in details),
        missing_from_plan=[r['compound'] for r in details if not r['lr_planned']],
        incomplete=[r['compound'] for r in details if not r['lr_complete']],
        publication_ready=False,
        counting_method='Distinct variable+branding+frequency+region; ignore year, member, version and grid label.')
    (destination/'hr_reference_summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    return summary
