"""Explain conditional DReq alternatives without counting unwritten products.

An exact global counterpart may satisfy a regional request, but it is not a
second generated product. Keep that distinction in every coverage report.
"""


def regional_global_alternative(compound, year, metadata, candidates):
    parts = compound.split('.')
    if len(parts) != 5 or parts[-1] == 'glb':
        return None
    requested = metadata.get(compound, {})
    note = ' '.join(requested.get('processing_note', '').lower().split())
    if not ('regional subset' in note and 'global equivalent' in note
            and 'omit this regional' in note):
        return None
    global_compound = '.'.join(parts[:-1] + ['glb'])
    global_metadata = metadata.get(global_compound)
    if global_metadata is None:
        return None
    # Do not treat another frequency, vertical shape or physical quantity as
    # equivalent merely because its short variable name matches.
    for field in ('units', 'standard_name', 'dimensions', 'cell_methods',
                  'temporal_shape'):
        if not requested.get(field) or requested[field] != global_metadata.get(field):
            return None
    if not any(r.get('status') == 'READY'
               for r in candidates.get((global_compound, year), [])):
        return None
    return global_compound


UNMAPPED = 'UNMAPPED: no implemented rule'
EXCLUSION_CATEGORIES = ('not_simulated', 'recom_configuration', 'global_equivalent',
                        'dropped_by_jan', 'not_requested', 'output_gap')


def load_exclusions(path):
    """Reviewed reasons for products without a rule (config/exclusions.yaml)."""
    import yaml
    document = yaml.safe_load(open(path))
    if document.get('schema_version') != 1 or not isinstance(document.get('exclusions'), dict):
        raise ValueError(f'Unsupported exclusions file: {path}')
    for compound, entry in document['exclusions'].items():
        if (not isinstance(entry, dict) or entry.get('category') not in EXCLUSION_CATEGORIES
                or not str(entry.get('reason', '')).strip()):
            raise ValueError(f'EXCLUSIONS: {compound} needs a known category and a reason')
    return document['exclusions']


def unmapped_reason(compound, exclusions):
    """Category and reviewed reason replacing the bare UNMAPPED reason, if any.

    A global_equivalent entry holds only while the global product is READY,
    which the planner reports itself (GLOBAL_EQUIVALENT_PLANNED). Without that
    the regional product is a real gap and stays UNMAPPED.
    """
    entry = exclusions.get(compound)
    if entry is None or entry['category'] == 'global_equivalent':
        return None, UNMAPPED
    return entry['category'], f"EXCLUDED_{entry['category'].upper()}: {entry['reason']}"
