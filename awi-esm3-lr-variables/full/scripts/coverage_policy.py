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
