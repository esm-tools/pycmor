"""Apply explicit run-specific scientific repairs and prevent known bad proxies.

The complete review matrix includes unresolved approximations. A corrected
constant does not certify every other transformation of the same product.
"""
import json
import csv
from collections import Counter, defaultdict
from pathlib import Path


def finding_action(finding, compound, rule=None):
    """A repaired product keeps its finding on record but is no longer blocked by it.

    Only the corrected recipe is released: it names the finding in its
    ``scientific_repairs``. Other routes to the same product stay blocked.
    """
    repaired=compound in finding.get('repaired',{})
    if rule is not None:
        repaired=repaired and finding['id'] in rule.get('scientific_repairs',[])
    return 'repaired_recipe' if repaired else finding['action']


def review_document(doc, identity, config):
    profile=config.get('scientific_profile')
    if not profile:
        return doc
    path=Path(__file__).resolve().parents[1]/'config/scientific_review.json'
    review=json.loads(path.read_text())
    builds={review['profile']:dict(fesom_sha256=review['fesom_sha256']),
            **review.get('compatible_profiles',{})}
    build=builds.get(profile,{})
    if (not build or identity.get('fesom_sha256')!=build['fesom_sha256']
            or (build.get('vector_frame') and identity.get('vector_frame')!=build['vector_frame'])):
        raise ValueError('UNREVIEWED_SCIENTIFIC_PROFILE: reviewed model build does not match this leg')
    rule=doc['rules'][0];compound=rule['compound_name']
    findings=[r for r in review['findings'] if compound in r['products']]
    blocked=[r for r in findings if finding_action(r,compound,rule)=='blocked_invalid_mapping']
    if blocked:
        raise ValueError('SCIENTIFIC_MAPPING_DISABLED: '+' | '.join(r['id']+': '+r['detail'] for r in blocked))
    name=compound.split('.')[1]
    if any(r['id']=='F41' for r in findings):
        from field_capacity import reviewed_parameters
        table,depth,dependencies=reviewed_parameters(config.get('field_capacity_context'),identity.get('period'))
        rule.update(field_capacity_theta_by_soil_type=table,soil_column_depth_m=depth,**dependencies)
    if any(r['id']=='F11' for r in findings):
        if 'rho_snow' in rule:rule['rho_snow']=290.
        if name=='sisnmass':rule['scale_factor']=290.
        if name=='snm':rule['scale_factor']=-290.
    if any(r['id']=='F12' for r in findings):rule['scale_factor']=290.
    if any(r['id']=='F13' for r in findings):rule['scale_factor']=910.
    if any(r['id']=='F20' for r in findings):rule['constant_where_positive']=True
    if any(r['id']=='F25' for r in findings):
        if name in ('masscello','masso') and 'scale_factor' in rule:rule['scale_factor']=1030.
        else:rule['reference_density']=1030.
    if any(r['id']=='F26' for r in findings):rule['overturning_reference_density']=1030.
    # Calculate nonlinear speed before the daily/monthly temporal reduction.
    # The native hourly vectors are on TCO95, not the monthly regular grid.
    if any(r['id']=='F04' for r in findings):
        rule['inputs'][0]['pattern']=r'atmos_1h_sfc_10u_.*\.nc'
        rule['second_input_pattern']=r'atmos_1h_sfc_10v_.*\.nc'
        rule['grid_label']='g235'
        rule['grid']='OpenIFS TCo95 native reduced Gaussian grid (40320 cells)'
        rule['lr_scientific_sampling_note']='Speed calculated from native hourly mean components before daily/monthly averaging; sub-hour variability is not recoverable.'
    if (name=='hurs' and compound.split('.')[-2] in ('day','mon')
            and '.tavg-' in compound):
        rule['inputs'][0]['pattern']=r'atmos_1h_sfc_2t_.*\.nc'
        rule['second_input_pattern']=r'atmos_1h_sfc_2d_.*\.nc'
        rule['grid_label']='g235'
        rule['grid']='OpenIFS TCo95 native reduced Gaussian grid (40320 cells)'
        rule['lr_scientific_sampling_note']='Relative humidity calculated from native hourly mean temperature and water-defined dewpoint before daily/monthly averaging; sub-hour covariance is not recoverable.'
    if rule.get('lr_scientific_sampling_note'):
        attrs=rule.setdefault('variable_attributes',{})
        attrs['comment']=' '.join(filter(None,[attrs.get('comment'),rule['lr_scientific_sampling_note']]))
    rule['lr_scientific_review']=[dict(id=r['id'],action=finding_action(r,compound,rule),verdict=r['verdict'],
                                       **({'repair':r['repaired'][compound]}
                                          if finding_action(r,compound,rule)=='repaired_recipe' else {}))
                                  for r in findings]
    return doc


def write_report(campaign, rows):
    """Keep correction, scientific uncertainty and output availability separate."""
    review=json.loads((Path(__file__).resolve().parents[1]/'config/scientific_review.json').read_text())
    index=defaultdict(list)
    for finding in review['findings']:
        for compound in finding['products']: index[compound].append(finding)
    grouped=defaultdict(list)
    for row in rows:grouped[row['compound']].append(row)
    records=[]
    for compound, tasks in sorted(grouped.items()):
        findings=index[compound]
        actions={finding_action(r,compound) for r in findings}
        blocked='blocked_invalid_mapping' in actions
        unresolved='review_required' in actions
        state=('DISABLED_INVALID_MAPPING' if blocked else 'UNRESOLVED_REVIEW_OR_APPROXIMATION' if unresolved
               else 'REPAIRS_APPLIED' if findings else 'NO_RECORDED_FINDING_NOT_CERTIFIED')
        records.append(dict(compound=compound, scientific_status=state,
            written=all(r['status']=='PASS' for r in tasks),
            findings='; '.join(r['id']+': '+r['title'] for r in findings),
            actions='; '.join(sorted(actions))))
    target=Path(campaign)/'report'
    with (target/'scientific_review.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(records[0]));writer.writeheader();writer.writerows(records)
    (target/'scientific_review.json').write_text(json.dumps(dict(
        statuses=dict(Counter(r['scientific_status'] for r in records)),publication_ready=False,
        limitation=review['limitations'],
        count_definition='Distinct compound products; unresolved reviews are retained even when conversion succeeds.'),indent=2)+'\n')
