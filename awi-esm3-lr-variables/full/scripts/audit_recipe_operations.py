#!/usr/bin/env python3
"""Account for temporal operations in every frozen READY recipe.

Produces evidence, not an automatic scientific approval. Derived pipelines and
missing source operation metadata remain explicit review items.
"""
import argparse
from collections import Counter
from functools import lru_cache
import json
from pathlib import Path
import re

import netCDF4
import yaml


@lru_cache(maxsize=8192)
def header(path):
    with netCDF4.Dataset(path) as ds:
        return {n:dict(dimensions=list(v.dimensions),shape=list(v.shape),
                 units=getattr(v,'units',None),cell_methods=getattr(v,'cell_methods',''))
                for n,v in ds.variables.items()}


def audit(campaign):
    rows=[]
    for task in json.loads((campaign/'tasks.json').read_text()):
        doc=yaml.safe_load(Path(task['recipe']).read_text());r=doc['rules'][0]
        compound=task['compound'];brand=compound.split('.')[2].split('-')[0]
        observations=[]
        sources=set(task.get('input_state',{}))
        for path in sorted(sources):
            if not path.endswith('.nc') or '/outdata/' not in path:continue
            variables=header(path)
            for name in ({r.get('model_variable')}|{t['variable'] for t in r.get('lr_terms',[])}):
                if name in variables:observations.append(dict(path=path,variable=name,**variables[name]))
        methods={x['cell_methods'] for x in observations}
        desired={'tpt':'point','tavg':'mean','tmax':'maximum','tmin':'minimum','ti':'fixed'}.get(brand,brand)
        matching={'point':r'time:\s*(?:point|instant)', 'mean':r'time:\s*mean',
                  'maximum':r'time:\s*(?:maximum|max)', 'minimum':r'time:\s*(?:minimum|min)'}
        status=('NOT_APPLICABLE' if desired=='fixed' else 'SOURCE_OPERATION_MATCH'
                if methods and all(re.search(matching.get(desired,r'(?!)'),m) for m in methods)
                else 'REVIEW_REQUIRED')
        steps=[s for p in doc.get('pipelines',[]) if p['name'] in r.get('pipelines',[]) for s in p.get('steps',[])]
        rows.append(dict(key=task['key'],compound=compound,requested=desired,status=status,
                         source_operations=observations,pipeline_steps=steps,
                         caveat='An operation match does not independently validate scale factors, units, signs or scientific derivations.'))
    out=campaign/'report/temporal_operation_audit.json'
    out.write_text(json.dumps(dict(counts=dict(Counter(x['status'] for x in rows)),tasks=rows,
                                  publication_ready=False),indent=2)+'\n')
    print(out,dict(Counter(x['status'] for x in rows)),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('campaign',type=Path)
    audit(p.parse_args().campaign)
