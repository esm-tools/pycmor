"""Explicit immutable geometry/provenance for cleaned-up completed run legs."""
import hashlib
import json
from pathlib import Path


def sha256(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(4*1024*1024),b''):
            h.update(chunk)
    return h.hexdigest()


def load_context(experiment, period, config):
    path=config.get('archived_contexts',{}).get(period)
    if not path:
        return None
    path=Path(path).expanduser().resolve(strict=True)
    context=json.loads(path.read_text())
    if Path(context['experiment']).resolve()!=Path(experiment).resolve() or context['period']!=period:
        raise ValueError('Archived context belongs to another experiment/leg')
    plan=Path(context['original_plan'])
    if sha256(plan)!=context['original_plan_sha256']:
        raise ValueError('Original archived plan changed')
    signature=json.loads(plan.read_text())['signature']
    if signature['experiment']!=str(Path(experiment).resolve()) or signature['period']!=period:
        raise ValueError('Archived plan identity does not match requested experiment/leg')
    if context['identity']!=signature['identity']:
        raise ValueError('Archived identity is not the original frozen identity')
    for name,info in context['files'].items():
        if sha256(info['path'])!=info['sha256']:
            raise ValueError(f'Archived context file changed: {name}')
    context['context_path']=str(path)
    return context
