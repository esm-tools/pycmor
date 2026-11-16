"""Reconstruct field capacity from reviewed model parameters and run logs."""
import hashlib
import itertools
import json
from pathlib import Path
import re

import numpy as np


def parameters_from_source(text):
    normalized=''.join(text.upper().split())
    if 'ZFAC=100._JPRB/9.8_JPRB' not in normalized or 'ZPSICAP=-0.10_JPRB*ZFAC' not in normalized:
        raise ValueError('Unreviewed field-capacity pressure-head convention')
    def vector(name):
        match=re.search(r'\b'+name+r'\(1:NSOTY\)\s*=\s*\(/(.*?)/\)',text,re.S|re.I)
        if not match:raise ValueError('Missing reviewed soil parameter '+name)
        values=[float(v.strip()) for v in match[1].replace('_JPRB','').replace('&','').split(',')]
        if len(values)!=7 or not np.isfinite(values).all():raise ValueError('Invalid soil parameter vector')
        return np.array(values)
    alpha,n,saturated,residual=[vector(name) for name in ('ZMVGALPHA','ZNFAC','ZWSATM','ZWRES')]
    if not ((alpha>0)&(n>1)&(residual>=0)&(saturated>residual)&(saturated<=1)).all():
        raise ValueError('Unphysical reviewed Van Genuchten parameters')
    head=-.10*100/9.8  # SUSSOIL's LEVGEN branch, metres of water.
    theta=residual+(saturated-residual)*(1/(1+abs(alpha*head)**n))**(1-1/n)
    layers=re.findall(r'IF\s*\(NCSS\s*>=\s*([1-4])\)\s+YDSOIL%RDAT\(\1\)\s*=\s*([0-9.]+)_JPRB',text,re.I)
    if [int(i) for i,_ in layers]!=[1,2,3,4]:raise ValueError('Unverified four-layer soil thicknesses')
    return {0:0.,**{i+1:float(value) for i,value in enumerate(theta)}},sum(float(v) for _,v in layers)


def reviewed_parameters(path,period):
    if not path:raise ValueError('UNREVIEWED_FIELD_CAPACITY: archived model parameters and actual run flags are required')
    path=Path(path);context=json.loads(path.read_text())
    if context.get('kind')!='htessel_van_genuchten_4layer' or context.get('period')!=period:
        raise ValueError('Field-capacity context does not describe this archived leg')
    source=Path(context['source_file']);prefix=Path(context['log_prefix_file']);log=Path(context['log_file'])
    if hashlib.sha256(source.read_bytes()).hexdigest()!=context['source_sha256']:
        raise ValueError('Reviewed soil model source changed')
    if hashlib.sha256(prefix.read_bytes()).hexdigest()!=context['prefix_sha256']:
        raise ValueError('Reviewed run-log evidence changed')
    if [log.stat().st_size,log.stat().st_mtime_ns]!=context['log_state']:
        raise ValueError('Archived run log changed')
    with log.open('rb') as stream:actual=b''.join(itertools.islice(stream,context['prefix_lines']))
    if hashlib.sha256(actual).hexdigest()!=context['prefix_sha256']:
        raise ValueError('Run-log excerpt is not from the archived run')
    flags=actual.decode('utf-8')
    if not re.search(r'\bLEVGEN\s*=\s*T\b',flags) or not re.search(r'\bNCSS\s*=\s*4\b',flags):
        raise ValueError('Field-capacity recipe requires verified LEVGEN=T and NCSS=4')
    table,depth=parameters_from_source(source.read_text())
    return table,depth,dict(field_capacity_evidence_file=str(path),field_capacity_source_file=str(source),
        field_capacity_log_excerpt_file=str(prefix),field_capacity_model_log_file=str(log))
