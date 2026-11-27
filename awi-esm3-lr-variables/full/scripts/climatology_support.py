"""Explicit frozen multi-year climatology, without a mutable external accumulator."""
from pathlib import Path
import re

import netCDF4
import numpy as np


def window_pattern(pattern, years, last):
    alternatives='(?:'+'|'.join(map(str,years))+')'
    if r'\d{4}' in pattern:
        return pattern.replace(r'\d{4}',alternatives)
    return re.sub(rf'(?<!\d){last}(?!\d)',lambda _:alternatives,pattern)


def configure(doc, first, last):
    from input_streams import validate_input_streams
    from pycmor.core.gather_inputs import filter_files_by_year_range
    rule=doc['rules'][0];effective=dict(doc['inherit'],**rule)
    years=list(range(first,last+1));dependencies=set()
    for inp in rule['inputs']:
        pattern=window_pattern(inp['pattern'],years,last)
        paths=sorted(p for p in Path(inp['path']).iterdir() if p.is_file() and re.fullmatch(pattern,p.name))
        if not paths:raise ValueError('CLIMATOLOGY_MISSING_SOURCE: '+pattern)
        validate_input_streams(paths,years,'mon')
        inp['pattern']='^(?:'+'|'.join(re.escape(p.name) for p in paths)+')$'
        dependencies.update(paths)
    for key,value in effective.items():
        if key.endswith('_pattern') and isinstance(value,str):
            path_key = key[:-len('_pattern')] + '_path'
            if path_key not in effective:
                continue
            pattern=window_pattern(value,years,last)
            paths = sorted(p for p in Path(effective[path_key]).iterdir()
                           if p.is_file() and re.fullmatch(pattern,p.name))
            paths = filter_files_by_year_range(paths,first,last)
            if not paths:
                raise ValueError('CLIMATOLOGY_MISSING_SECONDARY: '+key)
            # Annual point streams end at the following January 1, 00 UTC.
            # Validate each complete annual stream including that endpoint.
            for year in years:
                validate_input_streams(filter_files_by_year_range(paths,year,year),[year],'mon')
            rule[key]='^(?:'+'|'.join(re.escape(p.name) for p in paths)+')$'
            dependencies.update(paths)
        if key.endswith('_file') and isinstance(value,str) and value.startswith('/'):
            p=Path(value)
            if not p.is_file():raise ValueError('CLIMATOLOGY_MISSING_AUXILIARY: '+value)
            dependencies.add(p)
    rule.update(climatology_window=[first,last],year_start=first,year_end=last)
    return sorted(dependencies)


def validate(paths, first, last):
    if len(paths)!=1:raise ValueError('Climatology must have one file for the selected window')
    with netCDF4.Dataset(paths[0]) as ds:
        t=ds['time'];name=getattr(t,'climatology','')
        if name not in ds.variables:raise ValueError('Missing climatology bounds')
        b=np.ma.asarray(ds[name][:])
        if b.shape!=(12,2) or np.ma.getmaskarray(b).any() or not np.isfinite(b).all():
            raise ValueError('Invalid climatology bounds')
        dates=netCDF4.num2date(b,t.units,t.calendar)
        for month,(lo,hi) in enumerate(dates,1):
            want_lo=(first,month,1)
            want_hi=(last+(month==12),month%12+1,1)
            if (lo.year,lo.month,lo.day)!=want_lo or (hi.year,hi.month,hi.day)!=want_hi:
                raise ValueError('Climatology bounds do not span every selected year')
        if not np.allclose(np.asarray(t[:]),np.asarray(b).mean(axis=1),rtol=0,atol=1e-7):
            raise ValueError('Climatology time is not at bounds midpoint')
