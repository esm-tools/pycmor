"""Represent the verified zero geothermal boundary of a specific FESOM build."""
import hashlib
import json
from pathlib import Path

import numpy as np
import xarray as xr


def verify_context(context):
    if not context or context.get('kind')!='zero_bottom_geothermal_heat_flux':
        raise ValueError('UNREVIEWED_GEOTHERMAL: an explicit model-boundary review is required')
    path=Path(context['evidence_file'])
    evidence=json.loads(path.read_text())
    if (evidence.get('represented_flux_W_m2')!=0.0
            or evidence.get('binary_sha256')!=context.get('binary_sha256')
            or evidence.get('conclusion')!='NO_GEOTHERMAL_SOURCE_ZERO_BOTTOM_HEAT_BOUNDARY'):
        raise ValueError('Geothermal evidence does not establish the represented zero boundary')
    for record in evidence['source_files']:
        if hashlib.sha256(Path(record['path']).read_bytes()).hexdigest()!=record['sha256']:
            raise ValueError('Reviewed geothermal model evidence changed')
    return path


def load_zero_boundary(data,rule):
    context=dict(kind='zero_bottom_geothermal_heat_flux',
                 evidence_file=rule.get('lr_geothermal_evidence_file'),
                 binary_sha256=rule.get('lr_geothermal_binary_sha256'))
    verify_context(context)
    with xr.open_dataset(rule.grid_file,decode_times=False) as grid:
        if 'depth_lev' not in grid:
            raise ValueError('Geothermal boundary needs the native wet-column mask')
        wet=np.asarray(grid.depth_lev.values)>0
        lon=np.asarray(grid.lon.values);lat=np.asarray(grid.lat.values)
    if wet.ndim!=1 or len(wet)!=len(lon) or len(lat)!=len(lon):
        raise ValueError('Inconsistent native ocean column geometry')
    result=xr.DataArray(np.where(wet,0.,np.nan),dims='nod2',
        coords={'lon':('nod2',lon),'lat':('nod2',lat)},name=rule.model_variable)
    result.attrs.update(units='W m-2',
        comment='Represented geothermal heat flux is zero: this reviewed FESOM configuration has no geothermal source and a zero bottom heat-flux boundary. This is the model-applied value, not an inferred observational climatology.')
    result.lon.attrs.update(standard_name='longitude',units='degrees_east')
    result.lat.attrs.update(standard_name='latitude',units='degrees_north')
    return result
