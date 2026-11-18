"""Surface extraction for the explicitly selected LR carbon profile."""
from pycmor.awiesm3_lr import transform


def transform_and_surface(data, rule):
    data = transform(data, rule)
    return surface_only(data, rule)


def surface_only(data, rule):
    if rule.get('surface_layer', False):
        dims = [d for d in ('nz','nz1','nz_sfc','depth','olevel') if d in data.dims]
        if len(dims) != 1:
            raise ValueError(f'Expected one ocean depth dimension for surface extraction; found {dims}')
        if dims[0] == 'nz_sfc' and data.sizes['nz_sfc'] != 1:
            raise ValueError('Native daily surface stream must have exactly one nz_sfc layer')
        # First physical layer only; preserve missing values, including cavities.
        data = data.isel({dims[0]: 0}, drop=True)
    return data
