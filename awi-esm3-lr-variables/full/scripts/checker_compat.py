"""Narrow compatibility correction for CF multi-dimensional character labels.

The installed CF check treats a character matrix as one string and crashes.
Present each decoded label to that same upstream check, keeping its region
vocabulary, severity, diagnostics and validation intact. No checks are skipped.
"""
import functools
import inspect
from types import SimpleNamespace

import netCDF4
import numpy as np


def region_label_adapter(original):
    @functools.wraps(original)
    def check(self, dataset):
        class RegionView:
            def get_variables_by_attributes(view, **attributes):
                result=[]
                for variable in dataset.get_variables_by_attributes(**attributes):
                    values=np.ma.asarray(variable[:])
                    if values.dtype.kind not in ('S','U','O'):
                        result.append(variable)
                        continue
                    values=np.ma.filled(values, b'' if values.dtype.kind=='S' else '')
                    labels=(netCDF4.chartostring(values)
                            if values.dtype.kind=='S' and values.dtype.itemsize==1 else values)
                    for label in np.asarray(labels).reshape(-1):
                        if isinstance(label,bytes):
                            label=label.decode('utf-8')
                        label=str(label).strip('\x00 ')
                        class Label:
                            name=variable.name
                            def __getitem__(proxy,key):
                                return proxy.values[key]
                        proxy=Label()
                        proxy.values=np.asarray(list(label),dtype='U1')
                        result.append(proxy)
                return result
            def __getattr__(view,name):
                return getattr(dataset,name)
        return original(self,RegionView())
    return check


def apply_compatibility():
    from compliance_checker.cf.cf_1_6 import CF1_6Check
    original=CF1_6Check.check_geographic_region
    if getattr(original,'_lr_label_adapter',False):
        return
    # Do not override an upstream implementation which already changed.
    if 'region.astype(str)' not in inspect.getsource(original):
        return
    patched=region_label_adapter(original)
    patched._lr_label_adapter=True
    CF1_6Check.check_geographic_region=patched
