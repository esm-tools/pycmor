"""OpenIFS run settings that decide whether an atmosphere recipe applies.

Read from the leg's own namelist (work/fort.4), not from template defaults.
"""
import re
from pathlib import Path


def namelist_value(text, group, key):
    block = re.search(rf'^\s*&{group}\b(.*?)^\s*/', text, re.S | re.M | re.I)
    if not block:
        return None
    value = re.search(rf'^\s*{re.escape(key)}\s*=\s*([^,\n]+)', block.group(1), re.M | re.I)
    return value.group(1).strip() if value else None


def _flag(value):
    return None if value is None else value.strip().strip('.').upper().startswith('T')


def identity(work):
    """ozone: prognostic/climatological/unknown; pextra_budget: per_step/run_accumulated/unknown."""
    path = Path(work) / 'fort.4'
    if not path.is_file():
        return dict(ozone='unknown', pextra_budget='unknown', oifs_tstep=None)
    text = path.read_text(errors='replace')
    ozone = _flag(namelist_value(text, 'NAMGFL', 'YO3_NL%LGP'))
    # sucldp.F90 defaults LCLDBUD_TIMEINT to .TRUE.: the budget fields then
    # accumulate over the run instead of holding one step's increment.
    timeint = _flag(namelist_value(text, 'NAMCLDP', 'LCLDBUD_TIMEINT'))
    tstep = namelist_value(text, 'NAMRIP', 'TSTEP')
    return dict(ozone={True: 'prognostic', False: 'climatological'}.get(ozone, 'unknown'),
                pextra_budget='per_step' if timeint is False else 'run_accumulated',
                oifs_tstep=float(tstep) if tstep else None)


def check_pextra(rule, identity):
    """Tendencies from pextra fields need per-step increments and the right step length."""
    if not rule.get('pextra_terms'):
        return
    if identity.get('pextra_budget') != 'per_step':
        raise ValueError(f"UNRESOLVED_PEXTRA_ACCUMULATION: pextra budget is {identity.get('pextra_budget', 'unknown')}; "
                         'with NAMCLDP LCLDBUD_TIMEINT=.true. (the default) the fields accumulate over the run and '
                         'their monthly means give no monthly tendency; the run needs LCLDBUD_TIMEINT=.false.')
    if identity.get('oifs_tstep') != float(rule['pextra_step_seconds']):
        raise ValueError(f"UNRESOLVED_PEXTRA_STEP: rule divides by {rule['pextra_step_seconds']} s, "
                         f"the run's TSTEP is {identity.get('oifs_tstep')}")
