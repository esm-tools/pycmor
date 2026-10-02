"""CLI overrides: apply command-line arguments on top of a loaded YAML config.

This module is CLI-agnostic — no ``click`` import. Errors are raised as
:class:`OverrideError` and translated to ``click.UsageError`` at the CLI
boundary. :func:`apply_overrides` always returns a new cfg dict; do not rely
on object identity.
"""
from __future__ import annotations

import dataclasses
import re
from typing import Any, Optional


class OverrideError(ValueError):
    """Raised when CLI overrides are inconsistent or under-specified."""


@dataclasses.dataclass
class CliOverrides:
    data_path: Optional[str] = None
    old_data_path: Optional[str] = None
    year_start: Optional[int] = None
    year_end: Optional[int] = None
    mesh_path: Optional[str] = None
    output_directory: Optional[str] = None
    memory: Optional[str] = None  # SLURM per-job memory, e.g. "512GB"


def apply_overrides(cfg: dict, ov: CliOverrides) -> dict:
    """Return a new cfg with CLI overrides applied.

    Does not mutate the input. Callers should reassign:
    ``cfg = apply_overrides(cfg, ov)``.
    """
    if ov.old_data_path is not None and ov.data_path is None:
        raise OverrideError("--old-data-path requires --data-path")

    # shallow copies of cfg, inherit, and each rule; full recursive copy
    # only when data_path triggers _subst_anchored
    cfg = dict(cfg)
    inherit = dict(cfg.get("inherit", {}))

    if ov.mesh_path is not None:
        inherit["mesh_path"] = ov.mesh_path
    if ov.output_directory is not None:
        inherit["output_directory"] = ov.output_directory

    if ov.memory is not None:
        jobqueue = dict(cfg.get("jobqueue", {}))
        slurm = dict(jobqueue.get("slurm", {}))
        slurm["memory"] = ov.memory
        jobqueue["slurm"] = slurm
        cfg["jobqueue"] = jobqueue

    rules = [dict(r) for r in cfg.get("rules", [])]
    for rule in rules:
        if ov.year_start is not None:
            rule["year_start"] = ov.year_start
        if ov.year_end is not None:
            rule["year_end"] = ov.year_end

    cfg["inherit"] = inherit
    cfg["rules"] = rules

    if ov.data_path is not None:
        old = ov.old_data_path
        if old is None:
            # Auto-detect: strip ``/outdata/<component>`` suffix from
            # ``inherit.data_path`` to get the run root.
            inherit_dp = inherit.get("data_path")
            if inherit_dp and "/outdata/" in inherit_dp:
                old = inherit_dp.split("/outdata/")[0]
            else:
                raise OverrideError(
                    "--data-path needs --old-data-path when the yaml has no "
                    "inherit.data_path of the form <run_root>/outdata/<component>"
                )
        old_norm = old.rstrip("/")
        new_norm = ov.data_path.rstrip("/")
        if old_norm != new_norm:
            pattern = re.compile(re.escape(old_norm) + r"(?=/|$)")
            cfg = _subst_anchored(cfg, pattern, new_norm)

    return cfg


def _subst_anchored(obj: Any, pattern: "re.Pattern", new: str) -> Any:
    if isinstance(obj, str):
        return pattern.sub(new, obj)
    if isinstance(obj, dict):
        return {k: _subst_anchored(v, pattern, new) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_subst_anchored(v, pattern, new) for v in obj]
    return obj
