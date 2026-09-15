"""Tests for the atomic-write reliability path in
``pycmor.std_lib.files`` (Option A + A.5 of
PLAN_save_dataset_reliability.md).

Covers:
- ``_is_tmpfs`` mount detection.
- ``_tmpfs_staging_available`` env-var and per-rule resolution.
- ``_atomic_to_netcdf`` three-stage write correctness, fallback,
  and failure cleanup.
"""
import os
from unittest.mock import patch, mock_open

import numpy as np
import pytest
import xarray as xr

from pycmor.std_lib import files


# ---------------- _is_tmpfs ----------------


@pytest.fixture(autouse=True)
def _reset_caches(monkeypatch):
    """Clear the module-level cache so each test sees a fresh detection."""
    files._reset_tmpfs_cache()
    for var in (
        "PYCMOR_TMPFS_STAGING",
        "PYCMOR_TMPFS_DIR",
        "PYCMOR_TMPFS_MIN_FREE_GB",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.mark.parametrize(
    "mounts, path, expected",
    [
        # Levante compute node shape: /tmp is tmpfs.
        ("tmpfs /tmp tmpfs rw,nosuid 0 0\n/dev/sda1 / ext4 rw 0 0\n", "/tmp", True),
        # Path under a tmpfs mount.
        ("tmpfs /tmp tmpfs rw 0 0\n", "/tmp/sub/file.nc", True),
        # Login node shape: /tmp is ext4-backed.
        ("/dev/sda2 /tmp ext4 rw 0 0\n", "/tmp", False),
        # Path entirely outside any tmpfs mount.
        ("tmpfs /dev/shm tmpfs rw 0 0\n", "/scratch/file", False),
        # Empty mounts (paranoid case).
        ("", "/tmp", False),
    ],
)
def test_is_tmpfs(mounts, path, expected):
    with patch("builtins.open", mock_open(read_data=mounts)):
        assert files._is_tmpfs(path) is expected


def test_is_tmpfs_no_proc_mounts():
    """Non-Linux / containerless environment: return False rather than raising."""
    with patch("builtins.open", side_effect=OSError("no /proc")):
        assert files._is_tmpfs("/tmp") is False


def test_is_tmpfs_picks_longest_prefix():
    """If /tmp/sub is its own tmpfs but /tmp is ext4, /tmp/sub/x → tmpfs."""
    mounts = (
        "/dev/sda1 /tmp ext4 rw 0 0\n"
        "tmpfs /tmp/sub tmpfs rw 0 0\n"
    )
    with patch("builtins.open", mock_open(read_data=mounts)):
        assert files._is_tmpfs("/tmp/sub/file.nc") is True
        assert files._is_tmpfs("/tmp/other.nc") is False


# ---------------- _rule_allows_tmpfs_staging ----------------


class _MockRule:
    """Minimal stand-in for a pycmor Rule that supports ``.get(key)``."""

    def __init__(self, **kw):
        self._d = kw

    def get(self, key, default=None):
        return self._d.get(key, default)


@pytest.mark.parametrize(
    "flag, expected",
    [
        (None, True),       # no flag → allowed
        (True, True),       # explicit true
        (False, False),     # explicit false
        ("false", False),   # string false
        ("no", False),
        ("off", False),
        ("0", False),
        ("true", True),
        ("yes", True),
    ],
)
def test_rule_allows_tmpfs_staging(flag, expected):
    rule = _MockRule() if flag is None else _MockRule(netcdf_tmpfs_staging=flag)
    assert files._rule_allows_tmpfs_staging(rule) is expected


def test_rule_allows_tmpfs_staging_none_rule():
    assert files._rule_allows_tmpfs_staging(None) is True


# ---------------- _tmpfs_staging_available ----------------


def test_tmpfs_staging_off_env(monkeypatch):
    monkeypatch.setenv("PYCMOR_TMPFS_STAGING", "off")
    assert files._tmpfs_staging_available() is False


def test_tmpfs_staging_on_env(monkeypatch):
    """`on` skips auto-detect safety checks (caller knows the FS)."""
    monkeypatch.setenv("PYCMOR_TMPFS_STAGING", "on")
    assert files._tmpfs_staging_available() is True


def test_tmpfs_staging_on_respects_per_rule_opt_out(monkeypatch):
    """`on` doesn't override per-rule opt-out."""
    monkeypatch.setenv("PYCMOR_TMPFS_STAGING", "on")
    rule = _MockRule(netcdf_tmpfs_staging=False)
    assert files._tmpfs_staging_available(rule) is False


def test_tmpfs_staging_auto_on_real_tmpfs(monkeypatch, tmp_path):
    """auto mode: /tmp is tmpfs with enough free space → enable."""
    monkeypatch.setenv("PYCMOR_TMPFS_DIR", str(tmp_path))
    monkeypatch.setenv("PYCMOR_TMPFS_MIN_FREE_GB", "0.0001")
    with patch.object(files, "_is_tmpfs", return_value=True):
        assert files._tmpfs_staging_available() is True


def test_tmpfs_staging_auto_off_non_tmpfs(monkeypatch, tmp_path):
    """auto mode: /tmp not tmpfs → disable (the login-node scenario)."""
    monkeypatch.setenv("PYCMOR_TMPFS_DIR", str(tmp_path))
    with patch.object(files, "_is_tmpfs", return_value=False):
        assert files._tmpfs_staging_available() is False


def test_tmpfs_staging_auto_off_too_little_free_space(monkeypatch, tmp_path):
    """auto mode: /tmp tmpfs but free space below threshold → disable."""
    monkeypatch.setenv("PYCMOR_TMPFS_DIR", str(tmp_path))
    monkeypatch.setenv("PYCMOR_TMPFS_MIN_FREE_GB", "1000000")  # 1 PB threshold
    with patch.object(files, "_is_tmpfs", return_value=True):
        assert files._tmpfs_staging_available() is False


def test_tmpfs_staging_auto_off_statvfs_fails(monkeypatch):
    """auto mode: cannot stat /tmp → disable."""
    monkeypatch.setenv("PYCMOR_TMPFS_DIR", "/this/does/not/exist")
    assert files._tmpfs_staging_available() is False


def test_tmpfs_staging_auto_caches(monkeypatch, tmp_path):
    """The auto-detect result is cached at module level (first call wins)."""
    monkeypatch.setenv("PYCMOR_TMPFS_DIR", str(tmp_path))
    with patch.object(files, "_is_tmpfs", return_value=True) as m:
        files._tmpfs_staging_available()
        files._tmpfs_staging_available()
        files._tmpfs_staging_available()
    # _is_tmpfs called exactly once: the second and third calls hit the cache.
    assert m.call_count == 1


# ---------------- _atomic_to_netcdf ----------------


def _tiny_dataset():
    return xr.Dataset(
        {"x": (("time",), np.array([1.0, 2.0, 3.0], dtype=np.float64))},
        coords={"time": [0, 1, 2]},
    )


def test_atomic_to_netcdf_produces_identical_output_to_direct(monkeypatch, tmp_path):
    """The end-to-end three-stage write produces a file byte-identical (or
    xr-identical after reload) to a direct ``_safe_to_netcdf`` write."""
    ds = _tiny_dataset()
    direct = tmp_path / "direct.nc"
    atomic = tmp_path / "atomic.nc"

    monkeypatch.setenv("PYCMOR_TMPFS_STAGING", "off")
    files._safe_to_netcdf(ds, str(direct), mode="w", format="NETCDF4")

    # tmpfs dir = tmp_path itself; force-on
    monkeypatch.setenv("PYCMOR_TMPFS_DIR", str(tmp_path))
    monkeypatch.setenv("PYCMOR_TMPFS_STAGING", "on")
    files._atomic_to_netcdf(ds, str(atomic), mode="w", format="NETCDF4")

    assert direct.exists()
    assert atomic.exists()
    a = xr.open_dataset(direct)
    b = xr.open_dataset(atomic)
    xr.testing.assert_identical(a, b)
    a.close()
    b.close()


def test_atomic_to_netcdf_falls_back_when_staging_disabled(monkeypatch, tmp_path):
    """If staging is off, ``_atomic_to_netcdf`` writes directly to the
    final path (no .tmp suffix appears)."""
    ds = _tiny_dataset()
    final = tmp_path / "out.nc"
    monkeypatch.setenv("PYCMOR_TMPFS_STAGING", "off")
    files._atomic_to_netcdf(ds, str(final), mode="w", format="NETCDF4")
    assert final.exists()
    # No .tmp residue
    assert not (tmp_path / "out.nc.tmp").exists()


def test_atomic_to_netcdf_no_partial_final_on_stage1_failure(monkeypatch, tmp_path):
    """If the tmpfs write (stage 1) raises, ``final_path`` is never created."""
    ds = _tiny_dataset()
    final = tmp_path / "out.nc"
    monkeypatch.setenv("PYCMOR_TMPFS_DIR", str(tmp_path))
    monkeypatch.setenv("PYCMOR_TMPFS_STAGING", "on")

    def boom(*args, **kwargs):
        raise RuntimeError("stage 1 exploded")

    with patch.object(files, "_safe_to_netcdf", side_effect=boom):
        with pytest.raises(RuntimeError, match="stage 1 exploded"):
            files._atomic_to_netcdf(ds, str(final), mode="w", format="NETCDF4")
    assert not final.exists()
    # Cleanup: no .tmp residue at the final path
    assert not (tmp_path / "out.nc.tmp").exists()


def test_atomic_to_netcdf_no_partial_final_on_stage2_failure(monkeypatch, tmp_path):
    """If the Lustre-side copy (stage 2) raises, ``final_path`` is never
    created. The tmpfs path is cleaned up."""
    ds = _tiny_dataset()
    final = tmp_path / "out.nc"
    monkeypatch.setenv("PYCMOR_TMPFS_DIR", str(tmp_path))
    monkeypatch.setenv("PYCMOR_TMPFS_STAGING", "on")

    import shutil as _shutil
    real_copy2 = _shutil.copy2

    def boom_copy(*args, **kwargs):
        raise OSError("disk full")

    with patch("pycmor.std_lib.files.shutil.copy2", side_effect=boom_copy) if False else patch("shutil.copy2", side_effect=boom_copy):
        with pytest.raises(OSError, match="disk full"):
            files._atomic_to_netcdf(ds, str(final), mode="w", format="NETCDF4")
    assert not final.exists()
    assert not (tmp_path / "out.nc.tmp").exists()


def test_atomic_to_netcdf_respects_per_rule_opt_out(monkeypatch, tmp_path):
    """A rule with ``netcdf_tmpfs_staging: false`` writes directly to final
    even when env says ``on``."""
    ds = _tiny_dataset()
    final = tmp_path / "rule_opt_out.nc"
    monkeypatch.setenv("PYCMOR_TMPFS_DIR", str(tmp_path))
    monkeypatch.setenv("PYCMOR_TMPFS_STAGING", "on")
    rule = _MockRule(netcdf_tmpfs_staging=False)

    with patch.object(files, "_safe_to_netcdf", wraps=files._safe_to_netcdf) as m:
        files._atomic_to_netcdf(ds, str(final), rule=rule, mode="w", format="NETCDF4")
        # Called exactly once with the final path (not a staging path).
        assert m.call_count == 1
        assert m.call_args.args[1] == str(final)
    assert final.exists()


def test_atomic_to_netcdf_final_appears_atomically(monkeypatch, tmp_path):
    """Mid-write, ``final_path`` is *never* visible — only ``final_path.tmp``
    is, until stage 3 rename completes. Verified by hooking ``os.rename``."""
    ds = _tiny_dataset()
    final = tmp_path / "atomic_check.nc"
    monkeypatch.setenv("PYCMOR_TMPFS_DIR", str(tmp_path))
    monkeypatch.setenv("PYCMOR_TMPFS_STAGING", "on")

    observed = {}
    real_rename = os.rename

    def spy_rename(src, dst):
        # At the moment we're about to rename: dst should NOT yet exist
        # under its final name; src is the .tmp marker.
        observed["final_existed_before_rename"] = os.path.exists(dst)
        observed["src_path"] = src
        observed["dst_path"] = dst
        return real_rename(src, dst)

    with patch.object(files.os, "rename", side_effect=spy_rename):
        files._atomic_to_netcdf(ds, str(final), mode="w", format="NETCDF4")

    assert observed["final_existed_before_rename"] is False
    assert observed["src_path"] == str(final) + ".tmp"
    assert observed["dst_path"] == str(final)
    assert final.exists()
