"""A repacked file that pyfive cannot parse gets one nccopy rewrite (cli127 FILE004a)."""

import os

from pycmor.std_lib import qc


def _tool(tmp_path, name, body):
    p = tmp_path / name
    p.write_text("#!/bin/sh\n" + body)
    p.chmod(0o755)
    return str(p)


def _setup(tmp_path):
    fp = tmp_path / "difvso_g131.nc"
    fp.write_text("original")
    log = tmp_path / "calls"
    repack = _tool(tmp_path, "cmip7repack", f'echo "repack $2" >> {log}\n')
    nccopy = _tool(tmp_path, "nccopy", f'echo "nccopy $1 $2" >> {log}\necho rewritten > "$2"\n')
    return fp, log, repack, nccopy


def test_overrun_file_is_rewritten(tmp_path, monkeypatch):
    fp, log, repack, nccopy = _setup(tmp_path)
    monkeypatch.setattr(qc, "_pyfive_overrun", lambda p: p.name == fp.name)
    qc._run_cmip7repack(repack, [fp], nccopy)
    assert fp.read_text() == "rewritten\n"
    assert log.read_text().split("\n")[:3] == [
        f"repack {fp}",
        f"nccopy {fp} {fp}_nccopy",
        f"repack {fp}_nccopy",
    ]
    assert not (tmp_path / (fp.name + "_nccopy")).exists()


def test_file_kept_when_the_copy_still_overruns(tmp_path, monkeypatch):
    fp, log, repack, nccopy = _setup(tmp_path)
    monkeypatch.setattr(qc, "_pyfive_overrun", lambda p: True)
    qc._run_cmip7repack(repack, [fp], nccopy)
    assert fp.read_text() == "original"
    assert not (tmp_path / (fp.name + "_nccopy")).exists()


def test_parseable_file_is_not_touched(tmp_path, monkeypatch):
    fp, log, repack, nccopy = _setup(tmp_path)
    monkeypatch.setattr(qc, "_pyfive_overrun", lambda p: False)
    qc._run_cmip7repack(repack, [fp], nccopy)
    assert fp.read_text() == "original"
    assert log.read_text() == f"repack {fp}\n"


def test_nccopy_next_to_the_plugin_path(tmp_path, monkeypatch):
    netcdf = tmp_path / "netcdf-c"
    (netcdf / "plugins").mkdir(parents=True)
    (netcdf / "bin").mkdir()
    _tool(netcdf / "bin", "nccopy", "")
    monkeypatch.setenv("HDF5_PLUGIN_PATH", str(netcdf / "plugins"))
    assert qc._nccopy_binary() == str(netcdf / "bin" / "nccopy")
    assert qc._nccopy_binary("/x/nccopy") == "/x/nccopy"


def test_other_pyfive_errors_are_left_to_the_checker(tmp_path):
    fp = tmp_path / "not_hdf5.nc"
    fp.write_bytes(os.urandom(64))
    assert qc._pyfive_overrun(fp) is False
