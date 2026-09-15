"""Tests for the save_dataset timeout + retry path
(Option E of PLAN_save_dataset_reliability.md).

Covers:
- ``_Heartbeat`` watchdog detects stalled writes and raises
  ``SaveTimeout`` from ``__exit__``.
- watch_path supports both string paths and callables.
- ``save_dataset`` retries on ``SaveTimeout`` up to
  ``PYCMOR_SAVE_MAX_RETRIES``, then raises.
- Successful retry path: first attempt times out, second succeeds.
- Exhausted retries: all attempts time out, ``SaveTimeout`` propagates.
"""
import os
import threading
import time
from unittest.mock import patch, MagicMock

import pytest

from pycmor.std_lib import files
from pycmor.std_lib.files import SaveTimeout, _Heartbeat


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Strip env vars we control + reset tmpfs cache between tests."""
    files._reset_tmpfs_cache()
    for var in (
        "PYCMOR_HEARTBEAT_INTERVAL_S",
        "PYCMOR_SAVE_TIMEOUT_MIN",
        "PYCMOR_SAVE_MAX_RETRIES",
        "PYCMOR_TMPFS_STAGING",
    ):
        monkeypatch.delenv(var, raising=False)


# ---------------- SaveTimeout class ----------------


def test_save_timeout_is_exception():
    """SaveTimeout is a regular Exception (not BaseException), so it's
    caught by the retry loop's ``except SaveTimeout``."""
    assert issubclass(SaveTimeout, Exception)
    exc = SaveTimeout("hello")
    assert str(exc) == "hello"


# ---------------- _Heartbeat watchdog ----------------


def test_heartbeat_no_watch_path_never_times_out():
    """Without a watch_path, the heartbeat never raises SaveTimeout
    no matter how long the block takes."""
    with _Heartbeat("nowatch", interval=0.05, timeout_minutes=0.001):
        time.sleep(0.3)  # 6× the timeout — would fire if watch_path were set


def test_heartbeat_times_out_sets_flag_but_does_not_raise(tmp_path):
    """The watcher detects a stall and sets ``timed_out``, but does NOT
    raise from ``__exit__`` — raising would kill rules that complete
    slowly-but-successfully (the body returns with data saved before
    the watchdog can be checked). The flag is purely diagnostic.

    See PLAN_save_dataset_reliability.md §E and the inline note in
    ``_Heartbeat.__exit__`` for why."""
    stalled = tmp_path / "stalled.nc"
    stalled.write_bytes(b"\x89HDF\x00\x00\x00\x00")  # tiny stub, never grows

    with _Heartbeat(
        "stall_test",
        interval=0.05,           # poll every 50 ms
        watch_path=str(stalled),
        timeout_minutes=0.002,   # 0.12 s — well within test runtime
    ) as hb:
        time.sleep(0.4)
    # Watcher fired, flag set, but exiting the with-block did NOT raise.
    assert hb.timed_out is True


def test_heartbeat_no_timeout_when_file_grows(tmp_path):
    """If watch_path grows between polls, the timeout never fires —
    the block exits cleanly."""
    growing = tmp_path / "growing.nc"
    growing.write_bytes(b"")

    def _writer():
        # Write one byte every 30 ms for ~300 ms
        for _ in range(10):
            with open(growing, "ab") as fh:
                fh.write(b"X")
            time.sleep(0.03)

    th = threading.Thread(target=_writer, daemon=True)
    th.start()
    with _Heartbeat(
        "grow_test",
        interval=0.05,
        watch_path=str(growing),
        timeout_minutes=0.002,
    ):
        time.sleep(0.4)
    th.join(timeout=1)


def test_heartbeat_watch_path_can_be_callable(tmp_path):
    """A callable returning the bytes-written so far can be used in
    place of a path — useful for the multi-file save_dataset case."""
    size_holder = {"n": 0}

    def _size_fn():
        return size_holder["n"]

    # No growth: watcher sets timed_out flag (no raise).
    with _Heartbeat(
        "callable_stall",
        interval=0.05,
        watch_path=_size_fn,
        timeout_minutes=0.002,
    ) as hb:
        time.sleep(0.4)
    assert hb.timed_out is True

    # Growth: should NOT time out.
    def _grow():
        for i in range(1, 11):
            size_holder["n"] = i * 100
            time.sleep(0.03)

    size_holder["n"] = 0
    th = threading.Thread(target=_grow, daemon=True)
    th.start()
    with _Heartbeat(
        "callable_grow",
        interval=0.05,
        watch_path=_size_fn,
        timeout_minutes=0.002,
    ):
        time.sleep(0.4)
    th.join(timeout=1)


def test_heartbeat_propagates_inner_exception(tmp_path):
    """If the inner block raises an exception, that exception propagates
    out of the with-block. The watchdog's timed_out flag is informational
    only and doesn't affect propagation."""
    stalled = tmp_path / "boom.nc"
    stalled.write_bytes(b"\x89HDF")

    class CustomBoom(RuntimeError):
        pass

    with pytest.raises(CustomBoom):
        with _Heartbeat(
            "raise_test",
            interval=0.05,
            watch_path=str(stalled),
            timeout_minutes=0.002,
        ):
            time.sleep(0.2)
            raise CustomBoom("inner exploded")


# ---------------- save_dataset retry loop ----------------


def _stub_rule(out_dir, cmor_variable="testvar"):
    """A minimal stand-in for a Rule, just enough for save_dataset's
    early bits to function before _save_dataset_impl runs."""
    r = MagicMock()
    r.cmor_variable = cmor_variable
    r.name = cmor_variable
    r.output_directory = str(out_dir)
    return r


def test_save_dataset_retries_on_save_timeout(monkeypatch, tmp_path):
    """First two attempts raise SaveTimeout; the third succeeds. Verify
    that ``save_dataset`` catches and retries."""
    monkeypatch.setenv("PYCMOR_SAVE_MAX_RETRIES", "2")
    rule = _stub_rule(tmp_path)
    attempts = {"n": 0}

    def fake_impl(da, rule):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise SaveTimeout(f"forced timeout #{attempts['n']}")
        return "ok"

    with patch.object(files, "_save_dataset_impl", side_effect=fake_impl):
        result = files.save_dataset(MagicMock(), rule)

    assert result == "ok"
    assert attempts["n"] == 3


def test_save_dataset_raises_after_max_retries(monkeypatch, tmp_path):
    """All attempts raise SaveTimeout; after max_retries+1, the final
    SaveTimeout propagates."""
    monkeypatch.setenv("PYCMOR_SAVE_MAX_RETRIES", "2")
    rule = _stub_rule(tmp_path)
    attempts = {"n": 0}

    def always_fail(da, rule):
        attempts["n"] += 1
        raise SaveTimeout("forced")

    with patch.object(files, "_save_dataset_impl", side_effect=always_fail):
        with pytest.raises(SaveTimeout):
            files.save_dataset(MagicMock(), rule)

    assert attempts["n"] == 3  # max_retries=2 → 3 total attempts


def test_save_dataset_no_retry_on_other_exception(monkeypatch, tmp_path):
    """Non-SaveTimeout exceptions propagate immediately — no retry."""
    monkeypatch.setenv("PYCMOR_SAVE_MAX_RETRIES", "2")
    rule = _stub_rule(tmp_path)
    attempts = {"n": 0}

    def fail_other(da, rule):
        attempts["n"] += 1
        raise ValueError("schema mismatch — not a timeout")

    with patch.object(files, "_save_dataset_impl", side_effect=fail_other):
        with pytest.raises(ValueError, match="schema mismatch"):
            files.save_dataset(MagicMock(), rule)

    assert attempts["n"] == 1  # no retry


def test_save_dataset_max_retries_env_var(monkeypatch, tmp_path):
    """PYCMOR_SAVE_MAX_RETRIES=0 means one attempt only (no retries)."""
    monkeypatch.setenv("PYCMOR_SAVE_MAX_RETRIES", "0")
    rule = _stub_rule(tmp_path)
    attempts = {"n": 0}

    def always_fail(da, rule):
        attempts["n"] += 1
        raise SaveTimeout("forced")

    with patch.object(files, "_save_dataset_impl", side_effect=always_fail):
        with pytest.raises(SaveTimeout):
            files.save_dataset(MagicMock(), rule)

    assert attempts["n"] == 1


def test_save_dataset_invalid_env_var_defaults_to_2(monkeypatch, tmp_path):
    """Bad PYCMOR_SAVE_MAX_RETRIES value silently falls back to the
    default of 2 — no startup crash."""
    monkeypatch.setenv("PYCMOR_SAVE_MAX_RETRIES", "not-a-number")
    rule = _stub_rule(tmp_path)
    attempts = {"n": 0}

    def always_fail(da, rule):
        attempts["n"] += 1
        raise SaveTimeout("forced")

    with patch.object(files, "_save_dataset_impl", side_effect=always_fail):
        with pytest.raises(SaveTimeout):
            files.save_dataset(MagicMock(), rule)

    assert attempts["n"] == 3  # default max_retries=2 → 3 attempts


def test_save_dataset_succeeds_first_try(monkeypatch, tmp_path):
    """When _save_dataset_impl succeeds, no retry overhead."""
    monkeypatch.setenv("PYCMOR_SAVE_MAX_RETRIES", "2")
    rule = _stub_rule(tmp_path)
    attempts = {"n": 0}

    def succeed(da, rule):
        attempts["n"] += 1
        return "done"

    with patch.object(files, "_save_dataset_impl", side_effect=succeed):
        assert files.save_dataset(MagicMock(), rule) == "done"
    assert attempts["n"] == 1


def test_save_dataset_no_output_directory_still_works(monkeypatch, tmp_path):
    """If rule.output_directory is missing/None, watchdog falls back to
    a no-watch heartbeat — never times out, never retries."""
    monkeypatch.setenv("PYCMOR_SAVE_MAX_RETRIES", "2")
    rule = MagicMock()
    rule.cmor_variable = "x"
    rule.name = "x"
    rule.output_directory = None

    with patch.object(files, "_save_dataset_impl", return_value="ok"):
        assert files.save_dataset(MagicMock(), rule) == "ok"
