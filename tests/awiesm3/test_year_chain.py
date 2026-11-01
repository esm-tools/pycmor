"""run_year_chain.sbatch against a fake SLURM: who submits what, and when."""

import json
import os
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture
def chain(tmp_path):
    """A campaign whose driver and SLURM commands only record their calls."""
    code = tmp_path / "code" / "examples"
    code.mkdir(parents=True)
    shutil.copy(REPO / "examples" / "run_year_chain.sbatch", code)
    (tmp_path / "campaign.yaml").write_text("name: t\n")
    (code / "campaign.py").write_text(
        textwrap.dedent(
            """
            import sys
            if sys.argv[1] == "env":
                print(open(sys.argv[2].replace("campaign.yaml", "env.sh")).read())
            """
        )
    )
    (code / "run_year_driver.py").write_text(
        textwrap.dedent(
            f"""
            import json, os, sys
            with open({str(tmp_path / "calls")!r}, "a") as fh:
                fh.write(json.dumps(["driver", sys.argv[2], sys.argv[3:]]) + "\\n")
            sys.exit(int(os.environ.get("DRIVER_RC", "0")))
            """
        )
    )
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    (bin_ / "sbatch").write_text(
        f'#!/bin/bash\necho "sbatch $*" >> {tmp_path / "sbatch"}\necho 999\nexit ${{SBATCH_RC:-0}}\n'
    )
    (bin_ / "squeue").write_text('#!/bin/bash\n[ "$2" = -j ] && [ "$3" = "$QUEUED" ] && echo "$3"\nexit 0\n')
    os.chmod(bin_ / "sbatch", 0o755)
    os.chmod(bin_ / "squeue", 0o755)
    (tmp_path / "conda.sh").write_text("conda() { :; }\n")
    work = tmp_path / "work"
    (tmp_path / "env.sh").write_text(
        textwrap.dedent(
            f"""
            export JOB_TAG=t LOGS_DIR={tmp_path} WORKROOT={work} OUTPUT_ROOT=/out RUN_ROOT=/run
            export ACCOUNT=ab0246 MAIL_USER=a@b LAST_YEAR=1855 PYCMOR_CODE_COMMIT=abc PYCMOR_DRS_VERSION=v1
            export DRIVER_ATTEMPTS=3 FIRST_YEAR_ATTEMPTS=2 MAX_CONSECUTIVE_FAILS=2
            export OVERLAP=${{TEST_OVERLAP:-no}}
            """
        )
    )

    def run(year, **env):
        full = {
            **os.environ,
            "PATH": f"{bin_}:{os.environ['PATH']}",
            "PYCMOR_CONDA_INIT": str(tmp_path / "conda.sh"),
            "SLURM_JOB_ID": "50",
            **env,
        }
        res = subprocess.run(
            ["bash", str(code / "run_year_chain.sbatch"), str(tmp_path / "campaign.yaml"), str(year)],
            env=full,
            capture_output=True,
            text=True,
        )
        calls = tmp_path / "calls"
        driver = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
        sb = tmp_path / "sbatch"
        return res, driver, sb.read_text().splitlines() if sb.exists() else []

    run.work = work
    return run


def _fail(chain, *years):
    for y in years:
        (chain.work / f"y{y}").mkdir(parents=True)
        (chain.work / f"y{y}" / f"{y}.FAILED").write_text("no\n")


def test_without_overlap_the_next_year_is_submitted_at_the_end(chain):
    res, driver, sbatch = chain(1851, TEST_OVERLAP="no", QUEUED="40", PREV_JOB="40")
    assert res.returncode == 0
    assert driver == [["driver", "1851", driver[0][2]]]
    assert "--previous-workdir" not in driver[0][2]
    assert len(sbatch) == 1 and "--dependency" not in sbatch[0]


def test_the_first_year_of_a_launch_runs_alone(chain):
    res, driver, sbatch = chain(1850, TEST_OVERLAP="yes", CHAIN_START="1")
    assert res.returncode == 0
    assert "--previous-workdir" not in driver[0][2]
    assert len(sbatch) == 1 and "--dependency" not in sbatch[0] and "PREV_JOB=50" in sbatch[0]


def test_overlap_holds_the_next_year_on_the_job_before_this_one(chain):
    res, driver, sbatch = chain(1852, TEST_OVERLAP="yes", QUEUED="40", PREV_JOB="40", CHAIN_FIRST="1850")
    assert res.returncode == 0
    assert len(sbatch) == 1 and "--dependency=afterany:40" in sbatch[0] and "PREV_JOB=50" in sbatch[0]
    args = driver[0][2]
    assert args[args.index("--previous-workdir") + 1].endswith("work/y1851")
    assert args[args.index("--after-job") + 1] == "40"


def test_a_job_already_gone_is_no_dependency(chain):
    _, _, sbatch = chain(1852, TEST_OVERLAP="yes", QUEUED="1", PREV_JOB="40", CHAIN_FIRST="1850")
    assert "--dependency" not in sbatch[0]


def test_the_last_year_submits_nothing(chain):
    res, driver, sbatch = chain(1855, TEST_OVERLAP="yes", QUEUED="40", PREV_JOB="40", CHAIN_FIRST="1850")
    assert res.returncode == 0 and len(driver) == 1 and sbatch == []


def test_a_failed_year_does_not_submit_again_under_overlap(chain):
    res, _, sbatch = chain(1852, TEST_OVERLAP="yes", QUEUED="40", PREV_JOB="40", DRIVER_RC="1", CHAIN_FIRST="1850")
    assert res.returncode == 1 and len(sbatch) == 1


def test_two_failed_years_behind_halt_the_chain_before_it_runs(chain):
    _fail(chain, 1850, 1851)
    res, driver, sbatch = chain(1852, TEST_OVERLAP="yes", QUEUED="40", PREV_JOB="40", CHAIN_FIRST="1850")
    assert "HALTING CHAIN" in res.stdout and driver == [] and sbatch == []


def test_failed_years_before_this_launch_do_not_count(chain):
    _fail(chain, 1850, 1851)
    res, driver, _ = chain(1852, TEST_OVERLAP="yes", QUEUED="40", PREV_JOB="40", CHAIN_FIRST="1852")
    assert "HALTING" not in res.stdout and len(driver) == 1


def test_a_failed_submit_still_runs_the_year_and_reports_it(chain):
    res, driver, _ = chain(1852, TEST_OVERLAP="yes", QUEUED="40", PREV_JOB="40", SBATCH_RC="1", CHAIN_FIRST="1850")
    assert len(driver) == 1 and res.returncode == 3
