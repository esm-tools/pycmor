"""Packing products into array tasks changes where they run, never what runs."""

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import full_cmorization as f
import retest_all_products as r
import task_batches as b


class PackingTests(unittest.TestCase):
    def test_group_size_one_keeps_every_product_alone_and_in_order(self):
        costs = [("a", 10), ("b", 900), ("c", 20)]
        self.assertEqual(b.plan_batches(costs, 1, 300), [(["a"], 10), (["b"], 900), (["c"], 20)])

    def test_big_products_alone_small_ones_packed_longest_first(self):
        costs = [("fx", 15), ("zooc", 1000), ("mon1", 20), ("ua6hr", 900), ("mon2", 20), ("day", 280)]
        groups = b.plan_batches(costs, 10, 300)
        self.assertEqual(groups[0], (["zooc"], 1000))
        self.assertEqual(groups[1], (["ua6hr"], 900))
        self.assertEqual(sorted(i for items, _ in groups for i in items), sorted(c for c, _ in costs))
        self.assertTrue(all(seconds <= 300 for items, seconds in groups if len(items) > 1))
        self.assertEqual([s for _, s in groups], sorted((s for _, s in groups), reverse=True))

    def test_product_count_limit(self):
        groups = b.plan_batches([(i, 1) for i in range(25)], 10, 300)
        self.assertEqual([len(items) for items, _ in groups], [10, 10, 5])

    def test_invalid_limits(self):
        with self.assertRaises(ValueError):
            b.plan_batches([("a", 1)], 0, 300)

    def test_expected_cost_classes(self):
        self.assertGreater(b.expected_seconds("ocnBgchem.zooc.tavg-ol-hxy-sea.day.glb"), 300)
        self.assertGreater(b.expected_seconds("atmos.ua.tpt-al-hxy-u.6hr.glb"), 300)
        self.assertLess(b.expected_seconds("land.orog.ti-u-hxy-u.fx.glb"), 60)
        self.assertLess(b.expected_seconds("aerosol.emibbbc.tavg-u-hxy-u.mon.glb"), 60)

    def test_history_uses_longest_recorded_time_per_compound(self):
        with tempfile.TemporaryDirectory() as directory:
            campaign = Path(directory)
            f.write(
                campaign / "status/3150__atmos.ua.tpt-al-hxy-u.6hr.glb.json",
                dict(
                    key="3150__atmos.ua.tpt-al-hxy-u.6hr.glb",
                    started="2026-11-25T19:00:00+00:00",
                    finished="2026-11-25T19:10:00+00:00",
                ),
            )
            f.write(
                campaign / "representative_test/status/3150__atmos.ua.tpt-al-hxy-u.6hr.glb.json",
                dict(
                    compound="atmos.ua.tpt-al-hxy-u.6hr.glb",
                    started="2026-11-25T19:00:00+00:00",
                    finished="2026-11-25T19:16:00+00:00",
                ),
            )
            f.write(campaign / "status/broken.json", dict(key="3150__x", status="FAILED", error="no times"))
            self.assertEqual(b.load_history([campaign]), {"atmos.ua.tpt-al-hxy-u.6hr.glb": 960.0})


def tasks(*compounds):
    return [dict(compound=c, key=f"3150__{c}", year=3150) for c in compounds]


class SubmissionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.campaign = Path(self.directory.name)
        self.compounds = [
            "land.orog.ti-u-hxy-u.fx.glb",
            "ocnBgchem.zooc.tavg-ol-hxy-sea.day.glb",
            "aerosol.emibbbc.tavg-u-hxy-u.mon.glb",
            "atmos.ua.tpt-al-hxy-u.6hr.glb",
        ]
        self.config = dict(python="python", concurrency=19, shards=192)

    def test_full_workers_pack_tasks_only_when_configured(self):
        f.write(self.campaign / "tasks.json", tasks(*self.compounds))
        for size, expected, array in [(1, ["--shards", "4"], "0-3"), (10, ["--shards", "3", "--batched"], "0-2")]:
            with self.subTest(size=size):
                (self.campaign / "submission.json").unlink(missing_ok=True)
                config = dict(self.config, batch_products=size)
                with patch.object(f, "batch", side_effect=["101", "102"]) as submit:
                    f.submit_workers(self.campaign, config)
                _, command, _, _, extra = submit.call_args_list[0].args
                self.assertTrue(command.endswith(" ".join(expected)))
                self.assertEqual(extra, [f"--array={array}%19"])
        groups = f.read(self.campaign / "worker_batches.json")["groups"]
        self.assertEqual(groups[:2], [[1], [3]])
        self.assertEqual(sorted(groups[2]), [0, 2])

    def test_batched_worker_runs_its_group_and_isolates_failures(self):
        f.write(self.campaign / "tasks.json", tasks(*self.compounds))
        f.write(self.campaign / "worker_batches.json", dict(groups=[[2, 0]]))
        seen = []

        def run_one(campaign, task, config, runner):
            seen.append((task["key"], runner))
            if task["key"].endswith("emibbbc.tavg-u-hxy-u.mon.glb"):
                raise ValueError("fixture failure")
            f.write(campaign / "status" / f"{task['key']}.json", dict(status="PASS"))

        with (
            patch.object(f, "verify_campaign", return_value=dict(batch_products=10)),
            patch.object(f, "run_one", side_effect=run_one),
            patch.object(f, "native_runner", return_value=f.contextlib.nullcontext("runner")),
        ):
            self.assertEqual(f.work(self.campaign, 0, 1, batched=True), 1)
        self.assertEqual(
            seen,
            [("3150__aerosol.emibbbc.tavg-u-hxy-u.mon.glb", "runner"), ("3150__land.orog.ti-u-hxy-u.fx.glb", "runner")],
        )
        failed = f.read(self.campaign / "status/3150__aerosol.emibbbc.tavg-u-hxy-u.mon.glb.json")
        self.assertEqual(
            failed, dict(status="FAILED", error="fixture failure", key="3150__aerosol.emibbbc.tavg-u-hxy-u.mon.glb")
        )

    def test_unbatched_configuration_starts_no_preloaded_interpreter(self):
        with f.native_runner(dict(batch_products=1)) as runner:
            self.assertIsNone(runner)

    def test_validation_array_packs_products_and_keeps_the_gate_list(self):
        dest = self.campaign / "representative_test"
        selected = tasks(*self.compounds)
        for size in (1, 10):
            with self.subTest(size=size):
                for name in ("submission.json", "batches.json"):
                    (dest / name).unlink(missing_ok=True)
                f.write(dest / "manifest.json", dict(selected=selected, config=dict(self.config, batch_products=size)))
                with patch.object(r, "verify"), patch.object(f, "batch", side_effect=["201", "202"]) as submit:
                    r.submit(self.campaign, full_after_pass=True)
                _, command, _, _, extra = submit.call_args_list[0].args
                submission = f.read(dest / "submission.json")
                self.assertEqual(submission["required"], [t["key"] for t in selected])
                if size == 1:
                    self.assertIn(" worker ", command)
                    self.assertEqual(extra, ["--array=0,1,2,3%19"])
                    self.assertFalse((dest / "batches.json").exists())
                else:
                    self.assertIn(" batch-worker ", command)
                    self.assertEqual(extra, ["--array=0-2%19"])
                    groups = f.read(dest / "batches.json")["groups"]
                    self.assertEqual(sorted(i for g in groups for i in g), [0, 1, 2, 3])
                    self.assertEqual(groups[0], [1])

    def test_validation_group_records_every_product(self):
        dest = self.campaign / "representative_test"
        f.write(dest / "manifest.json", dict(selected=tasks(*self.compounds), config=dict(batch_products=10)))
        f.write(dest / "batches.json", dict(groups=[[0, 2, 3]]))
        calls = []

        def worker(campaign, index, runner):
            calls.append((index, runner))
            if index == 2:
                raise OSError("unexpected")
            return int(index == 0)

        with (
            patch.object(r, "worker", side_effect=worker),
            patch.object(f, "native_runner", return_value=f.contextlib.nullcontext("runner")),
        ):
            self.assertEqual(r.batch_worker(self.campaign, 0), 1)
        self.assertEqual(calls, [(0, "runner"), (2, "runner"), (3, "runner")])


class ConfigTests(unittest.TestCase):
    def test_batch_settings_default_to_one_product_per_task(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "site.json"
            base = dict(
                metadata="m.json", cv_dir="cv", mesh_dir="mesh", ghg_dir="ghg", results="results", esgvoc_home="cache"
            )
            f.write(path, base)
            config = f.load_config(path)
            self.assertEqual(
                (config["batch_products"], config["batch_target_seconds"], config["batch_cost_history"]), (1, 300, [])
            )
            f.write(path, dict(base, batch_products=8, batch_cost_history=["old"]))
            config = f.load_config(path)
            self.assertEqual(config["batch_cost_history"], [str(Path(directory).resolve() / "old")])
            for value in (0, True, "8"):
                f.write(path, dict(base, batch_products=value))
                with self.assertRaisesRegex(ValueError, "batch_products"):
                    f.load_config(path)


class NativeRecipeTests(unittest.TestCase):
    def test_forked_runner_receives_the_same_command_and_log_stays_ordered(self):
        import yaml

        name = "ocean.so.tavg-ol-hxy-sea.day.glb"
        rules = [dict(compound_name=name, grid_label=g, pipelines=["p"]) for g in ("g236", "g131")]
        doc = dict(rules=rules, inherit=dict(grid_label="g236"))

        class Runner:
            calls = []

            def run(self, argv, env, cwd, log, timeout):
                self.calls.append((argv, env, cwd, log, timeout))
                with open(log, "a") as stream:
                    stream.write(f"output of {Path(argv[-1]).name}\n")
                return f.subprocess.CompletedProcess(argv, 0)

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            path = base / "recipe.yaml"
            path.write_text(yaml.safe_dump(doc))
            with (base / "log").open("w") as log:
                result = f.run_native_recipe(doc, path, "python", "native_cli.py", base, {"A": "1"}, log, 50, Runner())
            self.assertEqual(result.returncode, 0)
            self.assertEqual(
                (base / "log").read_text().splitlines(),
                [
                    "Converting recipe-g236.yaml",
                    "output of recipe-g236.yaml",
                    "Converting recipe-g131.yaml",
                    "output of recipe-g131.yaml",
                ],
            )
            argv, env, cwd, log, timeout = Runner.calls[0]
            self.assertEqual(argv, ["python", "native_cli.py", "process", str(base / "recipe-g236.yaml")])
            self.assertEqual((env, cwd, log), ({"A": "1"}, base, str(base / "log")))
            self.assertLessEqual(timeout, 50)


if __name__ == "__main__":
    unittest.main()
