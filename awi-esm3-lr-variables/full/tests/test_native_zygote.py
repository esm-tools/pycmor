"""A forked conversion must look to the launcher exactly like a fresh interpreter."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

import native_zygote as z

LAUNCHER = textwrap.dedent("""
    import json, os, sys, tempfile
    IMPORTED = os.environ.get('MARK')
    if __name__ == '__main__':
        print('stdout line', flush=True)
        print('stderr line', file=sys.stderr, flush=True)
        mode = sys.argv[2] if len(sys.argv) > 2 else ''
        if mode == 'sleep':
            import time; time.sleep(60)
        if mode == 'raise':
            raise RuntimeError('boom')
        if mode == 'kill':
            os.kill(os.getpid(), 9)
        state = dict(argv=sys.argv, cwd=os.getcwd(), path0=sys.path[0], mark=os.environ.get('MARK'),
                     tmp=tempfile.gettempdir(), file=__file__, stdin=sys.stdin.read())
        print(json.dumps(state), flush=True)
        sys.exit(int(mode) if mode.isdigit() else 0)
""")


class ForkLauncherTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)
        (self.base / "bin").mkdir()
        self.launcher = self.base / "bin/launcher.py"
        self.launcher.write_text(LAUNCHER)
        self.out = open(os.devnull, "w")
        self.addCleanup(self.out.close)

    def env(self, name):
        scratch = self.base / name
        (scratch / "tmp").mkdir(parents=True)
        return scratch, dict(
            PATH=os.environ.get("PATH", ""),
            MARK="same",
            TMPDIR=str(scratch / "tmp"),
            PREFECT_HOME=str(scratch / "prefect"),
        )

    def run_both(self, runner, name, mode):
        """Run once through the forked launcher and once as a plain subprocess."""
        results = []
        for kind in ("fork", "subprocess"):
            scratch, env = self.env(f"{name}-{kind}")
            log = scratch / "conversion.log"
            argv = [sys.executable, str(self.launcher), "process", mode]
            with log.open("w") as stream:
                print("Converting", file=stream, flush=True)
                if kind == "fork":
                    result = runner.run(argv, env, scratch, str(log), 30)
                    stream.seek(0, os.SEEK_END)
                else:
                    result = subprocess.run(
                        argv,
                        env=env,
                        cwd=scratch,
                        stdout=stream,
                        stderr=subprocess.STDOUT,
                        stdin=subprocess.DEVNULL,
                        timeout=30,
                    )
                print("after", file=stream, flush=True)
            text = log.read_text().replace(f"{name}-{kind}", name)
            results.append((result.returncode, text, (scratch / "prefect").is_dir()))
        return results

    def test_child_sees_fresh_interpreter_state_and_exit_codes(self):
        with z.ForkLauncher(out=self.out) as runner:
            for name, mode in [("ok", "0"), ("code", "3"), ("error", "raise"), ("killed", "kill")]:
                with self.subTest(mode=mode):
                    forked, fresh = self.run_both(runner, name, mode)
                    self.assertEqual(forked[0], fresh[0])
                    self.assertTrue(forked[2])
                    if mode == "raise":
                        self.assertIn("RuntimeError: boom", forked[1])
                        self.assertTrue(forked[1].rstrip().endswith("after"))
                    else:
                        self.assertEqual(forked[1], fresh[1])
            self.assertFalse(runner.disabled)

    def test_timeout_kills_only_that_product(self):
        scratch, env = self.env("slow")
        argv = [sys.executable, str(self.launcher), "process", "sleep"]
        with z.ForkLauncher(out=self.out) as runner:
            with self.assertRaises(subprocess.TimeoutExpired) as caught:
                runner.run(argv, env, scratch, str(scratch / "log"), 1)
            self.assertEqual(caught.exception.cmd, argv)
            self.assertEqual(caught.exception.timeout, 1)
            scratch, env = self.env("next")
            argv[-1] = "0"
            self.assertEqual(runner.run(argv, env, scratch, str(scratch / "log"), 30).returncode, 0)

    def test_changed_settings_start_a_new_interpreter(self):
        with z.ForkLauncher(out=self.out) as runner:
            scratch, env = self.env("first")
            argv = [sys.executable, str(self.launcher), "process", "0"]
            runner.run(argv, env, scratch, str(scratch / "log"), 30)
            first = runner._process.pid
            scratch, env = self.env("second")
            runner.run(argv, env, scratch, str(scratch / "log"), 30)
            self.assertEqual(runner._process.pid, first)
            scratch, env = self.env("third")
            env["MARK"] = "other"
            runner.run(argv, env, scratch, str(scratch / "log"), 30)
            self.assertNotEqual(runner._process.pid, first)
            state = json.loads((scratch / "log").read_text().splitlines()[-1])
            self.assertEqual(state["mark"], "other")

    def test_unloadable_launcher_falls_back_to_subprocess(self):
        broken = self.base / "bin/broken.py"
        broken.write_text("raise SystemExit('cannot preload')\nprint('direct')\n")
        scratch, env = self.env("broken")
        with z.ForkLauncher(out=self.out) as runner:
            result = runner.run([sys.executable, str(broken), "process"], env, scratch, str(scratch / "log"), 30)
            self.assertTrue(runner.disabled)
        self.assertEqual(result.returncode, 1)
        self.assertIn("cannot preload", (scratch / "log").read_text())


if __name__ == "__main__":
    unittest.main()
