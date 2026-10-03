"""Socket-free controls for smoke failure propagation and exit-code admission."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('smoke.sh').resolve()


class SmokeGuards(unittest.TestCase):
    def run_smoke(self, **overrides):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = {
                'gst-inspect-1.0': '''#!/usr/bin/env bash
if [[ ${INSPECT_RC:-0} != 0 ]]; then exit "$INSPECT_RC"; fi
if [[ $1 == moqsink ]]; then
  [[ ${MISSING_PAD:-0} == 0 ]] && echo 'video_%u audio_%u'
else echo 'MoQ source'; fi
exit 0
''',
                'timeout': '''#!/usr/bin/env bash
shift
exec "$@"
''',
                'gst-launch-1.0': '''#!/usr/bin/env bash
if [[ $* == *mp4mux* ]]; then exit "${LAUNCH_RC:-0}"; fi
exit "${FALLBACK_RC:-0}"
''',
                'python3': '''#!/usr/bin/env bash
if [[ $1 == -c ]]; then exit "${PROBE_RC:-0}"; fi
cat >/dev/null
exit "${CYCLE_RC:-0}"
''',
            }
            for name, text in scripts.items():
                path = root/name; path.write_text(text); path.chmod(0o755)
            env = dict(os.environ, PATH=str(root)+os.pathsep+os.environ['PATH'], **overrides)
            return subprocess.run(['bash', str(SCRIPT), str(root)], env=env,
                                  capture_output=True, text=True, timeout=5)

    def test_success_and_expected_element_error(self):
        for rc in ('0', '1'):
            with self.subTest(rc=rc):
                result = self.run_smoke(LAUNCH_RC=rc)
                self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
                self.assertIn('smoke OK', result.stdout)

    def test_inspection_failure_and_missing_pad(self):
        for config in (dict(INSPECT_RC='1'), dict(MISSING_PAD='1')):
            with self.subTest(config=config):
                result = self.run_smoke(**config)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn('smoke OK', result.stdout)

    def test_bad_launch_statuses(self):
        for rc in ('2', '124', '125', '127', '139'):
            with self.subTest(rc=rc):
                result = self.run_smoke(LAUNCH_RC=rc)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn('smoke OK', result.stdout)

    def test_state_cycle_failure(self):
        for rc in ('1', '124', '139'):
            with self.subTest(rc=rc):
                result = self.run_smoke(CYCLE_RC=rc)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn('smoke OK', result.stdout)

    def test_fallback_statuses(self):
        for rc in ('0', '1', '2', '124', '125', '139'):
            with self.subTest(rc=rc):
                result = self.run_smoke(PROBE_RC='1', FALLBACK_RC=rc)
                self.assertEqual(result.returncode == 0, rc in ('0', '1'))
                self.assertEqual('smoke OK' in result.stdout, rc in ('0', '1'))


if __name__ == '__main__':
    unittest.main()
