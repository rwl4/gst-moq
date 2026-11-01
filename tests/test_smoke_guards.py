"""Socket-free controls for exact startup outcomes and smoke failure propagation."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('smoke.sh').resolve()
SINK = 'ERROR: from element /GstPipeline:pipeline0/GstMoqSink:ms: '
MUX = ('ERROR: from element /GstPipeline:pipeline0/GstMP4Mux:mp4mux0: '
       'Could not multiplex stream.\nAdditional debug info:\nFailed to push sample.')
DEADLINE = 'MoQ sender startup deadline expired (phase=registration)'


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
while [[ ${1:-} == --* ]]; do shift; done
shift
exec "$@"
''',
                'gst-launch-1.0': '''#!/usr/bin/env bash
[[ -z ${BEFORE_ERROR:-} ]] || printf '%s\\n' "$BEFORE_ERROR"
[[ -z ${PRIMARY_ERROR:-} ]] || printf '%s\\n' "$PRIMARY_ERROR"
[[ -z ${AFTER_ERROR:-} ]] || printf '%s\\n' "$AFTER_ERROR"
exit "$LAUNCH_RC"
''',
                'python3': '''#!/usr/bin/env bash
# Run the real oracle's stdin Python; only GI/element operations are stubbed.
if [[ ${4:-} == --startup-outcome ]]; then exec "$GUARD_REAL_PYTHON" "$@"; fi
if [[ $1 == -c ]]; then exit "${PROBE_RC:-0}"; fi
cat >/dev/null
if [[ ${2:-} == --construct ]]; then exit "${CONSTRUCTION_RC:-0}"; fi
exit "${CYCLE_RC:-0}"
''',
            }
            for name, text in scripts.items():
                path = root/name
                path.write_text(text)
                path.chmod(0o755)
            env = dict(os.environ, PATH=str(root)+os.pathsep+os.environ['PATH'])
            env.update(GUARD_REAL_PYTHON=sys.executable, LAUNCH_RC='1',
                       PRIMARY_ERROR=SINK+DEADLINE, BEFORE_ERROR='', AFTER_ERROR='')
            env.update(overrides)
            env.pop('GST_MOQ_PYTHON', None)
            return subprocess.run(['bash', str(SCRIPT), str(root)], env=env,
                                  capture_output=True, text=True, timeout=5)

    def assert_passes(self, **config):
        result = self.run_smoke(**config)
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertIn('bounded startup outcome OK:', result.stdout)
        self.assertIn('smoke OK', result.stdout)

    def assert_fails(self, **config):
        result = self.run_smoke(**config)
        self.assertNotEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertNotIn('smoke OK', result.stdout)

    def test_exact_deadlines_and_native_closed(self):
        messages = ['MoQ sender startup deadline expired (phase='+phase+')'
                    for phase in ('registration', 'readiness')]
        messages += ['MoQ sender startup failed (phase='+phase+', rc=-4)'
                     for phase in ('connect', 'registration', 'readiness')]
        for message in messages:
            for extra in ('', MUX):
                with self.subTest(message=message, extra=extra):
                    self.assert_passes(PRIMARY_ERROR=SINK+message, AFTER_ERROR=extra)

    def test_rejects_wrong_phase_native_code_and_generic_errors(self):
        for message in (
            'MoQ sender startup deadline expired (phase=connect)',
            'MoQ sender startup deadline expired (phase=unknown)',
            'MoQ sender startup deadline expired',
            'MoQ sender startup failed (phase=registration, rc=1)',
            'MoQ sender startup failed (phase=readiness, rc=-2)',
            'MoQ sender startup failed (phase=registration, rc=-1)',
            'MoQ sender startup failed (phase=connect, rc=-14)',
            'MoQ sender startup failed (phase=readiness, rc=-5)',
            'MoQ sender startup failed (phase=unknown, rc=-4)',
            'could not prepare MoQ sender (rc=1)',
            'could not prepare MoQ sender (rc=-4)',
        ):
            with self.subTest(message=message):
                self.assert_fails(PRIMARY_ERROR=SINK+message)

    def test_rejects_additional_or_untied_bus_errors(self):
        for config in (
            dict(BEFORE_ERROR=MUX),
            dict(AFTER_ERROR=SINK+'incomplete fragmented MP4 at EOS on pad video_0'),
            dict(AFTER_ERROR='ERROR: unrelated pipeline failure'),
            dict(AFTER_ERROR=MUX.replace('Failed to push sample.', 'Invalid timestamps.')),
            dict(AFTER_ERROR=MUX.replace('GstMP4Mux:mp4mux0', 'GstMP4Mux:another')),
            dict(AFTER_ERROR=MUX+'\n'+SINK+DEADLINE),
            dict(AFTER_ERROR=SINK+DEADLINE),
        ):
            with self.subTest(config=config):
                self.assert_fails(**config)

    def test_construction_failure_and_unclassified_exit_one(self):
        for config in (dict(CONSTRUCTION_RC='1'), dict(PRIMARY_ERROR=''),
                       dict(PRIMARY_ERROR='ERROR: pipeline could not be constructed: no element x264enc')):
            with self.subTest(config=config):
                self.assert_fails(**config)

    def test_bad_launch_statuses_including_unproved_success(self):
        for rc in ('0', '2', '124', '125', '127', '137', '139'):
            with self.subTest(rc=rc):
                self.assert_fails(LAUNCH_RC=rc)

    def test_inspection_failure_and_missing_pad(self):
        for config in (dict(INSPECT_RC='1'), dict(MISSING_PAD='1')):
            with self.subTest(config=config):
                self.assert_fails(**config)

    def test_state_cycle_failure(self):
        for rc in ('1', '124', '139'):
            with self.subTest(rc=rc):
                self.assert_fails(CYCLE_RC=rc)

    def test_missing_pygobject_cannot_claim_state_cycle_success(self):
        self.assert_fails(PROBE_RC='1')


if __name__ == '__main__':
    unittest.main()
