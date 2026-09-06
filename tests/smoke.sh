#!/usr/bin/env bash
# Smoke test: the plugin loads, both elements inspect cleanly, a CMAF
# publish pipeline runs against an unreachable relay without crashing, and
# a state cycle on moqsink does not return FAILURE or hang.
#
# Neither run reaches a real relay (127.0.0.1:1 refuses the connection), so
# this never asserts anything about the wire protocol -- the real LOC/CMAF
# path is exercised against a live relay in the regression check described
# in docs/relay-playback-debugging.md. What this test guards is: the
# element does not crash, deadlock, or trip a GLib critical while a
# connection attempt fails and the pipeline is torn down.
set -uo pipefail
export GST_PLUGIN_PATH="$1"

# Capture full output before grepping: `producer | grep -q pattern` under
# pipefail can report failure via SIGPIPE on the producer even when the
# pattern is found, if grep exits (on first match) before the producer has
# finished writing.
moqsink_info="$(gst-inspect-1.0 moqsink)"
echo "$moqsink_info" | grep -q "video_%u"
echo "$moqsink_info" | grep -q "audio_%u"
moqsrc_info="$(gst-inspect-1.0 moqsrc)"
echo "$moqsrc_info" | grep -q "MoQ source"

# -- 1. CMAF publish pipeline against a refused connection ------------------
#
# moq_endpoint_connect() does not fail synchronously against a closed port
# on this stack (see the historical note this replaced), so the pipeline is
# expected to either run to completion/EOS (exit 0) or fail with a
# gst-launch-reported element error (exit 1) once buffers start flowing and
# the sender notices the endpoint is dead. What must never happen is a
# crash or an unhandled signal (exit code >= 128), and it must never hang
# (the `timeout` wrapper turns a hang into exit 124, which is treated as a
# failure below).
echo "-- CMAF publish smoke --"
G_DEBUG=fatal-criticals timeout 60 gst-launch-1.0 -e moqsink name=ms \
    host=127.0.0.1 port=1 relay-path=/moq \
    videotestsrc num-buffers=30 ! x264enc tune=zerolatency key-int-max=30 \
    ! h264parse ! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss \
    ! ms.video_0
rc=$?
echo "CMAF publish pipeline exit code: $rc"
if [ "$rc" -eq 124 ]; then
  echo "FAIL: CMAF publish pipeline timed out (hang)"
  exit 1
fi
if [ "$rc" -ge 128 ]; then
  echo "FAIL: CMAF publish pipeline died on a signal (exit $rc)"
  exit 1
fi

# -- 2. moqsink state cycle --------------------------------------------------
#
# A plain `videotestsrc ! fakesink` state cycle exercises nothing in this
# plugin. Instead, cycle moqsink itself through READY -> PAUSED -> READY ->
# NULL directly via PyGObject and assert no state change returns FAILURE,
# except where the relay connection is refused (recorded, not asserted, since
# NULL_TO_READY's connect failure is allowed to fail the change outright).
echo "-- moqsink state cycle --"
if python3 -c "import gi; gi.require_version('Gst', '1.0')" >/dev/null 2>&1; then
  G_DEBUG=fatal-criticals timeout 30 python3 - "$GST_PLUGIN_PATH" <<'EOF'
import sys
import gi
gi.require_version("Gst", "1.0")
from gi.repository import Gst

Gst.init(None)

sink = Gst.ElementFactory.make("moqsink", "ms")
if sink is None:
    print("FAIL: could not create moqsink")
    sys.exit(1)

sink.set_property("host", "127.0.0.1")
sink.set_property("port", 1)
sink.set_property("relay-path", "/moq")

results = {}
for name, state in (
    ("NULL->READY", Gst.State.READY),
    ("READY->PAUSED", Gst.State.PAUSED),
    ("PAUSED->READY", Gst.State.READY),
    ("READY->NULL", Gst.State.NULL),
):
    ret = sink.set_state(state)
    results[name] = ret
    print(f"{name}: {ret}")

# NULL_TO_READY dials the (refused) relay in start(); a FAILURE there is
# expected and recorded, not asserted. Every other transition must not fail.
failed = [n for n, r in results.items()
          if r == Gst.StateChangeReturn.FAILURE and n != "NULL->READY"]
if failed:
    print(f"FAIL: unexpected state-change failures: {failed}")
    sys.exit(1)

if results["NULL->READY"] == Gst.StateChangeReturn.FAILURE:
    print("NOTE: NULL->READY failed as expected (relay connection refused)")

print("state cycle OK")
sys.exit(0)
EOF
  rc=$?
  echo "moqsink state cycle exit code: $rc"
  if [ "$rc" -eq 124 ]; then
    echo "FAIL: moqsink state cycle timed out (hang)"
    exit 1
  fi
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: moqsink state cycle reported a failure"
    exit 1
  fi
else
  echo "PyGObject unavailable; falling back to a short publish run"
  G_DEBUG=fatal-criticals timeout 30 gst-launch-1.0 \
      videotestsrc num-buffers=5 ! x264enc ! h264parse \
      ! moqsink host=127.0.0.1 port=1
  rc=$?
  echo "fallback publish run exit code: $rc"
  if [ "$rc" -eq 124 ]; then
    echo "FAIL: fallback publish run timed out (hang)"
    exit 1
  fi
  if [ "$rc" -ge 128 ]; then
    echo "FAIL: fallback publish run died on a signal (exit $rc)"
    exit 1
  fi
fi

echo "smoke OK"
