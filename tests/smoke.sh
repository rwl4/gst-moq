#!/usr/bin/env bash
# Smoke test: plugin loading, construction, bounded failed startup, state cycle.
# Targets localhost UDP port 1; a local deadline does not prove peer refusal.
# Only an explicitly phased local startup deadline or native CLOSED qualifies.
# This checks failure/teardown behavior, not publication or live wire conformance.
set -euo pipefail
export GST_PLUGIN_PATH="$1"
fixture_python=${GST_MOQ_PYTHON:-python3}

# Capture full output before grepping: `producer | grep -q pattern` under
# pipefail can report failure via SIGPIPE on the producer even when the
# pattern is found, if grep exits (on first match) before the producer has
# finished writing.
moqsink_info="$(gst-inspect-1.0 moqsink)"
echo "$moqsink_info" | grep -q "video_%u"
echo "$moqsink_info" | grep -q "audio_%u"
moqsrc_info="$(gst-inspect-1.0 moqsrc)"
echo "$moqsrc_info" | grep -q "MoQ source"

# -- 1. CMAF pipeline against an unavailable destination --------------------
#
# Require the actual pipeline to construct before admitting a startup failure.
# Parse errors, missing factories, and failed links must fail this check.
"$fixture_python" - --construct <<'EOF'
import gi
gi.require_version("Gst", "1.0")
from gi.repository import Gst
Gst.init(None)
pipeline = Gst.parse_launch(
    "moqsink name=ms host=127.0.0.1 port=1 relay-path=/moq "
    "videotestsrc num-buffers=30 ! x264enc tune=zerolatency key-int-max=30 "
    "! h264parse ! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss "
    "! ms.video_0")
pipeline.set_state(Gst.State.NULL)
EOF
echo "-- CMAF publish smoke --"
publish_log=$(mktemp)
trap 'rm -f "$publish_log"' EXIT
if G_DEBUG=fatal-criticals timeout --foreground --kill-after=5s 60 gst-launch-1.0 -e moqsink name=ms \
    host=127.0.0.1 port=1 relay-path=/moq \
    videotestsrc num-buffers=30 ! x264enc tune=zerolatency key-int-max=30 \
    ! h264parse ! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss \
    ! ms.video_0 >"$publish_log" 2>&1
then rc=0; else rc=$?; fi
cat "$publish_log"
echo "CMAF publish pipeline exit code: $rc"
"$fixture_python" - "$publish_log" "$rc" --startup-outcome <<'OUTCOME'
from pathlib import Path
import re
import sys

text = Path(sys.argv[1]).read_text()
if sys.argv[2] != "1":
    raise SystemExit("FAIL: expected bounded startup failure; success, harness timeout and signals do not qualify")
headers = []
for number, line in enumerate(text.splitlines()):
    if "ERROR:" not in line:
        continue
    match = re.fullmatch(r"ERROR: from element (\S+): (.+)", line)
    if match is None:
        raise SystemExit("FAIL: unclassified pipeline error")
    headers.append((number, match.group(1), match.group(2)))
if not headers:
    raise SystemExit("FAIL: no exact primary startup failure")
primary = headers[0]
sink = "/GstPipeline:pipeline0/GstMoqSink:ms"
if primary[1] != sink:
    raise SystemExit("FAIL: startup failure is not the primary pipeline error")
local = re.fullmatch(r"MoQ sender startup deadline expired \(phase=(registration|readiness)\)", primary[2])
native = re.fullmatch(r"MoQ sender startup failed \(phase=(connect|registration|readiness), rc=-4\)", primary[2])
if not (local or native):
    raise SystemExit("FAIL: startup outcome, phase or native code does not qualify")
# Only the observed mux consequence may follow this exact primary failure.
# Its own diagnostic block must identify a failed downstream sample push.
if len(headers) > 2:
    raise SystemExit("FAIL: unexpected additional pipeline errors")
if len(headers) == 2:
    line, element, message = headers[1]
    if (element != "/GstPipeline:pipeline0/GstMP4Mux:mp4mux0"
            or message != "Could not multiplex stream."
            or "Failed to push sample." not in text.splitlines()[line + 1:]):
        raise SystemExit("FAIL: additional error is not the tied mux sample-push abort")
print("bounded startup outcome OK:", primary[2], "(no publication or peer-refusal claim)")
OUTCOME

# -- 2. moqsink state cycle --------------------------------------------------
#
# Exercise moqsink itself without media: no linked track set is committed, so
# startup acquisition is not expected. Every state transition must succeed.
echo "-- moqsink state cycle --"
if "$fixture_python" -c "import gi; gi.require_version('Gst', '1.0')" >/dev/null 2>&1; then
  if G_DEBUG=fatal-criticals timeout --foreground --kill-after=5s 30 "$fixture_python" - "$GST_PLUGIN_PATH" <<'EOF'
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

first = sink.set_state(Gst.State.READY)
print(f"NULL->READY: {first}")
if first == Gst.StateChangeReturn.FAILURE:
    sink.set_state(Gst.State.NULL)
    print("FAIL: NULL->READY failed without a committed track set")
    sys.exit(1)

results = {}
for name, state in (
    ("READY->PAUSED", Gst.State.PAUSED),
    ("PAUSED->READY", Gst.State.READY),
    ("READY->NULL", Gst.State.NULL),
):
    ret = sink.set_state(state)
    results[name] = ret
    print(f"{name}: {ret}")

failed = [n for n, r in results.items()
          if r == Gst.StateChangeReturn.FAILURE]
if failed:
    print(f"FAIL: unexpected state-change failures: {failed}")
    sys.exit(1)

print("state cycle OK")
sys.exit(0)
EOF
  then rc=0; else rc=$?; fi
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
  echo "FAIL: PyGObject is required for the state-cycle test; set GST_MOQ_PYTHON"
  exit 1
fi

echo "smoke OK"
