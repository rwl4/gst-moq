#!/usr/bin/env bash
# Offline smoke: the plugin loads and both elements inspect cleanly.
#
# A LOC pipeline against an unreachable relay (127.0.0.1:1) was tried here
# too, but moq_endpoint_connect() does not fail synchronously against a
# closed port on this stack: a short (num-buffers=5) pipeline just runs to
# EOS without ever surfacing the connection failure as an element error, so
# there is nothing reliable to grep for. Per the fallback in the task brief,
# this smoke test is inspect-only; the real LOC path is exercised against a
# live relay in the Step 6 regression check (see task-3-report.md).
set -euo pipefail
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

echo "smoke OK"
