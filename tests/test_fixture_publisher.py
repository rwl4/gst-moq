"""Construct the real fixture publisher at NULL, without codecs or sockets."""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True

os.environ["GST_PLUGIN_SYSTEM_PATH_1_0"] = ""
os.environ["GST_PLUGIN_PATH_1_0"] = ""

import gi
gi.require_version("Gst", "1.0")
from gi.repository import Gst

Gst.init(None)
plugins = Path(subprocess.check_output(
    ["pkg-config", "--variable=pluginsdir", "gstreamer-1.0"], text=True).strip())
suffix = ".dylib" if sys.platform == "darwin" else ".dll" if sys.platform == "win32" else ".so"
prefix = "" if sys.platform == "win32" else "lib"
for name in ["coreelements", "app"]:
    Gst.Plugin.load_file(str(plugins / f"{prefix}gst{name}{suffix}"))
Gst.Plugin.load_file(sys.argv[1])
spec = importlib.util.spec_from_file_location("local_fixture", Path(__file__).with_name("local_fixture.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
fixture.Gst = Gst
pipeline, sources = fixture.publisher(
    None, {"namespace": "fixture/construction"}, "127.0.0.1", 1, "/moq-relay")
assert pipeline.current_state == Gst.State.NULL
assert len(sources) == 2
assert [source.get_static_pad("src").get_peer().get_name() for source in sources] == ["video", "audio_0"]
print("fixture publisher links at NULL")
