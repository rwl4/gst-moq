#!/opt/homebrew/bin/python3
"""Small offline fixture/observer and future owned live participants.

generate/check/plan are offline. publish/receive must only be run under the
native owner's explicit traffic release. No relay is started by this script.
"""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import struct
import time

PLUGINS = ["coreelements", "app", "videotestsrc", "videoconvertscale",
           "x264", "videoparsersbad", "audiotestsrc", "audioconvert",
           "audioresample", "libav", "audioparsers", "isomp4"]
NAMES = ["finite-loc", "continuing-aac"]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def boxes(data):
    offset = 0
    while offset < len(data):
        if len(data) - offset < 8:
            raise ValueError("partial MP4 header")
        size, kind = struct.unpack_from(">I4s", data, offset)
        header = 8
        if size == 1:
            size = struct.unpack_from(">Q", data, offset + 8)[0]
            header = 16
        if size < header or offset + size > len(data):
            raise ValueError("invalid/partial MP4 box")
        yield kind, data[offset:offset + size], data[offset + header:offset + size]
        offset += size


def fragment_fields(data):
    count, decode = 0, None
    for kind, _, body in boxes(data):
        if kind != b"moof":
            continue
        for child, _, contents in boxes(body):
            if child != b"traf":
                continue
            for leaf, _, fields in boxes(contents):
                if leaf == b"tfdt":
                    decode = struct.unpack_from(">Q" if fields[0] else ">I", fields, 4)[0]
                if leaf == b"trun":
                    count += struct.unpack_from(">I", fields, 4)[0]
    if decode is None or not count:
        raise ValueError("missing fragment decode time/sample inventory")
    return decode, count


def collect(pipeline, sink, expected=None, seconds=30, ready=None, prefix=1):
    """Same receiver-output observation for offline appsrc and future moqsrc."""
    deadline = time.monotonic() + seconds
    seen = []
    pipeline.set_state(Gst.State.PLAYING)
    eos = False
    try:
        while time.monotonic() < deadline:
            sample = sink.emit("try-pull-sample", 100 * Gst.MSECOND)
            if sample:
                buffer = sample.get_buffer()
                payload = buffer.extract_dup(0, buffer.get_size())
                row = {"size": len(payload), "sha256": sha(payload),
                       "pts": int(buffer.pts), "duration": int(buffer.duration)}
                seen.append((row, payload))
                if ready and len(seen) == prefix:
                    actual = [(r["size"], r["sha256"]) for r, _ in seen]
                    wanted = [(r["size"], r["sha256"]) for r in expected[:prefix]]
                    if actual != wanted:
                        raise RuntimeError("priming payload mismatch; receiver is not ready")
                    # Exclusive creation prevents replacing another run's receipt.
                    ready.parent.mkdir(parents=True, exist_ok=True)
                    temporary = ready.with_name(ready.name + f".{os.getpid()}.tmp")
                    temporary.write_text(json.dumps({"pid": os.getpid(), "prefix": prefix}))
                    try:
                        os.link(temporary, ready)  # Atomic publication, exclusive destination.
                    finally:
                        temporary.unlink()
                print(json.dumps({"event": "buffer", "index": len(seen)-1,
                                  "monotonic": time.monotonic(), **row}), flush=True)
                continue
            msg = pipeline.get_bus().pop_filtered(Gst.MessageType.ERROR | Gst.MessageType.EOS)
            if msg and msg.type == Gst.MessageType.ERROR:
                error, debug = msg.parse_error()
                raise RuntimeError(f"pipeline error: {error}; {debug}")
            if msg and msg.type == Gst.MessageType.EOS:
                eos = True
            if eos:
                if expected is not None:
                    actual = [(r["size"], r["sha256"]) for r, _ in seen]
                    wanted = [(r["size"], r["sha256"]) for r in expected]
                    if actual != wanted:
                        raise RuntimeError(f"payload inventory mismatch: {actual} != {wanted}")
                print(json.dumps({"event": "pipeline-eos", "count": len(seen),
                                  "monotonic": time.monotonic()}), flush=True)
                return seen
        raise TimeoutError("observation deadline; this is NOT EOS")
    finally:
        # Never synthesize EOS on timeout/error/teardown.
        pipeline.set_state(Gst.State.NULL)


def save(root, name, data, **extra):
    (root / name).write_bytes(data)
    return {"file": name, "size": len(data), "sha256": sha(data), **extra}


def generate(root):
    a_recipe = ("videotestsrc num-buffers=12 pattern=ball ! "
                "video/x-raw,format=I420,width=160,height=120,framerate=10/1 ! "
                "x264enc tune=zerolatency speed-preset=ultrafast key-int-max=1 "
                "byte-stream=true threads=1 ! h264parse config-interval=-1 ! "
                "video/x-h264,stream-format=byte-stream,alignment=au ! "
                "appsink name=out sync=false wait-on-eos=false")
    b_recipe = ("audiotestsrc num-buffers=256 samplesperbuffer=1024 wave=sine freq=440 ! "
                "audio/x-raw,format=F32LE,rate=48000,channels=1 ! "
                "avenc_aac bitrate=96000 ! aacparse ! "
                "mp4mux fragment-duration=500 streamable=true ! "
                "appsink name=out sync=false wait-on-eos=false")
    a = Gst.parse_launch(a_recipe)
    au = collect(a, a.get_by_name("out"))
    if len(au) != 12:
        raise RuntimeError("finite LOC fixture must contain exactly 12 access units")
    track_a = [save(root, f"a-{i:03d}.h264", data, pts_ns=i*100_000_000,
                    duration_ns=100_000_000) for i, (_, data) in enumerate(au)]
    b = Gst.parse_launch(b_recipe)
    muxed = b"".join(data for _, data in collect(b, b.get_by_name("out")))
    save(root, "b-complete.mp4", muxed)
    init, fragments, pending = b"", [], None
    skipped = []
    for kind, whole, _ in boxes(muxed):
        if kind in (b"ftyp", b"moov"):
            init += whole
        elif kind == b"moof":
            if pending is not None:
                raise ValueError("unpaired moof")
            pending = whole
        elif kind == b"mdat":
            if pending is None:
                raise ValueError("unpaired mdat")
            fragments.append(pending + whole)
            pending = None
        else:
            skipped.append(kind.decode("ascii"))
    if pending or not init or not fragments:
        raise ValueError("incomplete CMAF fixture")
    init_row = save(root, "b-init.mp4", init, pts_ns=0, duration_ns=0)
    track_b = []
    for i, data in enumerate(fragments):
        decode, samples = fragment_fields(data)
        track_b.append(save(root, f"b-{i:03d}.m4s", data,
                            pts_ns=decode*Gst.SECOND//48000,
                            duration_ns=samples*1024*Gst.SECOND//48000,
                            decode_ticks=decode, sample_count=samples))
    if not any(r["pts_ns"] > 1_200_000_000 for r in track_b):
        raise ValueError("B must continue after A's finite endpoint")
    manifest = {"namespace": "gst-drain/local16", "draft": 16,
                "recipes": [a_recipe, b_recipe], "gst": Gst.version_string(),
                "plugins": PLUGINS, "skipped_mp4_boxes": skipped,
                "tracks": [{"name": NAMES[0], "packaging": "LOC",
                            "interpretation": "one Annex-B H264 access unit per object",
                            "objects": track_a},
                           {"name": NAMES[1], "packaging": "CMAF",
                            "interpretation": "one moof+mdat per object, not one AAC sample",
                            "timescale": 48000, "init": init_row, "objects": track_b}]}
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")


def verify(root):
    manifest = json.loads((root / "manifest.json").read_text())
    for track in manifest["tracks"]:
        for row in track["objects"] + ([track["init"]] if "init" in track else []):
            data = (root / row["file"]).read_bytes()
            if len(data) != row["size"] or sha(data) != row["sha256"]:
                raise ValueError(f"fixture receipt mismatch: {row['file']}")
    return manifest


def expected_rows(track):
    return ([track["init"]] if "init" in track else []) + track["objects"]


def replay(root, manifest):
    for track in manifest["tracks"]:
        pipeline = Gst.parse_launch("appsrc name=input format=time ! appsink name=out sync=false wait-on-eos=false")
        source = pipeline.get_by_name("input")
        for row in expected_rows(track):
            data = (root / row["file"]).read_bytes()
            buffer = Gst.Buffer.new_wrapped(data)
            buffer.pts = row["pts_ns"]
            buffer.duration = row["duration_ns"]
            if source.emit("push-buffer", buffer) != Gst.FlowReturn.OK:
                raise RuntimeError("offline replay rejected buffer")
        source.emit("end-of-stream")
        collect(pipeline, pipeline.get_by_name("out"), expected_rows(track))


def publisher(root, manifest, host, port, path, draft=16):
    pipeline = Gst.Pipeline.new("single-publisher")
    sink = Gst.ElementFactory.make("moqsink", "publisher")
    for key, value in {"host": host, "port": port, "relay-path": path,
                       "namespace": manifest["namespace"], "draft": draft,
                       "insecure": False, "sync": False,
                       "track-name": NAMES[0]}.items():
        sink.set_property(key, value)
    pipeline.add(sink)
    sources = []
    for i, caps in enumerate(["video/x-h264,stream-format=byte-stream,alignment=au",
                              "video/quicktime,variant=iso"]):
        source = Gst.ElementFactory.make("appsrc", f"input-{i}")
        source.set_property("caps", Gst.Caps.from_string(caps))
        source.set_property("format", Gst.Format.TIME)
        pipeline.add(source)
        target = sink.get_static_pad("sink") if i == 0 else sink.request_pad_simple("audio_%u")
        if i:
            target.set_property("track-name", NAMES[i])
        if source.get_static_pad("src").link(target) != Gst.PadLinkReturn.OK:
            raise RuntimeError("publisher link refused")
        sources.append(source)
    return pipeline, sources


def publish(root, manifest, pipeline, sources, ready_dir):
    ready_files = [ready_dir / (name + ".ready.json") for name in NAMES]
    if any(path.exists() for path in ready_files):
        raise RuntimeError("stale readiness files; use a fresh owned run directory")
    timeline, primers = [], []
    for i, track in enumerate(manifest["tracks"]):
        rows = expected_rows(track)
        prefix = 1 if i == 0 else 2
        primers.extend((i, row) for row in rows[:prefix])
        for row in rows[prefix:]:
            timeline.append((row["pts_ns"], i, row))
        end = max(r["pts_ns"] + r["duration_ns"] for r in track["objects"])
        timeline.append((end, i, None))
    timeline.sort(key=lambda r: (r[0], r[1]))
    deadline = time.monotonic() + 30
    pipeline.set_state(Gst.State.PLAYING)
    def submit(i, row):
        buffer = Gst.Buffer.new_wrapped((root / row["file"]).read_bytes())
        buffer.pts, buffer.duration = row["pts_ns"], row["duration_ns"]
        if sources[i].emit("push-buffer", buffer) != Gst.FlowReturn.OK:
            raise RuntimeError("publisher input refused")
    try:
        # First valid media establishes real reception; only then send the
        # counted suffix. Primers stay in the expected inventory, not discarded.
        for i, row in primers:
            submit(i, row)
        while not all(path.exists() for path in ready_files):
            if time.monotonic() >= deadline - 10:
                raise TimeoutError("actual receiver priming did not complete")
            msg = pipeline.get_bus().pop_filtered(Gst.MessageType.ERROR)
            if msg:
                raise RuntimeError(str(msg.parse_error()))
            time.sleep(0.005)
        for path, count in zip(ready_files, (1, 2), strict=True):
            if json.loads(path.read_text())["prefix"] != count:
                raise RuntimeError("incorrect readiness receipt")
        start = time.monotonic()
        for at, i, row in timeline:
            if time.monotonic() >= deadline:
                raise TimeoutError("publisher work deadline")
            while time.monotonic() - start < at / Gst.SECOND:
                msg = pipeline.get_bus().pop_filtered(Gst.MessageType.ERROR)
                if msg:
                    raise RuntimeError(str(msg.parse_error()))
                time.sleep(0.005)
            if row is None:
                sources[i].emit("end-of-stream")
                print(json.dumps({"event": "input-eos", "track": NAMES[i],
                                  "monotonic": time.monotonic()}), flush=True)
            else:
                submit(i, row)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("publisher EOS deadline")
        msg = pipeline.get_bus().timed_pop_filtered(int(remaining*Gst.SECOND), Gst.MessageType.ERROR | Gst.MessageType.EOS)
        if not msg or msg.type != Gst.MessageType.EOS:
            raise RuntimeError("publisher failed or timed out; no successful EOS")
        print(json.dumps({"event": "publisher-eos", "monotonic": time.monotonic()}), flush=True)
    finally:
        pipeline.set_state(Gst.State.NULL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["generate", "check", "plan", "publish", "receive"])
    parser.add_argument("root", type=Path)
    parser.add_argument("--plugin", type=Path)
    parser.add_argument("--ready-dir", type=Path)
    parser.add_argument("--track", choices=NAMES, default=NAMES[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=45443)
    parser.add_argument("--path", default="/moq-relay")
    parser.add_argument("--draft", type=int, choices=(16, 18), default=16)
    args = parser.parse_args()
    if args.mode == "publish" and not args.ready_dir:
        parser.error("publish requires a fresh owned --ready-dir for both receivers")
    args.root.mkdir(parents=True, exist_ok=True)
    # Isolate this fixture's dependency set. Broken unrelated host plugins are
    # excluded explicitly, not scanned and then ignored as acceptable warnings.
    os.environ["GST_PLUGIN_SYSTEM_PATH_1_0"] = ""
    os.environ["GST_PLUGIN_PATH_1_0"] = ""
    os.environ["GST_REGISTRY"] = str(args.root / "registry.bin")
    import gi
    gi.require_version("Gst", "1.0")
    from gi.repository import Gst
    if args.mode == "receive":
        os.environ["GST_DEBUG"] = "moqsrc:6"
        os.environ["GST_DEBUG_FILE"] = str(args.root / (args.track + ".debug.log"))
    Gst.init(None)
    library = Path("/opt/homebrew/opt/gstreamer/lib/gstreamer-1.0")
    for name in PLUGINS:
        if not Gst.Plugin.load_file(str(library / f"libgst{name}.dylib")):
            raise RuntimeError(f"required plugin {name} unavailable")
    if args.plugin and not Gst.Plugin.load_file(str(args.plugin.resolve())):
        raise RuntimeError("MOQ plugin unavailable")
    if args.mode == "generate":
        generate(args.root)
    else:
        manifest = verify(args.root)
        if args.mode == "check":
            replay(args.root, manifest)
        elif args.mode in ("plan", "publish"):
            pipeline, sources = publisher(args.root, manifest, args.host, args.port, args.path, args.draft)
            if args.mode == "publish":
                publish(args.root, manifest, pipeline, sources, args.ready_dir)
            else:
                # Validate both actual receiver pipeline descriptions at NULL;
                # starting moqsrc is intentionally reserved for the live release.
                for track in manifest["tracks"]:
                    Gst.parse_launch(
                        f'moqsrc host={args.host} port={args.port} relay-path="{args.path}" '
                        f'namespace="{manifest["namespace"]}" track-name="{track["name"]}" '
                        f'draft={args.draft} insecure=false latency=0 ! appsink sync=false')
                replay(args.root, manifest)
                print(json.dumps({"event": "offline-plan", "publisher_count": 1,
                                  "tracks": NAMES, "draft": args.draft, "insecure": False,
                                  "priming_prefixes": [1, 2]}), flush=True)
        else:
            track = next(t for t in manifest["tracks"] if t["name"] == args.track)
            pipeline = Gst.parse_launch(
                f'moqsrc host={args.host} port={args.port} relay-path="{args.path}" '
                f'namespace="{manifest["namespace"]}" track-name="{args.track}" '
                f'draft={args.draft} insecure=false latency=0 ! appsink name=out sync=false wait-on-eos=false')
            ready = args.ready_dir / (args.track + ".ready.json") if args.ready_dir else None
            collect(pipeline, pipeline.get_by_name("out"), expected_rows(track),
                    ready=ready, prefix=1 if args.track == NAMES[0] else 2)
            log = (args.root / (args.track + ".debug.log")).read_text()
            kinds = re.findall(r"selected-track terminal event kind=(\d+) name=" +
                               re.escape(args.track), log)
            if "4" not in kinds or ("selected-track queue drained; sending EOS name=" +
                                    args.track) not in log or "endpoint closed; sending EOS" in log:
                raise RuntimeError("EOS lacks selected service-ended/queue-drained provenance")
            print(json.dumps({"event": "service-ended-observed", "track": args.track,
                              "kind": 4, "wire_end_of_track_proven": False,
                              "note": "Public TRACK_ENDED also includes peer rejection"}), flush=True)
