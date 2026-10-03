# CMAF packaging and CMSF-01 catalog support for gst-moq

Date: 2026-09-06. Branch: `feat/cmaf-cmsf01`, based on `fix/playa-playback-interop`.

## Goal

Add CMAF (fragmented MP4) packaging as an option next to the existing LOC
path, for video (H.264) plus audio (AAC-LC and Opus), published under the
CMSF-01 catalog shape that libmoq authors for CMAF tracks: `packaging: "cmaf"`,
string `version: "1"`, a root `initDataList` and per-track `initRef`. Both
elements take part: `moqsink` publishes CMAF tracks, `moqsrc` consumes them.

## Decisions already taken

- Structure C: `moqsink` gains request pads and keeps one libmoq sender so the
  catalog is authored once; `moqsrc` stays a single-pad push source with one
  subscription per instance. An audio/video player runs two `moqsrc` instances.
- CMAF fragments are produced upstream (`mp4mux fragment-duration=N
  fragment-mode=dash-or-mss`, or `cmafmux`). `moqsink` splits the fMP4 byte
  stream into an init segment and moof+mdat objects. No muxer is written.
- Packaging is chosen by pad, not by a property: the always `sink` pad stays
  LOC for H.264 byte-stream; the request pads are CMAF.
- The draft-16 default and the LOC fixes from `fix/playa-playback-interop`
  are unchanged.
- Not in scope: `altGroup` switching sets, content protection, multi-pad
  `moqsrc`, an internal muxer.

## moqsink

### Pads

| Pad | Availability | Caps | Packaging |
|---|---|---|---|
| `sink` | always | `video/x-h264, stream-format=byte-stream, alignment=au` | LOC (unchanged) |
| `video_%u` | request | `video/quicktime` (fragmented MP4) | CMAF |
| `audio_%u` | request | `video/quicktime` (fragmented MP4) | CMAF |

The element changes its base class from `GstBaseSink` to `GstElement`. The
always pad keeps a chain function equivalent to today's `render()`, so the
README pipelines and `! moqsink` keep working.

Each pad has a private struct: libmoq track handle, box reassembly buffer,
pending init segment (ftyp+moov), media type from the template, per-pad
properties, and counters.

Pad properties: `track-name` (default `video` for `video_%u`, `audio` for
`audio_%u`), `bitrate` (advertised catalog bitrate, defaults as today for
video and 128 kbit/s for audio).

Element properties added: `sap-timeline` (bool, default false, maps to
libmoq `emit_sap_timeline` on every CMAF track), `max-fragment-size` (bytes,
default 16 MiB, bound for the reassembly buffer).

### Lifecycle

The element's `change_state` opens the endpoint and attaches the sender on
NULL to READY and tears them down on READY to NULL, the same steps `start()`
and `stop()` perform today. Tracks are added lazily by their pads. Releasing
a request pad while running ends its track (`moq_media_sender_end_track`) so
subscribers see TRACK_ENDED. Per-pad EOS ends that pad's track; once every
pad has seen EOS the existing bounded drain runs before the endpoint stops.

### fMP4 splitting

The chain function appends bytes to the pad's reassembly buffer and walks
top-level ISO BMFF boxes (32-bit size, 64-bit `largesize`, `size == 0` is
rejected as unbounded). Rules:

- `ftyp` and `moov` accumulate into the init segment.
- The first complete `moof` triggers `add_track`: `packaging = CMAF`,
  `init_data = ftyp+moov`, media type from the template, codec string,
  width/height or samplerate/channels, and `timescale` from
  `moq_cmaf_parse_init`, `bitrate` from the pad property.
- Each `moof` plus the `mdat` that follows it is one object.
- `styp`, `sidx`, `prft`, `emsg`, `free` and any other box between fragments
  is skipped.
- Bytes before the first `ftyp` are dropped with a warning; a `moof` before
  any `moov` is a STREAM/FORMAT error.

### Objects, groups, SAP

- One moof+mdat pair is one `moq_media_send_object_t` with `properties = NULL`
  (CMAF timing lives in the fragment) and `payload` wrapping the fragment
  bytes zero-copy, released on the network thread as the LOC path does.
- `moq_cmaf_parse_fragment` gives the first sample's flags. A fragment whose
  first sample is a sync sample sets `is_sync` and `starts_group`; others
  extend the open group. A group is therefore one GOP of one or more
  fragments.
- Every group-start fragment declares `sap_type = TYPE_1` (mp4mux marks only
  IDR frames as sync; every AAC or Opus frame is independently decodable, so
  audio fragments are always group starts of TYPE_1).
- libmoq `validate_cmaf` stays on.

### Codec strings

From the init segment's first sample entry: `avc1.PPCCLL` from the avcC
profile, compatibility and level bytes; `mp4a.40.2` for AAC-LC (from the
esds audio object type; other AOTs map to `mp4a.40.<aot>`); `opus` for an
`Opus` sample entry. Unsupported sample entries make `add_track` fail with
the parsed codec kind in the error message.

## moqsrc

`moqsrc` stays a `GstPushSrc`. Changes:

- The `caps` property becomes optional. When unset, caps are derived at
  TRACK_ADDED for the wanted track: `video/quicktime, variant=iso` for CMAF
  tracks (video or audio; `qtdemux` accepts both), or
  `video/x-h264, stream-format=byte-stream, alignment=au` for LOC H.264. A
  LOC track of another codec with no explicit `caps` raises CORE/NEGOTIATION.
- For CMAF tracks the first buffer is the init segment from the descriptor,
  then every object's full `fragment` (moof+mdat) as one buffer. Buffers carry
  no PTS; `tfdt` inside the fragments drives `qtdemux`. `DELTA_UNIT` mirrors
  the object's `keyframe` flag.
- A later TRACK_ADDED for the wanted track with a new init segment causes the
  new init to be pushed before the next fragment; caps are re-set only when
  the media type changes.
- New property `discovery-timeout` (ms, default 10000): if the wanted track has
  not been announced by then, RESOURCE/NOT_FOUND is raised listing the track
  names the catalog did announce.
- The LOC path keeps the running-time anchoring and `latency` property.

## Error handling summary

| Condition | Result |
|---|---|
| Box size < 8, `size == 0`, or larger than `max-fragment-size` | STREAM/FORMAT error on that pad |
| `moof` before `moov` | STREAM/FORMAT error on that pad |
| `add_track` rejects init | RESOURCE/OPEN_WRITE error naming the codec kind; other pads continue |
| `write` returns `MOQ_ERR_INVAL` (CMAF validation, non-SAP group start) | RESOURCE/WRITE error with group/object numbers |
| `WOULD_BLOCK` / `INTERRUPTED` / `CLOSED` | drop / FLUSHING / EOS, unchanged |
| CMAF object with empty fragment (source) | skipped with a warning |
| Wanted track not announced in time (source) | RESOURCE/NOT_FOUND with announced names |

## Testing

- Unit (ctest, no GStreamer pipeline): the box splitter in an internal header,
  fed synthetic fMP4 in odd chunk sizes, checks init, fragments and skipped
  boxes appear exactly once, and that truncated or oversized boxes error.
  Codec-string derivation from hand-built `avc1`, `mp4a` and `Opus` sample
  entries.
- Element smoke (ctest, no network): `gst-inspect-1.0` on both elements; a
  `videotestsrc ! x264enc ! mp4mux fragment-duration=500 ! moqsink` pipeline
  against an unreachable host exercises request pads, negotiation and the
  error path.
- End to end against `moq-relay.red5.net:4433/moq`: video plus AAC, and video
  plus Opus, through `mp4mux` into `moqsink.video_0` and `moqsink.audio_0`.
  Playa shows the CMSF-01 catalog (`packaging: "cmaf"`, `initDataList`,
  `initRef`) and plays both through MSE with no decode errors; two
  `moqsrc ! qtdemux` pipelines decode video and audio. The LOC pipelines are
  re-run for regression.
- Interop: Playa's node-publisher CMSF-01 fixture consumed by
  `moqsrc ! qtdemux`.
- Docs: README pipelines and pad names; a CMAF section in
  `docs/relay-playback-debugging.md` with verified results.
