# gst-moq

GStreamer elements for [Media over QUIC Transport](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/),
backed by [MOQ5](https://github.com/openmoq/moq5) through its high-level media
service tier.

- **`moqsink`** — publish a GStreamer stream to a MoQ relay.
- **`moqsrc`** — subscribe to a MoQ track and inject it into a pipeline.

## Design

- Thin adapters over the MOQ5 **service tier** (`moq_endpoint_t` +
  `moq_media_sender_t` / `moq_media_receiver_t`). MOQ5 owns the network thread,
  version negotiation, TLS, and the MSF catalog; the elements only translate
  GstBuffers ⇄ MoQ media objects.
- WebTransport client to a relay (the service tier is client-only — there is no
  peer-to-peer mode).
- H.264 byte-stream carried as LOC-01 media objects: a keyframe opens a MoQ
  group, delta frames extend it, timestamps are stream-relative.

## Build

Requires the GStreamer 1.x development packages (`gstreamer-1.0`,
`gstreamer-base-1.0`) and an installed MOQ5 with the service component
(`libmoq-service` / `moq::service`).

On Linux the plugin is a shared object, so every static library it links
(picotls, picoquic, libmoq) must be built position-independent
(`-DCMAKE_POSITION_INDEPENDENT_CODE=ON`). Install picoquic (with `BUILD_HTTP=ON`
for WebTransport) and libmoq into one prefix, then point CMake at it:

```sh
cmake -B build -DCMAKE_PREFIX_PATH=<moq5-prefix>   # auto-found at ../../local-prefix
cmake --build build
export GST_PLUGIN_PATH="$PWD/build/plugins"
gst-inspect-1.0 moqsink
gst-inspect-1.0 moqsrc
```

## Usage

Publish a stream to a relay:

```sh
gst-launch-1.0 -e \
  videotestsrc ! x264enc tune=zerolatency key-int-max=30 ! h264parse \
  ! video/x-h264,stream-format=byte-stream,alignment=au \
  ! moqsink host=RELAY port=4433 relay-path=/moq-relay namespace=example track-name=video
```

Subscribe, decode, and display:

```sh
gst-launch-1.0 \
  moqsrc host=RELAY port=4433 relay-path=/moq-relay namespace=example track-name=video \
    caps="video/x-h264,stream-format=byte-stream,alignment=au" \
  ! h264parse ! avdec_h264 ! autovideosink
```

Use `h264parse config-interval=-1` so every keyframe carries SPS/PPS: `moqsink`
adds the track on the first keyframe and publishes the SPS/PPS as an avcC
`initData` in the catalog (WebCodecs players need the decoder description),
together with a `codec` string derived from the SPS profile/level and the
width, height and framerate from the caps.

Common properties: `host`, `port`, `relay-path`, `namespace`, `track-name`,
`insecure` (skip TLS verification — test only), `draft` (MoQ Transport draft to
negotiate, default 16; 0 offers every draft libmoq supports). `moqsink` adds
`codec` (override the derived catalog codec string), `bitrate`, `sync`,
`sap-timeline` and `max-fragment-size`; `moqsrc` adds `discovery-timeout` and
`caps`, which is now optional — it is derived from the catalog when omitted.

`moqsrc` maps LOC presentation times onto the pipeline clock: the first object
is anchored at the running time of its arrival and later objects keep their
offset from it, so synced sinks play at the publisher's cadence.

Keep `draft` the same on the publisher and on every subscriber. Relays that
bridge draft-16 and draft-18 sessions have been seen to forward the LOC
property block verbatim, and the two drafts encode its integers differently
(QUIC varint vs vi64), so a cross-draft subscriber rejects the objects.

### CMAF (fragmented MP4) with audio

`moqsink` also takes fragmented MP4 on request pads `video_%u` and `audio_%u`
and publishes each as a CMAF track under one CMSF-01 catalog (`packaging:
"cmaf"`, root `initDataList`, per-track `initRef`). Feed each pad from its own
fragmented muxer; one moof+mdat fragment becomes one MoQ object, and a
fragment starting on a sync sample opens a new group, so keep
`fragment-duration` a multiple of the GOP length.

```sh
gst-launch-1.0 -e moqsink name=ms host=RELAY port=4433 relay-path=/moq namespace=example \
  videotestsrc is-live=true ! x264enc tune=zerolatency key-int-max=30 ! h264parse \
    ! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss ! ms.video_0 \
  audiotestsrc is-live=true ! voaacenc ! aacparse \
    ! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss ! ms.audio_0
```

Opus works the same way (`opusenc ! opusparse ! mp4mux ...`). Pad properties
`track-name` and `bitrate` set the catalog entry per pad (`ms.video_0::track-name=hd`).
Element property `sap-timeline` adds a CMSF SAP event timeline track per CMAF track.

Play a CMAF track back with `qtdemux`; `caps` is derived from the catalog:

```sh
gst-launch-1.0 moqsrc host=RELAY port=4433 relay-path=/moq namespace=example track-name=video \
  ! qtdemux ! h264parse ! avdec_h264 ! autovideosink
gst-launch-1.0 moqsrc host=RELAY port=4433 relay-path=/moq namespace=example track-name=audio \
  ! qtdemux ! aacparse ! avdec_aac ! autoaudiosink
```

## Status

H.264 byte-stream over WebTransport, validated end to end through a MoQ relay
with both `moqsrc` and the Red5 Playa browser player (draft-16 and draft-18).
CMAF (fragmented MP4) video plus AAC or Opus audio under a CMSF-01 catalog is
validated the same way, with `moqsrc` deriving caps from the catalog. A
raw-QUIC (`moqt://`) transport option is a follow-up.

## Author

Raymond Lucke and the Red5 Team

## License

Apache-2.0. See [LICENSE](LICENSE).
