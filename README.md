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
`codec` (override the derived catalog codec string) and `bitrate`; `moqsrc`
requires `caps` and adds `latency` (ms reported to the pipeline, default 200).

`moqsrc` maps LOC presentation times onto the pipeline clock: the first object
is anchored at the running time of its arrival and later objects keep their
offset from it, so synced sinks play at the publisher's cadence.

Keep `draft` the same on the publisher and on every subscriber. Relays that
bridge draft-16 and draft-18 sessions have been seen to forward the LOC
property block verbatim, and the two drafts encode its integers differently
(QUIC varint vs vi64), so a cross-draft subscriber rejects the objects.

## Status

H.264 byte-stream over WebTransport, validated end to end through a MoQ relay
with both `moqsrc` and the Red5 Playa browser player (draft-16 and draft-18).
Opus audio, a self-describing catalog (so `moqsrc` needs no `caps`), and a
raw-QUIC (`moqt://`) transport option are follow-ups.

## Author

Raymond Lucke and the Red5 Team

## License

Apache-2.0. See [LICENSE](LICENSE).
