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

```sh
cmake -B build            # -DCMAKE_PREFIX_PATH=<moq5-prefix> if not auto-found
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

Common properties: `host`, `port`, `relay-path`, `namespace`, `track-name`,
`insecure` (skip TLS verification — test only). `moqsink` adds `codec` and
`bitrate`; `moqsrc` requires `caps`.

## Status

H.264 byte-stream over WebTransport, validated end to end through a MoQ relay.
Opus audio, a self-describing catalog (so `moqsrc` needs no `caps`), and a
raw-QUIC (`moqt://`) transport option are follow-ups.

## Author

Raymond Lucke and the Red5 Team

## License

Apache-2.0. See [LICENSE](LICENSE).
