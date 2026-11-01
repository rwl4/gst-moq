# gst-moq

**Media over QUIC for GStreamer.**

Publish live audio and video to a MoQ relay, or bring a remote track into
your pipeline. gst-moq is written in Rust and uses
[MOQ5](https://github.com/openmoq/moq5) for connections, catalogs and media
delivery. Encoding, decoding and playback stay in GStreamer.

| Element | What it does |
| --- | --- |
| `moqsink` | Publishes one or more tracks under a shared catalog. |
| `moqsrc` | Discovers a catalog and receives the track you select. |

[Build guide](docs/building.md)
· [Pipelines and properties](docs/usage.md)
· [Compatibility](docs/compatibility.md)

## Build

You need Rust 1.92+, GStreamer 1.22+ development packages, `pkg-config`,
libclang, and an installed MOQ5 SDK with its managed PicoQUIC WebTransport
backend.
The SDK must match the MOQ5 revision pinned in [Cargo.toml](Cargo.toml).
The [build guide](docs/building.md) covers that setup and the current native
SDK requirements.

From this repository, with the SDK installed:

```sh
export MOQ_PREFIX=/path/to/moq5-prefix
cargo build --release --locked
export GST_PLUGIN_PATH="$PWD/target/release${GST_PLUGIN_PATH:+:$GST_PLUGIN_PATH}"
gst-inspect-1.0 moqsink
gst-inspect-1.0 moqsrc
```

Export `MOQ_PREFIX` for both builds and tests, selecting the SDK built from
this repository's pinned MOQ5 revision. See the [build guide](docs/building.md)
for installing it into a separate, durable prefix.

## Publish video

Replace `RELAY`, the port and the path with your relay's settings. These
examples negotiate draft 22, then 18, then 16, and require the named GStreamer
encoders, parsers and decoders. TLS certificate verification is enabled.

```sh
gst-launch-1.0 -e \
  videotestsrc is-live=true ! video/x-raw,framerate=30/1 \
  ! x264enc tune=zerolatency key-int-max=30 ! h264parse config-interval=-1 \
  ! video/x-h264,stream-format=byte-stream,alignment=au \
  ! moqsink host=RELAY port=4433 relay-path=/moq-relay \
      namespace=example track-name=video draft=0
```

## Receive and play

Each `moqsrc` receives one track. Use multiple source elements to receive
audio and video together. Use the same namespace, track name and draft as
the publisher:

```sh
gst-launch-1.0 \
  moqsrc host=RELAY port=4433 relay-path=/moq-relay \
    namespace=example track-name=video draft=0 \
  ! h264parse ! avdec_h264 ! autovideosink
```

`moqsrc` derives H.264 Annex B and AAC caps from the catalog. See the
[pipeline guide](docs/usage.md) for audio, CMAF, explicit caps and multiple
tracks. `moqsink` is a plain `GstElement`: PAUSED does not suspend publishing.

## Media support

| Input to `moqsink` | Pad | Packaging |
| --- | --- | --- |
| H.264 Annex B access units | `video` | LOC |
| Raw AAC-LC access units | `audio` | LOC |
| Fragmented MP4 video or audio | `video_%u`, `audio_%u` | CMAF with a CMSF-01 catalog |

The plugin connects to relays over WebTransport. The default `draft=0` offers
MoQT 22, 18 and 16 in that preference order. Set `draft=22`, `draft=18` or
`draft=16` to require a specific version.

gst-moq is pre-1.0. See the
[compatibility notes](docs/compatibility.md) for codec, relay and backend limits.

## Authenticated relays

Both elements accept a write-only `setup-auth-token` property containing the
token payload as canonical padded Base64. `setup-auth-token-type` defaults to
`1`, the CAT4MOQ type. Set credentials programmatically before PAUSED to keep
them out of shell history and process listings.

Catalog-driven receive needs permission for both the catalog and the selected
media track. See [authentication](docs/usage.md#authentication) for scope,
validation, lifecycle and the distinction between SETUP and request credentials.

## Development

With the build dependencies installed:

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

Tests run without an external relay. Unit tests use a service test double;
smoke tests load the plugin and check cleanup against an unavailable loopback
destination. They do not replace live interoperability tests. The
[build guide](docs/building.md#tests) lists their additional dependencies.

If something breaks, [open an issue](https://github.com/rwl4/gst-moq/issues)
with your pipeline, GStreamer and MOQ5 versions, relay, protocol draft and
relevant logs. Keep credentials out of reports.

## Contributors

Created and maintained by Raymond Lucke, with contributions from the
[Red5](https://www.red5.net/) team and our
[contributors](https://github.com/rwl4/gst-moq/graphs/contributors).
The Rust port was contributed by Vitor Azevedo.

Licensed under [Apache-2.0](LICENSE).
