# Building gst-moq

gst-moq uses GStreamer's Rust bindings and MOQ5's safe media-service API.
Cargo builds the plugin; MOQ5 and its transport dependencies are installed
separately as a native SDK.

## Requirements

- Rust 1.92 or newer.
- GStreamer development packages for `gstreamer-1.0` and
  `gstreamer-base-1.0`, version 1.22 or newer.
- `pkg-config` and libclang, used by `moq5-sys` to generate bindings.
- An installed MOQ5 service SDK with the managed PicoQUIC WebTransport backend.

Build the native SDK from the same MOQ5 revision as the Rust dependency in
[Cargo.toml](../Cargo.toml). The current pin is
`f81a6188741877bd93aa7771181b9bf995d6cc65`. The plugin needs
`Sender::attach_with_tracks` to publish its initial track set atomically.

## Build the native SDK

Use a separate MOQ5 checkout at the pinned revision. On macOS with Homebrew,
set the OpenSSL location before preparing dependencies:

```sh
export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"
```

From the MOQ5 checkout, prepare its development dependencies:

```sh
export MOQ_DEPS_DIR="$PWD/.deps/gst-moq"
scripts/setup_picoquic_deps.sh
```

The helper selects picoquic `70b5fd5aafdd2d6c349ff783202ece9d778bcaf2`
and picotls `7f0d38c1c54c920d56b4e13ceb1c2dfa2b1325e1`, applying the
MOQ5 checkout's `patches/picoquic/series` and `patches/picotls/series`.
Keep their headers and libraries together; do not substitute another checkout's
headers for these built libraries.

Choose a separate, durable install prefix, then configure, build and install:

```sh
export MOQ_SDK_PREFIX="$PWD/../local-prefix-gst-moq"
cmake --preset install -DCMAKE_INSTALL_PREFIX="$MOQ_SDK_PREFIX" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DMOQ_BUILD_ADAPTER_PICOQUIC=ON \
  -DMOQ_BUILD_ADAPTER_PICO_WT=ON -DMOQ_BUILD_PICO_WT_MANAGED=ON \
  -DMOQ_PICOQUIC_SOURCE_DIR="$MOQ_DEPS_DIR/picoquic" \
  -DMOQ_PICOTLS_PREFIX="$MOQ_DEPS_DIR/picotls/build"
cmake --build build/install
cmake --install build/install
```

On macOS, also pass `-DOPENSSL_ROOT_DIR="$OPENSSL_ROOT_DIR"` and
`-DCMAKE_FIND_FRAMEWORK=LAST` to the configure command so unrelated frameworks
do not override the chosen OpenSSL installation.

The explicit install prefix keeps this SDK separate from other consumers.
Keep the SDK and its provider dependency directories outside temporary storage.
The managed backend is required: a service SDK built without it can load the
plugin but reports `UNSUPPORTED` when connecting. On Linux, every static dependency linked into
the plugin must be built position-independent.

This is a development SDK recipe with temporary provider patches. It does not
establish support for unmodified upstream providers or distribution packages.
The [compatibility notes](compatibility.md) describe the qualification boundary.

## Build and load the plugin

From the gst-moq repository:

```sh
export MOQ_PREFIX=/absolute/path/to/your/moq5-prefix
cargo build --release --locked
export GST_PLUGIN_PATH="$PWD/target/release${GST_PLUGIN_PATH:+:$GST_PLUGIN_PATH}"
gst-inspect-1.0 moqsink
gst-inspect-1.0 moqsrc
```

Select the matching SDK explicitly with `MOQ_PREFIX` for every build and test
shell. The repository supplies no implicit SDK directory. Reuse the prefix
chosen above; an existing installation belonging to another consumer may have
incompatible headers or a different transport backend.

The underlying binding also supports standard `pkg-config` discovery. Inspect
its selected package when diagnosing an SDK mismatch:

```sh
PKG_CONFIG_PATH="$MOQ_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}" \
  pkg-config --variable=prefix libmoq-service
```

`moq5-sys` reads `libmoq-service.pc` and its dependency closure. Select linkage
with `LIBMOQ_SERVICE_STATIC=1` or `LIBMOQ_SERVICE_DYNAMIC=1` when needed.
`MOQ_EXTRA_LIBS` is not supported. Shared dependencies must also be visible to
the platform's dynamic loader at runtime.

## Tests

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

The integration tests also need Python with PyGObject/GStreamer bindings,
the GStreamer tools, and the encoders/parsers/muxers named in
[tests/smoke.sh](../tests/smoke.sh). Set `GST_MOQ_PYTHON` if that interpreter
is not `python3` on PATH. Missing Python bindings fail the check.

Unit tests cover codecs, fragmented MP4, track preparation, delivery,
authentication, ownership and lifecycle against a service test double.
Smoke tests load a freshly built plugin, inspect both elements, exercise
an unavailable loopback destination and cycle element states. Their expected
startup failure proves bounded failure and cleanup, not successful publication
or a peer's refusal. No external relay is needed for `cargo test`.

For offline CMAF inspection:

```sh
cargo run --locked --bin inspect_cmaf_fixture -- file.mp4
```

The tool prints the init/fragment inventory as the plugin splits the file and
MOQ5 parses it. [tests/local_fixture.py](../tests/local_fixture.py) can generate
encoded fixtures, check their hashes and construct publisher/receiver pipelines
at NULL without connecting. Its `publish` and `receive` commands generate real
traffic and require an explicitly chosen test destination and run setup.
