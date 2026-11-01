# Compatibility

gst-moq is pre-1.0. Its media and lifecycle behavior depends on both the
installed MOQ5 SDK and the relay. Use a matching SDK and the same protocol
draft for publisher and subscriber.

## Supported inputs

- H.264 Annex B LOC publishing and automatic H.264 LOC receive caps.
- Raw AAC-LC LOC publishing and automatic AAC LOC receive caps, subject to the
  [AudioSpecificConfig restrictions](usage.md#loc-video-and-audio).
- CMAF video/audio publishing and receiving with a CMSF-01 catalog. Codec
  strings cover AVC, AAC and Opus.
- WebTransport client connections to a relay. Raw QUIC and peer-to-peer
  operation are not exposed by these elements.

The default `draft=0` negotiates supported versions in preference order:
22, then 18, then 16. Explicit `draft=22`, `draft=18` or `draft=16` requires
that one version. A backend incapable of draft 22 can omit it from the
automatic offer; an explicit unsupported choice fails. Mixed-draft relay
conversion is not assumed. Version preference is separate from the tested
media combinations below.

## Native SDK

The [build recipe](building.md) uses the exact MOQ5 revision in
[Cargo.toml](../Cargo.toml) with its managed PicoQUIC WebTransport backend.
That development graph still requires MOQ5's pinned provider patch series.
Unmodified upstream provider builds failed native close-reason and terminal
stream guards during qualification. System-package and other-platform support
has not been established by the plugin's development-SDK checks.

The native SDK must provide service-aware endpoint drain. Transport-only
drain cannot establish the sender's completion contract.

## What has been checked

The current Rust source passes formatting, strict all-target Clippy, build,
unit, completion and plugin smoke tests with Rust 1.92 and the matching SDK.
The Rust dependencies resolve from the published MOQ5 Git revision without
local source overrides.

With the default setting left unchanged on both elements, controlled loopback
tests selected draft 22 against a peer offering 22/18/16, draft 18 against an
18-only peer, and draft 16 against a 16-only peer. Each run delivered all 101
expected H.264 LOC objects in order with byte-identical payloads and EOS at
both ends. TLS verification used a process-local test trust anchor. These
checks used a retained MOQ5 relay binary as the test peer; they do not certify
a current relay release or decoder/browser playback.

The SETUP-token implementation was separately checked through a protected
moqx 0.3.5 relay on draft 18 before the default-version change. It delivered all 101
expected H.264 LOC objects in order with byte-identical payloads and EOS at
both ends. TLS verification remained enabled. That private listener uses
explicit token type 16; the standard CAT type remains 1. A catalog-capable
credential for the wrong namespace was refused. This proves that scoped
authenticated delivery/completion path; the later default-negotiation tests
used anonymous loopback fixtures.

Older moqx 0.2.1/draft-16 bootstrap checks reached catalog discovery,
subscription and first frame, but did not pass the full browser bootstrap
window. The paced-input check failed with partial-group warnings. Those older
relay cases remain unresolved; their errors are not proven to have one common
cause or to originate in the plugin.

Other source/SDK combinations have passed LOC/CMAF/AAC delivery, offline
AVC/AAC decoding and Red5/browser playback checks. Those results do not
automatically qualify the current combination. The
[September playback notes](relay-playback-debugging.md) are a historical
investigation, not current build instructions or a current support matrix.

Current checks do not qualify every relay, arbitrary LOC framing, Opus
playback, draft-18 audio or CMAF completion, other transport backends, or
other operating systems. When reporting an interoperability problem, include
the SDK revision, relay version, protocol draft and pipeline so the failing
combination can be reproduced.
