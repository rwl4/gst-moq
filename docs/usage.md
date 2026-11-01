# Pipelines and properties

The [README](../README.md) has a basic H.264 publish/playback pair. These
examples use `RELAY` as a placeholder; choose the port and WebTransport path
for your relay. They use automatic version negotiation, preferring draft 22,
then 18, then 16. TLS verification is on by default.

Each `moqsrc` selects one track by name. To receive audio and video together,
use a separate source element for each track.

## LOC video and audio

The always-present `video` pad accepts H.264 Annex B access units. A keyframe
opens a group; subsequent delta frames extend it. Use
`h264parse config-interval=-1` so every keyframe carries SPS/PPS. The first
keyframe supplies the catalog's avcC `initData`, SPS-derived codec string,
and dimensions/framerate from the caps.

MOQ5 selects the media companion for the negotiated transport version:
LOC-04 metadata on draft 22, LOC-01 properties on drafts 18 and 16.

The `audio` pad accepts raw AAC-LC access units. Both tracks share one catalog:

```sh
gst-launch-1.0 -e moqsink name=ms host=RELAY port=4433 \
    relay-path=/moq-relay namespace=example draft=0 \
  videotestsrc is-live=true ! video/x-raw,framerate=30/1 \
    ! x264enc tune=zerolatency key-int-max=30 ! h264parse config-interval=-1 \
    ! video/x-h264,stream-format=byte-stream,alignment=au ! ms.video \
  audiotestsrc is-live=true ! voaacenc ! aacparse \
    ! audio/mpeg,mpegversion=4,stream-format=raw ! ms.audio
```

Use raw AAC, not ADTS: the catalog carries the AudioSpecificConfig from
`codec_data`, along with `samplerate` and `channelConfig`. LOC publishing
accepts a supported two-byte AAC-LC config with an indexed sample rate and
explicit channel configuration, consistent with the caps. HE-AAC,
explicit-frequency, extension/trailing and program-config-element forms are
rejected. These restrictions do not apply to CMAF codec-string support.

The element's `track-name` and `bitrate` properties describe the video pad.
The audio pad has its own properties, for example
`ms.audio::track-name=commentary`; its default track name is `audio`.
Audio groups follow observed video keyframes, with `loc-audio-group-ms`
(default 1000) as an upper bound. This does not guarantee simultaneous relay
delivery of the two tracks.

To receive the audio track:

```sh
gst-launch-1.0 moqsrc host=RELAY port=4433 relay-path=/moq-relay \
    namespace=example track-name=audio draft=0 \
  ! aacparse ! avdec_aac ! autoaudiosink
```

## CMAF

Request pads `video_%u` and `audio_%u` accept fragmented MP4. Feed each pad
from its own muxer. The init segment goes into the CMSF-01 catalog; each
moof+mdat fragment becomes a MoQ object. A fragment starting on a sync sample
opens a new group. Match fragment duration to the GOP length.

```sh
gst-launch-1.0 -e moqsink name=ms host=RELAY port=4433 \
    relay-path=/moq-relay namespace=example draft=0 \
  videotestsrc is-live=true ! video/x-raw,framerate=30/1 \
    ! x264enc tune=zerolatency key-int-max=30 ! h264parse \
    ! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss ! ms.video_0 \
  audiotestsrc is-live=true ! voaacenc ! aacparse \
    ! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss ! ms.audio_0
```

For Opus input, replace the audio branch with
`audiotestsrc is-live=true ! opusenc ! opusparse ! mp4mux ...`.
Each request pad has `track-name` and `bitrate` properties, for example
`ms.video_0::track-name=hd`.

Receive a CMAF track through `qtdemux`; `moqsrc` derives its caps and emits
the catalog init segment before fragments:

```sh
gst-launch-1.0 moqsrc host=RELAY port=4433 relay-path=/moq-relay \
    namespace=example track-name=video draft=0 \
  ! qtdemux ! h264parse ! avdec_h264 ! autovideosink

gst-launch-1.0 moqsrc host=RELAY port=4433 relay-path=/moq-relay \
    namespace=example track-name=audio draft=0 \
  ! qtdemux ! aacparse ! avdec_aac ! autoaudiosink
```

Encrypted CMAF requires `drm-systems`: semicolon-separated system UUIDs with
optional comma-separated `laurl`, `certurl`, `authurl`, `pssh` (Base64) and
`robustness` fields. Values cannot contain commas or semicolons. The scheme
and default KID come from the init segment. Missing coherent protection
signaling fails preparation before connecting; ciphertext is never described
as clear content. The plugin advertises metadata; it does not obtain keys
or decrypt the stream.

`sap-timeline=true` rejects CMAF preparation because sample flags alone do not
establish exact SAP classification or the earliest presentation time across
all samples. Ordinary CMAF publishing retains sync group boundaries.

## Connection and track settings

Use `gst-inspect-1.0 moqsink` and `gst-inspect-1.0 moqsrc` for the full
property reference.

| Property | Default | Meaning |
| --- | --- | --- |
| `host` | `localhost` | Relay hostname. |
| `port` | `4443` | Relay UDP port. |
| `relay-path` | `/moq-relay` | WebTransport path. |
| `namespace` | `example` | Slash-separated track namespace. |
| `track-name` | `video` | Published video track or selected receive track. |
| `draft` | `0` | Negotiate 22, then 18, then 16; set 22, 18 or 16 to require one version. |
| `insecure` | `false` | Skip TLS verification; use only for tests. |

On `moqsink`, `codec` can override the H.264 catalog codec string, and
`bitrate` sets its advertised maximum bitrate. `max-fragment-size` limits
each ISO BMFF box on a CMAF pad; its default is 16 MiB.

Fallback happens during version negotiation, not by retrying after a TLS,
authentication or media failure. Native backend capabilities also determine
which versions can be offered.

Relays bridging different drafts must convert LOC metadata. If your relay
cannot do that, set the publisher and subscribers to the same explicit draft.
See [compatibility](compatibility.md) for tested combinations.

`moqsrc` derives caps for CMAF, H.264 Annex B LOC and AAC-LC LOC. Other LOC
codecs need explicit compatible `caps`. For length-prefixed AVC, supply
`video/x-h264,stream-format=avc,alignment=au` caps with the matching
`codec_data`; automatic Annex B caps do not perform that conversion.

The receiver anchors the first LOC object's presentation time to pipeline
running time, preserving subsequent offsets. `latency` defaults to 200 ms and
is reported through the LATENCY query. `discovery-timeout` defaults to
10,000 ms; zero fails immediately if the track is not available.

## Startup, flush and completion

Link every publishing pad before the first buffer. The sender waits for
every linked pad to prepare its first track, then publishes the initial
catalog atomically. `catalog-wait-ms` defaults to 3000; an incomplete set
fails without connecting. Put queues before pads fed by a common upstream
streaming task so one pad's wait cannot prevent another from preparing.
Empty streams finish without opening a connection.

`moqsink` is a plain `GstElement`, not a `GstBaseSink`. It does not preroll,
and PAUSED does not suspend publishing. `sync=true` paces buffers against the
pipeline clock; BaseSink properties such as `async`, `qos`, `max-lateness`,
`ts-offset`, `blocksize`, `enable-last-sample` and `render-delay` do not exist.
Capture times use each pad's segment running time, preserving audio/video
offsets through encoder timestamp shifts.

Publisher EOS uses one three-second acceptance/drain budget and requires
MOQ5's service-aware endpoint drain. Errors and cancellation report failure
instead of successful EOS. A local drain does not prove peer receipt or
decode. Released pads retain their end obligations until sender teardown.
FLUSH_STOP does not reopen an ended track; restart through READY to create
a fresh sender and change the initial stream set.

`moqsrc` delivers accepted queued objects before reporting a selected track's
local failure. Unrelated-track and fill-only failures do not terminate its
selected live track. Native polling/wait errors are not treated as empty queues.

`catalog-refresh-ms` defaults to zero. Enable periodic refresh only when a
relay needs it for catalog-cache compatibility.

## Authentication

Both elements accept the following properties in NULL or READY:

| Property | Meaning | Default |
| --- | --- | --- |
| `setup-auth-token` | Write-only string: canonical padded standard Base64 of the opaque token payload. | NULL: anonymous. |
| `setup-auth-token-type` | Unsigned 64-bit token type. | `1`: standard CAT. |

For CAT4MOQ, obtain the CBOR/CWT bytes from your issuer and Base64-encode
them. Set the properties programmatically before starting the pipeline:

```python
element.set_property("setup-auth-token-type", 1)
element.set_property("setup-auth-token", token_base64)
```

Configure each publisher and subscriber with credentials for its role,
namespace and tracks. Catalog-driven receive also needs access to `catalog`;
an exact `video` capability may exclude it. A namespace-wide subscriber
capability can cover both without granting other namespaces.

Do not put tokens in command lines or logs. Do not include a `base64:` prefix,
an HTTP Authorization header or a serialized MoQ Token structure. Private
deployments using another token type must select it explicitly: the test lab's
type `16` is not the standard CAT number.

NULL clears the credential. Empty, noncanonical or oversized configured values
fail before connecting, without anonymous fallback. Changes after READY are
rejected. Each connection owns a snapshot; flush preserves it and restart
reuses the configured credential. Independent elements have independent settings.

The frontend allows up to 65,535 decoded bytes / 87,380 encoded characters.
MOQ5 also checks the complete SETUP against its native arena budget, normally
4096 bytes including other options. The frontend limit does not guarantee
connection admission. Types retain all 64 bits; draft 16 permits only
`2^62-1` or below, and native profile admission (including stricter AUTO
admission) may refuse a value. Type `0` is explicit, not absence.

This API presents SETUP credentials; the relay validates them. It does not
implement an issuer, client claim validation, refresh or DPoP. A relay may
retain SETUP authorization for later operations, but explicit per-request
credentials need a separate safe MOQ5 resource-auth binding, which the pinned
dependency does not expose. SETUP success alone is not media authorization.
