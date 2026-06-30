/*
 * moqsink — publish a GStreamer stream over Media-over-QUIC using the
 * libmoq service tier (moq_endpoint_t + moq_media_sender_t).
 *
 * The service tier owns the network thread, version negotiation, the
 * sticky MSF catalog, and the bounded send queue. This element is a thin
 * adapter over it: start() opens a WebTransport endpoint to a MoQ relay
 * and attaches a media sender with one video track; render() wraps each
 * GstBuffer zero-copy and submits it as a media object carrying typed
 * timing/keyframe fields — the service derives and publishes the catalog
 * and generates the LOC-01 property block. No transport pump, no manual
 * publisher, no group/object bookkeeping in this element.
 *
 * Ownership: moq_media_sender_write() takes the payload rcbuf on MOQ_OK;
 * the rcbuf's release callback (invoked on the service's network thread)
 * unmaps and unrefs the GstBuffer. On any non-OK return the caller still
 * owns the rcbuf and decrefs it here, which runs the same release. The
 * rcbuf is created on the streaming thread and freed on the network
 * thread — a clean single-owner handoff, never a shared refcount — and
 * GstBuffer ref/unref/unmap are themselves thread-safe.
 *
 * Topology: the service endpoint is a CLIENT. moqsink and moqsrc both
 * dial a relay (e.g. moqx over WebTransport) rather than each other; the
 * service tier has no listen/server mode.
 *
 * Interrupt: unlock() latches the endpoint interrupt so a blocked write()
 * returns immediately; unlock_stop() clears it.
 */
#include "gstmoqsink.h"

#include <moq/endpoint.h>
#include <moq/media_sender.h>
#include <moq/rcbuf.h>

#include <string.h>

GST_DEBUG_CATEGORY_STATIC (gst_moq_sink_debug);
#define GST_CAT_DEFAULT gst_moq_sink_debug

#define DEFAULT_HOST       "localhost"
#define DEFAULT_PORT       4443
#define DEFAULT_RELAY_PATH "/moq-relay"
#define DEFAULT_NAMESPACE  "example"
#define DEFAULT_TRACK_NAME "video"
#define DEFAULT_CODEC      "avc1.42e01e"
#define DEFAULT_BITRATE    2000000       /* catalog max bitrate (MSF-01 §5.2.22) */

/* On EOS, bound how long the streaming thread waits for the send queue
 * (final GOP + END_OF_TRACK) to drain before teardown. */
#define EOS_DRAIN_TIMEOUT_US (3 * G_USEC_PER_SEC)

enum
{
  PROP_0,
  PROP_HOST,
  PROP_PORT,
  PROP_RELAY_PATH,
  PROP_INSECURE,
  PROP_NAMESPACE,
  PROP_TRACK_NAME,
  PROP_CODEC,
  PROP_BITRATE,
};

/* A mapped access unit, kept alive by the in-flight payload rcbuf. */
typedef struct
{
  GstBuffer *buffer;
  GstMapInfo map;
} SinkFrame;

struct _GstMoqSink
{
  GstBaseSink parent;

  /* properties */
  gchar   *host;
  guint    port;
  gchar   *relay_path;
  gboolean insecure;
  gchar   *namespace_str;
  gchar   *track_name;
  gchar   *codec;
  guint64  bitrate;

  /* runtime, created in start() */
  moq_endpoint_t     *ep;
  moq_media_sender_t *sender;
  moq_media_track_t  *track;
  gboolean            started;

  /* namespace split into borrowed parts (stable for element lifetime) */
  gchar      **ns_tokens;
  moq_bytes_t *ns_bytes;
  guint        ns_count;

  guint64 objects_sent;
  GstClockTime base_pts;   /* first buffer PTS; LOC times are relative to it */
};

G_DEFINE_TYPE (GstMoqSink, gst_moq_sink, GST_TYPE_BASE_SINK)

static GstStaticPadTemplate sink_template =
GST_STATIC_PAD_TEMPLATE ("sink",
    GST_PAD_SINK,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("video/x-h264, "
        "stream-format = (string) byte-stream, "
        "alignment = (string) au"));

/* -- frame node helpers -------------------------------------------------- */

static void
sink_frame_free (SinkFrame *f)
{
  if (!f)
    return;
  gst_buffer_unmap (f->buffer, &f->map);
  gst_buffer_unref (f->buffer);
  g_free (f);
}

/* rcbuf release callback: runs on the service network thread on final
 * decref. Drops our hold on the GstBuffer. */
static void
sink_frame_release (void *ctx, const uint8_t *data, size_t len)
{
  (void) data;
  (void) len;
  sink_frame_free (ctx);
}

/* -- namespace ----------------------------------------------------------- */

static gboolean
gst_moq_sink_build_namespace (GstMoqSink *self)
{
  g_clear_pointer (&self->ns_tokens, g_strfreev);
  g_clear_pointer (&self->ns_bytes, g_free);
  self->ns_count = 0;

  gchar **toks = g_strsplit (self->namespace_str, "/", -1);
  guint n = 0;
  for (gchar **p = toks; *p; p++) {
    if (**p == '\0') {
      GST_ERROR_OBJECT (self, "namespace has an empty part: '%s'",
          self->namespace_str);
      g_strfreev (toks);
      return FALSE;
    }
    n++;
  }
  if (n == 0) {
    GST_ERROR_OBJECT (self, "namespace is empty");
    g_strfreev (toks);
    return FALSE;
  }

  self->ns_tokens = toks;
  self->ns_bytes = g_new0 (moq_bytes_t, n);
  for (guint i = 0; i < n; i++) {
    self->ns_bytes[i].data = (const uint8_t *) toks[i];
    self->ns_bytes[i].len = strlen (toks[i]);
  }
  self->ns_count = n;
  return TRUE;
}

/* -- GstBaseSink vmethods ------------------------------------------------ */

static gboolean
gst_moq_sink_start (GstBaseSink *bsink)
{
  GstMoqSink *self = GST_MOQ_SINK (bsink);

  if (!gst_moq_sink_build_namespace (self))
    return FALSE;

  self->ep = NULL;
  self->sender = NULL;
  self->track = NULL;
  self->objects_sent = 0;
  self->base_pts = GST_CLOCK_TIME_NONE;

  gchar *url = g_strdup_printf ("https://%s:%u%s", self->host, self->port,
      self->relay_path);

  moq_endpoint_cfg_t ec;
  moq_endpoint_cfg_init (&ec);
  ec.url.data = (const uint8_t *) url;
  ec.url.len = strlen (url);
  ec.protocol = MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT;
  ec.insecure_skip_verify = self->insecure;

  moq_result_t rc = moq_endpoint_connect (&ec, &self->ep);
  g_free (url);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("could not open MoQ endpoint to relay %s:%u (rc=%d)",
            self->host, self->port, (int) rc), (NULL));
    goto fail_ns;
  }

  moq_media_sender_cfg_t sc;
  moq_media_sender_cfg_init_live (&sc);
  sc.endpoint = NULL;           /* attach: we own the endpoint above */
  sc.namespace_.parts = self->ns_bytes;
  sc.namespace_.count = self->ns_count;

  rc = moq_media_sender_attach (self->ep, &sc, &self->sender);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("could not attach media sender (rc=%d)", (int) rc), (NULL));
    goto fail_ep;
  }

  moq_media_track_cfg_t tc;
  moq_media_track_cfg_init (&tc);
  tc.name.data = (const uint8_t *) self->track_name;
  tc.name.len = strlen (self->track_name);
  tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
  tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
  tc.codec.data = (const uint8_t *) self->codec;
  tc.codec.len = strlen (self->codec);
  tc.is_live = TRUE;
  tc.bitrate = self->bitrate;   /* MSF-01 §5.2.22: required for media tracks */

  rc = moq_media_sender_add_track (self->sender, &tc, &self->track);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("could not add track \"%s\" (rc=%d)", self->track_name, (int) rc),
        (NULL));
    goto fail_sender;
  }

  self->started = TRUE;
  GST_INFO_OBJECT (self, "started: publishing ns=%s name=%s via relay %s:%u%s",
      self->namespace_str, self->track_name, self->host, self->port,
      self->relay_path);
  return TRUE;

fail_sender:
  moq_media_sender_destroy (self->sender);
  self->sender = NULL;
  self->track = NULL;
fail_ep:
  moq_endpoint_stop (self->ep);
  moq_endpoint_destroy (self->ep);
  self->ep = NULL;
fail_ns:
  g_clear_pointer (&self->ns_tokens, g_strfreev);
  g_clear_pointer (&self->ns_bytes, g_free);
  self->ns_count = 0;
  return FALSE;
}

static gboolean
gst_moq_sink_stop (GstBaseSink *bsink)
{
  GstMoqSink *self = GST_MOQ_SINK (bsink);

  if (!self->started)
    return TRUE;

  /* Wake any blocked write(), then tear down child-before-endpoint. */
  if (self->ep)
    moq_endpoint_set_interrupted (self->ep, TRUE);

  if (self->sender) {
    moq_media_sender_stats_t st;
    if (moq_media_sender_get_stats (self->sender, &st, sizeof st) == MOQ_OK)
      GST_INFO_OBJECT (self, "sender stats: written=%" G_GUINT64_FORMAT
          " sent=%" G_GUINT64_FORMAT " queued=%" G_GUINT64_FORMAT
          " dropped=%" G_GUINT64_FORMAT, st.objects_written, st.objects_sent,
          st.objects_queued, st.objects_dropped);
    moq_media_sender_destroy (self->sender);
    self->sender = NULL;
    self->track = NULL;
  }
  if (self->ep) {
    moq_endpoint_stop (self->ep);
    moq_endpoint_destroy (self->ep);
    self->ep = NULL;
  }

  g_clear_pointer (&self->ns_tokens, g_strfreev);
  g_clear_pointer (&self->ns_bytes, g_free);
  self->ns_count = 0;
  self->started = FALSE;

  GST_INFO_OBJECT (self, "stopped (published %" G_GUINT64_FORMAT " objects)",
      self->objects_sent);
  return TRUE;
}

static gboolean
gst_moq_sink_unlock (GstBaseSink *bsink)
{
  GstMoqSink *self = GST_MOQ_SINK (bsink);
  if (self->ep)
    moq_endpoint_set_interrupted (self->ep, TRUE);
  return TRUE;
}

static gboolean
gst_moq_sink_unlock_stop (GstBaseSink *bsink)
{
  GstMoqSink *self = GST_MOQ_SINK (bsink);
  if (self->ep)
    moq_endpoint_set_interrupted (self->ep, FALSE);
  return TRUE;
}

static GstFlowReturn
gst_moq_sink_render (GstBaseSink *bsink, GstBuffer *buffer)
{
  GstMoqSink *self = GST_MOQ_SINK (bsink);

  if (moq_media_sender_is_fatal (self->sender)) {
    GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
        ("media sender failed (code=%" G_GUINT64_FORMAT ")",
            (guint64) moq_media_sender_fatal_code (self->sender)), (NULL));
    return GST_FLOW_ERROR;
  }

  SinkFrame *f = g_new0 (SinkFrame, 1);
  f->buffer = gst_buffer_ref (buffer);
  if (!gst_buffer_map (f->buffer, &f->map, GST_MAP_READ)) {
    gst_buffer_unref (f->buffer);
    g_free (f);
    GST_ELEMENT_ERROR (self, STREAM, FORMAT, ("failed to map buffer"), (NULL));
    return GST_FLOW_ERROR;
  }

  gboolean keyframe =
      !GST_BUFFER_FLAG_IS_SET (buffer, GST_BUFFER_FLAG_DELTA_UNIT);
  /* Rebase to the first PTS so LOC carries small, stream-relative times.
   * A huge wall-clock-ish base misleads the receiver and is rejected by
   * some relays' object-extension parsers. */
  GstClockTime pts = GST_BUFFER_PTS (buffer);
  GstClockTime dts = GST_BUFFER_DTS (buffer);
  if (GST_CLOCK_TIME_IS_VALID (pts) && !GST_CLOCK_TIME_IS_VALID (self->base_pts))
    self->base_pts = pts;
  guint64 pts_us = (GST_CLOCK_TIME_IS_VALID (pts) &&
      GST_CLOCK_TIME_IS_VALID (self->base_pts) && pts >= self->base_pts)
      ? (pts - self->base_pts) / GST_USECOND : 0;
  guint64 dts_us = (GST_CLOCK_TIME_IS_VALID (dts) &&
      GST_CLOCK_TIME_IS_VALID (self->base_pts) && dts >= self->base_pts)
      ? (dts - self->base_pts) / GST_USECOND : pts_us;

  moq_rcbuf_t *payload = NULL;
  if (moq_rcbuf_wrap (moq_alloc_default (), f->map.data, f->map.size,
          sink_frame_release, f, &payload) != MOQ_OK) {
    sink_frame_free (f);
    GST_ELEMENT_ERROR (self, RESOURCE, WRITE, ("rcbuf wrap failed"), (NULL));
    return GST_FLOW_ERROR;
  }

  /* GOP-per-group: a keyframe is the random-access point and opens a new
   * group (which closes any open one); deltas extend it. The service owns
   * subgroup/object numbering and the LOC property block. */
  moq_media_send_object_t o;
  memset (&o, 0, sizeof o);
  o.struct_size = sizeof o;
  o.payload = payload;
  o.properties = NULL;
  o.is_sync = keyframe;
  o.starts_group = keyframe;
  o.ends_group = FALSE;
  o.decode_time_us = dts_us;
  o.presentation_time_us = pts_us;
  o.has_capture_time = FALSE;

  moq_result_t rc = moq_media_sender_write (self->sender, self->track, &o);
  switch (rc) {
    case MOQ_OK:
      self->objects_sent++;
      GST_LOG_OBJECT (self, "wrote %s pts=%" G_GUINT64_FORMAT "us (%zu B)",
          keyframe ? "keyframe" : "delta", pts_us, f->map.size);
      return GST_FLOW_OK;
    case MOQ_ERR_WOULD_BLOCK:
      /* Live policy: a GOP the queue can't hold, or a delta before the
       * first keyframe. Drop it (the service never blocks the encoder). */
      moq_rcbuf_decref (payload);
      GST_LOG_OBJECT (self, "dropped %s frame under backpressure",
          keyframe ? "keyframe" : "delta");
      return GST_FLOW_OK;
    case MOQ_ERR_INTERRUPTED:
      moq_rcbuf_decref (payload);
      return GST_FLOW_FLUSHING;
    case MOQ_ERR_CLOSED:
      moq_rcbuf_decref (payload);
      GST_INFO_OBJECT (self, "endpoint closed; ending stream");
      return GST_FLOW_EOS;
    default:
      moq_rcbuf_decref (payload);
      GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
          ("media write failed (rc=%d)", (int) rc), (NULL));
      return GST_FLOW_ERROR;
  }
}

/* Signal a clean end-of-track on EOS so subscribers see TRACK_ENDED rather
 * than a dropped connection, then drain the send queue (bounded) so the
 * final GOP and the END_OF_TRACK reach the wire before stop() tears the
 * endpoint down. */
static gboolean
gst_moq_sink_event (GstBaseSink *bsink, GstEvent *event)
{
  GstMoqSink *self = GST_MOQ_SINK (bsink);

  if (GST_EVENT_TYPE (event) == GST_EVENT_EOS && self->sender && self->track) {
    moq_result_t rc = moq_media_sender_end_track (self->sender, self->track);
    if (rc == MOQ_OK) {
      gint64 deadline = g_get_monotonic_time () + EOS_DRAIN_TIMEOUT_US;
      for (;;) {
        moq_media_sender_stats_t st;
        if (moq_media_sender_get_stats (self->sender, &st, sizeof st) != MOQ_OK)
          break;
        if (st.objects_queued == 0) {
          GST_INFO_OBJECT (self, "ended track on EOS (send queue drained)");
          break;
        }
        if (moq_media_sender_is_fatal (self->sender))
          break;
        if (g_get_monotonic_time () >= deadline) {
          GST_WARNING_OBJECT (self, "EOS drain timed out, %" G_GUINT64_FORMAT
              " objects still queued", st.objects_queued);
          break;
        }
        g_usleep (5000);
      }
      /* Flush the transport send queues so the final GOP and the END_OF_TRACK
       * actually leave the wire before stop() tears the endpoint down. */
      if (self->ep)
        moq_endpoint_drain (self->ep, EOS_DRAIN_TIMEOUT_US);
    } else if (rc != MOQ_ERR_WRONG_STATE) {  /* WRONG_STATE = already ended */
      GST_WARNING_OBJECT (self, "end_track failed (rc=%d)", (int) rc);
    }
  }

  return GST_BASE_SINK_CLASS (gst_moq_sink_parent_class)->event (bsink, event);
}

/* -- GObject ------------------------------------------------------------- */

static void
gst_moq_sink_set_property (GObject *object, guint prop_id,
    const GValue *value, GParamSpec *pspec)
{
  GstMoqSink *self = GST_MOQ_SINK (object);
  switch (prop_id) {
    case PROP_HOST:
      g_free (self->host);
      self->host = g_value_dup_string (value);
      break;
    case PROP_PORT:
      self->port = g_value_get_uint (value);
      break;
    case PROP_RELAY_PATH:
      g_free (self->relay_path);
      self->relay_path = g_value_dup_string (value);
      break;
    case PROP_INSECURE:
      self->insecure = g_value_get_boolean (value);
      break;
    case PROP_NAMESPACE:
      g_free (self->namespace_str);
      self->namespace_str = g_value_dup_string (value);
      break;
    case PROP_TRACK_NAME:
      g_free (self->track_name);
      self->track_name = g_value_dup_string (value);
      break;
    case PROP_CODEC:
      g_free (self->codec);
      self->codec = g_value_dup_string (value);
      break;
    case PROP_BITRATE:
      self->bitrate = g_value_get_uint64 (value);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
  }
}

static void
gst_moq_sink_get_property (GObject *object, guint prop_id,
    GValue *value, GParamSpec *pspec)
{
  GstMoqSink *self = GST_MOQ_SINK (object);
  switch (prop_id) {
    case PROP_HOST:
      g_value_set_string (value, self->host);
      break;
    case PROP_PORT:
      g_value_set_uint (value, self->port);
      break;
    case PROP_RELAY_PATH:
      g_value_set_string (value, self->relay_path);
      break;
    case PROP_INSECURE:
      g_value_set_boolean (value, self->insecure);
      break;
    case PROP_NAMESPACE:
      g_value_set_string (value, self->namespace_str);
      break;
    case PROP_TRACK_NAME:
      g_value_set_string (value, self->track_name);
      break;
    case PROP_CODEC:
      g_value_set_string (value, self->codec);
      break;
    case PROP_BITRATE:
      g_value_set_uint64 (value, self->bitrate);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
  }
}

static void
gst_moq_sink_finalize (GObject *object)
{
  GstMoqSink *self = GST_MOQ_SINK (object);
  g_free (self->host);
  g_free (self->relay_path);
  g_free (self->namespace_str);
  g_free (self->track_name);
  g_free (self->codec);
  G_OBJECT_CLASS (gst_moq_sink_parent_class)->finalize (object);
}

static void
gst_moq_sink_class_init (GstMoqSinkClass *klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  GstElementClass *element_class = GST_ELEMENT_CLASS (klass);
  GstBaseSinkClass *basesink_class = GST_BASE_SINK_CLASS (klass);

  GST_DEBUG_CATEGORY_INIT (gst_moq_sink_debug, "moqsink", 0, "MoQ sink");

  gobject_class->set_property = gst_moq_sink_set_property;
  gobject_class->get_property = gst_moq_sink_get_property;
  gobject_class->finalize = gst_moq_sink_finalize;

  g_object_class_install_property (gobject_class, PROP_HOST,
      g_param_spec_string ("host", "Host", "MoQ relay host to connect to",
          DEFAULT_HOST, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_PORT,
      g_param_spec_uint ("port", "Port", "MoQ relay UDP/QUIC port", 1, 65535,
          DEFAULT_PORT, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_RELAY_PATH,
      g_param_spec_string ("relay-path", "Relay path",
          "WebTransport endpoint path on the relay", DEFAULT_RELAY_PATH,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_INSECURE,
      g_param_spec_boolean ("insecure", "Insecure",
          "Skip TLS verification (demo/test only)", FALSE,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_NAMESPACE,
      g_param_spec_string ("namespace", "Track namespace",
          "Slash-separated MoQ track namespace", DEFAULT_NAMESPACE,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_TRACK_NAME,
      g_param_spec_string ("track-name", "Track name", "MoQ track name",
          DEFAULT_TRACK_NAME, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_CODEC,
      g_param_spec_string ("codec", "Codec",
          "Catalog codec string for the published track", DEFAULT_CODEC,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_BITRATE,
      g_param_spec_uint64 ("bitrate", "Bitrate",
          "Advertised catalog max bitrate (bits/s)", 1, G_MAXUINT64,
          DEFAULT_BITRATE, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  gst_element_class_add_static_pad_template (element_class, &sink_template);
  gst_element_class_set_static_metadata (element_class,
      "MoQ sink", "Sink/Network",
      "Publish a stream over Media-over-QUIC (libmoq service tier)",
      "Ray L <ray@raylucke.com>");

  basesink_class->start = GST_DEBUG_FUNCPTR (gst_moq_sink_start);
  basesink_class->stop = GST_DEBUG_FUNCPTR (gst_moq_sink_stop);
  basesink_class->render = GST_DEBUG_FUNCPTR (gst_moq_sink_render);
  basesink_class->unlock = GST_DEBUG_FUNCPTR (gst_moq_sink_unlock);
  basesink_class->unlock_stop = GST_DEBUG_FUNCPTR (gst_moq_sink_unlock_stop);
  basesink_class->event = GST_DEBUG_FUNCPTR (gst_moq_sink_event);
}

static void
gst_moq_sink_init (GstMoqSink *self)
{
  self->host = g_strdup (DEFAULT_HOST);
  self->port = DEFAULT_PORT;
  self->relay_path = g_strdup (DEFAULT_RELAY_PATH);
  self->namespace_str = g_strdup (DEFAULT_NAMESPACE);
  self->track_name = g_strdup (DEFAULT_TRACK_NAME);
  self->codec = g_strdup (DEFAULT_CODEC);
  self->bitrate = DEFAULT_BITRATE;
}
