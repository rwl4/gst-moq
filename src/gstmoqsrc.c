/*
 * moqsrc — subscribe to a Media-over-QUIC track and push buffers using the
 * libmoq service tier (moq_endpoint_t + moq_media_receiver_t).
 *
 * The service tier owns the network thread, the catalog-track subscription,
 * MSF catalog parsing, per-track auto-subscription, and a bounded object
 * queue. This element is a thin adapter: start() opens a WebTransport
 * endpoint to a MoQ relay and attaches a media receiver for the configured
 * namespace; create() drains track-discovery events (TRACK_ADDED /
 * CATALOG_READY) and media objects directly from the receiver and turns
 * each object into a GstBuffer. No transport pump, no manual subscriber,
 * no LOC parsing in this element — the receiver delivers parsed timing and
 * keyframe flags.
 *
 * Copy semantics: a polled object transfers exclusive ownership of its
 * backing buffers to us until moq_media_object_cleanup(), which the service
 * permits on the polling thread. We COPY the payload into a GstBuffer
 * (gst_buffer_new_memdup) and clean the object up immediately, so no libmoq
 * rcbuf is ever freed on an arbitrary downstream thread.
 *
 * Topology: the service endpoint is a CLIENT. moqsrc and moqsink both dial
 * a relay (e.g. moqx over WebTransport) rather than each other.
 *
 * Timestamps: LOC presentation times are publisher-relative (moqsink rebases
 * them to its first frame, other publishers may use wall-clock epochs), so
 * they are meaningless on this pipeline's clock. The first delivered object
 * is pinned to the pipeline's running time at arrival and every later PTS
 * keeps its offset from that anchor, which preserves the publisher's frame
 * cadence. The "latency" property is reported through the LATENCY query so
 * synced sinks absorb network jitter instead of dropping late frames.
 *
 * Interrupt: unlock() latches the endpoint interrupt so a blocked poll/wait
 * returns immediately; unlock_stop() clears it.
 */
#include "gstmoqsrc.h"

#include <moq/endpoint.h>
#include <moq/media_receiver.h>

#include <string.h>

GST_DEBUG_CATEGORY_STATIC (gst_moq_src_debug);
#define GST_CAT_DEFAULT gst_moq_src_debug

#define DEFAULT_HOST       "localhost"
#define DEFAULT_PORT       4443
#define DEFAULT_RELAY_PATH "/moq-relay"
#define DEFAULT_NAMESPACE  "example"
#define DEFAULT_TRACK_NAME "video"
#define WAIT_TIMEOUT_US    (100 * 1000)
#define DEFAULT_LATENCY_MS 200
#define DEFAULT_DRAFT      16            /* browsers (playa) default to draft-16; 0 = offer all */

enum
{
  PROP_0,
  PROP_HOST,
  PROP_PORT,
  PROP_RELAY_PATH,
  PROP_INSECURE,
  PROP_NAMESPACE,
  PROP_TRACK_NAME,
  PROP_CAPS,
  PROP_LATENCY,
  PROP_DRAFT,
};

struct _GstMoqSrc
{
  GstPushSrc parent;

  /* properties */
  gchar   *host;
  guint    port;
  gchar   *relay_path;
  gboolean insecure;
  gchar   *namespace_str;
  gchar   *track_name;
  GstCaps *caps;
  guint    latency_ms;
  guint    draft;          /* MoQT draft to negotiate, 0 = auto */

  /* runtime */
  moq_endpoint_t       *ep;
  moq_media_receiver_t *receiver;
  moq_media_track_t    *want_track;   /* handle matching track-name, or NULL */
  gboolean              started;
  gboolean              caps_pushed;
  gboolean              catalog_ready;

  /* namespace parts (stable for element lifetime) */
  gchar      **ns_tokens;
  moq_bytes_t *ns_bytes;
  guint        ns_count;

  guint64 objects_recv;

  /* timestamp anchor: first object's LOC time -> pipeline running time */
  gboolean     have_anchor;
  guint64      anchor_loc_us;
  GstClockTime anchor_running;
};

G_DEFINE_TYPE (GstMoqSrc, gst_moq_src, GST_TYPE_PUSH_SRC)

static GstStaticPadTemplate src_template =
GST_STATIC_PAD_TEMPLATE ("src",
    GST_PAD_SRC,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS_ANY);

/* -- namespace ----------------------------------------------------------- */

static gboolean
gst_moq_src_build_namespace (GstMoqSrc *self)
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

/* -- GstBaseSrc / GstPushSrc vmethods ------------------------------------ */

static gboolean
gst_moq_src_start (GstBaseSrc *bsrc)
{
  GstMoqSrc *self = GST_MOQ_SRC (bsrc);

  if (!self->caps || gst_caps_is_any (self->caps)) {
    GST_ELEMENT_ERROR (self, CORE, NEGOTIATION,
        ("the \"caps\" property must be set to the track's media caps"),
        (NULL));
    return FALSE;
  }
  if (!gst_moq_src_build_namespace (self))
    return FALSE;

  self->ep = NULL;
  self->receiver = NULL;
  self->want_track = NULL;
  self->caps_pushed = FALSE;
  self->catalog_ready = FALSE;
  self->objects_recv = 0;
  self->have_anchor = FALSE;

  gchar *url = g_strdup_printf ("https://%s:%u%s", self->host, self->port,
      self->relay_path);

  moq_endpoint_cfg_t ec;
  moq_endpoint_cfg_init (&ec);
  ec.url.data = (const uint8_t *) url;
  ec.url.len = strlen (url);
  ec.protocol = MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT;
  ec.insecure_skip_verify = self->insecure;
  moq_version_t pinned = (moq_version_t) self->draft;
  if (self->draft != 0) {
    ec.versions.struct_size = sizeof ec.versions;
    ec.versions.policy = MOQ_VERSION_POLICY_EXACT;
    ec.versions.versions = &pinned;
    ec.versions.version_count = 1;
  }

  moq_result_t rc = moq_endpoint_connect (&ec, &self->ep);
  g_free (url);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_READ,
        ("could not open MoQ endpoint to relay %s:%u (rc=%d)",
            self->host, self->port, (int) rc), (NULL));
    goto fail_ns;
  }

  moq_media_receiver_cfg_t rcfg;
  moq_media_receiver_cfg_init_live (&rcfg);
  rcfg.endpoint = NULL;         /* attach: we own the endpoint above */
  rcfg.namespace_.parts = self->ns_bytes;
  rcfg.namespace_.count = self->ns_count;
  rcfg.auto_subscribe = TRUE;

  rc = moq_media_receiver_attach (self->ep, &rcfg, &self->receiver);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_READ,
        ("could not attach media receiver (rc=%d)", (int) rc), (NULL));
    goto fail_ep;
  }

  self->started = TRUE;
  GST_INFO_OBJECT (self, "started: receiving ns=%s name=%s via relay %s:%u%s",
      self->namespace_str, self->track_name, self->host, self->port,
      self->relay_path);
  return TRUE;

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
gst_moq_src_stop (GstBaseSrc *bsrc)
{
  GstMoqSrc *self = GST_MOQ_SRC (bsrc);

  if (!self->started)
    return TRUE;

  if (self->ep)
    moq_endpoint_set_interrupted (self->ep, TRUE);

  if (self->receiver) {
    moq_media_receiver_destroy (self->receiver);
    self->receiver = NULL;
    self->want_track = NULL;
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

  GST_INFO_OBJECT (self, "stopped (received %" G_GUINT64_FORMAT " objects)",
      self->objects_recv);
  return TRUE;
}

static gboolean
gst_moq_src_unlock (GstBaseSrc *bsrc)
{
  GstMoqSrc *self = GST_MOQ_SRC (bsrc);
  if (self->ep)
    moq_endpoint_set_interrupted (self->ep, TRUE);
  return TRUE;
}

static gboolean
gst_moq_src_unlock_stop (GstBaseSrc *bsrc)
{
  GstMoqSrc *self = GST_MOQ_SRC (bsrc);
  if (self->ep)
    moq_endpoint_set_interrupted (self->ep, FALSE);
  return TRUE;
}

static gboolean
gst_moq_src_query (GstBaseSrc *bsrc, GstQuery *query)
{
  GstMoqSrc *self = GST_MOQ_SRC (bsrc);

  if (GST_QUERY_TYPE (query) == GST_QUERY_LATENCY) {
    GstClockTime lat = self->latency_ms * GST_MSECOND;
    gst_query_set_latency (query, TRUE, lat, GST_CLOCK_TIME_NONE);
    return TRUE;
  }
  return GST_BASE_SRC_CLASS (gst_moq_src_parent_class)->query (bsrc, query);
}

/* Map a LOC presentation time onto this pipeline's running time. */
static GstClockTime
gst_moq_src_map_pts (GstMoqSrc *self, guint64 loc_us)
{
  if (!self->have_anchor) {
    GstClock *clock = gst_element_get_clock (GST_ELEMENT (self));
    GstClockTime now = GST_CLOCK_TIME_NONE;
    if (clock) {
      GstClockTime base = gst_element_get_base_time (GST_ELEMENT (self));
      GstClockTime abs = gst_clock_get_time (clock);
      if (GST_CLOCK_TIME_IS_VALID (base) && abs >= base)
        now = abs - base;
      gst_object_unref (clock);
    }
    self->anchor_running = GST_CLOCK_TIME_IS_VALID (now) ? now : 0;
    self->anchor_loc_us = loc_us;
    self->have_anchor = TRUE;
    GST_INFO_OBJECT (self, "anchored LOC time %" G_GUINT64_FORMAT
        "us at running time %" GST_TIME_FORMAT, loc_us,
        GST_TIME_ARGS (self->anchor_running));
  }
  if (loc_us >= self->anchor_loc_us)
    return self->anchor_running + (loc_us - self->anchor_loc_us) * GST_USECOND;
  /* Earlier than the anchor (reordered / late object): clamp. */
  GstClockTime back = (self->anchor_loc_us - loc_us) * GST_USECOND;
  return back < self->anchor_running ? self->anchor_running - back : 0;
}

/* Drain pending track-discovery events. Logs catalog discovery and binds
 * want_track to the handle whose catalog name matches track-name. */
static void
gst_moq_src_drain_track_events (GstMoqSrc *self)
{
  moq_media_track_event_t ev;
  while (moq_media_receiver_poll_track (self->receiver, &ev, sizeof (ev)) ==
      MOQ_OK) {
    switch (ev.kind) {
      case MOQ_MEDIA_TRACK_ADDED: {
        const moq_media_track_desc_t *d = ev.desc;
        GST_INFO_OBJECT (self, "TRACK_ADDED name=%.*s codec=%.*s",
            d ? (int) d->name.len : 0,
            d && d->name.data ? (const char *) d->name.data : "",
            d ? (int) d->codec.len : 0,
            d && d->codec.data ? (const char *) d->codec.data : "");
        if (d && d->name.len == strlen (self->track_name) &&
            memcmp (d->name.data, self->track_name, d->name.len) == 0)
          self->want_track = ev.track;
        break;
      }
      case MOQ_MEDIA_CATALOG_READY:
        self->catalog_ready = TRUE;
        GST_INFO_OBJECT (self, "CATALOG_READY");
        break;
      default:
        break;
    }
  }
}

static GstFlowReturn
gst_moq_src_create (GstPushSrc *psrc, GstBuffer **out)
{
  GstMoqSrc *self = GST_MOQ_SRC (psrc);

  if (!self->caps_pushed) {
    gst_base_src_set_caps (GST_BASE_SRC (self), self->caps);
    self->caps_pushed = TRUE;
  }

  for (;;) {
    /* Discovery first: every handle is known before its first object. */
    gst_moq_src_drain_track_events (self);

    moq_media_object_t obj;
    moq_result_t rc =
        moq_media_receiver_poll_object (self->receiver, &obj, sizeof (obj));

    if (rc == MOQ_OK) {
      /* When track-name resolved to a handle, deliver only that track. */
      if (self->want_track && obj.track != self->want_track) {
        moq_media_object_cleanup (&obj);
        continue;
      }
      if (obj.payload.len == 0) {
        moq_media_object_cleanup (&obj);
        continue;
      }
      GstBuffer *buf = gst_buffer_new_memdup (obj.payload.data, obj.payload.len);
      GST_BUFFER_PTS (buf) = gst_moq_src_map_pts (self, obj.presentation_time_us);
      if (!obj.keyframe)
        GST_BUFFER_FLAG_SET (buf, GST_BUFFER_FLAG_DELTA_UNIT);

      self->objects_recv++;
      GST_LOG_OBJECT (self, "received %s pts=%" G_GUINT64_FORMAT "us (%zu B)",
          obj.keyframe ? "keyframe" : "delta", obj.presentation_time_us,
          obj.payload.len);
      moq_media_object_cleanup (&obj);

      *out = buf;
      return GST_FLOW_OK;
    }

    if (rc == MOQ_ERR_INTERRUPTED)
      return GST_FLOW_FLUSHING;

    if (moq_media_receiver_is_fatal (self->receiver)) {
      GST_ELEMENT_ERROR (self, RESOURCE, READ,
          ("media receiver failed (code=%" G_GUINT64_FORMAT ")",
              (guint64) moq_media_receiver_fatal_code (self->receiver)),
          (NULL));
      return GST_FLOW_ERROR;
    }

    if (rc == MOQ_ERR_CLOSED) {
      GST_INFO_OBJECT (self, "endpoint closed; sending EOS");
      return GST_FLOW_EOS;
    }

    /* rc == MOQ_DONE: nothing queued. Wait for the next wakeup. */
    moq_result_t w = moq_media_receiver_wait (self->receiver, WAIT_TIMEOUT_US);
    if (w == MOQ_ERR_INTERRUPTED)
      return GST_FLOW_FLUSHING;
    /* MOQ_OK (woke) / MOQ_DONE (timeout) / MOQ_ERR_CLOSED: loop; the next
     * poll classifies terminal vs more data deterministically. */
  }
}

/* -- GObject ------------------------------------------------------------- */

static void
gst_moq_src_set_property (GObject *object, guint prop_id,
    const GValue *value, GParamSpec *pspec)
{
  GstMoqSrc *self = GST_MOQ_SRC (object);
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
    case PROP_CAPS:
      gst_caps_replace (&self->caps, (GstCaps *) gst_value_get_caps (value));
      break;
    case PROP_LATENCY:
      self->latency_ms = g_value_get_uint (value);
      break;
    case PROP_DRAFT:
      self->draft = g_value_get_uint (value);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
  }
}

static void
gst_moq_src_get_property (GObject *object, guint prop_id,
    GValue *value, GParamSpec *pspec)
{
  GstMoqSrc *self = GST_MOQ_SRC (object);
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
    case PROP_CAPS:
      gst_value_set_caps (value, self->caps);
      break;
    case PROP_LATENCY:
      g_value_set_uint (value, self->latency_ms);
      break;
    case PROP_DRAFT:
      g_value_set_uint (value, self->draft);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
  }
}

static void
gst_moq_src_finalize (GObject *object)
{
  GstMoqSrc *self = GST_MOQ_SRC (object);
  g_free (self->host);
  g_free (self->relay_path);
  g_free (self->namespace_str);
  g_free (self->track_name);
  gst_caps_replace (&self->caps, NULL);
  G_OBJECT_CLASS (gst_moq_src_parent_class)->finalize (object);
}

static void
gst_moq_src_class_init (GstMoqSrcClass *klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  GstElementClass *element_class = GST_ELEMENT_CLASS (klass);
  GstBaseSrcClass *basesrc_class = GST_BASE_SRC_CLASS (klass);
  GstPushSrcClass *pushsrc_class = GST_PUSH_SRC_CLASS (klass);

  GST_DEBUG_CATEGORY_INIT (gst_moq_src_debug, "moqsrc", 0, "MoQ source");

  gobject_class->set_property = gst_moq_src_set_property;
  gobject_class->get_property = gst_moq_src_get_property;
  gobject_class->finalize = gst_moq_src_finalize;

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
      g_param_spec_string ("track-name", "Track name",
          "MoQ track name to deliver (matched against the catalog)",
          DEFAULT_TRACK_NAME, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_CAPS,
      g_param_spec_boxed ("caps", "Caps",
          "Output caps describing the subscribed track's media",
          GST_TYPE_CAPS, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_LATENCY,
      g_param_spec_uint ("latency", "Latency",
          "Latency reported to the pipeline (ms) to absorb network jitter",
          0, 10000, DEFAULT_LATENCY_MS,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_DRAFT,
      g_param_spec_uint ("draft", "MoQT draft",
          "MoQ Transport draft version to negotiate (16 or 18); "
          "0 offers every draft libmoq supports", 0, 18, DEFAULT_DRAFT,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  gst_element_class_add_static_pad_template (element_class, &src_template);
  gst_element_class_set_static_metadata (element_class,
      "MoQ source", "Source/Network",
      "Subscribe to a Media-over-QUIC track (libmoq service tier)",
      "Ray L <ray@raylucke.com>");

  basesrc_class->start = GST_DEBUG_FUNCPTR (gst_moq_src_start);
  basesrc_class->stop = GST_DEBUG_FUNCPTR (gst_moq_src_stop);
  basesrc_class->unlock = GST_DEBUG_FUNCPTR (gst_moq_src_unlock);
  basesrc_class->unlock_stop = GST_DEBUG_FUNCPTR (gst_moq_src_unlock_stop);
  basesrc_class->query = GST_DEBUG_FUNCPTR (gst_moq_src_query);
  pushsrc_class->create = GST_DEBUG_FUNCPTR (gst_moq_src_create);
}

static void
gst_moq_src_init (GstMoqSrc *self)
{
  self->host = g_strdup (DEFAULT_HOST);
  self->port = DEFAULT_PORT;
  self->relay_path = g_strdup (DEFAULT_RELAY_PATH);
  self->namespace_str = g_strdup (DEFAULT_NAMESPACE);
  self->track_name = g_strdup (DEFAULT_TRACK_NAME);
  self->latency_ms = DEFAULT_LATENCY_MS;
  self->draft = DEFAULT_DRAFT;

  gst_base_src_set_live (GST_BASE_SRC (self), TRUE);
  gst_base_src_set_format (GST_BASE_SRC (self), GST_FORMAT_TIME);
}
