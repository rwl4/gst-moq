/*
 * moqsink — publish GStreamer streams over Media-over-QUIC using the
 * libmoq service tier (moq_endpoint_t + moq_media_sender_t).
 *
 * One element owns one endpoint and one media sender, so every pad's track
 * lands in a single MSF/CMSF catalog. Pads:
 *
 *   sink       (always)  H.264 Annex B access units -> LOC objects. The
 *                        track is added on the first keyframe so the catalog
 *                        carries an avcC initData and an SPS-derived codec.
 *   video_%u   (request) fragmented MP4 -> CMAF objects (one moof+mdat each);
 *   audio_%u   (request) ftyp+moov becomes the catalog init segment.
 *
 * The service tier owns the network thread, version negotiation, the sticky
 * catalog and the bounded per-track send queues; chain functions only
 * translate buffers into media objects. moq_media_sender_write() takes the
 * payload rcbuf on MOQ_OK and its release callback (on the network thread)
 * drops our GstBuffer/GBytes reference.
 *
 * Sync: with sync=true (default) each chain waits on the pipeline clock for
 * the buffer's running time, so non-live sources are paced like a normal
 * sink. Live sources already arrive at rate.
 */
#include "gstmoqsink.h"
#include "gstmoqsinkpad.h"
#include "gstmoqcodec.h"

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
#define DEFAULT_DRAFT      16            /* browsers (playa) default to draft-16; 0 = offer all */
#define DEFAULT_BITRATE    2000000       /* catalog max bitrate (MSF-01 §5.2.22) */
#define DEFAULT_AUDIO_BITRATE 128000
#define DEFAULT_MAX_FRAGMENT (16u << 20)

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
  PROP_DRAFT,
  PROP_SYNC,
  PROP_SAP_TIMELINE,
  PROP_MAX_FRAGMENT_SIZE,
};

struct _GstMoqSink
{
  GstElement parent;

  /* properties */
  gchar   *host;
  guint    port;
  gchar   *relay_path;
  gboolean insecure;
  gchar   *namespace_str;
  gchar   *codec;          /* LOC only; NULL = derive from the SPS */
  guint    draft;          /* MoQT draft to negotiate, 0 = auto */
  gboolean sync;
  gboolean sap_timeline;
  guint64  max_fragment_size;

  /* runtime */
  moq_endpoint_t     *ep;
  moq_media_sender_t *sender;
  gboolean            started;
  gboolean            eos_posted;

  gchar      **ns_tokens;
  moq_bytes_t *ns_bytes;
  guint        ns_count;

  GstMoqSinkPad *locpad;    /* the always pad */
  guint          pad_serial;
};

G_DEFINE_TYPE (GstMoqSink, gst_moq_sink, GST_TYPE_ELEMENT)

static GstStaticPadTemplate sink_template =
GST_STATIC_PAD_TEMPLATE ("sink", GST_PAD_SINK, GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("video/x-h264, "
        "stream-format = (string) byte-stream, "
        "alignment = (string) au"));

static GstStaticPadTemplate video_template =
GST_STATIC_PAD_TEMPLATE ("video_%u", GST_PAD_SINK, GST_PAD_REQUEST,
    GST_STATIC_CAPS ("video/quicktime"));

static GstStaticPadTemplate audio_template =
GST_STATIC_PAD_TEMPLATE ("audio_%u", GST_PAD_SINK, GST_PAD_REQUEST,
    GST_STATIC_CAPS ("video/quicktime"));

/* -- payload release ------------------------------------------------------ */

/* A mapped GstBuffer kept alive by the in-flight payload rcbuf. */
typedef struct
{
  GstBuffer *buffer;
  GstMapInfo map;
} SinkFrame;

static void
sink_frame_free (SinkFrame *f)
{
  if (!f)
    return;
  gst_buffer_unmap (f->buffer, &f->map);
  gst_buffer_unref (f->buffer);
  g_free (f);
}

/* rcbuf release callback: runs on the service network thread. */
static void
sink_frame_release (void *ctx, const uint8_t *data, size_t len)
{
  (void) data;
  (void) len;
  sink_frame_free (ctx);
}

/* Same for a GBytes payload (CMAF fragments). Used by Task 4's CMAF chain. */
static void G_GNUC_UNUSED
sink_bytes_release (void *ctx, const uint8_t *data, size_t len)
{
  (void) data;
  (void) len;
  g_bytes_unref (ctx);
}

/* -- namespace (verbatim from the previous version) ---------------------- */

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

/* -- H.264 parameter sets (verbatim from the previous version) ----------- */

static gboolean
find_sps_pps (const guint8 *data, gsize len, const guint8 **sps, gsize *sps_len,
    const guint8 **pps, gsize *pps_len)
{
  *sps = *pps = NULL;
  *sps_len = *pps_len = 0;

  gsize i = 0;
  gsize nal_start = 0;
  gboolean in_nal = FALSE;

  while (i + 3 <= len) {
    if (data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1) {
      if (in_nal) {
        gsize end = i;
        while (end > nal_start && data[end - 1] == 0)
          end--;
        guint8 type = data[nal_start] & 0x1f;
        if (type == 7 && !*sps) {
          *sps = data + nal_start;
          *sps_len = end - nal_start;
        } else if (type == 8 && !*pps) {
          *pps = data + nal_start;
          *pps_len = end - nal_start;
        }
      }
      i += 3;
      nal_start = i;
      in_nal = TRUE;
      continue;
    }
    i++;
  }
  if (in_nal && nal_start < len) {
    guint8 type = data[nal_start] & 0x1f;
    if (type == 7 && !*sps) {
      *sps = data + nal_start;
      *sps_len = len - nal_start;
    } else if (type == 8 && !*pps) {
      *pps = data + nal_start;
      *pps_len = len - nal_start;
    }
  }
  return *sps && *sps_len >= 4 && *pps && *pps_len >= 1;
}

static GBytes *
build_avcc (const guint8 *sps, gsize sps_len, const guint8 *pps, gsize pps_len)
{
  GByteArray *a = g_byte_array_sized_new (11 + sps_len + pps_len);
  guint8 hdr[6] = { 1, sps[1], sps[2], sps[3], 0xfc | 3, 0xe0 | 1 };
  guint8 be16[2];

  g_byte_array_append (a, hdr, sizeof hdr);
  be16[0] = sps_len >> 8; be16[1] = sps_len & 0xff;
  g_byte_array_append (a, be16, 2);
  g_byte_array_append (a, sps, sps_len);
  hdr[0] = 1;
  g_byte_array_append (a, hdr, 1);
  be16[0] = pps_len >> 8; be16[1] = pps_len & 0xff;
  g_byte_array_append (a, be16, 2);
  g_byte_array_append (a, pps, pps_len);
  return g_byte_array_free_to_bytes (a);
}

/* -- shared write path ---------------------------------------------------- */

/* Submit one object and map the libmoq result onto a flow return. On any
 * non-OK result the payload is released here. */
static GstFlowReturn
gst_moq_sink_write_object (GstMoqSink *self, GstMoqSinkPad *pad,
    moq_rcbuf_t *payload, moq_media_send_object_t *o, gsize len)
{
  moq_result_t rc = moq_media_sender_write (self->sender, pad->track, o);
  switch (rc) {
    case MOQ_OK:
      pad->objects_sent++;
      GST_LOG_OBJECT (pad, "wrote %s object (%" G_GSIZE_FORMAT " B)",
          o->is_sync ? "sync" : "delta", len);
      return GST_FLOW_OK;
    case MOQ_ERR_WOULD_BLOCK:
      moq_rcbuf_decref (payload);
      GST_LOG_OBJECT (pad, "dropped object under backpressure");
      return GST_FLOW_OK;
    case MOQ_ERR_INTERRUPTED:
      moq_rcbuf_decref (payload);
      return GST_FLOW_FLUSHING;
    case MOQ_ERR_CLOSED:
      moq_rcbuf_decref (payload);
      GST_INFO_OBJECT (pad, "endpoint closed; ending stream");
      return GST_FLOW_EOS;
    case MOQ_ERR_INVAL:
      moq_rcbuf_decref (payload);
      GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
          ("libmoq rejected object %" G_GUINT64_FORMAT " on track \"%s\" "
              "(rc=%d: invalid object, e.g. CMAF validation failed or a "
              "group start that is not a SAP)", pad->objects_sent,
              pad->track_name, (int) rc), (NULL));
      return GST_FLOW_ERROR;
    default:
      moq_rcbuf_decref (payload);
      GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
          ("media write failed (rc=%d)", (int) rc), (NULL));
      return GST_FLOW_ERROR;
  }
}

/* -- clock sync ----------------------------------------------------------- */

/* Wait until the buffer's running time when sync is on. Returns FLUSHING if
 * the wait was unscheduled by a flush or state change. */
static GstFlowReturn
gst_moq_sink_sync (GstMoqSink *self, GstMoqSinkPad *pad, GstBuffer *buf)
{
  if (!self->sync || pad->segment.format != GST_FORMAT_TIME)
    return GST_FLOW_OK;
  GstClockTime ts = GST_BUFFER_DTS_OR_PTS (buf);
  if (!GST_CLOCK_TIME_IS_VALID (ts))
    return GST_FLOW_OK;
  GstClockTime rt = gst_segment_to_running_time (&pad->segment,
      GST_FORMAT_TIME, ts);
  if (!GST_CLOCK_TIME_IS_VALID (rt))
    return GST_FLOW_OK;

  GstClock *clock = gst_element_get_clock (GST_ELEMENT (self));
  if (!clock)
    return GST_FLOW_OK;
  GstClockTime base = gst_element_get_base_time (GST_ELEMENT (self));
  if (!GST_CLOCK_TIME_IS_VALID (base)) {
    gst_object_unref (clock);
    return GST_FLOW_OK;
  }

  GST_OBJECT_LOCK (self);
  if (pad->flushing) {
    GST_OBJECT_UNLOCK (self);
    gst_object_unref (clock);
    return GST_FLOW_FLUSHING;
  }
  pad->clock_id = gst_clock_new_single_shot_id (clock, base + rt);
  GST_OBJECT_UNLOCK (self);

  GstClockReturn cr = gst_clock_id_wait (pad->clock_id, NULL);

  GST_OBJECT_LOCK (self);
  gst_clock_id_unref (pad->clock_id);
  pad->clock_id = NULL;
  GST_OBJECT_UNLOCK (self);
  gst_object_unref (clock);

  return cr == GST_CLOCK_UNSCHEDULED ? GST_FLOW_FLUSHING : GST_FLOW_OK;
}

static void
gst_moq_sink_unschedule (GstMoqSink *self, GstMoqSinkPad *pad, gboolean flushing)
{
  GST_OBJECT_LOCK (self);
  pad->flushing = flushing;
  if (pad->clock_id)
    gst_clock_id_unschedule (pad->clock_id);
  GST_OBJECT_UNLOCK (self);
}

/* -- LOC path (always pad) ----------------------------------------------- */

static gboolean
gst_moq_sink_add_loc_track (GstMoqSink *self, GstMoqSinkPad *pad,
    const guint8 *au, gsize au_len)
{
  const guint8 *sps, *pps;
  gsize sps_len, pps_len;
  GBytes *avcc = NULL;
  gchar *codec = NULL;

  if (find_sps_pps (au, au_len, &sps, &sps_len, &pps, &pps_len)) {
    avcc = build_avcc (sps, sps_len, pps, pps_len);
    if (!self->codec)
      codec = g_strdup_printf ("avc1.%02x%02x%02x", sps[1], sps[2], sps[3]);
  } else {
    GST_WARNING_OBJECT (pad, "keyframe carries no SPS/PPS; publishing "
        "without initData (use h264parse config-interval=-1)");
  }
  if (!codec)
    codec = g_strdup (self->codec ? self->codec : "avc1.42e01e");

  moq_media_track_cfg_t tc;
  moq_media_track_cfg_init (&tc);
  tc.name.data = (const uint8_t *) pad->track_name;
  tc.name.len = strlen (pad->track_name);
  tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
  tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
  tc.codec.data = (const uint8_t *) codec;
  tc.codec.len = strlen (codec);
  tc.is_live = TRUE;
  tc.bitrate = pad->bitrate;
  if (avcc) {
    gsize n;
    tc.init_data.data = g_bytes_get_data (avcc, &n);
    tc.init_data.len = n;
  }
  if (pad->width > 0 && pad->height > 0) {
    tc.width = pad->width;
    tc.height = pad->height;
  }
  if (pad->fps_n > 0 && pad->fps_d > 0)
    tc.framerate_millis = gst_util_uint64_scale_int (1000, pad->fps_n, pad->fps_d);

  moq_result_t rc = moq_media_sender_add_track (self->sender, &tc, &pad->track);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("could not add track \"%s\" (rc=%d)", pad->track_name, (int) rc),
        (NULL));
  } else {
    GST_INFO_OBJECT (pad, "added LOC track %s codec=%s %dx%d initData=%zu B",
        pad->track_name, codec, pad->width, pad->height,
        (gsize) tc.init_data.len);
  }
  g_free (codec);
  g_clear_pointer (&avcc, g_bytes_unref);
  return rc == MOQ_OK;
}

static GstFlowReturn
gst_moq_sink_chain_loc (GstMoqSink *self, GstMoqSinkPad *pad, GstBuffer *buffer)
{
  if (moq_media_sender_is_fatal (self->sender)) {
    GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
        ("media sender failed (code=%" G_GUINT64_FORMAT ")",
            (guint64) moq_media_sender_fatal_code (self->sender)), (NULL));
    gst_buffer_unref (buffer);
    return GST_FLOW_ERROR;
  }

  GstFlowReturn sr = gst_moq_sink_sync (self, pad, buffer);
  if (sr != GST_FLOW_OK) {
    gst_buffer_unref (buffer);
    return sr;
  }

  SinkFrame *f = g_new0 (SinkFrame, 1);
  f->buffer = buffer;          /* takes the chain's reference */
  if (!gst_buffer_map (f->buffer, &f->map, GST_MAP_READ)) {
    gst_buffer_unref (f->buffer);
    g_free (f);
    GST_ELEMENT_ERROR (self, STREAM, FORMAT, ("failed to map buffer"), (NULL));
    return GST_FLOW_ERROR;
  }

  gboolean keyframe = !GST_BUFFER_FLAG_IS_SET (buffer, GST_BUFFER_FLAG_DELTA_UNIT);
  if (!pad->track) {
    if (!keyframe) {
      GST_LOG_OBJECT (pad, "dropping delta frame before the first keyframe");
      sink_frame_free (f);
      return GST_FLOW_OK;
    }
    if (!gst_moq_sink_add_loc_track (self, pad, f->map.data, f->map.size)) {
      sink_frame_free (f);
      return GST_FLOW_ERROR;
    }
  }

  /* Rebase to the first PTS so LOC carries small, stream-relative times. */
  GstClockTime pts = GST_BUFFER_PTS (buffer);
  GstClockTime dts = GST_BUFFER_DTS (buffer);
  if (GST_CLOCK_TIME_IS_VALID (pts) && !GST_CLOCK_TIME_IS_VALID (pad->base_pts))
    pad->base_pts = pts;
  guint64 pts_us = (GST_CLOCK_TIME_IS_VALID (pts) &&
      GST_CLOCK_TIME_IS_VALID (pad->base_pts) && pts >= pad->base_pts)
      ? (pts - pad->base_pts) / GST_USECOND : 0;
  guint64 dts_us = (GST_CLOCK_TIME_IS_VALID (dts) &&
      GST_CLOCK_TIME_IS_VALID (pad->base_pts) && dts >= pad->base_pts)
      ? (dts - pad->base_pts) / GST_USECOND : pts_us;

  moq_rcbuf_t *payload = NULL;
  if (moq_rcbuf_wrap (moq_alloc_default (), f->map.data, f->map.size,
          sink_frame_release, f, &payload) != MOQ_OK) {
    sink_frame_free (f);
    GST_ELEMENT_ERROR (self, RESOURCE, WRITE, ("rcbuf wrap failed"), (NULL));
    return GST_FLOW_ERROR;
  }

  moq_media_send_object_t o;
  memset (&o, 0, sizeof o);
  o.struct_size = sizeof o;
  o.payload = payload;
  o.is_sync = keyframe;
  o.starts_group = keyframe;
  o.decode_time_us = dts_us;
  o.presentation_time_us = pts_us;

  return gst_moq_sink_write_object (self, pad, payload, &o, f->map.size);
}

/* CMAF chain: implemented in Task 4. */
static GstFlowReturn gst_moq_sink_chain_cmaf (GstMoqSink *self,
    GstMoqSinkPad *pad, GstBuffer *buffer);

/* -- pad functions -------------------------------------------------------- */

static GstFlowReturn
gst_moq_sink_chain (GstPad *gpad, GstObject *parent, GstBuffer *buffer)
{
  GstMoqSink *self = GST_MOQ_SINK (parent);
  GstMoqSinkPad *pad = GST_MOQ_SINK_PAD (gpad);

  if (!self->started || !self->sender) {
    gst_buffer_unref (buffer);
    return GST_FLOW_FLUSHING;
  }
  if (pad->packaging == MOQ_MEDIA_PACKAGING_CMAF)
    return gst_moq_sink_chain_cmaf (self, pad, buffer);
  return gst_moq_sink_chain_loc (self, pad, buffer);
}

static void
gst_moq_sink_end_track (GstMoqSink *self, GstMoqSinkPad *pad)
{
  if (!self->sender || !pad->track)
    return;
  moq_result_t rc = moq_media_sender_end_track (self->sender, pad->track);
  if (rc != MOQ_OK && rc != MOQ_ERR_WRONG_STATE)
    GST_WARNING_OBJECT (pad, "end_track failed (rc=%d)", (int) rc);
}

/* Drain the send queue (bounded) so the final objects and END_OF_TRACK
 * markers reach the wire before the endpoint is torn down. */
static void
gst_moq_sink_drain (GstMoqSink *self)
{
  gint64 deadline = g_get_monotonic_time () + EOS_DRAIN_TIMEOUT_US;
  for (;;) {
    moq_media_sender_stats_t st;
    if (moq_media_sender_get_stats (self->sender, &st, sizeof st) != MOQ_OK)
      break;
    if (st.objects_queued == 0) {
      GST_INFO_OBJECT (self, "send queue drained on EOS");
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
  if (self->ep)
    moq_endpoint_drain (self->ep, EOS_DRAIN_TIMEOUT_US);
}

/* TRUE when every pad (always + requested) has seen EOS. */
static gboolean
gst_moq_sink_all_eos (GstMoqSink *self)
{
  gboolean all = TRUE;
  GST_OBJECT_LOCK (self);
  for (GList *l = GST_ELEMENT (self)->sinkpads; l; l = l->next)
    if (!GST_MOQ_SINK_PAD (l->data)->eos)
      all = FALSE;
  GST_OBJECT_UNLOCK (self);
  return all;
}

static gboolean
gst_moq_sink_event (GstPad *gpad, GstObject *parent, GstEvent *event)
{
  GstMoqSink *self = GST_MOQ_SINK (parent);
  GstMoqSinkPad *pad = GST_MOQ_SINK_PAD (gpad);

  switch (GST_EVENT_TYPE (event)) {
    case GST_EVENT_CAPS: {
      GstCaps *caps;
      gst_event_parse_caps (event, &caps);
      GstStructure *st = gst_caps_get_structure (caps, 0);
      pad->width = pad->height = 0;
      pad->fps_n = pad->fps_d = 0;
      gst_structure_get_int (st, "width", &pad->width);
      gst_structure_get_int (st, "height", &pad->height);
      gst_structure_get_fraction (st, "framerate", &pad->fps_n, &pad->fps_d);
      GST_DEBUG_OBJECT (pad, "caps %" GST_PTR_FORMAT, caps);
      break;
    }
    case GST_EVENT_SEGMENT:
      gst_event_copy_segment (event, &pad->segment);
      break;
    case GST_EVENT_FLUSH_START:
      gst_moq_sink_unschedule (self, pad, TRUE);
      if (self->ep)
        moq_endpoint_set_interrupted (self->ep, TRUE);
      break;
    case GST_EVENT_FLUSH_STOP:
      gst_moq_sink_unschedule (self, pad, FALSE);
      if (self->ep)
        moq_endpoint_set_interrupted (self->ep, FALSE);
      pad->eos = FALSE;
      gst_segment_init (&pad->segment, GST_FORMAT_UNDEFINED);
      break;
    case GST_EVENT_EOS:
      pad->eos = TRUE;
      gst_moq_sink_end_track (self, pad);
      if (gst_moq_sink_all_eos (self) && !self->eos_posted) {
        gst_moq_sink_drain (self);
        self->eos_posted = TRUE;
        gst_element_post_message (GST_ELEMENT (self),
            gst_message_new_eos (GST_OBJECT (self)));
      }
      break;
    default:
      break;
  }
  gst_event_unref (event);
  return TRUE;
}

static GstPad *
gst_moq_sink_request_new_pad (GstElement *element, GstPadTemplate *templ,
    const gchar *name, const GstCaps *caps)
{
  GstMoqSink *self = GST_MOQ_SINK (element);
  (void) caps;
  gboolean is_audio = g_str_has_prefix (templ->name_template, "audio");
  gchar *padname = name ? g_strdup (name)
      : g_strdup_printf (templ->name_template, self->pad_serial++);

  GstMoqSinkPad *pad = gst_moq_sink_pad_new (templ, padname,
      is_audio ? MOQ_MEDIA_TYPE_AUDIO : MOQ_MEDIA_TYPE_VIDEO,
      MOQ_MEDIA_PACKAGING_CMAF,
      is_audio ? "audio" : "video",
      is_audio ? DEFAULT_AUDIO_BITRATE : DEFAULT_BITRATE,
      self->max_fragment_size);
  g_free (padname);

  gst_pad_set_chain_function (GST_PAD (pad), gst_moq_sink_chain);
  gst_pad_set_event_function (GST_PAD (pad), gst_moq_sink_event);
  gst_pad_set_active (GST_PAD (pad), TRUE);
  gst_element_add_pad (element, GST_PAD (pad));
  GST_INFO_OBJECT (pad, "requested CMAF %s pad", is_audio ? "audio" : "video");
  return GST_PAD (pad);
}

static void
gst_moq_sink_release_pad (GstElement *element, GstPad *gpad)
{
  GstMoqSink *self = GST_MOQ_SINK (element);
  GstMoqSinkPad *pad = GST_MOQ_SINK_PAD (gpad);
  gst_moq_sink_unschedule (self, pad, TRUE);
  gst_moq_sink_end_track (self, pad);
  gst_pad_set_active (gpad, FALSE);
  gst_element_remove_pad (element, gpad);
}

/* -- lifecycle ------------------------------------------------------------ */

static gboolean
gst_moq_sink_start (GstMoqSink *self)
{
  if (!gst_moq_sink_build_namespace (self))
    return FALSE;

  self->ep = NULL;
  self->sender = NULL;
  self->eos_posted = FALSE;
  GST_OBJECT_LOCK (self);
  for (GList *l = GST_ELEMENT (self)->sinkpads; l; l = l->next)
    gst_moq_sink_pad_reset (GST_MOQ_SINK_PAD (l->data));
  GST_OBJECT_UNLOCK (self);

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
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("could not open MoQ endpoint to relay %s:%u (rc=%d)",
            self->host, self->port, (int) rc), (NULL));
    goto fail_ns;
  }

  moq_media_sender_cfg_t sc;
  moq_media_sender_cfg_init_live (&sc);
  sc.endpoint = NULL;
  sc.namespace_.parts = self->ns_bytes;
  sc.namespace_.count = self->ns_count;

  rc = moq_media_sender_attach (self->ep, &sc, &self->sender);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("could not attach media sender (rc=%d)", (int) rc), (NULL));
    goto fail_ep;
  }

  self->started = TRUE;
  GST_INFO_OBJECT (self, "started: publishing ns=%s via relay %s:%u%s",
      self->namespace_str, self->host, self->port, self->relay_path);
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

static void
gst_moq_sink_stop (GstMoqSink *self)
{
  if (!self->started)
    return;

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
  }
  if (self->ep) {
    moq_endpoint_stop (self->ep);
    moq_endpoint_destroy (self->ep);
    self->ep = NULL;
  }

  GST_OBJECT_LOCK (self);
  for (GList *l = GST_ELEMENT (self)->sinkpads; l; l = l->next)
    GST_MOQ_SINK_PAD (l->data)->track = NULL;
  GST_OBJECT_UNLOCK (self);

  g_clear_pointer (&self->ns_tokens, g_strfreev);
  g_clear_pointer (&self->ns_bytes, g_free);
  self->ns_count = 0;
  self->started = FALSE;
  GST_INFO_OBJECT (self, "stopped");
}

static GstStateChangeReturn
gst_moq_sink_change_state (GstElement *element, GstStateChange transition)
{
  GstMoqSink *self = GST_MOQ_SINK (element);

  switch (transition) {
    case GST_STATE_CHANGE_NULL_TO_READY:
      if (!gst_moq_sink_start (self))
        return GST_STATE_CHANGE_FAILURE;
      break;
    case GST_STATE_CHANGE_PAUSED_TO_READY:
      GST_OBJECT_LOCK (self);
      for (GList *l = element->sinkpads; l; l = l->next) {
        GstMoqSinkPad *p = GST_MOQ_SINK_PAD (l->data);
        p->flushing = TRUE;
        if (p->clock_id)
          gst_clock_id_unschedule (p->clock_id);
      }
      GST_OBJECT_UNLOCK (self);
      if (self->ep)
        moq_endpoint_set_interrupted (self->ep, TRUE);
      break;
    default:
      break;
  }

  GstStateChangeReturn ret =
      GST_ELEMENT_CLASS (gst_moq_sink_parent_class)->change_state (element,
      transition);

  switch (transition) {
    case GST_STATE_CHANGE_READY_TO_PAUSED:
      GST_OBJECT_LOCK (self);
      for (GList *l = element->sinkpads; l; l = l->next)
        GST_MOQ_SINK_PAD (l->data)->flushing = FALSE;
      GST_OBJECT_UNLOCK (self);
      if (self->ep)
        moq_endpoint_set_interrupted (self->ep, FALSE);
      break;
    case GST_STATE_CHANGE_READY_TO_NULL:
      gst_moq_sink_stop (self);
      break;
    default:
      break;
  }
  return ret;
}

/* -- GObject -------------------------------------------------------------- */

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
    case PROP_TRACK_NAME:          /* forwarded to the always pad */
      g_object_set_property (G_OBJECT (self->locpad), "track-name", value);
      break;
    case PROP_CODEC:
      g_free (self->codec);
      self->codec = g_value_dup_string (value);
      break;
    case PROP_BITRATE:             /* forwarded to the always pad */
      g_object_set_property (G_OBJECT (self->locpad), "bitrate", value);
      break;
    case PROP_DRAFT:
      self->draft = g_value_get_uint (value);
      break;
    case PROP_SYNC:
      self->sync = g_value_get_boolean (value);
      break;
    case PROP_SAP_TIMELINE:
      self->sap_timeline = g_value_get_boolean (value);
      break;
    case PROP_MAX_FRAGMENT_SIZE:
      self->max_fragment_size = g_value_get_uint64 (value);
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
      g_object_get_property (G_OBJECT (self->locpad), "track-name", value);
      break;
    case PROP_CODEC:
      g_value_set_string (value, self->codec);
      break;
    case PROP_BITRATE:
      g_object_get_property (G_OBJECT (self->locpad), "bitrate", value);
      break;
    case PROP_DRAFT:
      g_value_set_uint (value, self->draft);
      break;
    case PROP_SYNC:
      g_value_set_boolean (value, self->sync);
      break;
    case PROP_SAP_TIMELINE:
      g_value_set_boolean (value, self->sap_timeline);
      break;
    case PROP_MAX_FRAGMENT_SIZE:
      g_value_set_uint64 (value, self->max_fragment_size);
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
  g_free (self->codec);
  G_OBJECT_CLASS (gst_moq_sink_parent_class)->finalize (object);
}

static void
gst_moq_sink_class_init (GstMoqSinkClass *klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  GstElementClass *element_class = GST_ELEMENT_CLASS (klass);

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
      g_param_spec_string ("track-name", "Track name",
          "MoQ track name for the always (LOC) sink pad", DEFAULT_TRACK_NAME,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_CODEC,
      g_param_spec_string ("codec", "Codec",
          "Catalog codec string for the LOC track "
          "(default: derived from the stream's SPS, e.g. avc1.42c01e)", NULL,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_BITRATE,
      g_param_spec_uint64 ("bitrate", "Bitrate",
          "Advertised catalog max bitrate for the LOC track (bits/s)", 1,
          G_MAXUINT64, DEFAULT_BITRATE,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_DRAFT,
      g_param_spec_uint ("draft", "MoQT draft",
          "MoQ Transport draft version to negotiate (16 or 18); "
          "0 offers every draft libmoq supports", 0, 18, DEFAULT_DRAFT,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_SYNC,
      g_param_spec_boolean ("sync", "Sync",
          "Pace buffers against the pipeline clock", TRUE,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_SAP_TIMELINE,
      g_param_spec_boolean ("sap-timeline", "SAP timeline",
          "Publish a CMSF SAP event timeline track for every CMAF track",
          FALSE, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_MAX_FRAGMENT_SIZE,
      g_param_spec_uint64 ("max-fragment-size", "Max fragment size",
          "Largest single ISO BMFF box accepted on a CMAF pad (bytes)",
          1024, G_MAXUINT32, DEFAULT_MAX_FRAGMENT,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  gst_element_class_add_static_pad_template_with_gtype (element_class,
      &sink_template, GST_TYPE_MOQ_SINK_PAD);
  gst_element_class_add_static_pad_template_with_gtype (element_class,
      &video_template, GST_TYPE_MOQ_SINK_PAD);
  gst_element_class_add_static_pad_template_with_gtype (element_class,
      &audio_template, GST_TYPE_MOQ_SINK_PAD);
  gst_element_class_set_static_metadata (element_class,
      "MoQ sink", "Sink/Network",
      "Publish LOC and CMAF tracks over Media-over-QUIC (libmoq service tier)",
      "Ray L <ray@raylucke.com>");

  element_class->change_state = GST_DEBUG_FUNCPTR (gst_moq_sink_change_state);
  element_class->request_new_pad = GST_DEBUG_FUNCPTR (gst_moq_sink_request_new_pad);
  element_class->release_pad = GST_DEBUG_FUNCPTR (gst_moq_sink_release_pad);
}

static void
gst_moq_sink_init (GstMoqSink *self)
{
  self->host = g_strdup (DEFAULT_HOST);
  self->port = DEFAULT_PORT;
  self->relay_path = g_strdup (DEFAULT_RELAY_PATH);
  self->namespace_str = g_strdup (DEFAULT_NAMESPACE);
  self->codec = NULL;
  self->draft = DEFAULT_DRAFT;
  self->sync = TRUE;
  self->max_fragment_size = DEFAULT_MAX_FRAGMENT;

  GstPadTemplate *templ = gst_static_pad_template_get (&sink_template);
  self->locpad = gst_moq_sink_pad_new (templ, "sink", MOQ_MEDIA_TYPE_VIDEO,
      MOQ_MEDIA_PACKAGING_RAW, DEFAULT_TRACK_NAME, DEFAULT_BITRATE,
      DEFAULT_MAX_FRAGMENT);
  gst_object_unref (templ);
  gst_pad_set_chain_function (GST_PAD (self->locpad), gst_moq_sink_chain);
  gst_pad_set_event_function (GST_PAD (self->locpad), gst_moq_sink_event);
  gst_element_add_pad (GST_ELEMENT (self), GST_PAD (self->locpad));

  GST_OBJECT_FLAG_SET (self, GST_ELEMENT_FLAG_SINK);
}

/* Task 4 replaces this stub. */
static GstFlowReturn
gst_moq_sink_chain_cmaf (GstMoqSink *self, GstMoqSinkPad *pad, GstBuffer *buffer)
{
  (void) pad;
  gst_buffer_unref (buffer);
  GST_ELEMENT_ERROR (self, STREAM, NOT_IMPLEMENTED,
      ("CMAF pads are not implemented yet"), (NULL));
  return GST_FLOW_ERROR;
}
