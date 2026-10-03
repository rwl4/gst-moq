/* Exercise the source's actual create vmethod with deterministic public-API
 * mocks. No relay, network thread, sleeps, or production test hooks. */
#include "../src/gstmoqsrc.c"

typedef struct
{
  moq_media_track_event_t events[4];
  guint event_count, event_index;
  moq_media_object_t objects[4];
  guint object_count, object_index;
  guint cleaned, waits;
  guint subscriptions;
  moq_media_track_t *subscribed_track;
  moq_result_t subscribe_result;
  moq_result_t empty_result;
} Receiver;

static Receiver receiver;
static guint8 video_handle, audio_handle;
static const guint8 video_payload[] = { 0, 0, 1, 0x65, 0x42 };
static const guint8 audio_payload[] = { 0xf8, 0xff };

moq_result_t
moq_media_receiver_subscribe_track (moq_media_receiver_t *r,
    moq_media_track_t *track, const moq_media_receiver_track_subscribe_cfg_t *cfg)
{
  g_assert_true (r == (moq_media_receiver_t *) &receiver);
  g_assert_null (cfg);
  receiver.subscriptions++;
  receiver.subscribed_track = track;
  return receiver.subscribe_result;
}

moq_result_t
moq_media_receiver_poll_track (moq_media_receiver_t *r,
    moq_media_track_event_t *ev, size_t size)
{
  g_assert_true (r == (moq_media_receiver_t *) &receiver);
  g_assert_cmpuint (size, ==, sizeof *ev);
  if (receiver.event_index == receiver.event_count)
    return MOQ_DONE;
  *ev = receiver.events[receiver.event_index++];
  ev->struct_size = sizeof *ev;
  return MOQ_OK;
}

moq_result_t
moq_media_receiver_poll_object (moq_media_receiver_t *r,
    moq_media_object_t *obj, size_t size)
{
  g_assert_true (r == (moq_media_receiver_t *) &receiver);
  g_assert_cmpuint (size, ==, sizeof *obj);
  if (receiver.object_index == receiver.object_count)
    return receiver.empty_result;
  *obj = receiver.objects[receiver.object_index++];
  obj->struct_size = sizeof *obj;
  return MOQ_OK;
}

void
moq_media_object_cleanup (moq_media_object_t *obj)
{
  receiver.cleaned++;
  memset (obj, 0, sizeof *obj);
}

moq_result_t
moq_media_receiver_wait (moq_media_receiver_t *r, uint64_t timeout)
{
  (void) r;
  (void) timeout;
  receiver.waits++;
  return MOQ_ERR_INTERRUPTED; /* Turn unexpected waits into a quick failure. */
}

bool moq_media_receiver_is_fatal (const moq_media_receiver_t *r)
{ (void) r; return false; }
uint64_t moq_media_receiver_fatal_code (const moq_media_receiver_t *r)
{ (void) r; return 0; }

/* Lifecycle calls are not part of these create-vmethod tests. */
void moq_endpoint_cfg_init (moq_endpoint_cfg_t *cfg)
{ (void) cfg; g_assert_not_reached (); }
moq_result_t moq_endpoint_connect (const moq_endpoint_cfg_t *cfg, moq_endpoint_t **out)
{ (void) cfg; (void) out; g_assert_not_reached (); return MOQ_ERR_INVAL; }
moq_result_t moq_endpoint_stop (moq_endpoint_t *ep)
{ (void) ep; g_assert_not_reached (); return MOQ_ERR_INVAL; }
void moq_endpoint_destroy (moq_endpoint_t *ep)
{ (void) ep; g_assert_not_reached (); }
void moq_endpoint_set_interrupted (moq_endpoint_t *ep, bool interrupted)
{ (void) ep; (void) interrupted; g_assert_not_reached (); }
void moq_media_receiver_cfg_init_live (moq_media_receiver_cfg_t *cfg)
{ (void) cfg; g_assert_not_reached (); }
moq_result_t moq_media_receiver_attach (moq_endpoint_t *ep,
    const moq_media_receiver_cfg_t *cfg, moq_media_receiver_t **out)
{ (void) ep; (void) cfg; (void) out; g_assert_not_reached (); return MOQ_ERR_INVAL; }
void moq_media_receiver_destroy (moq_media_receiver_t *r)
{ (void) r; g_assert_not_reached (); }

static GstMoqSrc *
make_source (void)
{
  memset (&receiver, 0, sizeof receiver);
  receiver.empty_result = MOQ_DONE;
  receiver.subscribe_result = MOQ_OK;
  GstMoqSrc *src = g_object_new (GST_TYPE_MOQ_SRC, NULL);
  gst_object_ref_sink (src);
  src->receiver = (moq_media_receiver_t *) &receiver;
  src->caps_pushed = TRUE; /* Negotiation is exercised separately by pipelines. */
  src->discover_deadline = G_MAXINT64;
  return src;
}

static void
queue_object (guint8 *handle, const guint8 *data, size_t len)
{
  moq_media_object_t *obj = &receiver.objects[receiver.object_count++];
  obj->track = (moq_media_track_t *) handle;
  obj->payload = (moq_bytes_t) { data, len };
  obj->keyframe = TRUE;
}

static GstFlowReturn
create (GstMoqSrc *src, GstBuffer **out)
{
  return GST_PUSH_SRC_GET_CLASS (src)->create (GST_PUSH_SRC (src), out);
}

static void
test_missing_track_drops_other_media (void)
{
  GstMoqSrc *src = make_source ();
  queue_object (&audio_handle, audio_payload, sizeof audio_payload);
  receiver.empty_result = MOQ_ERR_CLOSED;
  GstBuffer *out = NULL;
  g_assert_cmpint (create (src, &out), ==, GST_FLOW_EOS);
  g_assert_null (out);
  g_assert_cmpuint (receiver.cleaned, ==, 1);
  gst_object_unref (src);
}

static void
test_selected_end_drains_media (gconstpointer event_kind)
{
  GstMoqSrc *src = make_source ();
  moq_media_track_desc_t desc = {0};
  desc.name = (moq_bytes_t) { (const guint8 *) "video", 5 };
  moq_media_track_desc_t audio_desc = {0};
  audio_desc.name = (moq_bytes_t) { (const guint8 *) "audio", 5 };
  receiver.events[0].kind = MOQ_MEDIA_TRACK_ADDED;
  receiver.events[0].track = (moq_media_track_t *) &audio_handle;
  receiver.events[0].desc = &audio_desc;
  receiver.events[1].kind = MOQ_MEDIA_TRACK_ADDED;
  receiver.events[1].track = (moq_media_track_t *) &video_handle;
  receiver.events[1].desc = &desc;
  receiver.events[2].kind = GPOINTER_TO_INT (event_kind);
  receiver.events[2].track = (moq_media_track_t *) &video_handle;
  receiver.event_count = 3;
  queue_object (&audio_handle, audio_payload, sizeof audio_payload);
  queue_object (&video_handle, video_payload, sizeof video_payload);
  queue_object (&video_handle, video_payload, sizeof video_payload);

  for (guint i = 0; i < 2; i++) {
    GstBuffer *out = NULL;
    g_assert_cmpint (create (src, &out), ==, GST_FLOW_OK);
    g_assert_nonnull (out);
    guint8 bytes[sizeof video_payload];
    g_assert_cmpuint (gst_buffer_extract (out, 0, bytes, sizeof bytes), ==, sizeof bytes);
    g_assert_cmpmem (bytes, sizeof bytes, video_payload, sizeof video_payload);
    gst_buffer_unref (out);
  }
  GstBuffer *out = NULL;
  g_assert_cmpint (create (src, &out), ==, GST_FLOW_EOS);
  g_assert_null (out);
  g_assert_cmpuint (receiver.cleaned, ==, 3);
  g_assert_cmpuint (receiver.waits, ==, 0);
  g_assert_cmpuint (receiver.subscriptions, ==, 1);
  g_assert_true (receiver.subscribed_track == (moq_media_track_t *) &video_handle);
  gst_object_unref (src);
}

static void
test_other_track_end_does_not_end_source (void)
{
  GstMoqSrc *src = make_source ();
  src->want_track = (moq_media_track_t *) &video_handle;
  receiver.events[0].kind = MOQ_MEDIA_TRACK_ENDED;
  receiver.events[0].track = (moq_media_track_t *) &audio_handle;
  receiver.event_count = 1;
  GstBuffer *out = NULL;
  g_assert_cmpint (create (src, &out), ==, GST_FLOW_FLUSHING);
  g_assert_null (out);
  g_assert_cmpuint (receiver.waits, ==, 1);
  gst_object_unref (src);
}

static void
test_discovery_deadline (void)
{
  GstMoqSrc *src = make_source ();
  GstBus *bus = gst_bus_new ();
  gst_element_set_bus (GST_ELEMENT (src), bus);
  src->discover_deadline = g_get_monotonic_time () - 1;
  GstBuffer *out = NULL;
  g_assert_cmpint (create (src, &out), ==, GST_FLOW_ERROR);
  g_assert_null (out);
  GstMessage *msg = gst_bus_pop_filtered (bus, GST_MESSAGE_ERROR);
  g_assert_nonnull (msg);
  GError *error = NULL;
  gst_message_parse_error (msg, &error, NULL);
  g_assert_error (error, GST_RESOURCE_ERROR, GST_RESOURCE_ERROR_NOT_FOUND);
  g_error_free (error);
  gst_message_unref (msg);
  gst_element_set_bus (GST_ELEMENT (src), NULL);
  gst_object_unref (bus);
  gst_object_unref (src);
}

static void
test_failed_subscription (void)
{
  GstMoqSrc *src = make_source ();
  receiver.subscribe_result = MOQ_ERR_CLOSED;
  moq_media_track_desc_t desc = {0};
  desc.name = (moq_bytes_t) { (const guint8 *) "video", 5 };
  receiver.events[0].kind = MOQ_MEDIA_TRACK_ADDED;
  receiver.events[0].track = (moq_media_track_t *) &video_handle;
  receiver.events[0].desc = &desc;
  receiver.event_count = 1;
  GstBuffer *out = NULL;
  g_assert_cmpint (create (src, &out), ==, GST_FLOW_ERROR);
  g_assert_null (out);
  g_assert_null (src->want_track);
  g_assert_cmpuint (receiver.waits, ==, 0);
  gst_object_unref (src);
}

static void
test_interruption_precedes_discovery_timeout (void)
{
  GstMoqSrc *src = make_source ();
  src->discover_deadline = g_get_monotonic_time () - 1;
  receiver.empty_result = MOQ_ERR_INTERRUPTED;
  GstBuffer *out = NULL;
  g_assert_cmpint (create (src, &out), ==, GST_FLOW_FLUSHING);
  g_assert_null (out);
  gst_object_unref (src);
}

int
main (int argc, char **argv)
{
  gst_init (&argc, &argv);
  g_test_init (&argc, &argv, NULL);
  g_test_add_func ("/src/missing-track", test_missing_track_drops_other_media);
  g_test_add_data_func ("/src/selected-end-drains", GINT_TO_POINTER (MOQ_MEDIA_TRACK_ENDED),
      test_selected_end_drains_media);
  g_test_add_data_func ("/src/selected-removal-drains", GINT_TO_POINTER (MOQ_MEDIA_TRACK_REMOVED),
      test_selected_end_drains_media);
  g_test_add_func ("/src/unrelated-end", test_other_track_end_does_not_end_source);
  g_test_add_func ("/src/discovery-deadline", test_discovery_deadline);
  g_test_add_func ("/src/subscription-failure", test_failed_subscription);
  g_test_add_func ("/src/interrupt-before-timeout", test_interruption_precedes_discovery_timeout);
  return g_test_run ();
}
