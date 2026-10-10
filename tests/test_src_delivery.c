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

moq_version_t moq_endpoint_negotiated_version (const moq_endpoint_t *ep)
{ (void) ep; return (moq_version_t) 16; }
/* Startup-failure mocks assert ownership and call/log ordering. */
static gboolean startup_active;
static guint8 endpoint_handle;
static GString *startup_calls;
static gchar *terminal_diagnostic;
static moq_result_t terminal_getter_result;
static moq_endpoint_terminal_reason_t terminal_reason;
static guint64 terminal_detail;

static void
startup_call (const moq_endpoint_t *ep, const gchar *call)
{
  g_assert_true (startup_active);
  g_assert_true (ep == (moq_endpoint_t *) &endpoint_handle);
  g_string_append (startup_calls, call);
}

moq_result_t moq_endpoint_get_terminal (const moq_endpoint_t *ep,
    moq_endpoint_terminal_t *out, size_t size)
{
  startup_call (ep, "get;");
  g_assert_cmpuint (size, ==, sizeof *out);
  if (terminal_getter_result == MOQ_OK) {
    out->struct_size = sizeof *out;
    out->reason = terminal_reason;
    out->detail_code = terminal_detail;
  }
  return terminal_getter_result;
}
void moq_endpoint_cfg_init (moq_endpoint_cfg_t *cfg)
{ g_assert_true (startup_active); memset (cfg, 0, sizeof *cfg); }
moq_result_t moq_endpoint_connect (const moq_endpoint_cfg_t *cfg, moq_endpoint_t **out)
{
  g_assert_true (startup_active);
  g_assert_false (cfg->insecure_skip_verify);
  g_assert_cmpint (cfg->versions.policy, ==, MOQ_VERSION_POLICY_EXACT);
  g_assert_cmpuint (cfg->versions.version_count, ==, 1);
  g_assert_cmpuint (cfg->versions.versions[0], ==, 16);
  g_string_append (startup_calls, "connect;");
  *out = (moq_endpoint_t *) &endpoint_handle;
  return MOQ_OK;
}
moq_result_t moq_endpoint_stop (moq_endpoint_t *ep)
{ startup_call (ep, "stop;"); return MOQ_OK; }
void moq_endpoint_destroy (moq_endpoint_t *ep)
{ startup_call (ep, "destroy;"); }
void moq_endpoint_set_interrupted (moq_endpoint_t *ep, bool interrupted)
{ (void) ep; (void) interrupted; g_assert_not_reached (); }
void moq_media_receiver_cfg_init_live (moq_media_receiver_cfg_t *cfg)
{ g_assert_true (startup_active); memset (cfg, 0, sizeof *cfg); }
moq_result_t moq_media_receiver_attach (moq_endpoint_t *ep,
    const moq_media_receiver_cfg_t *cfg, moq_media_receiver_t **out)
{
  startup_call (ep, "attach;");
  g_assert_null (cfg->endpoint);
  g_assert_false (cfg->auto_subscribe);
  g_assert_cmpuint (cfg->namespace_.count, >, 0);
  *out = NULL;
  return MOQ_ERR_CLOSED;
}
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
  src->caps = gst_caps_new_any ();
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
  desc.codec = (moq_bytes_t) { (const guint8 *) "avc1.42c01e", 11 };
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
  desc.codec = (moq_bytes_t) { (const guint8 *) "avc1.42c01e", 11 };
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

static void
test_cmaf_tail (gconstpointer kind)
{
  GstMoqSrc *src = make_source ();
  static const guint8 init[] = { 0, 0, 0, 8, 'f', 't', 'y', 'p' };
  static const guint8 fragment[] = { 0, 0, 0, 8, 'm', 'o', 'o', 'f' };
  moq_media_track_desc_t desc = {0};
  desc.name = (moq_bytes_t) { (const guint8 *) "video", 5 };
  desc.info.packaging = MOQ_MEDIA_PACKAGING_CMAF;
  desc.init_data = (moq_bytes_t) { init, sizeof init };
  receiver.events[0].kind = MOQ_MEDIA_TRACK_ADDED;
  receiver.events[0].track = (moq_media_track_t *) &video_handle;
  receiver.events[0].desc = &desc;
  receiver.events[1] = receiver.events[0]; /* duplicate must not resubscribe */
  receiver.events[2].kind = GPOINTER_TO_INT (kind);
  receiver.events[2].track = (moq_media_track_t *) &video_handle;
  receiver.event_count = 3;
  for (guint i = 0; i < 2; i++) {
    queue_object (&video_handle, NULL, 0);
    receiver.objects[i].packaging = MOQ_MEDIA_PACKAGING_CMAF;
    receiver.objects[i].fragment = (moq_bytes_t) { fragment, sizeof fragment };
  }
  for (guint i = 0; i < 3; i++) {
    GstBuffer *out = NULL;
    g_assert_cmpint (create (src, &out), ==, GST_FLOW_OK);
    guint8 bytes[8];
    g_assert_cmpuint (gst_buffer_extract (out, 0, bytes, sizeof bytes), ==, 8);
    g_assert_cmpmem (bytes, 8, i == 0 ? init : fragment, 8);
    g_assert_cmpint (GST_BUFFER_FLAG_IS_SET (out, GST_BUFFER_FLAG_HEADER), ==, i == 0);
    gst_buffer_unref (out);
  }
  GstBuffer *out = NULL;
  g_assert_cmpint (create (src, &out), ==, GST_FLOW_EOS);
  g_assert_cmpuint (receiver.subscriptions, ==, 1);
  g_assert_cmpuint (receiver.cleaned, ==, 2);
  g_assert_cmpuint (receiver.waits, ==, 0);
  gst_object_unref (src);
}

static void
capture_terminal_log (GstDebugCategory *category, GstDebugLevel level,
    const gchar *file, const gchar *function, gint line, GObject *object,
    GstDebugMessage *message, gpointer data)
{
  (void) level; (void) file; (void) function; (void) line;
  (void) object; (void) data;
  const gchar *text = gst_debug_message_get (message);
  if (category == gst_moq_src_debug && g_str_has_prefix (text, "endpoint terminal")) {
    g_assert_true (startup_active);
    g_assert_cmpstr (startup_calls->str, ==, "connect;attach;get;");
    g_assert_null (terminal_diagnostic);
    terminal_diagnostic = g_strdup (text);
    g_string_append (startup_calls, "log;");
  }
}

static void
test_startup_terminal (gconstpointer scenario)
{
  guint mode = GPOINTER_TO_UINT (scenario);
  startup_active = TRUE;
  startup_calls = g_string_new (NULL);
  terminal_diagnostic = NULL;
  terminal_getter_result = mode == 1 ? MOQ_ERR_INVAL : MOQ_OK;
  terminal_reason = mode == 2 ? MOQ_ENDPOINT_TERMINAL_NONE :
      MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE;
  terminal_detail = mode == 2 ? 0 : 0x130;
  GstMoqSrc *src = g_object_new (GST_TYPE_MOQ_SRC, NULL);
  gst_object_ref_sink (src);
  GstBus *bus = gst_bus_new ();
  gst_element_set_bus (GST_ELEMENT (src), bus);
  GstDebugLevel previous = gst_debug_category_get_threshold (gst_moq_src_debug);
  gst_debug_remove_log_function (gst_debug_log_default);
  gst_debug_add_log_function (capture_terminal_log, NULL, NULL);
  gst_debug_category_set_threshold (gst_moq_src_debug, GST_LEVEL_INFO);
  g_assert_false (GST_BASE_SRC_GET_CLASS (src)->start (GST_BASE_SRC (src)));
  gst_debug_remove_log_function (capture_terminal_log);
  gst_debug_add_log_function (gst_debug_log_default, NULL, NULL);
  gst_debug_category_set_threshold (gst_moq_src_debug, previous);
  g_assert_cmpstr (startup_calls->str, ==, "connect;attach;get;log;stop;destroy;");
  const gchar *expected = mode == 1 ? "endpoint terminal unavailable getter_rc=-2" :
      mode == 2 ? "endpoint terminal unclassified reason=0 detail=0" :
      "endpoint terminal reason=3 detail=304";
  g_assert_cmpstr (terminal_diagnostic, ==, expected);
  g_assert_null (src->ep);
  g_assert_null (src->receiver);
  g_assert_false (src->started);
  GstMessage *message = gst_bus_pop (bus);
  g_assert_nonnull (message);
  g_assert_cmpint (GST_MESSAGE_TYPE (message), ==, GST_MESSAGE_ERROR);
  GError *error = NULL;
  gchar *debug = NULL;
  gst_message_parse_error (message, &error, &debug);
  g_assert_cmpstr (error->message, ==, "could not attach media receiver (rc=-4)");
  g_assert_cmpint (error->code, ==, GST_RESOURCE_ERROR_OPEN_READ);
  g_assert_null (gst_bus_pop (bus)); /* No successful EOS or second error. */
  gst_message_unref (message);
  g_clear_error (&error);
  g_free (debug);
  gst_element_set_bus (GST_ELEMENT (src), NULL);
  gst_object_unref (src);
  gst_object_unref (bus);
  g_free (terminal_diagnostic);
  g_string_free (startup_calls, TRUE);
  startup_active = FALSE;
}

static void test_loc_aac_caps (gconstpointer explicit_caps)
{
  GstMoqSrc *src = make_source ();
  gst_caps_replace (&src->caps, NULL);
  if (GPOINTER_TO_INT (explicit_caps))
    src->caps = gst_caps_from_string ("audio/mpeg,mpegversion=4,stream-format=raw");
  static const guint8 asc[] = {0x11, 0x90};
  moq_media_track_desc_t d = {0};
  d.name = (moq_bytes_t) {(const guint8 *) "video", 5};
  d.codec = (moq_bytes_t) {(const guint8 *) "mp4a.40.2", 9};
  d.info.packaging = MOQ_MEDIA_PACKAGING_RAW;
  d.has_samplerate = TRUE; d.samplerate = 48000;
  d.channel_config = (moq_bytes_t) {(const guint8 *) "2", 1};
  d.init_data = (moq_bytes_t) {asc, sizeof asc};
  receiver.events[0].kind = MOQ_MEDIA_TRACK_ADDED;
  receiver.events[0].track = (moq_media_track_t *) &video_handle;
  receiver.events[0].desc = &d;
  receiver.event_count = 1;
  gst_moq_src_drain_track_events (src);
  g_assert_cmpuint (src->want_init.len, ==, 0);
  if (GPOINTER_TO_INT (explicit_caps)) {
    g_assert_null (src->derived_caps);
    src->receiver = NULL; gst_object_unref (src); return;
  }
  g_assert_nonnull (src->derived_caps);
  GstStructure *st = gst_caps_get_structure (src->derived_caps, 0);
  gint version = 0, rate = 0, channels = 0;
  g_assert_true (gst_structure_get_int (st, "mpegversion", &version));
  g_assert_cmpint (version, ==, 4);
  g_assert_false (gst_structure_has_field (st, "mpeg-version"));
  g_assert_true (gst_structure_get_int (st, "rate", &rate));
  g_assert_true (gst_structure_get_int (st, "channels", &channels));
  g_assert_cmpint (rate, ==, 48000); g_assert_cmpint (channels, ==, 2);
  const GValue *v = gst_structure_get_value (st, "codec_data");
  GstBuffer *cd = gst_value_get_buffer (v);
  guint8 bytes[2]; g_assert_cmpuint (gst_buffer_extract (cd, 0, bytes, 2), ==, 2);
  g_assert_cmpmem (bytes, 2, asc, 2);
  g_assert_cmpuint (src->want_init.len, ==, 0);
  src->receiver = NULL; gst_object_unref (src);
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
  g_test_add_data_func ("/src/cmaf-ended-tail", GINT_TO_POINTER (MOQ_MEDIA_TRACK_ENDED), test_cmaf_tail);
  g_test_add_data_func ("/src/cmaf-removed-tail", GINT_TO_POINTER (MOQ_MEDIA_TRACK_REMOVED), test_cmaf_tail);
  g_test_add_data_func ("/src/startup-terminal-known", GUINT_TO_POINTER (0), test_startup_terminal);
  g_test_add_data_func ("/src/startup-terminal-getter-failure", GUINT_TO_POINTER (1), test_startup_terminal);
  g_test_add_data_func ("/src/startup-terminal-none", GUINT_TO_POINTER (2), test_startup_terminal);
  g_test_add_data_func ("/src/loc-aac-caps", GINT_TO_POINTER (0), test_loc_aac_caps);
  g_test_add_data_func ("/src/loc-aac-explicit-caps", GINT_TO_POINTER (1), test_loc_aac_caps);
  return g_test_run ();
}
