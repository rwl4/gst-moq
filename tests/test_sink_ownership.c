/* The mocked sender consumes the payload before write() returns. Run this
 * test with AddressSanitizer to detect any access after ownership transfer. */
#include <glib.h>
static gint64 fake_now;
#define g_get_monotonic_time() fake_now
#include "../src/gstmoqsink.c"
#undef g_get_monotonic_time

static guint writes, end_calls, wait_calls, drain_calls;
static guint refusals;
static moq_result_t end_result, wait_result, drain_result;
static guint64 drain_budget;
static gboolean fatal;
static guint8 ep_handle;
static guint8 sender_handle, track_handle;

moq_result_t
moq_media_sender_write (moq_media_sender_t *s, moq_media_track_t *track,
    const moq_media_send_object_t *obj)
{
  g_assert_true (s == (moq_media_sender_t *) &sender_handle);
  g_assert_true (track == (moq_media_track_t *) &track_handle);
  g_assert_cmpuint (moq_rcbuf_len (obj->payload), ==, 16);
  writes++;
  moq_rcbuf_decref (obj->payload); /* Runs sink_frame_release immediately. */
  return MOQ_OK;
}

bool moq_media_sender_is_fatal (const moq_media_sender_t *s)
{ (void) s; return fatal; }
uint64_t moq_media_sender_fatal_code (const moq_media_sender_t *s)
{ (void) s; return 0; }

/* None of these lifecycle calls should run during a render-only test. */
moq_result_t moq_endpoint_wait (moq_endpoint_t *ep, uint64_t timeout)
{ (void) ep; g_assert_cmpuint (timeout, <=, 5000); wait_calls++;
  fake_now += G_USEC_PER_SEC; return wait_result; }
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
moq_result_t moq_endpoint_drain (moq_endpoint_t *ep, uint64_t timeout)
{ (void) ep; drain_calls++; drain_budget = timeout; return drain_result; }
void moq_media_sender_cfg_init_live (moq_media_sender_cfg_t *cfg)
{ (void) cfg; g_assert_not_reached (); }
moq_result_t moq_media_sender_attach (moq_endpoint_t *ep,
    const moq_media_sender_cfg_t *cfg, moq_media_sender_t **out)
{ (void) ep; (void) cfg; (void) out; g_assert_not_reached (); return MOQ_ERR_INVAL; }
void moq_media_sender_destroy (moq_media_sender_t *s)
{ (void) s; g_assert_not_reached (); }
void moq_media_track_cfg_init (moq_media_track_cfg_t *cfg)
{ (void) cfg; g_assert_not_reached (); }
moq_result_t moq_media_sender_add_track (moq_media_sender_t *s,
    const moq_media_track_cfg_t *cfg, moq_media_track_t **out)
{ (void) s; (void) cfg; (void) out; g_assert_not_reached (); return MOQ_ERR_INVAL; }
moq_result_t moq_media_sender_end_track (moq_media_sender_t *s, moq_media_track_t *track)
{ (void) s; (void) track; end_calls++;
  if (refusals) { refusals--; return MOQ_ERR_WOULD_BLOCK; }
  return end_result; }
moq_result_t moq_media_sender_get_stats (const moq_media_sender_t *s,
    moq_media_sender_stats_t *out, size_t size)
{ (void) s; (void) out; (void) size; g_assert_not_reached (); return MOQ_ERR_INVAL; }

static void
test_immediate_release (void)
{
  GstMoqSink *sink = g_object_new (GST_TYPE_MOQ_SINK, NULL);
  gst_object_ref_sink (sink);
  sink->sender = (moq_media_sender_t *) &sender_handle;
  sink->track = (moq_media_track_t *) &track_handle;
  sink->base_pts = GST_CLOCK_TIME_NONE;
  GstBuffer *buf = gst_buffer_new_allocate (NULL, 16, NULL);
  GST_BUFFER_PTS (buf) = GST_SECOND;
  gst_debug_set_threshold_for_name ("moqsink", GST_LEVEL_LOG);
  g_assert_cmpint (GST_BASE_SINK_GET_CLASS (sink)->render (
          GST_BASE_SINK (sink), buf), ==, GST_FLOW_OK);
  g_assert_cmpuint (writes, ==, 1);
  g_assert_cmpuint (sink->objects_sent, ==, 1);
  gst_buffer_unref (buf);
  gst_object_unref (sink);
}

static GstMoqSink *
eos_sink (void)
{
  end_calls = wait_calls = drain_calls = refusals = 0;
  end_result = wait_result = drain_result = MOQ_OK;
  fatal = FALSE;
  fake_now = 100;
  GstMoqSink *sink = g_object_new (GST_TYPE_MOQ_SINK, NULL);
  gst_object_ref_sink (sink);
  sink->ep = (moq_endpoint_t *) &ep_handle;
  sink->sender = (moq_media_sender_t *) &sender_handle;
  sink->track = (moq_media_track_t *) &track_handle;
  return sink;
}

static void
test_eos_retry_budget (void)
{
  GstMoqSink *sink = eos_sink ();
  refusals = 2;
  g_assert_true (gst_moq_sink_finish_eos (sink));
  g_assert_cmpuint (end_calls, ==, 3);
  g_assert_cmpuint (wait_calls, ==, 2);
  g_assert_cmpuint (drain_calls, ==, 1);
  g_assert_cmpuint (drain_budget, ==, G_USEC_PER_SEC);
  gst_object_unref (sink);
}

static void
test_eos_refusal_timeout (void)
{
  GstMoqSink *sink = eos_sink ();
  refusals = 100;
  g_assert_false (gst_moq_sink_finish_eos (sink));
  g_assert_cmpuint (wait_calls, ==, 3);
  g_assert_cmpuint (drain_calls, ==, 0);
  gst_object_unref (sink);
}

static void
test_eos_failures (void)
{
  const moq_result_t results[] = { MOQ_DONE, MOQ_ERR_INTERRUPTED,
    MOQ_ERR_CLOSED, MOQ_ERR_UNSUPPORTED, MOQ_ERR_WRONG_STATE };
  for (guint i = 0; i < G_N_ELEMENTS (results); i++) {
    GstMoqSink *sink = eos_sink ();
    GstBus *bus = gst_bus_new ();
    gst_element_set_bus (GST_ELEMENT (sink), bus);
    drain_result = results[i];
    g_assert_false (gst_moq_sink_event (GST_BASE_SINK (sink), gst_event_new_eos ()));
    GstMessage *msg = gst_bus_pop (bus);
    g_assert_nonnull (msg);
    g_assert_cmpint (GST_MESSAGE_TYPE (msg), ==, GST_MESSAGE_ERROR);
    gst_message_unref (msg);
    g_assert_null (gst_bus_pop (bus));
    gst_element_set_bus (GST_ELEMENT (sink), NULL);
    gst_object_unref (bus);
    gst_object_unref (sink);
  }
}

static void
test_eos_end_error (void)
{
  GstMoqSink *sink = eos_sink ();
  end_result = MOQ_ERR_WRONG_STATE;
  g_assert_false (gst_moq_sink_finish_eos (sink));
  g_assert_cmpuint (drain_calls, ==, 0);
  gst_object_unref (sink);
}

static void
test_eos_cancel_retry (void)
{
  GstMoqSink *sink = eos_sink ();
  refusals = 1;
  wait_result = MOQ_ERR_INTERRUPTED;
  g_assert_false (gst_moq_sink_finish_eos (sink));
  g_assert_cmpuint (end_calls, ==, 1);
  g_assert_cmpuint (drain_calls, ==, 0);
  gst_object_unref (sink);
}

static void
test_eos_fatal (void)
{
  GstMoqSink *sink = eos_sink ();
  fatal = TRUE;
  g_assert_false (gst_moq_sink_finish_eos (sink));
  g_assert_cmpuint (end_calls, ==, 0);
  g_assert_cmpuint (drain_calls, ==, 0);
  fatal = FALSE;
  gst_object_unref (sink);
}

static void
test_eos_no_track (void)
{
  GstMoqSink *sink = eos_sink ();
  sink->track = NULL;
  g_assert_true (gst_moq_sink_finish_eos (sink));
  g_assert_cmpuint (end_calls, ==, 0);
  g_assert_cmpuint (drain_calls, ==, 1);
  gst_object_unref (sink);
}

int
main (int argc, char **argv)
{
  gst_init (&argc, &argv);
  g_test_init (&argc, &argv, NULL);
  g_test_add_func ("/sink/immediate-release", test_immediate_release);
  g_test_add_func ("/sink/eos-retry-budget", test_eos_retry_budget);
  g_test_add_func ("/sink/eos-refusal-timeout", test_eos_refusal_timeout);
  g_test_add_func ("/sink/eos-drain-failures", test_eos_failures);
  g_test_add_func ("/sink/eos-end-error", test_eos_end_error);
  g_test_add_func ("/sink/eos-cancel-retry", test_eos_cancel_retry);
  g_test_add_func ("/sink/eos-fatal", test_eos_fatal);
  g_test_add_func ("/sink/eos-no-track", test_eos_no_track);
  return g_test_run ();
}
