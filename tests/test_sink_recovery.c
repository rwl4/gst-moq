/* Fresh evidence for reconstructed consumer sources, public service mocks. */
#include <glib.h>
static gint64 now;
static void pause_retry (gulong us);
#define g_get_monotonic_time() now
#define g_usleep(us) pause_retry (us)
#include "../src/gstmoqsink.c"
#undef g_get_monotonic_time
#undef g_usleep

static guint8 ep_id, sender_id, track_ids[16];
static guint tracks, writes, ends, drains, connects, destroys, refusals;
static gboolean interrupted, ended[16], cancel_drain;
static moq_result_t drain_result;
static guint64 budget;
static GstMoqSink *active;
static GstMoqSinkPad *continuing, *canceller;
static moq_media_sender_callbacks_t ready_callbacks;
static gboolean ready_during_attach, ready_during_destroy;
static guint ready_logs;

void
moq_endpoint_cfg_init (moq_endpoint_cfg_t *c)
{
  memset (c, 0, sizeof *c);
  c->struct_size = sizeof *c;
}
moq_result_t
moq_endpoint_connect (const moq_endpoint_cfg_t *c, moq_endpoint_t **out)
{
  (void)c;
  connects++;
  interrupted = FALSE;
  *out = (moq_endpoint_t *)&ep_id;
  return MOQ_OK;
}
moq_result_t
moq_endpoint_stop (moq_endpoint_t *ep)
{
  (void)ep;
  return MOQ_OK;
}
moq_result_t moq_endpoint_post (moq_endpoint_t *ep, moq_endpoint_task_fn fn, void *ctx)
{ (void) fn (ep, (moq_session_t *) ep, 0, ctx); return MOQ_OK; }
void
moq_endpoint_destroy (moq_endpoint_t *ep)
{
  (void)ep;
  destroys++;
}
void
moq_endpoint_set_interrupted (moq_endpoint_t *ep, bool v)
{
  (void)ep;
  interrupted = v;
}
moq_result_t
moq_endpoint_wait (moq_endpoint_t *ep, uint64_t us)
{
  (void)ep;
  (void)us;
  now += G_USEC_PER_SEC;
  return interrupted ? MOQ_ERR_INTERRUPTED : MOQ_OK;
}
moq_result_t
moq_endpoint_drain (moq_endpoint_t *ep, uint64_t us)
{
  (void)ep;
  drains++;
  budget = us;
  if (cancel_drain)
    gst_moq_sink_set_pad_flush (active, canceller, TRUE);
  return interrupted ? MOQ_ERR_INTERRUPTED : drain_result;
}
void
moq_media_sender_cfg_init_live_sized (moq_media_sender_cfg_t *c, size_t size)
{
  g_assert_cmpuint (size, ==, sizeof *c);
  memset (c, 0, sizeof *c);
  c->struct_size = sizeof *c;
}
void
moq_media_sender_callbacks_init_sized (moq_media_sender_callbacks_t *c, size_t size)
{
  g_assert_cmpuint (size, ==, sizeof *c);
  memset (c, 0, sizeof *c);
  c->struct_size = sizeof *c;
}
moq_result_t
moq_media_sender_attach (moq_endpoint_t *ep, const moq_media_sender_cfg_t *c,
                         moq_media_sender_t **out)
{
  (void)ep;
  g_assert_cmpuint (c->struct_size, ==, sizeof *c);
  g_assert_cmpuint (c->callbacks.struct_size, ==, sizeof c->callbacks);
  g_assert_nonnull (c->callbacks.on_ready);
  ready_callbacks = c->callbacks;
  if (ready_during_attach) {
    g_assert_null (active->sender);
    ready_callbacks.on_ready (ready_callbacks.ctx, (moq_media_sender_t *) &sender_id);
  }
  *out = (moq_media_sender_t *)&sender_id;
  return MOQ_OK;
}
void
moq_media_sender_destroy (moq_media_sender_t *s)
{
  if (ready_during_destroy)
    ready_callbacks.on_ready (ready_callbacks.ctx, s);
}
void
moq_media_track_cfg_init (moq_media_track_cfg_t *c)
{
  memset (c, 0, sizeof *c);
  c->struct_size = sizeof *c;
}
moq_result_t
moq_media_sender_add_track (moq_media_sender_t *s,
                            const moq_media_track_cfg_t *c,
                            moq_media_track_t **out)
{
  (void)s;
  (void)c;
  g_assert_cmpuint (tracks, <, 16);
  *out = (moq_media_track_t *)&track_ids[tracks++];
  return MOQ_OK;
}
moq_result_t
moq_media_sender_end_track (moq_media_sender_t *s, moq_media_track_t *t)
{
  (void)s;
  ends++;
  if (interrupted)
    return MOQ_ERR_INTERRUPTED;
  if (refusals)
    {
      refusals--;
      return MOQ_ERR_WOULD_BLOCK;
    }
  for (guint i = 0; i < tracks; i++)
    if (t == (moq_media_track_t *)&track_ids[i])
      {
        ended[i] = TRUE;
        return MOQ_OK;
      }
  return MOQ_ERR_INVAL;
}
moq_result_t
moq_media_sender_write (moq_media_sender_t *s, moq_media_track_t *t,
                        const moq_media_send_object_t *o)
{
  (void)s;
  if (interrupted)
    return MOQ_ERR_INTERRUPTED;
  for (guint i = 0; i < tracks; i++)
    if (t == (moq_media_track_t *)&track_ids[i])
      {
        if (ended[i])
          return MOQ_ERR_WRONG_STATE;
        writes++;
        moq_rcbuf_decref (o->payload);
        return MOQ_OK;
      }
  return MOQ_ERR_INVAL;
}
moq_result_t
moq_media_sender_get_stats (const moq_media_sender_t *s,
                            moq_media_sender_stats_t *out, size_t n)
{
  (void)s;
  g_assert_cmpuint (n, ==, sizeof *out);
  memset (out, 0, n);
  out->struct_size = n;
  return MOQ_OK;
}
bool
moq_media_sender_is_fatal (const moq_media_sender_t *s)
{
  (void)s;
  return false;
}
uint64_t
moq_media_sender_fatal_code (const moq_media_sender_t *s)
{
  (void)s;
  return 0;
}

static void
write_other (GstMoqSinkPad *p)
{
  static const guint8 byte = 42;
  moq_rcbuf_t *payload = NULL;
  g_assert_cmpint (
      moq_rcbuf_wrap (moq_alloc_default (), &byte, 1, NULL, NULL, &payload),
      ==, MOQ_OK);
  moq_media_send_object_t o = { 0 };
  o.struct_size = sizeof o;
  o.payload = payload;
  g_assert_cmpint (gst_moq_sink_write_object (active, p, payload, &o, 1), ==,
                   GST_FLOW_OK);
}
static void
pause_retry (gulong us)
{
  g_assert_cmpuint (us, <=, 5000);
  g_assert_true (g_mutex_trylock (&active->send_lock));
  g_mutex_unlock (&active->send_lock);
  if (continuing)
    write_other (continuing);
  now += G_USEC_PER_SEC;
}
static GstMoqSink *
make_sink (void)
{
  tracks = writes = ends = drains = connects = destroys = refusals = 0;
  now = 100;
  memset (ended, 0, sizeof ended);
  drain_result = MOQ_OK;
  cancel_drain = FALSE;
  continuing = canceller = NULL;
  active = g_object_new (GST_TYPE_MOQ_SINK, "sync", FALSE, NULL);
  gst_object_ref_sink (active);
  g_assert_cmpint (
      gst_element_set_state (GST_ELEMENT (active), GST_STATE_READY), !=,
      GST_STATE_CHANGE_FAILURE);
  return active;
}
static GstMoqSinkPad *
pad (GstMoqSink *s, gboolean track)
{
  GstMoqSinkPad *p = GST_MOQ_SINK_PAD (
      gst_element_request_pad_simple (GST_ELEMENT (s), "audio_%u"));
  if (track)
    {
      if (!s->sender) {
        s->preparing_tracks = g_ptr_array_new_with_free_func (prepared_track_free);
        g_assert_true (gst_moq_sink_open_sender (s));
        g_clear_pointer (&s->preparing_tracks, g_ptr_array_unref);
      }
      moq_media_track_cfg_t c;
      moq_media_track_cfg_init (&c);
      g_assert_cmpint (moq_media_sender_add_track (s->sender, &c, &p->track),
                       ==, MOQ_OK);
    }
  return p;
}
static void
release (GstMoqSink *s, GstMoqSinkPad *p)
{
  gst_element_release_request_pad (GST_ELEMENT (s), GST_PAD (p));
  gst_object_unref (p);
}
static void
finish (GstMoqSink *s)
{
  gst_element_set_state (GST_ELEMENT (s), GST_STATE_NULL);
  gst_object_unref (s);
}
static gboolean
eos (GstMoqSink *s, GstMoqSinkPad *p)
{
  return gst_moq_sink_event (GST_PAD (p), GST_OBJECT (s),
                             gst_event_new_eos ());
}
static void
flush (GstMoqSink *s, GstMoqSinkPad *p, gboolean start)
{
  g_assert_true (gst_moq_sink_event (GST_PAD (p), GST_OBJECT (s),
                                     start ? gst_event_new_flush_start ()
                                           : gst_event_new_flush_stop (TRUE)));
}

static void
test_incomplete_cmaf_eos (gconstpointer pending_moof)
{
  GstMoqSink *s = make_sink ();
  GstMoqSinkPad *p = pad (s, TRUE);
  GstBus *bus = gst_bus_new ();
  gst_element_set_bus (GST_ELEMENT (s), bus);
  static const guint8 partial_header[] = { 0, 0, 0 };
  static const guint8 missing_mdat[] = {
    0, 0, 0, 8, 'f', 't', 'y', 'p',
    0, 0, 0, 8, 'm', 'o', 'o', 'v',
    0, 0, 0, 8, 'm', 'o', 'o', 'f'
  };
  const guint8 *bytes = pending_moof ? missing_mdat : partial_header;
  gsize len = pending_moof ? sizeof missing_mdat : sizeof partial_header;
  g_assert_true (gst_moq_fmp4_splitter_push (&p->splitter, bytes, len, NULL));
  g_assert_false (eos (s, p));
  g_assert_cmpuint (ends, ==, 0);
  g_assert_cmpuint (drains, ==, 0);
  g_assert_true (s->eos_failed);
  GstMessage *msg = gst_bus_pop_filtered (bus, GST_MESSAGE_ERROR);
  g_assert_nonnull (msg);
  GError *error = NULL;
  gst_message_parse_error (msg, &error, NULL);
  g_assert_error (error, GST_STREAM_ERROR, GST_STREAM_ERROR_FORMAT);
  g_error_free (error);
  gst_message_unref (msg);
  g_assert_null (gst_bus_pop_filtered (bus, GST_MESSAGE_EOS));
  gst_element_set_bus (GST_ELEMENT (s), NULL);
  gst_object_unref (bus);
  release (s, p);
  finish (s);
}

static void
test_budget (void)
{
  GstMoqSink *s = make_sink ();
  GstMoqSinkPad *p = pad (s, TRUE);
  refusals = 2;
  g_assert_true (eos (s, p));
  g_assert_cmpuint (ends, ==, 3);
  g_assert_cmpuint (budget, ==, G_USEC_PER_SEC);
  release (s, p);
  finish (s);
}
static void
test_independent (void)
{
  GstMoqSink *s = make_sink ();
  GstMoqSinkPad *a = pad (s, TRUE), *b = pad (s, TRUE);
  continuing = b;
  refusals = 1;
  g_assert_true (eos (s, a));
  g_assert_cmpuint (writes, ==, 1);
  g_assert_true (ended[0]);
  g_assert_false (ended[1]);
  g_assert_cmpuint (drains, ==, 0);
  continuing = NULL;
  release (s, a);
  release (s, b);
  finish (s);
}
static void
test_released (void)
{
  GstMoqSink *s = make_sink ();
  GstMoqSinkPad *a = pad (s, TRUE), *b = pad (s, TRUE);
  flush (s, b, TRUE);
  release (s, a);
  g_assert_false (ended[0]);
  g_assert_true (interrupted);
  flush (s, b, FALSE);
  g_assert_true (ended[0]);
  g_assert_false (ended[1]);
  release (s, b);
  finish (s);
}
static void
test_flush (void)
{
  GstMoqSink *s = make_sink ();
  GstMoqSinkPad *a = pad (s, FALSE), *b = pad (s, FALSE);
  flush (s, a, TRUE);
  flush (s, a, TRUE);
  flush (s, b, FALSE);
  g_assert_cmpint (s->flush_count, ==, 1);
  flush (s, b, TRUE);
  release (s, a);
  g_assert_cmpint (s->flush_count, ==, 1);
  g_assert_true (interrupted);
  s->state_flushing = TRUE;
  flush (s, b, FALSE);
  g_assert_true (interrupted);
  release (s, b);
  finish (s);
}
static void
test_failed (void)
{
  const moq_result_t results[]
      = { MOQ_DONE, MOQ_ERR_INTERRUPTED, MOQ_ERR_CLOSED, MOQ_ERR_UNSUPPORTED,
          MOQ_ERR_WRONG_STATE };
  for (guint i = 0; i < G_N_ELEMENTS (results); i++)
    {
      GstMoqSink *s = make_sink ();
      GstMoqSinkPad *p = pad (s, TRUE);
      GstBus *bus = gst_bus_new ();
      gst_element_set_bus (GST_ELEMENT (s), bus);
      drain_result = results[i];
      g_assert_false (eos (s, p));
      GstMessage *m = gst_bus_pop (bus);
      g_assert_nonnull (m);
      g_assert_cmpint (GST_MESSAGE_TYPE (m), ==, GST_MESSAGE_ERROR);
      gst_message_unref (m);
      g_assert_null (gst_bus_pop (bus));
      release (s, p);
      gst_element_set_bus (GST_ELEMENT (s), NULL);
      gst_object_unref (bus);
      finish (s);
    }
}
static void
test_cancel (void)
{
  GstMoqSink *s = make_sink ();
  GstMoqSinkPad *p = pad (s, TRUE);
  canceller = p;
  cancel_drain = TRUE;
  g_assert_false (eos (s, p));
  g_assert_true (interrupted);
  cancel_drain = FALSE;
  release (s, p);
  finish (s);
}
static void
test_aggregation (void)
{
  GstMoqSink *s = make_sink ();
  GstMoqSinkPad *a = pad (s, TRUE), *b = pad (s, TRUE);
  a->eos = TRUE;
  g_assert_true (eos (s, b));
  g_assert_false (s->eos_posted);
  g_assert_cmpuint (drains, ==, 0);
  release (s, a);
  g_assert_cmpuint (drains, ==, 1);
  release (s, b);
  finish (s);
}
static void
test_restart (void)
{
  GstMoqSink *s = make_sink ();
  gst_element_set_state (GST_ELEMENT (s), GST_STATE_PAUSED);
  GstMoqSinkPad *p = pad (s, TRUE);
  g_assert_true (eos (s, p));
  g_byte_array_append (p->splitter.buf, (const guint8 *)"x", 1);
  gst_element_set_state (GST_ELEMENT (s), GST_STATE_READY);
  g_assert_cmpuint (destroys, ==, 1);
  gst_element_set_state (GST_ELEMENT (s), GST_STATE_PAUSED);
  g_assert_null (p->track);
  g_assert_false (p->eos);
  g_assert_cmpuint (p->splitter.buf->len, ==, 0);
  g_assert_cmpuint (connects, ==, 1); /* reconnect is deferred until media */
  release (s, p);
  finish (s);
}
static void
test_deadline (void)
{
  GstMoqSink *s = make_sink ();
  GstMoqSinkPad *a = pad (s, TRUE), *b = pad (s, TRUE);
  flush (s, b, TRUE);
  release (s, a);
  now += EOS_DRAIN_TIMEOUT_US;
  guint before = ends;
  flush (s, b, FALSE);
  g_assert_cmpuint (ends, ==, before);
  g_assert_true (s->eos_failed);
  g_assert_false (ended[0]);
  release (s, b);
  finish (s);
}
static void
capture_ready (GstDebugCategory *category, GstDebugLevel level,
    const gchar *file, const gchar *function, gint line, GObject *object,
    GstDebugMessage *message, gpointer data)
{
  (void) level; (void) file; (void) function; (void) line;
  (void) object; (void) data;
  if (category == gst_moq_sink_debug &&
      g_str_has_prefix (gst_debug_message_get (message), "native sender ready")) {
    g_assert_cmpstr (gst_debug_message_get (message), ==,
        "native sender ready namespace=example");
    ready_logs++;
  }
}

static void
test_ready_callback (gconstpointer early)
{
  ready_logs = 0;
  ready_during_attach = GPOINTER_TO_INT (early);
  gpointer klass = g_type_class_ref (GST_TYPE_MOQ_SINK);
  GstDebugLevel previous = gst_debug_category_get_threshold (gst_moq_sink_debug);
  gst_debug_remove_log_function (gst_debug_log_default);
  gst_debug_add_log_function (capture_ready, NULL, NULL);
  gst_debug_category_set_threshold (gst_moq_sink_debug, GST_LEVEL_INFO);
  GstMoqSink *s = make_sink ();
  gst_element_set_state (GST_ELEMENT (s), GST_STATE_PAUSED);
  g_assert_true (s->started);
  GstMoqSinkPad *p = pad (s, TRUE);
  g_assert_cmpuint (ready_logs, ==, ready_during_attach ? 1 : 0);
  if (!ready_during_attach) {
    /* Attach success is not readiness; only the callback emits the record. */
    ready_callbacks.on_ready (ready_callbacks.ctx, s->sender);
    g_assert_cmpuint (ready_logs, ==, 1);
  }
  ready_during_destroy = TRUE; /* Context must survive callback settlement. */
  gst_element_set_state (GST_ELEMENT (s), GST_STATE_READY);
  g_assert_cmpuint (ready_logs, ==, 2);
  g_assert_null (s->ready_diagnostic);
  ready_during_destroy = ready_during_attach = FALSE;
  gst_debug_remove_log_function (capture_ready);
  gst_debug_add_log_function (gst_debug_log_default, NULL, NULL);
  gst_debug_category_set_threshold (gst_moq_sink_debug, previous);
  release (s, p);
  finish (s);
  g_type_class_unref (klass);
}

int
main (int argc, char **argv)
{
  gst_init (&argc, &argv);
  g_test_init (&argc, &argv, NULL);
  g_test_add_data_func ("/sink/eos-partial-box", GINT_TO_POINTER (0), test_incomplete_cmaf_eos);
  g_test_add_data_func ("/sink/eos-missing-mdat", GINT_TO_POINTER (1), test_incomplete_cmaf_eos);
  g_test_add_func ("/sink/budget", test_budget);
  g_test_add_func ("/sink/independent", test_independent);
  g_test_add_func ("/sink/released", test_released);
  g_test_add_func ("/sink/flush", test_flush);
  g_test_add_func ("/sink/failed", test_failed);
  g_test_add_func ("/sink/cancel", test_cancel);
  g_test_add_func ("/sink/aggregation", test_aggregation);
  g_test_add_func ("/sink/restart", test_restart);
  g_test_add_func ("/sink/deadline", test_deadline);
  g_test_add_data_func ("/sink/ready-after-attach", GINT_TO_POINTER (0), test_ready_callback);
  g_test_add_data_func ("/sink/ready-during-attach", GINT_TO_POINTER (1), test_ready_callback);
  return g_test_run ();
}
