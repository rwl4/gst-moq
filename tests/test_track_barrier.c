/* tests/test_track_barrier.c -- the initial-catalog track barrier.
 *
 * REGRESSION TARGET. libmoq freezes the INITIAL catalog on the sender's first
 * pass after the session reaches ESTABLISHED, from whatever tracks exist at
 * that instant. A track added later stages a generation that is only installed
 * where the catalog already has demand, so with catalog-refresh-ms=0 and no
 * subscriber yet it never reaches the wire. moqsink derives each LOC track from
 * its pad's first keyframe, and the audio pad's first buffer can precede the
 * video pad's by a second, so attaching on the first pad published an
 * audio-only catalog that stayed stale for the whole session -- every
 * subscriber saw one track. gst_moq_sink_await_tracks holds the endpoint
 * connect and the sender attach until every LINKED pad has prepared its track.
 *
 * PRIMARY ORACLE (deterministic, no timing): the call log interleaves a marker
 * written by the test thread with the libmoq calls the element makes. The
 * marker goes in immediately BEFORE the video buffer is pushed, so
 *
 *     MARK("push-video")  must precede  CONNECT
 *
 * is an ordering assertion, not a race: on the pre-barrier code the audio
 * buffer alone drove connect + attach + add_track, so CONNECT would be logged
 * before the marker and the assertion fails. Two add_tracks must likewise
 * precede any write -- the session must not be able to establish with a
 * partial track set.
 *
 * SECONDARY (bounded wait, corroborating): the audio push must still be
 * blocked inside the barrier when the marker is written. This confirms the
 * thread is parked in await_tracks rather than having returned early, and is
 * the only timing-dependent check here.
 *
 * ANTI-VACUITY: /barrier/unlinked-pad-excluded drives the same element with the
 * audio pad left unlinked and asserts CONNECT *does* happen on the video buffer
 * alone. If the ordering oracle above could never observe an early connect, or
 * if the barrier deadlocked a single-pad pipeline, that test fails.
 *
 * White-box only in its linkage: the libmoq entry points are replaced at link
 * time with -Wl,--wrap, so nothing here dials a relay or opens a socket. The
 * element source is unmodified.
 */
#include "gstmoqsink.h"

#include <gst/gst.h>
#include <moq/endpoint.h>
#include <moq/media_sender.h>

#include <string.h>

/* -- call log ------------------------------------------------------------- */

typedef enum
{
  OP_MARK,
  OP_CONNECT,
  OP_ATTACH,
  OP_ADD_TRACK,
  OP_WRITE,
} LogOp;

typedef struct
{
  LogOp op;
  gchar *detail;                /* track name, or marker label */
} LogEntry;

static GMutex log_lock;
static GArray *call_log;        /* LogEntry */

static void
log_reset (void)
{
  g_mutex_lock (&log_lock);
  if (call_log) {
    for (guint i = 0; i < call_log->len; i++)
      g_free (g_array_index (call_log, LogEntry, i).detail);
    g_array_set_size (call_log, 0);
  } else {
    call_log = g_array_new (FALSE, FALSE, sizeof (LogEntry));
  }
  g_mutex_unlock (&log_lock);
}

static void
log_add (LogOp op, const gchar *detail)
{
  LogEntry e = { op, g_strdup (detail) };
  g_mutex_lock (&log_lock);
  g_array_append_val (call_log, e);
  g_mutex_unlock (&log_lock);
}

/* Index of the first entry matching op (and detail, when non-NULL), or -1. */
static gint
log_find (LogOp op, const gchar *detail)
{
  gint found = -1;
  g_mutex_lock (&log_lock);
  for (guint i = 0; i < call_log->len && found < 0; i++) {
    LogEntry *e = &g_array_index (call_log, LogEntry, i);
    if (e->op == op && (!detail || g_strcmp0 (e->detail, detail) == 0))
      found = (gint) i;
  }
  g_mutex_unlock (&log_lock);
  return found;
}

static guint
log_count (LogOp op)
{
  guint n = 0;
  g_mutex_lock (&log_lock);
  for (guint i = 0; i < call_log->len; i++)
    if (g_array_index (call_log, LogEntry, i).op == op)
      n++;
  g_mutex_unlock (&log_lock);
  return n;
}

/* -- libmoq seams (-Wl,--wrap) -------------------------------------------- *
 * Opaque handles the element only ever passes back to us. */
static int fake_endpoint;
static int fake_sender;
static int fake_track_video;
static int fake_track_audio;

moq_result_t __wrap_moq_endpoint_connect (const moq_endpoint_cfg_t *cfg,
    moq_endpoint_t **out);
moq_result_t
__wrap_moq_endpoint_connect (const moq_endpoint_cfg_t *cfg,
    moq_endpoint_t **out)
{
  (void) cfg;
  log_add (OP_CONNECT, NULL);
  *out = (moq_endpoint_t *) &fake_endpoint;
  return MOQ_OK;
}

moq_result_t __wrap_moq_endpoint_stop (moq_endpoint_t *ep);
moq_result_t
__wrap_moq_endpoint_stop (moq_endpoint_t *ep)
{
  (void) ep;
  return MOQ_OK;
}

void __wrap_moq_endpoint_destroy (moq_endpoint_t *ep);
void
__wrap_moq_endpoint_destroy (moq_endpoint_t *ep)
{
  (void) ep;
}

void __wrap_moq_endpoint_set_interrupted (moq_endpoint_t *ep, bool interrupted);
void
__wrap_moq_endpoint_set_interrupted (moq_endpoint_t *ep, bool interrupted)
{
  (void) ep;
  (void) interrupted;
}

moq_result_t __wrap_moq_endpoint_drain (moq_endpoint_t *ep, uint64_t timeout_us);
moq_result_t
__wrap_moq_endpoint_drain (moq_endpoint_t *ep, uint64_t timeout_us)
{
  (void) ep;
  (void) timeout_us;
  return MOQ_OK;
}

moq_result_t __wrap_moq_media_sender_attach (moq_endpoint_t *ep,
    const moq_media_sender_cfg_t *cfg, moq_media_sender_t **out);
moq_result_t
__wrap_moq_media_sender_attach (moq_endpoint_t *ep,
    const moq_media_sender_cfg_t *cfg, moq_media_sender_t **out)
{
  (void) ep;
  (void) cfg;
  log_add (OP_ATTACH, NULL);
  *out = (moq_media_sender_t *) &fake_sender;
  return MOQ_OK;
}

moq_result_t __wrap_moq_media_sender_add_track (moq_media_sender_t *s,
    const moq_media_track_cfg_t *cfg, moq_media_track_t **out);
moq_result_t
__wrap_moq_media_sender_add_track (moq_media_sender_t *s,
    const moq_media_track_cfg_t *cfg, moq_media_track_t **out)
{
  (void) s;
  gchar *name = g_strndup ((const gchar *) cfg->name.data, cfg->name.len);
  log_add (OP_ADD_TRACK, name);
  *out = (moq_media_track_t *) (cfg->media_type == MOQ_MEDIA_TYPE_AUDIO
      ? &fake_track_audio : &fake_track_video);
  g_free (name);
  return MOQ_OK;
}

moq_result_t __wrap_moq_media_sender_write (moq_media_sender_t *s,
    moq_media_track_t *track, const moq_media_send_object_t *obj);
moq_result_t
__wrap_moq_media_sender_write (moq_media_sender_t *s,
    moq_media_track_t *track, const moq_media_send_object_t *obj)
{
  (void) s;
  (void) obj;
  log_add (OP_WRITE, track == (moq_media_track_t *) &fake_track_audio
      ? "audio" : "video");
  return MOQ_OK;
}

moq_result_t __wrap_moq_media_sender_end_track (moq_media_sender_t *s,
    moq_media_track_t *track);
moq_result_t
__wrap_moq_media_sender_end_track (moq_media_sender_t *s,
    moq_media_track_t *track)
{
  (void) s;
  (void) track;
  return MOQ_OK;
}

bool __wrap_moq_media_sender_is_fatal (const moq_media_sender_t *s);
bool
__wrap_moq_media_sender_is_fatal (const moq_media_sender_t *s)
{
  (void) s;
  return false;
}

uint64_t __wrap_moq_media_sender_fatal_code (const moq_media_sender_t *s);
uint64_t
__wrap_moq_media_sender_fatal_code (const moq_media_sender_t *s)
{
  (void) s;
  return 0;
}

moq_result_t __wrap_moq_media_sender_get_stats (const moq_media_sender_t *s,
    moq_media_sender_stats_t *out, size_t out_size);
moq_result_t
__wrap_moq_media_sender_get_stats (const moq_media_sender_t *s,
    moq_media_sender_stats_t *out, size_t out_size)
{
  (void) s;
  memset (out, 0, out_size);
  return MOQ_OK;
}

void __wrap_moq_media_sender_destroy (moq_media_sender_t *s);
void
__wrap_moq_media_sender_destroy (moq_media_sender_t *s)
{
  (void) s;
}

/* -- fixture -------------------------------------------------------------- */

/* avcC: version 1, profile 0x42, compat 0xc0, level 0x1e. prepare_loc_track's
 * stream-format=avc path needs only these four bytes to be well-formed. */
static const guint8 avcc[] = { 1, 0x42, 0xc0, 0x1e, 0xff, 0xe1 };
/* AudioSpecificConfig: AOT 2 (AAC-LC), 48 kHz, stereo. */
static const guint8 asc[] = { 0x11, 0x90 };

static GstCaps *
video_caps (void)
{
  GstBuffer *cd = gst_buffer_new_memdup (avcc, sizeof avcc);
  GstCaps *caps = gst_caps_new_simple ("video/x-h264",
      "stream-format", G_TYPE_STRING, "avc",
      "alignment", G_TYPE_STRING, "au",
      "width", G_TYPE_INT, 1280,
      "height", G_TYPE_INT, 720,
      "framerate", GST_TYPE_FRACTION, 24, 1,
      "codec_data", GST_TYPE_BUFFER, cd, NULL);
  gst_buffer_unref (cd);
  return caps;
}

static GstCaps *
audio_caps (void)
{
  GstBuffer *cd = gst_buffer_new_memdup (asc, sizeof asc);
  /* BOTH spellings on purpose. "mpegversion" is what aacparse names the field;
   * the pad template spells it "mpeg-version", and accept-caps is a SUBSET test
   * (gst_caps_is_subset), so caps missing the template's field are rejected
   * outright -- the CAPS event never reaches the element. A real pipeline never
   * trips on this because negotiation intersects with the template and folds
   * "mpeg-version" in; pushing caps by hand here does not, so the test has to
   * supply what negotiation would have produced. The template's spelling looks
   * like a typo (nothing reads it, and no decoder emits it), but it is load
   * bearing for accept-caps, so leave it to a separate change. */
  GstCaps *caps = gst_caps_new_simple ("audio/mpeg",
      "mpegversion", G_TYPE_INT, 4,
      "mpeg-version", G_TYPE_INT, 4,
      "stream-format", G_TYPE_STRING, "raw",
      "rate", G_TYPE_INT, 48000,
      "channels", G_TYPE_INT, 2,
      "codec_data", GST_TYPE_BUFFER, cd, NULL);
  gst_buffer_unref (cd);
  return caps;
}

/* A src pad linked to `name` on `sink`, activated, with the sticky events a
 * chain function is entitled to assume. */
static GstPad *
open_src_pad (GstElement *sink, const gchar *name, const gchar *stream_id,
    GstCaps *caps)
{
  GstPad *src = gst_pad_new (NULL, GST_PAD_SRC);
  GstPad *peer = gst_element_get_static_pad (sink, name);
  g_assert_nonnull (peer);
  g_assert_cmpint (gst_pad_link (src, peer), ==, GST_PAD_LINK_OK);

  gst_pad_set_active (src, TRUE);
  g_assert_true (gst_pad_push_event (src, gst_event_new_stream_start (stream_id)));
  gst_pad_push_event (src, gst_event_new_caps (caps));
  gst_caps_unref (caps);
  /* gst_pad_push_event returns TRUE for a caps event the peer REJECTED -- it
   * only marks the sticky event pending -- so check negotiation explicitly.
   * Without this a caps mismatch surfaces later as a confusing flow error from
   * the chain function instead of a failure here. */
  g_assert_true (gst_pad_has_current_caps (peer));

  GstSegment seg;
  gst_segment_init (&seg, GST_FORMAT_TIME);
  g_assert_true (gst_pad_push_event (src, gst_event_new_segment (&seg)));
  gst_object_unref (peer);
  return src;
}

/* A keyframe (no DELTA_UNIT flag) at `pts`. Payload content is irrelevant on
 * the stream-format=avc path -- the avcC comes from the caps. */
static GstBuffer *
keyframe (GstClockTime pts, gsize len)
{
  GstBuffer *b = gst_buffer_new_allocate (NULL, len, NULL);
  gst_buffer_memset (b, 0, 0, len);
  GST_BUFFER_PTS (b) = pts;
  GST_BUFFER_DTS (b) = pts;
  return b;
}

static GstElement *
new_sink (void)
{
  GstElement *sink = g_object_new (GST_TYPE_MOQ_SINK,
      /* Never dialled: moq_endpoint_connect is wrapped. */
      "host", "127.0.0.1", "port", 1,
      /* No clock waits -- the barrier is what this test times, nothing else. */
      "sync", FALSE,
      NULL);
  g_assert_nonnull (sink);
  return sink;
}

/* -- the blocked audio push ---------------------------------------------- */

typedef struct
{
  GstPad *src;
  GstBuffer *buffer;
  GstFlowReturn ret;
  GMutex lock;
  GCond cond;
  gboolean entered;
  gboolean done;
} PushJob;

static gpointer
push_thread (gpointer data)
{
  PushJob *j = data;
  g_mutex_lock (&j->lock);
  j->entered = TRUE;
  g_cond_broadcast (&j->cond);
  g_mutex_unlock (&j->lock);

  GstFlowReturn ret = gst_pad_push (j->src, j->buffer);

  g_mutex_lock (&j->lock);
  j->ret = ret;
  j->done = TRUE;
  g_cond_broadcast (&j->cond);
  g_mutex_unlock (&j->lock);
  return NULL;
}

/* -- tests ---------------------------------------------------------------- */

/* Audio's first buffer arrives first and must NOT open the session: the
 * catalog cannot be published until the video pad has produced its track too.
 */
static void
test_initial_catalog_complete (void)
{
  log_reset ();
  GstElement *sink = new_sink ();

  /* PAUSED first: the element activates its sink pads on READY->PAUSED, and an
   * inactive pad rejects the sticky events open_src_pad pushes. */
  g_assert_cmpint (gst_element_set_state (sink, GST_STATE_PAUSED), !=,
      GST_STATE_CHANGE_FAILURE);

  GstPad *asrc = open_src_pad (sink, "audio", "audio-stream", audio_caps ());
  GstPad *vsrc = open_src_pad (sink, "sink", "video-stream", video_caps ());

  /* Audio first, on its own streaming thread: it must park in the barrier. */
  PushJob job;
  memset (&job, 0, sizeof job);
  job.src = asrc;
  job.buffer = keyframe (0, 64);
  job.ret = GST_FLOW_OK;
  g_mutex_init (&job.lock);
  g_cond_init (&job.cond);
  GThread *t = g_thread_new ("audio-push", push_thread, &job);

  g_mutex_lock (&job.lock);
  while (!job.entered)
    g_cond_wait (&job.cond, &job.lock);
  /* SECONDARY, bounded: the push must still be in flight. A barrier that
   * released on one pad would have completed it by now. */
  gint64 deadline = g_get_monotonic_time () + 250 * G_TIME_SPAN_MILLISECOND;
  while (!job.done && g_cond_wait_until (&job.cond, &job.lock, deadline))
    ;
  gboolean released_early = job.done;
  g_mutex_unlock (&job.lock);
  g_assert_false (released_early);

  /* PRIMARY: everything the element does from here on is logged after this
   * marker, so an early connect is an ordering violation, not a race. */
  log_add (OP_MARK, "push-video");
  g_assert_cmpint (gst_pad_push (vsrc, keyframe (0, 64)), ==, GST_FLOW_OK);

  g_thread_join (t);
  g_assert_cmpint (job.ret, ==, GST_FLOW_OK);

  gint mark = log_find (OP_MARK, "push-video");
  gint connect = log_find (OP_CONNECT, NULL);
  gint attach = log_find (OP_ATTACH, NULL);
  g_assert_cmpint (mark, >=, 0);
  g_assert_cmpint (connect, >, mark);
  g_assert_cmpint (attach, >, connect);

  /* One session, both tracks, and both added before anything is written: the
   * session cannot reach ESTABLISHED with a partial track set. */
  g_assert_cmpuint (log_count (OP_CONNECT), ==, 1);
  g_assert_cmpuint (log_count (OP_ATTACH), ==, 1);
  g_assert_cmpuint (log_count (OP_ADD_TRACK), ==, 2);

  gint audio_track = log_find (OP_ADD_TRACK, "audio");
  gint video_track = log_find (OP_ADD_TRACK, "video");
  g_assert_cmpint (audio_track, >, attach);
  g_assert_cmpint (video_track, >, attach);

  gint first_write = log_find (OP_WRITE, NULL);
  g_assert_cmpint (first_write, >, audio_track);
  g_assert_cmpint (first_write, >, video_track);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_pad_set_active (asrc, FALSE);
  gst_pad_set_active (vsrc, FALSE);
  gst_object_unref (asrc);
  gst_object_unref (vsrc);
  gst_object_unref (sink);
  g_mutex_clear (&job.lock);
  g_cond_clear (&job.cond);
}

/* The always pads exist even when a pipeline feeds only one of them. An
 * unlinked pad must not hold the barrier shut -- and this is what keeps the
 * ordering assertion above honest: here CONNECT *does* land on the first
 * buffer, so the oracle can observe an early connect when one is correct.
 */
static void
test_unlinked_pad_excluded (void)
{
  log_reset ();
  GstElement *sink = new_sink ();

  g_assert_cmpint (gst_element_set_state (sink, GST_STATE_PAUSED), !=,
      GST_STATE_CHANGE_FAILURE);

  /* "audio" deliberately left unlinked. */
  GstPad *vsrc = open_src_pad (sink, "sink", "video-stream", video_caps ());

  log_add (OP_MARK, "push-video");
  g_assert_cmpint (gst_pad_push (vsrc, keyframe (0, 64)), ==, GST_FLOW_OK);

  gint mark = log_find (OP_MARK, "push-video");
  gint connect = log_find (OP_CONNECT, NULL);
  g_assert_cmpint (connect, >, mark);
  g_assert_cmpuint (log_count (OP_CONNECT), ==, 1);
  g_assert_cmpuint (log_count (OP_ADD_TRACK), ==, 1);
  g_assert_cmpint (log_find (OP_ADD_TRACK, "video"), >=, 0);
  g_assert_cmpint (log_find (OP_ADD_TRACK, "audio"), ==, -1);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_pad_set_active (vsrc, FALSE);
  gst_object_unref (vsrc);
  gst_object_unref (sink);
}

int
main (int argc, char **argv)
{
  gst_init (&argc, &argv);
  g_test_init (&argc, &argv, NULL);
  g_mutex_init (&log_lock);
  log_reset ();
  g_test_add_func ("/barrier/initial-catalog-complete",
      test_initial_catalog_complete);
  g_test_add_func ("/barrier/unlinked-pad-excluded", test_unlinked_pad_excluded);
  return g_test_run ();
}
