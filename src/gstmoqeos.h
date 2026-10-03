/* Private consumer helpers. Callers serialize sender mutations and keep the
 * endpoint alive. No endpoint tasks are posted by this plugin. */
#ifndef GST_MOQ_EOS_H
#define GST_MOQ_EOS_H

#include <glib.h>
#include <moq/endpoint.h>
#include <moq/media_sender.h>

#define EOS_DRAIN_TIMEOUT_US (3 * G_USEC_PER_SEC)

static guint64
gst_moq_eos_remaining (gint64 deadline)
{
  gint64 remaining = deadline - g_get_monotonic_time ();
  return remaining > 0 ? (guint64) remaining : 0;
}

static moq_result_t
gst_moq_eos_end (moq_endpoint_t *ep, moq_media_sender_t *sender,
    moq_media_track_t *track, gint64 deadline)
{
  for (;;) {
    if (moq_media_sender_is_fatal (sender))
      return MOQ_ERR_CLOSED;
    if (!gst_moq_eos_remaining (deadline))
      return MOQ_DONE;
    moq_result_t rc = moq_media_sender_end_track (sender, track);
    if (rc != MOQ_ERR_WOULD_BLOCK)
      return rc;
    guint64 remaining = gst_moq_eos_remaining (deadline);
    if (!remaining)
      return MOQ_DONE;
    /* Endpoint activity, rather than sender write readiness, avoids a
     * level-triggered spin when an end marker needs more queue capacity. */
    rc = moq_endpoint_wait (ep, MIN (remaining, 5000));
    if (rc != MOQ_OK && rc != MOQ_DONE)
      return rc;
    if (!gst_moq_eos_remaining (deadline))
      return MOQ_DONE;
  }
}

static moq_result_t
gst_moq_eos_drain (moq_endpoint_t *ep, gint64 deadline)
{
  guint64 remaining = gst_moq_eos_remaining (deadline);
  return remaining ? moq_endpoint_drain (ep, remaining) : MOQ_DONE;
}

#endif
