/* moqsink — publish a GStreamer stream over Media-over-QUIC (libmoq). */
#ifndef GST_MOQ_SINK_H
#define GST_MOQ_SINK_H

#include <gst/base/gstbasesink.h>

G_BEGIN_DECLS

#define GST_TYPE_MOQ_SINK (gst_moq_sink_get_type())
G_DECLARE_FINAL_TYPE(GstMoqSink, gst_moq_sink, GST, MOQ_SINK, GstBaseSink)

G_END_DECLS

#endif /* GST_MOQ_SINK_H */
