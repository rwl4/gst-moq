/* moqsrc — subscribe to a Media-over-QUIC track and push buffers (libmoq). */
#ifndef GST_MOQ_SRC_H
#define GST_MOQ_SRC_H

#include <gst/base/gstpushsrc.h>

G_BEGIN_DECLS

#define GST_TYPE_MOQ_SRC (gst_moq_src_get_type())
G_DECLARE_FINAL_TYPE(GstMoqSrc, gst_moq_src, GST, MOQ_SRC, GstPushSrc)

G_END_DECLS

#endif /* GST_MOQ_SRC_H */
