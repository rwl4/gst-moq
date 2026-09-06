#ifndef GST_MOQ_CODEC_H
#define GST_MOQ_CODEC_H

#include <glib.h>
#include <moq/cmaf.h>

G_BEGIN_DECLS

/* MSF codec string for a parsed CMAF init segment: "avc1.PPCCLL",
 * "mp4a.40.<aot>" or "opus". NULL for codecs this plugin does not publish
 * (HEVC, AV1) or when the codec config is too short to read. */
gchar *gst_moq_codec_string_from_init (const moq_cmaf_init_info_t *info);

G_END_DECLS

#endif
