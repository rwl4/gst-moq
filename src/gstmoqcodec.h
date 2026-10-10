#ifndef GST_MOQ_CODEC_H
#define GST_MOQ_CODEC_H

#include <glib.h>
#include <moq/cmaf.h>

G_BEGIN_DECLS

/* MSF codec string for a parsed CMAF init segment: "avc1.PPCCLL",
 * "mp4a.40.<aot>" or "opus". NULL for codecs this plugin does not publish
 * (HEVC, AV1) or when the codec config is too short to read. */
gchar *gst_moq_codec_string_from_init (const moq_cmaf_init_info_t *info);

/* MSF codec string for a bare AudioSpecificConfig (the `codec_data` an AAC
 * caps carries): "mp4a.40.<audioObjectType>". NULL when the ASC is too short
 * or names an object type this plugin does not publish. */
gchar *gst_moq_codec_string_from_aac_asc (const guint8 *asc, gsize len);

/* samplerate and channel count for the supported AAC-LC ASC subset. Either
 * output may be NULL. Returns FALSE (outputs untouched) unless the ASC is the
 * two-byte AAC-LC form without extensions, core dependency, or an explicit
 * sampling frequency. Caps values must match the ASC when present. */
gboolean gst_moq_codec_aac_asc_params (const guint8 *asc, gsize len,
    gint *rate, gint *channels);

G_END_DECLS

#endif
