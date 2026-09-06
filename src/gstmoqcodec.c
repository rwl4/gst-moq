#include "gstmoqcodec.h"

gchar *
gst_moq_codec_string_from_init (const moq_cmaf_init_info_t *info)
{
  const guint8 *d = info->codec_config.data;
  gsize n = info->codec_config.len;

  switch (info->codec_kind) {
    case MOQ_CMAF_CODEC_AVC:
      /* AVCDecoderConfigurationRecord: version, profile, compat, level */
      if (n < 4 || d[0] != 1)
        return NULL;
      return g_strdup_printf ("avc1.%02x%02x%02x", d[1], d[2], d[3]);
    case MOQ_CMAF_CODEC_AAC: {
      /* AudioSpecificConfig: audioObjectType is the top 5 bits. 31 is the
       * escape form for types >= 32, which this plugin does not publish. */
      if (n < 1)
        return NULL;
      guint aot = d[0] >> 3;
      if (aot == 0 || aot == 31)
        return NULL;
      return g_strdup_printf ("mp4a.40.%u", aot);
    }
    case MOQ_CMAF_CODEC_OPUS:
      return g_strdup ("opus");
    default:
      return NULL;
  }
}
