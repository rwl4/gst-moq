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
    case MOQ_CMAF_CODEC_AAC:
      return gst_moq_codec_string_from_aac_asc (d, n);
    case MOQ_CMAF_CODEC_OPUS:
      return g_strdup ("opus");
    default:
      return NULL;
  }
}

gchar *
gst_moq_codec_string_from_aac_asc (const guint8 *asc, gsize len)
{
  /* AudioSpecificConfig: audioObjectType is the top 5 bits. 31 is the escape
   * form for types >= 32, which this plugin does not publish. */
  if (!asc || len < 1)
    return NULL;
  guint aot = asc[0] >> 3;
  if (aot == 0 || aot == 31)
    return NULL;
  return g_strdup_printf ("mp4a.40.%u", aot);
}

gboolean
gst_moq_codec_aac_asc_params (const guint8 *asc, gsize len, gint *rate,
    gint *channels)
{
  /* ISO/IEC 14496-3 1.6.2.1: audioObjectType(5), samplingFrequencyIndex(4),
   * then either a 24-bit explicit frequency (index 15) or straight on to
   * channelConfiguration(4). */
  static const gint freq[13] = { 96000, 88200, 64000, 48000, 44100, 32000,
    24000, 22050, 16000, 12000, 11025, 8000, 7350 };
  if (!asc || len != 2 || (asc[0] >> 3) != 2 || (asc[1] & 0x03) != 0)
    return FALSE;
  guint idx = ((asc[0] & 0x07) << 1) | (asc[1] >> 7);
  if (idx >= G_N_ELEMENTS (freq))
    return FALSE;               /* 13/14 reserved, 15 = explicit; not published */
  guint chan = (asc[1] >> 3) & 0x0f;
  if (chan == 0 || chan > 7)
    return FALSE;               /* 0 = defined in the payload, 8..15 reserved */
  if (rate)
    *rate = freq[idx];
  if (channels)
    *channels = chan == 7 ? 8 : (gint) chan;   /* 7 means 7.1 */
  return TRUE;
}
