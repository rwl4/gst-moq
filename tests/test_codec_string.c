/* tests/test_codec_string.c */
#include "gstmoqcodec.h"
#include <moq/cmaf.h>

static void
test_avc (void)
{
  /* avcC: version 1, profile 0x42, compat 0xc0, level 0x1e, ... */
  static const guint8 avcc[] = { 1, 0x42, 0xc0, 0x1e, 0xff, 0xe1 };
  moq_cmaf_init_info_t i;
  moq_cmaf_init_info_init (&i);
  i.codec_kind = MOQ_CMAF_CODEC_AVC;
  i.codec_config.data = avcc;
  i.codec_config.len = sizeof avcc;
  gchar *s = gst_moq_codec_string_from_init (&i);
  g_assert_cmpstr (s, ==, "avc1.42c01e");
  g_free (s);
}

static void
test_aac_lc (void)
{
  /* AudioSpecificConfig: AOT 2 (AAC-LC) = 00010 in the top 5 bits -> 0x12 0x10 */
  static const guint8 asc[] = { 0x12, 0x10 };
  moq_cmaf_init_info_t i;
  moq_cmaf_init_info_init (&i);
  i.codec_kind = MOQ_CMAF_CODEC_AAC;
  i.codec_config.data = asc;
  i.codec_config.len = sizeof asc;
  gchar *s = gst_moq_codec_string_from_init (&i);
  g_assert_cmpstr (s, ==, "mp4a.40.2");
  g_free (s);
}

static void
test_he_aac (void)
{
  /* AOT 5 (SBR) = 00101 -> 0x2a */
  static const guint8 asc[] = { 0x2a, 0x10 };
  moq_cmaf_init_info_t i;
  moq_cmaf_init_info_init (&i);
  i.codec_kind = MOQ_CMAF_CODEC_AAC;
  i.codec_config.data = asc;
  i.codec_config.len = sizeof asc;
  gchar *s = gst_moq_codec_string_from_init (&i);
  g_assert_cmpstr (s, ==, "mp4a.40.5");
  g_free (s);
}

static void
test_opus (void)
{
  moq_cmaf_init_info_t i;
  moq_cmaf_init_info_init (&i);
  i.codec_kind = MOQ_CMAF_CODEC_OPUS;
  gchar *s = gst_moq_codec_string_from_init (&i);
  g_assert_cmpstr (s, ==, "opus");
  g_free (s);
}

static void
test_unsupported (void)
{
  moq_cmaf_init_info_t i;
  moq_cmaf_init_info_init (&i);
  i.codec_kind = MOQ_CMAF_CODEC_HEVC;
  g_assert_null (gst_moq_codec_string_from_init (&i));

  /* AVC with a truncated avcC */
  static const guint8 short_avcc[] = { 1, 0x42 };
  i.codec_kind = MOQ_CMAF_CODEC_AVC;
  i.codec_config.data = short_avcc;
  i.codec_config.len = sizeof short_avcc;
  g_assert_null (gst_moq_codec_string_from_init (&i));
}

int
main (int argc, char **argv)
{
  g_test_init (&argc, &argv, NULL);
  g_test_add_func ("/codec/avc", test_avc);
  g_test_add_func ("/codec/aac-lc", test_aac_lc);
  g_test_add_func ("/codec/he-aac", test_he_aac);
  g_test_add_func ("/codec/opus", test_opus);
  g_test_add_func ("/codec/unsupported", test_unsupported);
  return g_test_run ();
}
