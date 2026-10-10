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

static void test_asc_params (void)
{
  const guint8 lc[] = {0x11, 0x90}, he[] = {0x2a, 0x10},
      explicit_rate[] = {0x17, 0x90}, pce[] = {0x11, 0x80},
      escape[] = {0xf9, 0x90}, surround[] = {0x11, 0xb8};
  gint rate = 7, channels = 9;
  g_assert_true (gst_moq_codec_aac_asc_params (lc, sizeof lc, &rate, &channels));
  g_assert_cmpint (rate, ==, 48000); g_assert_cmpint (channels, ==, 2);
  g_assert_true (gst_moq_codec_aac_asc_params (surround, sizeof surround, NULL, &channels));
  g_assert_cmpint (channels, ==, 8);
  const guint8 *bad[] = {he, explicit_rate, pce, escape, lc};
  for (guint i = 0; i < G_N_ELEMENTS (bad); i++) {
    rate = 7; channels = 9;
    g_assert_false (gst_moq_codec_aac_asc_params (bad[i], i == 4 ? 1 : 2, &rate, &channels));
    g_assert_cmpint (rate, ==, 7); g_assert_cmpint (channels, ==, 9);
  }
  const guint8 dependent[] = {0x11, 0x92}, extended[] = {0x11, 0x91},
      trailing[] = {0x11, 0x90, 0x56};
  g_assert_false (gst_moq_codec_aac_asc_params (dependent, 2, NULL, NULL));
  g_assert_false (gst_moq_codec_aac_asc_params (extended, 2, NULL, NULL));
  g_assert_false (gst_moq_codec_aac_asc_params (trailing, 3, NULL, NULL));
  g_assert_false (gst_moq_codec_aac_asc_params (NULL, 0, NULL, NULL));
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
  g_test_add_func ("/codec/asc-params", test_asc_params);
  return g_test_run ();
}
