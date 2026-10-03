/* Offline fixture inventory through the plugin splitter and exact SDK parser. */
#include "gstmoqfmp4.h"
#include "gstmoqcodec.h"
#include <moq/cmaf.h>
#include <stdio.h>

int
main (int argc, char **argv)
{
  if (argc != 2)
    return 2;
  gchar *data = NULL;
  gsize length = 0;
  GError *error = NULL;
  if (!g_file_get_contents (argv[1], &data, &length, &error)) {
    g_printerr ("%s\n", error->message);
    g_error_free (error);
    return 1;
  }
  GstMoqFmp4Splitter splitter;
  gst_moq_fmp4_splitter_init (&splitter, 16u << 20);
  for (gsize offset = 0; offset < length; offset += 7) {
    if (!gst_moq_fmp4_splitter_push (&splitter, (guint8 *) data + offset,
            MIN ((gsize) 7, length - offset), &error)) {
      g_printerr ("%s\n", error->message);
      g_error_free (error);
      gst_moq_fmp4_splitter_clear (&splitter);
      g_free (data);
      return 1;
    }
  }
  g_free (data);
  if (splitter.buf->len || splitter.moof) {
    gst_moq_fmp4_splitter_clear (&splitter);
    return 1;
  }
  guint fragments = 0;
  GBytes *init = NULL;
  moq_cmaf_init_info_t info;
  moq_cmaf_init_info_init (&info);
  GstMoqFmp4Unit *unit;
  int result = 0;
  while ((unit = gst_moq_fmp4_splitter_pull (&splitter))) {
    gsize n;
    const guint8 *bytes = g_bytes_get_data (unit->data, &n);
    gchar *hash = g_compute_checksum_for_data (G_CHECKSUM_SHA256, bytes, n);
    if (unit->kind == GST_MOQ_FMP4_INIT) {
      init = g_bytes_ref (unit->data);
      if (moq_cmaf_parse_init ((moq_bytes_t) { bytes, n }, &info) != MOQ_OK) {
        result = 1;
      } else {
        gchar *codec = gst_moq_codec_string_from_init (&info);
        printf ("{\"kind\":\"init\",\"size\":%zu,\"sha256\":\"%s\","
            "\"timescale\":%u,\"codec\":\"%s\"}\n", n, hash,
            info.timescale, codec ? codec : "");
        g_free (codec);
      }
    } else {
      moq_cmaf_sample_t samples[256];
      moq_cmaf_fragment_info_t fragment;
      moq_cmaf_fragment_info_init (&fragment, samples, G_N_ELEMENTS (samples));
      if (!init || moq_cmaf_parse_fragment ((moq_bytes_t) { bytes, n },
              &fragment) != MOQ_OK || !fragment.has_base_decode_time ||
          fragment.track_id != info.track_id) {
        result = 1;
      } else {
        printf ("{\"kind\":\"fragment\",\"index\":%u,\"size\":%zu,"
            "\"sha256\":\"%s\",\"decode_ticks\":%" G_GUINT64_FORMAT
            ",\"sample_count\":%zu}\n", fragments++, n, hash,
            fragment.base_decode_time, fragment.sample_count);
      }
    }
    g_free (hash);
    gst_moq_fmp4_unit_free (unit);
    if (result)
      break;
  }
  g_clear_pointer (&init, g_bytes_unref);
  gst_moq_fmp4_splitter_clear (&splitter);
  return result || !fragments ? 1 : 0;
}
