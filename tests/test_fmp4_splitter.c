#include "gstmoqfmp4.h"
#include <string.h>

/* Build one box: 32-bit size, fourcc, payload. */
static GByteArray *
box (const gchar *fourcc, const guint8 *payload, gsize len)
{
  GByteArray *b = g_byte_array_new ();
  guint32 size = 8 + len;
  guint8 hdr[8] = { size >> 24, size >> 16, size >> 8, size,
    fourcc[0], fourcc[1], fourcc[2], fourcc[3] };
  g_byte_array_append (b, hdr, 8);
  if (len)
    g_byte_array_append (b, payload, len);
  return b;
}

static void
cat (GByteArray *dst, GByteArray *src)
{
  g_byte_array_append (dst, src->data, src->len);
  g_byte_array_unref (src);
}

static GByteArray *
sample_stream (void)
{
  static const guint8 p1[4] = { 1, 1, 1, 1 };
  static const guint8 p2[6] = { 2, 2, 2, 2, 2, 2 };
  GByteArray *s = g_byte_array_new ();
  cat (s, box ("ftyp", p1, 4));
  cat (s, box ("moov", p2, 6));
  cat (s, box ("styp", p1, 4));        /* skipped */
  cat (s, box ("moof", p1, 4));
  cat (s, box ("mdat", p2, 6));
  cat (s, box ("sidx", p1, 4));        /* skipped */
  cat (s, box ("moof", p2, 6));
  cat (s, box ("mdat", p1, 4));
  return s;
}

static void
test_splits_whole_stream (void)
{
  GstMoqFmp4Splitter s;
  GError *err = NULL;
  GByteArray *in = sample_stream ();

  gst_moq_fmp4_splitter_init (&s, 1 << 20);
  g_assert_true (gst_moq_fmp4_splitter_push (&s, in->data, in->len, &err));
  g_assert_no_error (err);

  GstMoqFmp4Unit *u = gst_moq_fmp4_splitter_pull (&s);
  g_assert_nonnull (u);
  g_assert_cmpint (u->kind, ==, GST_MOQ_FMP4_INIT);
  g_assert_cmpuint (g_bytes_get_size (u->data), ==, 12 + 14);
  gst_moq_fmp4_unit_free (u);

  u = gst_moq_fmp4_splitter_pull (&s);
  g_assert_nonnull (u);
  g_assert_cmpint (u->kind, ==, GST_MOQ_FMP4_FRAGMENT);
  g_assert_cmpuint (g_bytes_get_size (u->data), ==, 12 + 14);
  g_assert_cmpint (memcmp (g_bytes_get_data (u->data, NULL) + 4, "moof", 4), ==, 0);
  gst_moq_fmp4_unit_free (u);

  u = gst_moq_fmp4_splitter_pull (&s);
  g_assert_nonnull (u);
  g_assert_cmpint (u->kind, ==, GST_MOQ_FMP4_FRAGMENT);
  g_assert_cmpuint (g_bytes_get_size (u->data), ==, 14 + 12);
  gst_moq_fmp4_unit_free (u);

  g_assert_null (gst_moq_fmp4_splitter_pull (&s));
  g_assert_cmpuint (gst_moq_fmp4_splitter_skipped (&s), ==, 2);

  gst_moq_fmp4_splitter_clear (&s);
  g_byte_array_unref (in);
}

/* Same stream, fed one byte at a time and in 3-byte chunks. */
static void
test_splits_in_odd_chunks (void)
{
  static const gsize chunk_sizes[] = { 1, 3, 7 };
  for (gsize c = 0; c < G_N_ELEMENTS (chunk_sizes); c++) {
    GstMoqFmp4Splitter s;
    GByteArray *in = sample_stream ();
    guint kinds[3] = { 0 };
    guint n = 0;

    gst_moq_fmp4_splitter_init (&s, 1 << 20);
    for (gsize off = 0; off < in->len; off += chunk_sizes[c]) {
      gsize len = MIN (chunk_sizes[c], in->len - off);
      g_assert_true (gst_moq_fmp4_splitter_push (&s, in->data + off, len, NULL));
      GstMoqFmp4Unit *u;
      while ((u = gst_moq_fmp4_splitter_pull (&s))) {
        g_assert_cmpuint (n, <, 3);
        kinds[n++] = u->kind;
        gst_moq_fmp4_unit_free (u);
      }
    }
    g_assert_cmpuint (n, ==, 3);
    g_assert_cmpint (kinds[0], ==, GST_MOQ_FMP4_INIT);
    g_assert_cmpint (kinds[1], ==, GST_MOQ_FMP4_FRAGMENT);
    g_assert_cmpint (kinds[2], ==, GST_MOQ_FMP4_FRAGMENT);
    gst_moq_fmp4_splitter_clear (&s);
    g_byte_array_unref (in);
  }
}

static void
test_largesize_box (void)
{
  /* size == 1 with a 64-bit largesize of 20: 16-byte header + 4 payload. */
  static const guint8 p[4] = { 9, 9, 9, 9 };
  GstMoqFmp4Splitter s;
  GByteArray *in = g_byte_array_new ();
  guint8 hdr[16] = { 0, 0, 0, 1, 'f', 't', 'y', 'p', 0, 0, 0, 0, 0, 0, 0, 20 };
  g_byte_array_append (in, hdr, 16);
  g_byte_array_append (in, p, 4);
  cat (in, box ("moov", p, 4));
  cat (in, box ("moof", p, 4));
  cat (in, box ("mdat", p, 4));

  gst_moq_fmp4_splitter_init (&s, 1 << 20);
  g_assert_true (gst_moq_fmp4_splitter_push (&s, in->data, in->len, NULL));
  GstMoqFmp4Unit *u = gst_moq_fmp4_splitter_pull (&s);
  g_assert_nonnull (u);
  g_assert_cmpint (u->kind, ==, GST_MOQ_FMP4_INIT);
  g_assert_cmpuint (g_bytes_get_size (u->data), ==, 20 + 12);
  gst_moq_fmp4_unit_free (u);
  gst_moq_fmp4_splitter_clear (&s);
  g_byte_array_unref (in);
}

static void
test_rejects_bad_sizes (void)
{
  GstMoqFmp4Splitter s;
  GError *err = NULL;
  const guint8 zero_size[8] = { 0, 0, 0, 0, 'f', 't', 'y', 'p' };
  const guint8 tiny[8] = { 0, 0, 0, 4, 'f', 't', 'y', 'p' };
  const guint8 huge[8] = { 0, 1, 0, 0, 'm', 'd', 'a', 't' }; /* 65536 */

  gst_moq_fmp4_splitter_init (&s, 1024);
  g_assert_false (gst_moq_fmp4_splitter_push (&s, zero_size, 8, &err));
  g_assert_error (err, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_BOX_SIZE);
  g_clear_error (&err);
  gst_moq_fmp4_splitter_clear (&s);

  gst_moq_fmp4_splitter_init (&s, 1024);
  g_assert_false (gst_moq_fmp4_splitter_push (&s, tiny, 8, &err));
  g_assert_error (err, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_BOX_SIZE);
  g_clear_error (&err);
  gst_moq_fmp4_splitter_clear (&s);

  gst_moq_fmp4_splitter_init (&s, 1024);
  g_assert_false (gst_moq_fmp4_splitter_push (&s, huge, 8, &err));
  g_assert_error (err, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_BOX_SIZE);
  g_clear_error (&err);
  gst_moq_fmp4_splitter_clear (&s);
}

static void
test_rejects_bad_order (void)
{
  static const guint8 p[4] = { 0, 0, 0, 0 };
  GstMoqFmp4Splitter s;
  GError *err = NULL;

  /* moof before any moov */
  GByteArray *in = box ("moof", p, 4);
  gst_moq_fmp4_splitter_init (&s, 1024);
  g_assert_false (gst_moq_fmp4_splitter_push (&s, in->data, in->len, &err));
  g_assert_error (err, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_ORDER);
  g_clear_error (&err);
  gst_moq_fmp4_splitter_clear (&s);
  g_byte_array_unref (in);

  /* mdat without moof */
  in = g_byte_array_new ();
  cat (in, box ("ftyp", p, 4));
  cat (in, box ("moov", p, 4));
  cat (in, box ("mdat", p, 4));
  gst_moq_fmp4_splitter_init (&s, 1024);
  g_assert_false (gst_moq_fmp4_splitter_push (&s, in->data, in->len, &err));
  g_assert_error (err, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_ORDER);
  g_clear_error (&err);
  gst_moq_fmp4_splitter_clear (&s);
  g_byte_array_unref (in);

  /* a box between moof and mdat */
  in = g_byte_array_new ();
  cat (in, box ("ftyp", p, 4));
  cat (in, box ("moov", p, 4));
  cat (in, box ("moof", p, 4));
  cat (in, box ("free", p, 4));
  gst_moq_fmp4_splitter_init (&s, 1024);
  g_assert_false (gst_moq_fmp4_splitter_push (&s, in->data, in->len, &err));
  g_assert_error (err, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_ORDER);
  g_clear_error (&err);
  gst_moq_fmp4_splitter_clear (&s);
  g_byte_array_unref (in);
}

/* A second ftyp+moov after fragments starts a new init segment. */
static void
test_reinit (void)
{
  static const guint8 p[4] = { 0, 0, 0, 0 };
  GstMoqFmp4Splitter s;
  GByteArray *in = g_byte_array_new ();
  cat (in, box ("ftyp", p, 4));
  cat (in, box ("moov", p, 4));
  cat (in, box ("moof", p, 4));
  cat (in, box ("mdat", p, 4));
  cat (in, box ("ftyp", p, 4));
  cat (in, box ("moov", p, 4));
  cat (in, box ("moof", p, 4));
  cat (in, box ("mdat", p, 4));

  gst_moq_fmp4_splitter_init (&s, 1024);
  g_assert_true (gst_moq_fmp4_splitter_push (&s, in->data, in->len, NULL));
  GstMoqFmp4Kind expect[4] = { GST_MOQ_FMP4_INIT, GST_MOQ_FMP4_FRAGMENT,
    GST_MOQ_FMP4_INIT, GST_MOQ_FMP4_FRAGMENT };
  for (guint i = 0; i < 4; i++) {
    GstMoqFmp4Unit *u = gst_moq_fmp4_splitter_pull (&s);
    g_assert_nonnull (u);
    g_assert_cmpint (u->kind, ==, expect[i]);
    gst_moq_fmp4_unit_free (u);
  }
  g_assert_null (gst_moq_fmp4_splitter_pull (&s));
  gst_moq_fmp4_splitter_clear (&s);
  g_byte_array_unref (in);
}

int
main (int argc, char **argv)
{
  g_test_init (&argc, &argv, NULL);
  g_test_add_func ("/fmp4/whole-stream", test_splits_whole_stream);
  g_test_add_func ("/fmp4/odd-chunks", test_splits_in_odd_chunks);
  g_test_add_func ("/fmp4/largesize", test_largesize_box);
  g_test_add_func ("/fmp4/bad-sizes", test_rejects_bad_sizes);
  g_test_add_func ("/fmp4/bad-order", test_rejects_bad_order);
  g_test_add_func ("/fmp4/reinit", test_reinit);
  return g_test_run ();
}
