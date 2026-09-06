/* src/gstmoqfmp4.c */
#include "gstmoqfmp4.h"

#include <string.h>

G_DEFINE_QUARK (gst-moq-fmp4-error-quark, gst_moq_fmp4_error)

void
gst_moq_fmp4_splitter_init (GstMoqFmp4Splitter *s, gsize max_box)
{
  memset (s, 0, sizeof *s);
  s->buf = g_byte_array_new ();
  s->init = g_byte_array_new ();
  s->max_box = max_box;
  g_queue_init (&s->out);
}

void
gst_moq_fmp4_unit_free (GstMoqFmp4Unit *u)
{
  if (!u)
    return;
  g_bytes_unref (u->data);
  g_free (u);
}

static void
unit_free_cb (gpointer u, gpointer unused)
{
  (void) unused;
  gst_moq_fmp4_unit_free (u);
}

void
gst_moq_fmp4_splitter_clear (GstMoqFmp4Splitter *s)
{
  g_clear_pointer (&s->buf, g_byte_array_unref);
  g_clear_pointer (&s->init, g_byte_array_unref);
  g_clear_pointer (&s->moof, g_byte_array_unref);
  g_queue_foreach (&s->out, unit_free_cb, NULL);
  g_queue_clear (&s->out);
}

guint
gst_moq_fmp4_splitter_skipped (const GstMoqFmp4Splitter *s)
{
  return s->skipped;
}

GstMoqFmp4Unit *
gst_moq_fmp4_splitter_pull (GstMoqFmp4Splitter *s)
{
  return g_queue_pop_head (&s->out);
}

static void
emit (GstMoqFmp4Splitter *s, GstMoqFmp4Kind kind, GByteArray *bytes)
{
  GstMoqFmp4Unit *u = g_new0 (GstMoqFmp4Unit, 1);
  u->kind = kind;
  u->data = g_bytes_new (bytes->data, bytes->len);
  g_queue_push_tail (&s->out, u);
}

static gboolean
is (const guint8 *fourcc, const gchar *name)
{
  return memcmp (fourcc, name, 4) == 0;
}

/* Parse a box header at p. Returns FALSE when more bytes are needed. */
static gboolean
read_header (const guint8 *p, gsize avail, guint64 *size, gsize *hdr)
{
  if (avail < 8)
    return FALSE;
  guint32 s32 = ((guint32) p[0] << 24) | ((guint32) p[1] << 16) |
      ((guint32) p[2] << 8) | p[3];
  if (s32 == 1) {
    if (avail < 16)
      return FALSE;
    guint64 s64 = 0;
    for (int i = 8; i < 16; i++)
      s64 = (s64 << 8) | p[i];
    *size = s64;
    *hdr = 16;
  } else {
    *size = s32;
    *hdr = 8;
  }
  return TRUE;
}

gboolean
gst_moq_fmp4_splitter_push (GstMoqFmp4Splitter *s, const guint8 *data,
    gsize len, GError **error)
{
  g_byte_array_append (s->buf, data, len);

  for (;;) {
    guint64 size;
    gsize hdr;
    const guint8 *p = s->buf->data;

    if (!read_header (p, s->buf->len, &size, &hdr))
      return TRUE;                          /* need more bytes */

    const guint8 *fourcc = p + 4;
    if (size < hdr || size > s->max_box) {
      g_set_error (error, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_BOX_SIZE,
          "box '%.4s' has invalid size %" G_GUINT64_FORMAT " (max %" G_GSIZE_FORMAT ")",
          (const gchar *) fourcc, size, s->max_box);
      return FALSE;
    }
    if (s->buf->len < size)
      return TRUE;                          /* box not complete yet */

    if (is (fourcc, "ftyp") || is (fourcc, "moov")) {
      if (s->moof) {
        g_set_error (error, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_ORDER,
            "box '%.4s' between moof and mdat", (const gchar *) fourcc);
        return FALSE;
      }
      if (s->init_emitted) {                /* a new init segment begins */
        g_byte_array_set_size (s->init, 0);
        s->init_emitted = FALSE;
      }
      g_byte_array_append (s->init, p, size);
    } else if (is (fourcc, "moof")) {
      if (s->moof) {
        g_set_error (error, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_ORDER,
            "moof not followed by mdat");
        return FALSE;
      }
      if (s->init->len == 0 && !s->init_emitted) {
        g_set_error (error, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_ORDER,
            "moof before any moov");
        return FALSE;
      }
      if (!s->init_emitted) {
        emit (s, GST_MOQ_FMP4_INIT, s->init);
        s->init_emitted = TRUE;
      }
      s->moof = g_byte_array_sized_new (size);
      g_byte_array_append (s->moof, p, size);
    } else if (is (fourcc, "mdat")) {
      if (!s->moof) {
        g_set_error (error, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_ORDER,
            "mdat without a preceding moof");
        return FALSE;
      }
      g_byte_array_append (s->moof, p, size);
      emit (s, GST_MOQ_FMP4_FRAGMENT, s->moof);
      g_clear_pointer (&s->moof, g_byte_array_unref);
    } else {
      if (s->moof) {
        g_set_error (error, GST_MOQ_FMP4_ERROR, GST_MOQ_FMP4_ERROR_ORDER,
            "box '%.4s' between moof and mdat", (const gchar *) fourcc);
        return FALSE;
      }
      s->skipped++;
    }

    g_byte_array_remove_range (s->buf, 0, size);
  }
}
