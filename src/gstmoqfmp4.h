/* src/gstmoqfmp4.h */
#ifndef GST_MOQ_FMP4_H
#define GST_MOQ_FMP4_H

#include <glib.h>

G_BEGIN_DECLS

/*
 * Fragmented-MP4 top-level box splitter.
 *
 * Feed it the byte stream a fragmented muxer produces (mp4mux
 * fragment-duration>0 / cmafmux). It groups `ftyp`+`moov` into one INIT unit
 * and every `moof`+`mdat` pair into one FRAGMENT unit. `styp`, `sidx`, `prft`,
 * `emsg`, `free` and any other top-level box between fragments is skipped.
 */

typedef enum
{
  GST_MOQ_FMP4_INIT = 1,        /* ftyp + moov */
  GST_MOQ_FMP4_FRAGMENT = 2,    /* moof + mdat */
} GstMoqFmp4Kind;

typedef struct
{
  GstMoqFmp4Kind kind;
  GBytes *data;
} GstMoqFmp4Unit;

typedef struct
{
  GByteArray *buf;        /* unparsed input */
  GByteArray *init;       /* accumulating ftyp + moov */
  gboolean    init_emitted;
  GByteArray *moof;       /* pending moof waiting for its mdat, or NULL */
  gsize       max_box;    /* largest single box accepted */
  GQueue      out;        /* GstMoqFmp4Unit * */
  guint       skipped;    /* top-level boxes ignored so far */
} GstMoqFmp4Splitter;

#define GST_MOQ_FMP4_ERROR gst_moq_fmp4_error_quark ()
GQuark gst_moq_fmp4_error_quark (void);

typedef enum
{
  GST_MOQ_FMP4_ERROR_BOX_SIZE,  /* size < header, size == 0, or > max_box */
  GST_MOQ_FMP4_ERROR_ORDER,     /* moof before moov, mdat without moof, ... */
} GstMoqFmp4Error;

void     gst_moq_fmp4_splitter_init  (GstMoqFmp4Splitter *s, gsize max_box);
void     gst_moq_fmp4_splitter_clear (GstMoqFmp4Splitter *s);
gboolean gst_moq_fmp4_splitter_push  (GstMoqFmp4Splitter *s,
                                      const guint8 *data, gsize len,
                                      GError **error);
GstMoqFmp4Unit *gst_moq_fmp4_splitter_pull (GstMoqFmp4Splitter *s);
guint    gst_moq_fmp4_splitter_skipped (const GstMoqFmp4Splitter *s);
void     gst_moq_fmp4_unit_free (GstMoqFmp4Unit *u);

G_END_DECLS

#endif
