/*
 * gst-moq plugin entry point.
 *
 * Registers the moqsrc and moqsink elements. Media-over-QUIC transport
 * is provided by libmoq ("MOQ5") over raw QUIC (picoquic, ALPN moqt-16).
 */
#include <gst/gst.h>
#include <moq/version.h>

#include "gstmoqsink.h"
#include "gstmoqsrc.h"

static gboolean
plugin_init(GstPlugin *plugin)
{
  gboolean ok = TRUE;

  /* Touch a libmoq symbol so the link chain (libmoq + picoquic + picotls
   * + OpenSSL) is exercised at load time and surfaces in diagnostics. */
  GST_INFO_OBJECT(plugin, "gst-moq backed by libmoq %s", moq_version_string());

  ok &= gst_element_register(plugin, "moqsink", GST_RANK_NONE,
      GST_TYPE_MOQ_SINK);
  ok &= gst_element_register(plugin, "moqsrc", GST_RANK_NONE,
      GST_TYPE_MOQ_SRC);

  return ok;
}

#ifndef PACKAGE
#define PACKAGE "gst-moq"
#endif
#ifndef PACKAGE_VERSION
#define PACKAGE_VERSION "0.1.0"
#endif

GST_PLUGIN_DEFINE(
    GST_VERSION_MAJOR,
    GST_VERSION_MINOR,
    moq,
    "Media over QUIC elements backed by MOQ5",
    plugin_init,
    PACKAGE_VERSION,
    "Apache-2.0",
    PACKAGE,
    "https://github.com/openmoq/moq5")
