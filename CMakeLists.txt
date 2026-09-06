cmake_minimum_required(VERSION 3.20)
project(gst-moq C)

#
# Standalone, out-of-tree GStreamer plugin exposing Media-over-QUIC
# elements (moqsrc / moqsink) backed by the libmoq service tier
# (moq_endpoint_t + moq_media_receiver_t / moq_media_sender_t) over
# WebTransport (picoquic + h3zero, ALPN moqt-16).
#
# Build against the system GStreamer (Homebrew 1.28) via pkg-config and
# the installed libmoq in ../../local-prefix via CMake config packages.
# The picoquic adapters ship no .pc file, so find_package is the way to
# resolve the transitive picoquic -> picohttp -> picotls -> OpenSSL link
# chain that the service tier's transport facades pull in.
#

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Default CMAKE_PREFIX_PATH to the repo's local-prefix so a plain
# `cmake -B build` works without extra flags. Override on the command
# line with -DCMAKE_PREFIX_PATH=... if libmoq is installed elsewhere.
get_filename_component(_repo_root "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
if(NOT CMAKE_PREFIX_PATH)
    set(CMAKE_PREFIX_PATH "${_repo_root}/local-prefix")
endif()
message(STATUS "gst-moq: CMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}")

find_package(PkgConfig REQUIRED)
pkg_check_modules(GST REQUIRED IMPORTED_TARGET
    gstreamer-1.0
    gstreamer-base-1.0)

# The service component pulls in the endpoint + media facades and their
# transport adapters (RAW_QUIC + WebTransport). moq::core / moq::loc /
# moq::cmaf / moq::media-object / moq::msf come unconditionally from
# libmoqTargets.cmake; the elements link the media libs directly because
# the static service archive references them as undefined symbols.
find_package(libmoq REQUIRED COMPONENTS service)

add_library(gstmoq MODULE
    src/gstmoqplugin.c
    src/gstmoqsink.c
    src/gstmoqsrc.c
    src/gstmoqfmp4.c)

target_link_libraries(gstmoq PRIVATE
    PkgConfig::GST
    moq::service
    moq::media-object
    moq::loc
    moq::cmaf
    moq::msf
    moq::core)

# An installed picoquic exports picoquic-core without chaining picoquic-log,
# yet picoquic-core references picoquic_set_qlog from it (and picoquic-log's
# export links back to core). Mirror moq5's source-tree fix-up: hang log off
# core's link interface so CMake sees the static-archive cycle and repeats
# the archives in the right order.
if(TARGET picoquic::picoquic-core AND TARGET picoquic::picoquic-log)
    get_target_property(_pq_core_libs picoquic::picoquic-core
        INTERFACE_LINK_LIBRARIES)
    if(NOT _pq_core_libs OR NOT "picoquic::picoquic-log" IN_LIST _pq_core_libs)
        set_property(TARGET picoquic::picoquic-core APPEND PROPERTY
            INTERFACE_LINK_LIBRARIES picoquic::picoquic-log)
    endif()
endif()

target_compile_options(gstmoq PRIVATE -Wall -Wextra)

# GStreamer discovers plugins by scanning a directory for libgst*.so;
# emit into a plugins/ subdir that GST_PLUGIN_PATH can point at.
set_target_properties(gstmoq PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "gstmoq"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/plugins")

# Unit tests: plain GLib executables, no GStreamer pipeline, no network.
include(CTest)
if(BUILD_TESTING)
    pkg_check_modules(GLIB REQUIRED IMPORTED_TARGET glib-2.0)

    add_executable(test_fmp4_splitter
        tests/test_fmp4_splitter.c
        src/gstmoqfmp4.c)
    target_include_directories(test_fmp4_splitter PRIVATE src)
    target_link_libraries(test_fmp4_splitter PRIVATE PkgConfig::GLIB)
    target_compile_options(test_fmp4_splitter PRIVATE -Wall -Wextra)
    add_test(NAME fmp4_splitter COMMAND test_fmp4_splitter)
endif()
