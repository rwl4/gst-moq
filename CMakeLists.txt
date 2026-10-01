cmake_minimum_required(VERSION 3.20)
# Let CMake de-duplicate static archives when the linker supports it, while
# retaining repetitions required by traditional linkers.
if(POLICY CMP0156)
    cmake_policy(SET CMP0156 NEW)
endif()
if(POLICY CMP0179)
    cmake_policy(SET CMP0179 NEW)
endif()
project(gst-moq C)

#
# Standalone, out-of-tree GStreamer plugin exposing Media-over-QUIC
# elements (moqsrc / moqsink) backed by the libmoq service tier
# (moq_endpoint_t + moq_media_receiver_t / moq_media_sender_t) over
# WebTransport (picoquic + h3zero, ALPN moqt-16).
#
# Build against the system GStreamer (Homebrew 1.28) via pkg-config and
# the installed libmoq in ../local-prefix via CMake config packages.
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
get_filename_component(_repo_root "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)
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
    src/gstmoqsinkpad.c
    src/gstmoqsrc.c
    src/gstmoqfmp4.c
    src/gstmoqcodec.c)

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

# Unit tests: no relay and no socket in any of them. The first two are plain
# GLib executables over a single source file; test_track_barrier drives the real
# moqsink element and so links GStreamer, faking libmoq at the link seam.
include(CTest)
if(BUILD_TESTING)
    pkg_check_modules(GLIB REQUIRED IMPORTED_TARGET glib-2.0)
    add_executable(inspect_cmaf_fixture tests/inspect_cmaf_fixture.c
        src/gstmoqfmp4.c src/gstmoqcodec.c)
    target_include_directories(inspect_cmaf_fixture PRIVATE src)
    target_link_libraries(inspect_cmaf_fixture PRIVATE PkgConfig::GLIB moq::cmaf moq::core)
    target_compile_options(inspect_cmaf_fixture PRIVATE -Wall -Wextra)
    add_executable(test_sink_recovery tests/test_sink_recovery.c
        src/gstmoqsinkpad.c src/gstmoqcodec.c src/gstmoqfmp4.c)
    target_include_directories(test_sink_recovery PRIVATE
        $<TARGET_PROPERTY:moq::service,INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(test_sink_recovery PRIVATE PkgConfig::GST moq::cmaf moq::core)
    target_compile_options(test_sink_recovery PRIVATE -Wall -Wextra)
    add_test(NAME sink_recovery COMMAND test_sink_recovery)
    set_tests_properties(sink_recovery PROPERTIES TIMEOUT 10)
    add_executable(test_src_delivery tests/test_src_delivery.c)
    target_include_directories(test_src_delivery PRIVATE
        $<TARGET_PROPERTY:moq::service,INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(test_src_delivery PRIVATE PkgConfig::GST)
    target_compile_options(test_src_delivery PRIVATE -Wall -Wextra)
    add_test(NAME src_delivery COMMAND test_src_delivery)
    set_tests_properties(src_delivery PROPERTIES TIMEOUT 10)

    add_executable(test_fmp4_splitter
        tests/test_fmp4_splitter.c
        src/gstmoqfmp4.c)
    target_include_directories(test_fmp4_splitter PRIVATE src)
    target_link_libraries(test_fmp4_splitter PRIVATE PkgConfig::GLIB)
    target_compile_options(test_fmp4_splitter PRIVATE -Wall -Wextra)
    add_test(NAME fmp4_splitter COMMAND test_fmp4_splitter)

    add_executable(test_codec_string
        tests/test_codec_string.c
        src/gstmoqcodec.c)
    target_include_directories(test_codec_string PRIVATE src)
    target_link_libraries(test_codec_string PRIVATE PkgConfig::GLIB moq::cmaf moq::core)
    target_compile_options(test_codec_string PRIVATE -Wall -Wextra)
    add_test(NAME codec_string COMMAND test_codec_string)

    # The track barrier drives the real moqsink element, so this one needs
    # GStreamer -- but still no relay and no socket: the libmoq entry points the
    # barrier reaches are replaced at link time with --wrap, and the test
    # supplies the handles. Keep the wrap list in sync with the libmoq calls on
    # the attach/add_track/write path in gstmoqsink.c.
    #
    # --wrap is a GNU ld / lld feature; Apple's linker has no equivalent, so the
    # target is skipped where the flag does not link rather than breaking the
    # build. The barrier itself is platform-independent, so losing the test on
    # macOS costs coverage, not correctness.
    include(CheckLinkerFlag)
    check_linker_flag(C "-Wl,--wrap=moq_endpoint_connect" HAVE_LD_WRAP)

    if(HAVE_LD_WRAP)
    add_executable(test_track_barrier
        tests/test_track_barrier.c
        src/gstmoqsink.c
        src/gstmoqsinkpad.c
        src/gstmoqfmp4.c
        src/gstmoqcodec.c)
    target_include_directories(test_track_barrier PRIVATE src)
    target_link_libraries(test_track_barrier PRIVATE
        PkgConfig::GST
        PkgConfig::GLIB
        moq::service
        moq::media-object
        moq::loc
        moq::cmaf
        moq::msf
        moq::core)
    target_compile_options(test_track_barrier PRIVATE -Wall -Wextra)
    target_link_options(test_track_barrier PRIVATE
        -Wl,--wrap=moq_endpoint_connect
        -Wl,--wrap=moq_endpoint_stop
        -Wl,--wrap=moq_endpoint_destroy
        -Wl,--wrap=moq_endpoint_set_interrupted
        -Wl,--wrap=moq_endpoint_drain
        -Wl,--wrap=moq_media_sender_attach
        -Wl,--wrap=moq_media_sender_add_track
        -Wl,--wrap=moq_media_sender_write
        -Wl,--wrap=moq_media_sender_end_track
        -Wl,--wrap=moq_media_sender_is_fatal
        -Wl,--wrap=moq_media_sender_fatal_code
        -Wl,--wrap=moq_media_sender_get_stats
        -Wl,--wrap=moq_media_sender_destroy)
    add_test(NAME track_barrier COMMAND test_track_barrier)
    else()
        message(STATUS "gst-moq: linker has no --wrap; skipping track_barrier test")
    endif()

    add_test(NAME smoke
        COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/tests/smoke.sh ${CMAKE_BINARY_DIR}/plugins)
endif()
