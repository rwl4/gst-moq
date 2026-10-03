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
    src/gstmoqsrc.c)

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

include(CTest)
if(BUILD_TESTING)
    add_executable(test_src_delivery tests/test_src_delivery.c)
    # Public MOQ5 headers only; the test supplies deterministic service mocks.
    target_include_directories(test_src_delivery PRIVATE
        $<TARGET_PROPERTY:moq::service,INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(test_src_delivery PRIVATE PkgConfig::GST)
    target_compile_options(test_src_delivery PRIVATE -Wall -Wextra)
    add_test(NAME src_delivery COMMAND test_src_delivery)
    set_tests_properties(src_delivery PROPERTIES TIMEOUT 10)
    add_executable(test_sink_ownership tests/test_sink_ownership.c)
    target_include_directories(test_sink_ownership PRIVATE
        $<TARGET_PROPERTY:moq::service,INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(test_sink_ownership PRIVATE PkgConfig::GST moq::core)
    target_compile_options(test_sink_ownership PRIVATE -Wall -Wextra)
    add_test(NAME sink_ownership COMMAND test_sink_ownership)
    set_tests_properties(sink_ownership PROPERTIES TIMEOUT 10)
endif()
