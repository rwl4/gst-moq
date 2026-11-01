//! Exercise the source's actual create vmethod with deterministic public-API
//! mocks. No relay, network thread, sleeps, or production test hooks.
//!
//! Port of tests/test_src_delivery.c.

use gst::prelude::*;
use gst::subclass::prelude::*;

use super::MoqSrc;
use crate::service::fake::{control, suite, DescData, Event, ObjectData, Receiver};
use crate::service::Track;
use moq5::{Error, Packaging, TerminalReason, TrackEventKind};

/// Fixture track handles. The double indexes its own state by them, so any
/// distinct values do.
const VIDEO: Track = Track(0);
const AUDIO: Track = Track(1);
const VIDEO_PAYLOAD: [u8; 5] = [0, 0, 1, 0x65, 0x42];
const AUDIO_PAYLOAD: [u8; 2] = [0xf8, 0xff];

/// A source holding the double's receiver, past discovery and negotiation.
fn make_source() -> (MoqSrc, crate::test_support::NullCleanup) {
    let src = gst::glib::Object::new::<MoqSrc>();
    let cleanup = crate::test_support::NullCleanup::new(&src);
    src.imp().settings.lock().unwrap().caps = Some(gst::Caps::new_any());
    let mut st = src.imp().lock_state();
    st.receiver = Some(Receiver::fixture());
    st.caps_pushed = true; // Negotiation is exercised separately by pipelines.
    st.discover_deadline = i64::MAX;
    drop(st);
    (src, cleanup)
}

fn queue_object(track: Track, payload: &[u8]) {
    control().objects.push(ObjectData {
        track: Some(track),
        is_keyframe: true,
        payload: payload.to_vec(),
        ..Default::default()
    });
}

fn create(src: &MoqSrc) -> Result<gst::Buffer, gst::FlowError> {
    src.imp().create_buffer()
}

fn added(track: Track, desc: DescData) -> Event {
    Event {
        kind: TrackEventKind::Added,
        raw_kind: 0,
        local_result: None,
        delivery_origin: None,
        track: Some(track),
        desc: Some(desc),
    }
}

fn terminal(kind: TrackEventKind, track: Track) -> Event {
    Event {
        kind,
        raw_kind: 0,
        local_result: None,
        delivery_origin: None,
        track: Some(track),
        desc: None,
    }
}

fn desc(name: &str, codec: &str) -> DescData {
    DescData {
        name: Some(name.as_bytes().to_vec()),
        codec: codec.as_bytes().to_vec(),
        ..Default::default()
    }
}

fn contents(buf: &gst::Buffer) -> Vec<u8> {
    buf.map_readable().unwrap().as_slice().to_vec()
}

/// Read owned observations before an assertion can unwind the CONTROL mutex.
fn observe_control<T>(read: impl FnOnce(&crate::service::fake::Control) -> T) -> T {
    let state = control();
    read(&state)
}

// -- cases -----------------------------------------------------------------------

#[test]
fn missing_track() {
    let _suite = suite();
    let (src, _null_source) = make_source();
    queue_object(AUDIO, &AUDIO_PAYLOAD);
    control().empty_result = Some(Error::CLOSED);
    assert_eq!(create(&src).err(), Some(gst::FlowError::Eos));
    assert_eq!(observe_control(|c| c.cleaned), 1);
}

fn selected_end_drains_media(event_kind: TrackEventKind) {
    let _suite = suite();
    let (src, _null_source) = make_source();
    let video = desc("video", "avc1.42c01e");
    let audio = DescData {
        name: Some(b"audio".to_vec()),
        ..Default::default()
    };
    control().events = vec![
        added(AUDIO, audio),
        added(VIDEO, video),
        terminal(event_kind, VIDEO),
    ];
    queue_object(AUDIO, &AUDIO_PAYLOAD);
    queue_object(VIDEO, &VIDEO_PAYLOAD);
    queue_object(VIDEO, &VIDEO_PAYLOAD);

    for _ in 0..2 {
        let out = create(&src).expect("a video buffer");
        assert_eq!(contents(&out), VIDEO_PAYLOAD);
    }
    assert_eq!(create(&src).err(), Some(gst::FlowError::Eos));
    let (cleaned, waits, subscriptions, track) =
        observe_control(|c| (c.cleaned, c.waits, c.subscriptions, c.subscribed_track));
    assert_eq!(cleaned, 3);
    assert_eq!(waits, 0);
    assert_eq!(subscriptions, 1);
    assert_eq!(track, Some(VIDEO));
}

#[test]
fn selected_end_drains() {
    selected_end_drains_media(TrackEventKind::Ended);
}

#[test]
fn selected_removal_drains() {
    selected_end_drains_media(TrackEventKind::Removed);
}

#[test]
fn unrelated_end() {
    let _suite = suite();
    let (src, _null_source) = make_source();
    src.imp().lock_state().want_track = Some(VIDEO);
    control().events = vec![terminal(TrackEventKind::Ended, AUDIO)];
    assert_eq!(create(&src).err(), Some(gst::FlowError::Flushing));
    assert_eq!(observe_control(|c| c.waits), 1);
}

#[test]
fn discovery_deadline() {
    let _suite = suite();
    let (src, _null_source) = make_source();
    let bus = gst::Bus::new();
    src.set_bus(Some(&bus));
    src.imp().lock_state().discover_deadline = gst::glib::monotonic_time() - 1;
    assert_eq!(create(&src).err(), Some(gst::FlowError::Error));
    let msg = bus
        .pop_filtered(&[gst::MessageType::Error])
        .expect("error message");
    let gst::MessageView::Error(err) = msg.view() else {
        unreachable!()
    };
    assert!(err.error().matches(gst::ResourceError::NotFound));
    src.set_bus(None);
}

#[test]
fn subscription_failure() {
    let _suite = suite();
    let (src, _null_source) = make_source();
    control().subscribe_result = Some(Error::CLOSED);
    let video = desc("video", "avc1.42c01e");
    control().events = vec![added(VIDEO, video)];
    assert_eq!(create(&src).err(), Some(gst::FlowError::Error));
    let observed = src.imp().lock_state().want_track.is_none();
    assert!(observed);
    assert_eq!(observe_control(|c| c.waits), 0);
}

#[test]
fn interrupt_before_timeout() {
    let _suite = suite();
    let (src, _null_source) = make_source();
    src.imp().lock_state().discover_deadline = gst::glib::monotonic_time() - 1;
    control().empty_result = Some(Error::INTERRUPTED);
    assert_eq!(create(&src).err(), Some(gst::FlowError::Flushing));
}

fn cmaf_tail(kind: TrackEventKind) {
    let _suite = suite();
    let (src, _null_source) = make_source();
    static INIT: [u8; 8] = [0, 0, 0, 8, b'f', b't', b'y', b'p'];
    static FRAGMENT: [u8; 8] = [0, 0, 0, 8, b'm', b'o', b'o', b'f'];
    let d = DescData {
        name: Some(b"video".to_vec()),
        packaging: Some(Packaging::Cmaf),
        init_data: INIT.to_vec(),
        ..Default::default()
    };
    let add = added(VIDEO, d);
    // A duplicate announcement must not resubscribe.
    control().events = vec![add.clone(), add, terminal(kind, VIDEO)];
    for _ in 0..2 {
        control().objects.push(ObjectData {
            track: Some(VIDEO),
            is_keyframe: true,
            packaging: Some(Packaging::Cmaf),
            fragment: FRAGMENT.to_vec(),
            ..Default::default()
        });
    }
    for i in 0..3 {
        let out = create(&src).expect("a buffer");
        assert_eq!(contents(&out), if i == 0 { INIT } else { FRAGMENT });
        assert_eq!(out.flags().contains(gst::BufferFlags::HEADER), i == 0);
    }
    assert_eq!(create(&src).err(), Some(gst::FlowError::Eos));
    let (subscriptions, cleaned, waits) =
        observe_control(|c| (c.subscriptions, c.cleaned, c.waits));
    assert_eq!(subscriptions, 1);
    assert_eq!(cleaned, 2);
    assert_eq!(waits, 0);
}

#[test]
fn cmaf_ended_tail() {
    cmaf_tail(TrackEventKind::Ended);
}

#[test]
fn cmaf_removed_tail() {
    cmaf_tail(TrackEventKind::Removed);
}

/// mode 0: classified terminal; 1: the getter fails; 2: unclassified.
fn startup_terminal(mode: u32) {
    let _suite = suite();
    {
        let mut c = control();
        // The receiver refuses to attach, which is what sends the element
        // down the terminal-diagnostic path.
        c.receiver_attach_result = Some(Error::CLOSED);
        c.terminal_getter_fails = mode == 1;
        c.terminal_reason = if mode == 2 {
            TerminalReason::None
        } else {
            TerminalReason::TlsCertificate
        };
        c.terminal_detail = if mode == 2 { 0 } else { 0x130 };
    }
    let src = gst::glib::Object::new::<MoqSrc>();
    let _null_source = crate::test_support::NullCleanup::new(&src);
    let bus = gst::Bus::new();
    src.set_bus(Some(&bus));
    let cat = *super::imp::CAT;
    let previous = cat.threshold();
    let capture = gst::log::add_log_function(|cat, _level, _file, _fn, _line, _obj, msg| {
        let Some(text) = msg.get() else { return };
        if cat.name() == "moqsrc" && text.starts_with("endpoint terminal") {
            let mut c = control();
            // Verify the complete recorded sequence after returning from
            // GStreamer's C logging callback, never unwind through that ABI.
            c.terminal_diagnostic = Some(text.to_string());
            c.startup_calls.push_str("log;");
        }
    });
    cat.set_threshold(gst::DebugLevel::Info);
    let log_cleanup = crate::test_support::LogCleanup::new(capture, cat, previous);

    // Through the class vmethod, as GstBaseSrc calls it: a failed start posts
    // its error message from there.
    let started = unsafe {
        use gst::glib::translate::ToGlibPtr;
        let base: &gst_base::BaseSrc = src.upcast_ref();
        let klass = gst::glib::gobject_ffi::g_type_class_peek(
            gst::glib::translate::IntoGlib::into_glib(MoqSrc::static_type()),
        ) as *const gst_base::ffi::GstBaseSrcClass;
        (*klass).start.unwrap()(base.to_glib_none().0)
    };
    assert_eq!(started, gst::glib::ffi::GFALSE);

    drop(log_cleanup);
    let expected = match mode {
        1 => "endpoint terminal unavailable getter_rc=-2",
        2 => "endpoint terminal unclassified reason=0 detail=0",
        _ => "endpoint terminal reason=3 detail=304",
    };
    {
        let (calls, diagnostic, dialled, receiver_cfg) = observe_control(|c| {
            (
                c.startup_calls.clone(),
                c.terminal_diagnostic.clone(),
                c.dialled.clone(),
                c.receiver_cfg.clone(),
            )
        });
        assert_eq!(calls, "connect;attach;get;log;stop;destroy;");
        assert_eq!(diagnostic.as_deref(), Some(expected));
        // The element dialled exactly the draft it was configured for, with
        // certificate verification left on.
        let dialled = dialled.expect("the element dialled");
        assert!(!dialled.insecure);
        assert_eq!(dialled.draft, 0);
        // The receiver was asked for an explicit, non-empty namespace.
        let rcfg = receiver_cfg.expect("a receiver config");
        assert!(!rcfg.auto_subscribe);
        assert!(rcfg.namespace.parts().count() > 0);
    }
    let observed = src.imp().ep.lock().unwrap().is_none();
    assert!(observed);
    {
        let (no_receiver, started) = {
            let st = src.imp().lock_state();
            (st.receiver.is_none(), st.started)
        };
        assert!(no_receiver);
        assert!(!started);
    }
    let message = bus.pop().expect("an error message");
    let gst::MessageView::Error(err) = message.view() else {
        panic!("expected an error, got {:?}", message.type_())
    };
    assert_eq!(
        err.error().message(),
        "could not attach media receiver (rc=-4)"
    );
    assert!(err.error().matches(gst::ResourceError::OpenRead));
    assert!(bus.pop().is_none()); // No successful EOS or second error.
    src.set_bus(None);
}

#[test]
fn startup_terminal_known() {
    startup_terminal(0);
}

#[test]
fn startup_terminal_getter_failure() {
    startup_terminal(1);
}

#[test]
fn startup_terminal_none() {
    startup_terminal(2);
}

fn loc_aac_caps(explicit_caps: bool) {
    let _suite = suite();
    let (src, _null_source) = make_source();
    src.imp().settings.lock().unwrap().caps = if explicit_caps {
        Some(
            "audio/mpeg,mpegversion=4,stream-format=raw"
                .parse()
                .unwrap(),
        )
    } else {
        None
    };
    static ASC: [u8; 2] = [0x11, 0x90];
    let mut d = desc("video", "mp4a.40.2");
    d.packaging = Some(Packaging::Raw);
    d.samplerate = Some(48000);
    d.channel_config = b"2".to_vec();
    d.init_data = ASC.to_vec();
    control().events = vec![added(VIDEO, d)];
    let (init_len, derived_caps) = {
        let mut st = src.imp().lock_state();
        src.imp().drain_track_events(&mut st);
        (st.want_init.len(), st.derived_caps.clone())
    };
    assert_eq!(init_len, 0);
    if explicit_caps {
        assert!(derived_caps.is_none());
    } else {
        let caps = derived_caps.expect("derived caps");
        let s = caps.structure(0).unwrap();
        assert_eq!(s.get::<i32>("mpegversion").unwrap(), 4);
        assert!(!s.has_field("mpeg-version"));
        assert_eq!(s.get::<i32>("rate").unwrap(), 48000);
        assert_eq!(s.get::<i32>("channels").unwrap(), 2);
        let cd = s.get::<gst::Buffer>("codec_data").unwrap();
        assert_eq!(contents(&cd), ASC);
    }
}

#[test]
fn loc_aac_caps_derived() {
    loc_aac_caps(false);
}

#[test]
fn loc_aac_explicit_caps() {
    loc_aac_caps(true);
}

#[test]
fn selected_local_failures_report_error_without_waiting() {
    for kind in [
        TrackEventKind::LocalFailure,
        TrackEventKind::UpdateLocalFailure,
        TrackEventKind::DeliveryLocalFailure,
    ] {
        let _suite = suite();
        let (src, _null_source) = make_source();
        let mut failure = terminal(kind, VIDEO);
        failure.local_result = Some(Error::NOMEM);
        failure.delivery_origin = Some(moq5::DeliveryOrigin::Live);
        control().events = vec![added(VIDEO, desc("video", "avc1.42c01e")), failure];
        assert_eq!(create(&src).err(), Some(gst::FlowError::Error));
        assert_eq!(observe_control(|c| c.waits), 0);
    }
}

#[test]
fn delivery_failure_preserves_accepted_tail_before_error() {
    let _suite = suite();
    let (src, _null_source) = make_source();
    let mut failure = terminal(TrackEventKind::DeliveryLocalFailure, VIDEO);
    failure.local_result = Some(Error::NOMEM);
    failure.delivery_origin = Some(moq5::DeliveryOrigin::Live);
    control().events = vec![added(VIDEO, desc("video", "avc1.42c01e")), failure];
    queue_object(VIDEO, &VIDEO_PAYLOAD);
    assert_eq!(
        contents(&create(&src).expect("accepted media survives failure")),
        VIDEO_PAYLOAD
    );
    assert_eq!(create(&src).err(), Some(gst::FlowError::Error));
    assert_eq!(observe_control(|c| c.waits), 0);
}

#[test]
fn unrelated_and_fill_failures_do_not_end_live_video() {
    for (kind, track, origin) in [
        (TrackEventKind::LocalFailure, AUDIO, None),
        (
            TrackEventKind::DeliveryLocalFailure,
            VIDEO,
            Some(moq5::DeliveryOrigin::Fill),
        ),
    ] {
        let _suite = suite();
        let (src, _null_source) = make_source();
        let mut failure = terminal(kind, track);
        failure.local_result = Some(Error::NOMEM);
        failure.delivery_origin = origin;
        control().events = vec![
            added(VIDEO, desc("video", "avc1.42c01e")),
            failure,
            terminal(TrackEventKind::Ended, VIDEO),
        ];
        queue_object(VIDEO, &VIDEO_PAYLOAD);
        assert_eq!(
            contents(&create(&src).expect("healthy live media")),
            VIDEO_PAYLOAD
        );
        assert_eq!(create(&src).err(), Some(gst::FlowError::Eos));
        assert_eq!(observe_control(|c| c.waits), 0);
    }
}

#[test]
fn native_poll_and_description_errors_are_not_empty_queues() {
    for mode in 0..3 {
        let _suite = suite();
        let (src, _null_source) = make_source();
        control().events = vec![added(VIDEO, desc("video", "avc1.42c01e"))];
        match mode {
            0 => control().track_poll_error = Some(Error::NOMEM),
            1 => control().description_error = Some(Error::ABI_MISMATCH),
            _ => control().empty_result = Some(Error::UNSUPPORTED),
        }
        assert_eq!(create(&src).err(), Some(gst::FlowError::Error));
        assert_eq!(observe_control(|c| c.waits), 0);
    }
}

#[test]
fn native_wait_errors_are_not_retried_forever() {
    let _suite = suite();
    let (src, _null_source) = make_source();
    control().events = vec![added(VIDEO, desc("video", "avc1.42c01e"))];
    control().wait_result = Some(Error::INVAL);
    assert_eq!(create(&src).err(), Some(gst::FlowError::Error));
    assert_eq!(observe_control(|c| c.waits), 1);
}

#[test]
fn loc_timestamp_overflow_is_a_flow_error_and_releases_object() {
    let _suite = suite();
    let (src, _null_source) = make_source();
    let bus = gst::Bus::new();
    src.set_bus(Some(&bus));
    src.imp().lock_state().want_track = Some(VIDEO);
    for time in [0, 1 << 61] {
        control().objects.push(ObjectData {
            track: Some(VIDEO),
            payload: VIDEO_PAYLOAD.to_vec(),
            presentation_time_us: time,
            ..Default::default()
        });
    }
    assert_eq!(create(&src).unwrap().pts(), Some(gst::ClockTime::ZERO));
    assert_eq!(create(&src).err(), Some(gst::FlowError::Error));
    assert_eq!(observe_control(|c| c.cleaned), 2);
    let msg = bus
        .pop_filtered(&[gst::MessageType::Error])
        .expect("format error");
    let gst::MessageView::Error(err) = msg.view() else {
        unreachable!()
    };
    assert!(err.error().matches(gst::StreamError::Format));
    src.set_bus(None);
    // Failure did not poison the state lock or prevent safe teardown.
    let observed = src.imp().lock_state().objects_recv;
    assert_eq!(observed, 1);
}

#[test]
fn loc_backward_timestamp_clamps_without_overflow() {
    let _suite = suite();
    let (src, _null_source) = make_source();
    src.imp().lock_state().want_track = Some(VIDEO);
    for time in [1 << 61, 0] {
        control().objects.push(ObjectData {
            track: Some(VIDEO),
            payload: VIDEO_PAYLOAD.to_vec(),
            presentation_time_us: time,
            ..Default::default()
        });
    }
    for _ in 0..2 {
        assert_eq!(create(&src).unwrap().pts(), Some(gst::ClockTime::ZERO));
    }
    assert_eq!(observe_control(|c| c.cleaned), 2);
}

#[test]
fn loc_timestamp_mapping_checks_clocktime_boundary_and_addition() {
    let _suite = suite();
    let (src, _null_source) = make_source();
    let max_us = (u64::MAX - 1) / 1000;
    let (backward, clamp, forward, maximum, overflow, addition) = {
        let mut st = src.imp().lock_state();
        st.have_anchor = true;
        st.anchor_loc_us = 10;
        st.anchor_running = gst::ClockTime::from_nseconds(1500);
        let backward = src.imp().map_pts(&mut st, 9);
        let clamp = src.imp().map_pts(&mut st, 8);
        let forward = src.imp().map_pts(&mut st, 11);
        st.anchor_loc_us = 0;
        st.anchor_running = gst::ClockTime::ZERO;
        let maximum = src.imp().map_pts(&mut st, max_us);
        let overflow = src.imp().map_pts(&mut st, max_us + 1);
        st.anchor_running = gst::ClockTime::from_nseconds(u64::MAX - 1000);
        let addition = src.imp().map_pts(&mut st, 1);
        (backward, clamp, forward, maximum, overflow, addition)
    };
    assert_eq!(backward.unwrap().nseconds(), 500);
    assert_eq!(clamp.unwrap().nseconds(), 0);
    assert_eq!(forward.unwrap().nseconds(), 2500);
    assert_eq!(maximum.unwrap().nseconds(), max_us * 1000);
    assert_eq!(overflow.err(), Some(gst::FlowError::Error));
    assert_eq!(addition.err(), Some(gst::FlowError::Error));
}

#[test]
fn unlock_blocks_pending_cmaf_until_stale_output_is_discarded() {
    use gst_base::subclass::prelude::BaseSrcImpl;
    let _suite = suite();
    let (src, _null_source) = make_source();
    {
        let mut st = src.imp().lock_state();
        st.want_track = Some(VIDEO);
        st.want_init = b"init".to_vec();
    }
    control().objects.push(ObjectData {
        track: Some(VIDEO),
        packaging: Some(Packaging::Cmaf),
        fragment: b"stale fragment".to_vec(),
        ..Default::default()
    });
    assert_eq!(contents(&create(&src).unwrap()), b"init");
    src.imp().unlock().unwrap();
    for _ in 0..2 {
        assert_eq!(create(&src).err(), Some(gst::FlowError::Flushing));
    }
    src.imp().unlock_stop().unwrap();
    queue_object(VIDEO, &VIDEO_PAYLOAD);
    assert_eq!(contents(&create(&src).unwrap()), VIDEO_PAYLOAD);
    assert_eq!(observe_control(|c| c.cleaned), 2);
}

#[test]
fn downstream_caps_rejection_is_not_latched_as_success() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    for accept in [false, true] {
        let _suite = suite();
        let (src, _null_source) = make_source();
        let observed = Arc::new(AtomicUsize::new(0));
        let count = observed.clone();
        let sink = gst::Pad::builder(gst::PadDirection::Sink)
            .event_function(move |_, _, event| {
                if matches!(event.view(), gst::EventView::Caps(_)) {
                    count.fetch_add(1, Ordering::SeqCst);
                    accept
                } else {
                    true
                }
            })
            .query_function(move |pad, parent, query| {
                if let gst::QueryViewMut::AcceptCaps(q) = query.view_mut() {
                    q.set_result(accept);
                    true
                } else {
                    gst::Pad::query_default(pad, parent, query)
                }
            })
            .build();
        sink.set_active(true).unwrap();
        let pad = src.static_pad("src").unwrap();
        // This test drives create synchronously. Activate the real pad without
        // starting BaseSrc's scheduler; caps still pass through the real pad
        // and the downstream event handler.
        // SAFETY: the source is private to this test, NULL and inactive. No
        // streaming thread can access the activation callback while replaced.
        unsafe {
            pad.set_activatemode_function(|_, _, _, _| Ok(()));
        }
        pad.set_active(true).unwrap();
        pad.link(&sink).unwrap();
        src.imp().settings.lock().unwrap().caps = Some(gst::Caps::builder("video/x-h264").build());
        {
            let mut st = src.imp().lock_state();
            st.want_track = Some(VIDEO);
            st.caps_pushed = false;
        }
        queue_object(VIDEO, &VIDEO_PAYLOAD);
        if accept {
            assert_eq!(contents(&create(&src).unwrap()), VIDEO_PAYLOAD);
        } else {
            assert_eq!(create(&src).err(), Some(gst::FlowError::NotNegotiated));
            assert_eq!(observe_control(|c| c.cleaned), 0);
        }
        if accept {
            assert!(observed.load(Ordering::SeqCst) > 0);
        }
        let observed = src.imp().lock_state().caps_pushed;
        assert_eq!(observed, accept);
        pad.unlink(&sink).unwrap();
        pad.set_active(false).unwrap();
        sink.set_active(false).unwrap();
    }
}

#[test]
fn sticky_caps_event_refusal_surfaces_at_data_flow() {
    use gst_base::prelude::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    let _suite = suite();
    let (src, _null_source) = make_source();
    let chained = Arc::new(AtomicBool::new(false));
    let chain_seen = chained.clone();
    let sink = gst::Pad::builder(gst::PadDirection::Sink)
        .query_function(|pad, parent, query| {
            if let gst::QueryViewMut::AcceptCaps(q) = query.view_mut() {
                q.set_result(true);
                true
            } else {
                gst::Pad::query_default(pad, parent, query)
            }
        })
        .event_function(|_, _, event| !matches!(event.view(), gst::EventView::Caps(_)))
        .chain_function(move |_, _, _| {
            chain_seen.store(true, Ordering::SeqCst);
            Ok(gst::FlowSuccess::Ok)
        })
        .build();
    sink.set_active(true).unwrap();
    let pad = src.src_pad();
    // SAFETY: private, NULL, inactive test source; no streaming task exists.
    unsafe {
        pad.set_activatemode_function(|_, _, _, _| Ok(()));
    }
    pad.set_active(true).unwrap();
    pad.link(&sink).unwrap();
    assert!(pad.push_event(gst::event::StreamStart::new("caps-refusal-test")));
    src.imp().settings.lock().unwrap().caps = Some(gst::Caps::builder("video/x-h264").build());
    {
        let mut st = src.imp().lock_state();
        st.want_track = Some(VIDEO);
        st.caps_pushed = false;
    }
    queue_object(VIDEO, &VIDEO_PAYLOAD);
    // GstPad stores non-EOS sticky events successfully even if the peer's
    // event handler refuses them. The subsequent data push performs the real
    // negotiation check; BaseSrc's scheduling loop uses that same pad push.
    let buffer = create(&src).unwrap();
    let segment = gst::FormattedSegment::<gst::ClockTime>::new();
    pad.push_event(gst::event::Segment::new(&segment));
    assert_eq!(pad.push(buffer), Err(gst::FlowError::NotNegotiated));
    assert_eq!(observe_control(|c| c.cleaned), 1);
    assert!(!chained.load(Ordering::SeqCst));
    pad.unlink(&sink).unwrap();
    pad.set_active(false).unwrap();
    sink.set_active(false).unwrap();
}

// -- Opaque SETUP credentials through actual source startup -----------------

#[test]
fn setup_auth_source_conversion_flush_restart_and_instances() {
    use gst_base::subclass::prelude::BaseSrcImpl;
    let _suite = suite();
    let src = gst::glib::Object::new::<MoqSrc>();
    let other = gst::glib::Object::new::<MoqSrc>();
    src.set_property("setup-auth-token", "AP8B");
    src.set_property("setup-auth-token-type", u64::MAX);
    assert!(src.imp().start().is_ok());
    let captured = observe_control(|c| c.setup_auth.clone());
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].token_type, u64::MAX);
    assert_eq!(captured[0].value, [0, 255, 1]);
    assert!(src.imp().unlock().is_ok());
    assert!(src.imp().unlock_stop().is_ok());
    assert_eq!(observe_control(|c| c.connects), 1);
    assert!(src.imp().stop().is_ok());
    assert!(other.imp().start().is_ok());
    assert!(observe_control(|c| c.setup_auth.is_empty()));
    assert!(other.imp().stop().is_ok());
    assert!(src.imp().start().is_ok());
    assert_eq!(observe_control(|c| c.setup_auth.clone()), captured);
    assert!(src.imp().stop().is_ok());
    src.set_property("setup-auth-token", None::<String>);
    assert!(src.imp().start().is_ok());
    assert!(observe_control(|c| c.setup_auth.is_empty()));
    assert!(src.imp().stop().is_ok());
}

#[test]
fn setup_auth_source_invalid_before_connect_and_refusal_no_fallback() {
    use gst_base::subclass::prelude::BaseSrcImpl;
    let _suite = suite();
    let src = gst::glib::Object::new::<MoqSrc>();
    for value in ["", "AA", "base64:AA=="] {
        src.set_property("setup-auth-token", value);
        assert!(src.imp().start().is_err());
        assert_eq!(observe_control(|c| c.connects), 0);
    }
    src.set_property("setup-auth-token", "AP8B");
    src.set_property("setup-auth-token-type", 16u64);
    control().connect_result = Some(Error::INVAL);
    assert!(src.imp().start().is_err());
    assert_eq!(observe_control(|c| c.connects), 1);
    let actual = observe_control(|c| c.setup_auth.clone());
    assert_eq!(actual[0].token_type, 16);
    assert_eq!(actual[0].value, [0, 255, 1]);
    assert!(src.imp().stop().is_ok());
}

#[test]
fn setup_auth_source_concurrent_admission() {
    use crate::auth_tests::{admission_interleave, transition_wins_interleave, Change};
    let _suite = suite();
    for change in [Change::Replace, Change::Type, Change::Clear] {
        let src = gst::glib::Object::new::<MoqSrc>();
        let _cleanup = crate::test_support::NullCleanup::new(&src);
        src.set_property("setup-auth-token", "AP8B");
        src.set_property("setup-auth-token-type", 16u64);
        assert!(src.set_state(gst::State::Ready).is_ok());
        let transitioning = src.clone();
        admission_interleave(
            src.upcast_ref(),
            change,
            || src.imp().settings.try_lock().is_err(),
            move || {
                assert!(transitioning.set_state(gst::State::Paused).is_ok());
            },
        );
        change.assert_snapshot(&observe_control(|c| c.setup_auth.clone()));
        assert!(src.set_state(gst::State::Null).is_ok());
    }
    let src = gst::glib::Object::new::<MoqSrc>();
    let _cleanup = crate::test_support::NullCleanup::new(&src);
    src.set_property("setup-auth-token", "AP8B");
    src.set_property("setup-auth-token-type", 16u64);
    assert!(src.set_state(gst::State::Ready).is_ok());
    let transitioning = src.clone();
    transition_wins_interleave(src.upcast_ref(), move || {
        assert!(transitioning.set_state(gst::State::Paused).is_ok());
    });
    let captured = observe_control(|c| c.setup_auth.clone());
    assert_eq!(captured[0].token_type, 16);
    assert_eq!(captured[0].value, [0, 255, 1]);
    assert!(src.set_state(gst::State::Ready).is_ok());
    src.set_property("setup-auth-token", "AQID");
    assert!(src.set_state(gst::State::Paused).is_ok());
    assert_eq!(
        observe_control(|c| c.setup_auth[0].value.clone()),
        [1, 2, 3]
    );
    assert!(src.set_state(gst::State::Null).is_ok());
}

#[test]
fn draft_negotiation_source_default_and_exact() {
    use gst_base::subclass::prelude::BaseSrcImpl;
    use moq5::{Version, VersionOffer};
    let _suite = suite();
    for selected in [None, Some(0u32), Some(22), Some(18), Some(16)] {
        let src = gst::glib::Object::new::<MoqSrc>();
        let _cleanup = crate::test_support::NullCleanup::new(&src);
        assert_eq!(
            src.property::<u32>("draft"),
            0,
            "newest-first native AUTO is the default"
        );
        if let Some(draft) = selected {
            src.set_property("draft", draft);
        }
        src.set_property("setup-auth-token", "AP8B");
        src.set_property("setup-auth-token-type", 16u64);
        let before = observe_control(|c| c.connects);
        assert!(src.imp().start().is_ok());
        let (dialled, offer, tokens, connects) = observe_control(|c| {
            (
                c.dialled.clone().unwrap(),
                c.version_offer.clone().unwrap(),
                c.setup_auth.clone(),
                c.connects,
            )
        });
        let wanted = selected.unwrap_or(0);
        assert_eq!(dialled.draft, wanted);
        assert_eq!(
            offer,
            if wanted == 0 {
                VersionOffer::Auto
            } else {
                VersionOffer::Exact(Version::from_draft(wanted))
            }
        );
        assert!(!dialled.insecure);
        assert_eq!(tokens[0].token_type, 16);
        assert_eq!(tokens[0].value, [0, 255, 1]);
        assert_eq!(connects, before + 1);
        assert!(src.imp().stop().is_ok());
    }
}
