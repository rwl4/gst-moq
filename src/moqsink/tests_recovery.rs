//! Sink EOS / flush / restart recovery against the service double.
//!
//! Port of tests/test_sink_recovery.c. Like the C suite, it drives the real
//! element internals and replaces the libmoq service calls and the clock: the
//! monotonic time only advances when the element waits or retries, which makes
//! deadlines deterministic.

use gst::prelude::*;
use gst::subclass::prelude::*;

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use super::imp::EOS_DRAIN_TIMEOUT_US;
use super::{MoqSink, MoqSinkPad};
use crate::service::fake::{control, suite as fake_suite, Suite as FakeSuite};
use crate::service::TrackCfg;
use moq5::{Buffer, Error, MediaType, Packaging, SendObject, WaitResult};

const USEC_PER_SEC: i64 = 1_000_000;

static NOW: AtomicI64 = AtomicI64::new(0);
static ACTIVE: Mutex<Option<MoqSink>> = Mutex::new(None);
static CONTINUING: Mutex<Option<MoqSinkPad>> = Mutex::new(None);

fn active() -> MoqSink {
    let sink = ACTIVE.lock().unwrap().clone();
    sink.expect("active sink")
}

/// Read owned observations before an assertion can unwind the CONTROL mutex.
fn observe_control<T>(read: impl FnOnce(&crate::service::fake::Control) -> T) -> T {
    let state = control();
    read(&state)
}

// -- clock: time only moves when the element waits or retries ----------------

fn now() -> i64 {
    NOW.load(Ordering::SeqCst)
}

fn write_other(p: &MoqSinkPad) {
    let payload = Buffer::copy_from(&[42]).unwrap();
    assert_eq!(
        active().imp().write_object(p, SendObject::new(payload), 1),
        Ok(gst::FlowSuccess::Ok)
    );
}

fn pause_retry(us: u64) {
    assert!(us <= 5000);
    // The retry pause must not hold sender serialization.
    let observed = active().imp().send.try_lock().is_ok();
    assert!(observed);
    let continuing = CONTINUING.lock().unwrap().clone();
    if let Some(p) = continuing {
        write_other(&p);
    }
    NOW.fetch_add(USEC_PER_SEC, Ordering::SeqCst);
}

// -- fixture -----------------------------------------------------------------

struct Suite {
    // Field drop order is significant: restore global hooks while serial
    // ownership is still held, on normal return and unwinding alike.
    _clock: crate::clock::ClockGuard,
    _fake: FakeSuite,
}

impl Drop for Suite {
    fn drop(&mut self) {
        let sink = ACTIVE.lock().unwrap().clone();
        if let Some(sink) = sink {
            let _ = sink.set_state(gst::State::Null);
        }
        *ACTIVE.lock().unwrap() = None;
        *CONTINUING.lock().unwrap() = None;
    }
}

fn suite() -> Suite {
    // fake_suite takes the shared serial lock and resets the control block.
    let fake = fake_suite();
    // Waiting on the endpoint is what advances this suite's clock.
    control().on_wait = Some(Box::new(|| {
        NOW.fetch_add(USEC_PER_SEC, Ordering::SeqCst);
    }));
    Suite {
        _clock: crate::clock::install(now, pause_retry),
        _fake: fake,
    }
}

fn actual_suite_clock_restored_before_release(panicking: bool) {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    let observed = Arc::new(AtomicUsize::new(0));
    let mut initially_restored = true;
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut fixture = suite();
        let restored = fixture._clock.restoration_probe();
        // A discriminating precondition: the actual Suite replaced both hooks.
        initially_restored = restored();
        let observation = observed.clone();
        fixture._fake.observe_before_release(move || {
            observation.store(if restored() { 2 } else { 1 }, Ordering::SeqCst);
        });
        if panicking {
            panic!("controlled actual Suite teardown panic");
        }
    }));
    // The observation happens under real SERIAL ownership; assertions do not.
    assert_eq!(result.is_err(), panicking);
    assert!(!initially_restored);
    assert_eq!(
        observed.load(Ordering::SeqCst),
        2,
        "actual recovery Suite released fake serial ownership before restoring its clock"
    );
}

#[test]
fn actual_suite_clock_restored_before_release_on_return() {
    actual_suite_clock_restored_before_release(false);
}

#[test]
fn actual_suite_clock_restored_before_release_on_panic() {
    actual_suite_clock_restored_before_release(true);
}

fn make_sink() -> (MoqSink, crate::test_support::NullCleanup) {
    NOW.store(100, Ordering::SeqCst);
    *CONTINUING.lock().unwrap() = None;
    let s = gst::glib::Object::builder::<MoqSink>()
        .property("sync", false)
        .build();
    let cleanup = crate::test_support::NullCleanup::new(&s);
    *ACTIVE.lock().unwrap() = Some(s.clone());
    assert!(s.set_state(gst::State::Ready).is_ok());
    (s, cleanup)
}

fn pad(s: &MoqSink, track: bool) -> MoqSinkPad {
    let p = s
        .request_pad_simple("audio_%u")
        .unwrap()
        .downcast::<MoqSinkPad>()
        .unwrap();
    if track {
        if s.imp().send_lock().sender.is_none() {
            let mut ts = s.imp().track_lock();
            ts.preparing_tracks = Some(initial_snapshot(&p));
            let generation = s.imp().obj_lock().flush_generation;
            let outcome = s.imp().open_sender(&mut ts, generation);
            ts.preparing_tracks = None;
            drop(ts);
            assert_eq!(outcome, Ok(()));
        } else {
            let cfg = recovery_track(&p);
            let added = {
                let send = s.imp().send_lock();
                send.sender
                    .as_ref()
                    .ok_or(Error::CLOSED)
                    .and_then(|sender| sender.add_track(&cfg))
            };
            let t = added.unwrap();
            p.set_track(Some(t));
        }
    }
    p
}

/// A real, nonempty AAC/LOC snapshot satisfying the public startup contract.
fn recovery_track(p: &MoqSinkPad) -> TrackCfg {
    TrackCfg::new(
        p.name().as_str(),
        MediaType::Audio,
        Packaging::Raw,
        "mp4a.40.2",
    )
    .bitrate(128000)
    .samplerate(48000)
    .channel_config("2")
    .init_data([0x11, 0x90])
}

fn initial_snapshot(p: &MoqSinkPad) -> Vec<(MoqSinkPad, TrackCfg)> {
    vec![(p.clone(), recovery_track(p))]
}

fn release(s: &MoqSink, p: MoqSinkPad) {
    s.release_request_pad(&p);
}

fn finish(s: MoqSink) {
    let _ = s.set_state(gst::State::Null);
}

fn eos(s: &MoqSink, p: &MoqSinkPad) -> bool {
    s.imp().sink_event(p, gst::event::Eos::new())
}

fn flush(s: &MoqSink, p: &MoqSinkPad, start: bool) {
    let ev = if start {
        gst::event::FlushStart::new()
    } else {
        gst::event::FlushStop::new(true)
    };
    assert!(s.imp().sink_event(p, ev));
}

// -- cases -------------------------------------------------------------------

fn incomplete_cmaf_eos(pending_moof: bool) {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let p = pad(&s, true);
    let bus = gst::Bus::new();
    s.set_bus(Some(&bus));
    let partial_header: &[u8] = &[0, 0, 0];
    let missing_mdat: &[u8] = &[
        0, 0, 0, 8, b'f', b't', b'y', b'p', 0, 0, 0, 8, b'm', b'o', b'o', b'v', 0, 0, 0, 8, b'm',
        b'o', b'o', b'f',
    ];
    let bytes = if pending_moof {
        missing_mdat
    } else {
        partial_header
    };
    let observed = p.state().splitter.push(bytes).is_ok();
    assert!(observed);
    assert!(!eos(&s, &p));
    assert_eq!(observe_control(|c| c.ends), 0);
    assert_eq!(observe_control(|c| c.drains), 0);
    let observed = s.imp().send_lock().eos_failed;
    assert!(observed);
    let msg = bus
        .pop_filtered(&[gst::MessageType::Error])
        .expect("error message");
    let gst::MessageView::Error(err) = msg.view() else {
        unreachable!()
    };
    assert!(err.error().matches(gst::StreamError::Format));
    assert!(bus.pop_filtered(&[gst::MessageType::Eos]).is_none());
    s.set_bus(None);
    release(&s, p);
    finish(s);
}

#[test]
fn eos_partial_box() {
    incomplete_cmaf_eos(false);
}

#[test]
fn eos_missing_mdat() {
    incomplete_cmaf_eos(true);
}

#[test]
fn budget() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let p = pad(&s, true);
    control().refuse_ends = 2;
    assert!(eos(&s, &p));
    assert_eq!(observe_control(|c| c.ends), 3);
    assert_eq!(observe_control(|c| c.budget), USEC_PER_SEC as u64);
    assert_eq!(
        crate::service::fake::log_count(crate::service::fake::Op::Complete),
        1
    );
    release(&s, p);
    finish(s);
}

#[test]
fn independent() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let a = pad(&s, true);
    let b = pad(&s, true);
    *CONTINUING.lock().unwrap() = Some(b.clone());
    control().refuse_ends = 1;
    assert!(eos(&s, &a));
    assert_eq!(observe_control(|c| c.writes), 1);
    assert!(observe_control(|c| c.ended[0]));
    assert!(!observe_control(|c| c.ended[1]));
    assert_eq!(observe_control(|c| c.drains), 0);
    assert_eq!(
        crate::service::fake::log_count(crate::service::fake::Op::Complete),
        0
    );
    *CONTINUING.lock().unwrap() = None;
    release(&s, a);
    assert_eq!(
        crate::service::fake::log_count(crate::service::fake::Op::Complete),
        0
    );
    release(&s, b);
    finish(s);
}

#[test]
fn released() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let a = pad(&s, true);
    let b = pad(&s, true);
    flush(&s, &b, true);
    release(&s, a);
    assert_eq!(
        crate::service::fake::log_count(crate::service::fake::Op::Complete),
        0
    );
    assert!(!observe_control(|c| c.ended[0]));
    assert!(observe_control(|c| c.interrupted));
    flush(&s, &b, false);
    assert!(observe_control(|c| c.ended[0]));
    assert!(!observe_control(|c| c.ended[1]));
    release(&s, b);
    finish(s);
}

#[test]
fn flush_count() {
    let _suite = suite();
    // The C suite ran this after /sink/released, whose teardown leaves the
    // interrupt latch set; with no endpoint opened here nothing may clear it.
    control().interrupted = true;
    let (s, _null_sink) = make_sink();
    let a = pad(&s, false);
    let b = pad(&s, false);
    flush(&s, &a, true);
    flush(&s, &a, true);
    flush(&s, &b, false);
    let observed = s.imp().obj_lock().flush_count;
    assert_eq!(observed, 1);
    flush(&s, &b, true);
    release(&s, a);
    let observed = s.imp().obj_lock().flush_count;
    assert_eq!(observed, 1);
    assert!(observe_control(|c| c.interrupted));
    s.imp().obj_lock().state_flushing = true;
    flush(&s, &b, false);
    assert!(observe_control(|c| c.interrupted));
    release(&s, b);
    finish(s);
}

#[test]
fn failed() {
    let _suite = suite();
    for result in [
        Ok(WaitResult::TimedOut),
        Err(Error::INTERRUPTED),
        Err(Error::CLOSED),
        Err(Error::UNSUPPORTED),
        Err(Error::WRONG_STATE),
    ] {
        let (s, _null_sink) = make_sink();
        let p = pad(&s, true);
        let bus = gst::Bus::new();
        s.set_bus(Some(&bus));
        control().drain_result = Some(result);
        assert!(!eos(&s, &p));
        let m = bus.pop().expect("a message");
        assert_eq!(m.type_(), gst::MessageType::Error, "drain={result:?}");
        assert!(bus.pop().is_none());
        release(&s, p);
        s.set_bus(None);
        finish(s);
    }
}

#[test]
fn cancel() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let p = pad(&s, true);
    // Flushing the draining pad from inside drain is what cancels the EOS.
    let canceller = p.clone();
    control().on_drain = Some(Box::new(move || {
        active().imp().set_pad_flush(&canceller, true);
    }));
    assert!(!eos(&s, &p));
    assert!(observe_control(|c| c.interrupted));
    control().on_drain = None;
    release(&s, p);
    finish(s);
}

#[test]
fn aggregation() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let a = pad(&s, true);
    let b = pad(&s, true);
    a.set_eos(true);
    assert!(eos(&s, &b));
    let observed = !s.imp().obj_lock().eos_posted;
    assert!(observed);
    assert_eq!(observe_control(|c| c.drains), 0);
    assert_eq!(
        crate::service::fake::log_count(crate::service::fake::Op::Complete),
        0
    );
    release(&s, a);
    assert_eq!(observe_control(|c| c.drains), 1);
    assert_eq!(
        crate::service::fake::log_count(crate::service::fake::Op::Complete),
        1
    );
    release(&s, b);
    finish(s);
}

#[test]
fn restart() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let _ = s.set_state(gst::State::Paused);
    let p = pad(&s, true);
    assert!(eos(&s, &p));
    p.state().splitter.buf.push(b'x');
    let _ = s.set_state(gst::State::Ready);
    assert_eq!(observe_control(|c| c.destroys), 1);
    let _ = s.set_state(gst::State::Paused);
    assert!(p.track().is_none());
    assert!(!p.eos());
    let observed = p.state().splitter.buf.is_empty();
    assert!(observed);
    assert_eq!(observe_control(|c| c.connects), 1); // reconnect is deferred until media
    release(&s, p);
    finish(s);
}

#[test]
fn deadline() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let a = pad(&s, true);
    let b = pad(&s, true);
    flush(&s, &b, true);
    release(&s, a);
    NOW.fetch_add(EOS_DRAIN_TIMEOUT_US, Ordering::SeqCst);
    let before = control().ends;
    flush(&s, &b, false);
    assert_eq!(observe_control(|c| c.ends), before);
    let observed = s.imp().send_lock().eos_failed;
    assert!(observed);
    assert!(!observe_control(|c| c.ended[0]));
    release(&s, b);
    finish(s);
}

/// Readiness is polled now, not delivered by a callback: the element logs the
/// record once, when `is_ready` first holds. `late` makes it hold only after
/// the element has waited, which is the case the old
/// `ready_after_attach`/`ready_during_attach` pair covered through libmoq's
/// deleted `on_ready` callback.
fn ready_record(late: bool) {
    let _suite = suite();
    // The peer selected 18 even though AUTO prefers 22. Report readback,
    // rather than the configured 0 or the first preferred version.
    control().negotiated_draft = 18;
    if late {
        control().not_ready_for = 2;
    }
    let logs = std::sync::Arc::new(Mutex::new(Vec::new()));
    let seen = logs.clone();
    let cat = *super::imp::CAT;
    let previous = cat.threshold();
    let capture =
        gst::log::add_log_function(move |cat, _level, _file, _function, _line, _obj, msg| {
            let Some(text) = msg.get() else { return };
            if cat.name() == "moqsink" && text.starts_with("native sender ready") {
                seen.lock().unwrap().push(text.to_string());
            }
        });
    cat.set_threshold(gst::DebugLevel::Info);
    let log_cleanup = crate::test_support::LogCleanup::new(capture, cat, previous);

    let (s, _null_sink) = make_sink();
    let _ = s.set_state(gst::State::Paused);
    assert!(s.imp().started.load(Ordering::SeqCst));
    let p = pad(&s, true);
    // Exactly one record, whether readiness held at once or after waiting.
    let actual_logs = logs.lock().unwrap().clone();
    assert_eq!(
        actual_logs,
        ["native sender ready namespace=example draft=18"]
    );
    if late {
        // It polled until readiness held rather than giving up.
        assert!(observe_control(|c| c.ready_calls) >= 3);
    }

    drop(log_cleanup);
    release(&s, p);
    finish(s);
}

#[test]
fn ready_at_attach() {
    ready_record(false);
}

#[test]
fn ready_after_waiting() {
    ready_record(true);
}

#[test]
fn flush_abandons_partial_cmaf_before_eos() {
    let _suite = suite();
    for input in [
        vec![0, 0, 0],
        vec![
            0, 0, 0, 8, b'f', b't', b'y', b'p', 0, 0, 0, 8, b'm', b'o', b'o', b'v', 0, 0, 0, 8,
            b'm', b'o', b'o', b'f',
        ],
    ] {
        let (s, _null_sink) = make_sink();
        let p = pad(&s, true);
        let pushed = p.state().splitter.push(&input);
        pushed.unwrap();
        let observed = p.state().splitter.has_partial();
        assert!(observed);
        flush(&s, &p, true);
        flush(&s, &p, false);
        let observed = !p.state().splitter.has_partial();
        assert!(observed);
        let observed = p.state().splitter.pull().is_none();
        assert!(observed);
        assert!(eos(&s, &p));
        release(&s, p);
        finish(s);
    }
}

fn startup_error(mode: u32) {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let p = pad(&s, false);
    let bus = gst::Bus::new();
    s.set_bus(Some(&bus));
    let expected = match mode {
        0 => {
            control().hold_startup = true;
            "MoQ sender startup deadline expired (phase=registration)".into()
        }
        1 => {
            control().not_ready_for = u32::MAX;
            "MoQ sender startup deadline expired (phase=readiness)".into()
        }
        2 => {
            control().fail_post = true;
            format!(
                "MoQ sender startup failed (phase=registration, rc={})",
                Error::CLOSED.code()
            )
        }
        3 => {
            control().fail_attach = true;
            format!(
                "MoQ sender startup failed (phase=registration, rc={})",
                Error::NOMEM.code()
            )
        }
        4 => {
            control().not_ready_for = u32::MAX;
            control().sender_wait_error = Some(Error::CLOSED);
            format!(
                "MoQ sender startup failed (phase=readiness, rc={})",
                Error::CLOSED.code()
            )
        }
        5 => {
            control().connect_result = Some(Error::CLOSED);
            format!(
                "MoQ sender startup failed (phase=connect, rc={})",
                Error::CLOSED.code()
            )
        }
        _ => unreachable!(),
    };
    let initial_time = now();
    let generation = s.imp().obj_lock().flush_generation;
    let outcome = {
        let mut ts = s.imp().track_lock();
        ts.preparing_tracks = Some(initial_snapshot(&p));
        s.imp().open_sender(&mut ts, generation)
    };
    assert_eq!(outcome, Err(gst::FlowError::Error));
    let msg = bus
        .pop_filtered(&[gst::MessageType::Error])
        .expect("primary startup failure");
    let gst::MessageView::Error(error) = msg.view() else {
        unreachable!()
    };
    assert_eq!(error.error().message(), expected);
    assert!(error.error().matches(gst::ResourceError::OpenWrite));
    if mode < 2 {
        assert_eq!(now() - initial_time, EOS_DRAIN_TIMEOUT_US);
    }
    assert_eq!(observe_control(|c| c.destroys), u32::from(mode != 5));
    // This is the abandoned tail which caused a second, misleading error.
    let pushed = p.state().splitter.push(&[0, 0, 0]);
    pushed.unwrap();
    assert!(!eos(&s, &p));
    assert!(bus.pop_filtered(&[gst::MessageType::Error]).is_none());
    assert!(bus.pop_filtered(&[gst::MessageType::Eos]).is_none());
    // The marker belongs only to its old generation. A later partial input
    // must retain the normal format check, even though hard failure is sticky.
    flush(&s, &p, true);
    flush(&s, &p, false);
    let pushed = p.state().splitter.push(&[0, 0, 0]);
    pushed.unwrap();
    assert!(!eos(&s, &p));
    let msg = bus
        .pop_filtered(&[gst::MessageType::Error])
        .expect("new-generation partial input");
    let gst::MessageView::Error(error) = msg.view() else {
        unreachable!()
    };
    assert!(error.error().matches(gst::StreamError::Format));
    // Full lifecycle restart clears the failure and succeeds normally.
    assert!(s.set_state(gst::State::Null).is_ok());
    control().hold_startup = false;
    control().not_ready_for = 0;
    control().fail_post = false;
    control().fail_attach = false;
    control().sender_wait_error = None;
    control().connect_result = None;
    assert!(s.set_state(gst::State::Ready).is_ok());
    let generation = s.imp().obj_lock().flush_generation;
    let outcome = {
        let mut ts = s.imp().track_lock();
        ts.preparing_tracks = Some(initial_snapshot(&p));
        s.imp().open_sender(&mut ts, generation)
    };
    assert_eq!(outcome, Ok(()));
    assert!(bus.pop_filtered(&[gst::MessageType::Error]).is_none());
    release(&s, p);
    finish(s);
}

#[test]
fn registration_deadline_is_local() {
    startup_error(0);
}
#[test]
fn readiness_deadline_is_local() {
    startup_error(1);
}
#[test]
fn native_registration_closed_is_preserved() {
    startup_error(2);
}
#[test]
fn native_registration_nomem_is_preserved() {
    startup_error(3);
}
#[test]
fn native_readiness_closed_is_preserved() {
    startup_error(4);
}
#[test]
fn native_connect_closed_is_preserved() {
    startup_error(5);
}

#[test]
fn startup_phases_share_one_budget() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let p = pad(&s, false);
    let initial = now();
    control().hold_startup = true;
    control().not_ready_for = u32::MAX;
    control().on_wait = Some(Box::new(move || {
        let previous = NOW.fetch_add(USEC_PER_SEC, Ordering::SeqCst);
        if previous + USEC_PER_SEC >= initial + 2 * USEC_PER_SEC {
            control().hold_startup = false;
        }
    }));
    let bus = gst::Bus::new();
    s.set_bus(Some(&bus));
    let generation = s.imp().obj_lock().flush_generation;
    let outcome = {
        let mut ts = s.imp().track_lock();
        ts.preparing_tracks = Some(initial_snapshot(&p));
        s.imp().open_sender(&mut ts, generation)
    };
    assert_eq!(outcome, Err(gst::FlowError::Error));
    assert_eq!(now() - initial, EOS_DRAIN_TIMEOUT_US);
    let msg = bus.pop_filtered(&[gst::MessageType::Error]).unwrap();
    let gst::MessageView::Error(error) = msg.view() else {
        unreachable!()
    };
    assert_eq!(
        error.error().message(),
        "MoQ sender startup deadline expired (phase=readiness)"
    );
    finish(s);
}

fn cancellation_at_startup_deadline(readiness: bool) {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let p = pad(&s, false);
    let bus = gst::Bus::new();
    s.set_bus(Some(&bus));
    let initial = now();
    if readiness {
        control().not_ready_for = u32::MAX;
    } else {
        control().hold_startup = true;
    }
    let flush_thread = super::tests_barrier::PendingThread::new();
    let callback_thread = flush_thread.slot();
    let callback_sink = s.clone();
    let callback_pad = p.clone();
    control().on_wait = Some(Box::new(move || {
        let previous = NOW.fetch_add(USEC_PER_SEC, Ordering::SeqCst);
        if previous + USEC_PER_SEC == initial + EOS_DRAIN_TIMEOUT_US {
            let sink = callback_sink.clone();
            let pad = callback_pad.clone();
            *callback_thread.lock().unwrap() = Some(super::tests_barrier::PushThread::spawn(
                &callback_sink,
                move || {
                    sink.imp().set_pad_flush(&pad, true);
                },
            ));
            // A causal handshake with the real flush handler, not elapsed
            // network/congestion timing. It blocks on track notification.
            let limit = std::time::Instant::now() + std::time::Duration::from_secs(1);
            while callback_sink.imp().obj_lock().flush_count == 0 {
                assert!(std::time::Instant::now() < limit);
                std::thread::yield_now();
            }
        }
    }));
    let generation = s.imp().obj_lock().flush_generation;
    let outcome = {
        let mut ts = s.imp().track_lock();
        ts.preparing_tracks = Some(initial_snapshot(&p));
        s.imp().open_sender(&mut ts, generation)
    };
    assert_eq!(outcome, Err(gst::FlowError::Flushing));
    flush_thread.join().unwrap();
    assert_eq!(now() - initial, EOS_DRAIN_TIMEOUT_US);
    assert!(bus.pop_filtered(&[gst::MessageType::Error]).is_none());
    let observed = s.imp().namespace.lock().unwrap().is_some();
    assert!(observed);
    assert_eq!(observe_control(|c| c.destroys), 1);
    // Cancellation cannot claim the abandoned-tail exception used by a
    // recorded hard startup failure.
    s.imp().set_pad_flush(&p, false);
    let pushed = p.state().splitter.push(&[0, 0, 0]);
    pushed.unwrap();
    assert!(!eos(&s, &p));
    let msg = bus.pop_filtered(&[gst::MessageType::Error]).unwrap();
    let gst::MessageView::Error(error) = msg.view() else {
        unreachable!()
    };
    assert!(error.error().matches(gst::StreamError::Format));
    release(&s, p);
    finish(s);
}

#[test]
fn cancellation_precedes_registration_deadline() {
    cancellation_at_startup_deadline(false);
}
#[test]
fn cancellation_precedes_readiness_deadline() {
    cancellation_at_startup_deadline(true);
}

#[test]
fn whole_element_completion_precedes_successful_eos() {
    use crate::service::fake;
    use crate::service::fake::Op;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let p = pad(&s, true);
    let bus = gst::Bus::new();
    s.set_bus(Some(&bus));
    write_other(&p);
    control().stats.objects_queued = 1;
    let premature_eos = Arc::new(AtomicBool::new(false));
    let observed = premature_eos.clone();
    let draining_bus = bus.clone();
    control().on_drain = Some(Box::new(move || {
        control().stats.objects_queued = 0;
        observed.store(
            draining_bus
                .pop_filtered(&[gst::MessageType::Eos])
                .is_some(),
            Ordering::SeqCst,
        );
    }));
    let accepted = eos(&s, &p);
    let message = bus.pop_filtered(&[gst::MessageType::Eos, gst::MessageType::Error]);
    let complete_count = fake::log_count(Op::Complete);
    let queued = observe_control(|c| c.stats.objects_queued);
    let ordered = fake::log_find(Op::Write, None) < fake::log_find(Op::EndTrack, None)
        && fake::log_find(Op::EndTrack, None) < fake::log_find(Op::Complete, None)
        && fake::log_find(Op::Complete, None) < fake::log_find(Op::Drain, None);
    control().on_drain = None;
    release(&s, p);
    s.set_bus(None);
    finish(s);
    assert!(accepted);
    assert_eq!(queued, 0, "pending media drained before bus EOS");
    assert_eq!(
        message.expect("terminal message").type_(),
        gst::MessageType::Eos
    );
    assert!(!premature_eos.load(Ordering::SeqCst));
    assert_eq!(
        complete_count, 1,
        "whole-element EOS must complete the broadcast"
    );
    assert!(ordered, "accepted media/end, completion, then drain");
}

#[test]
fn completion_refusal_preserves_error_without_successful_eos() {
    use crate::service::fake;
    use crate::service::fake::Op;

    let _suite = suite();
    for error in [
        Error::LIMIT,
        Error::NOMEM,
        Error::WOULD_BLOCK,
        Error::INTERRUPTED,
    ] {
        let (s, _null_sink) = make_sink();
        let p = pad(&s, true);
        let bus = gst::Bus::new();
        s.set_bus(Some(&bus));
        let drains = observe_control(|c| c.drains);
        let completes = fake::log_count(Op::Complete);
        control().complete_result = Some(error);
        let accepted = eos(&s, &p);
        let message = bus.pop_filtered(&[gst::MessageType::Error, gst::MessageType::Eos]);
        let no_duplicate = eos(&s, &p);
        let later_message = bus.pop_filtered(&[gst::MessageType::Error, gst::MessageType::Eos]);
        let observed_drains = observe_control(|c| c.drains);
        control().complete_result = None;
        release(&s, p);
        s.set_bus(None);
        finish(s);
        assert!(!accepted);
        assert!(no_duplicate);
        assert!(
            later_message.is_none(),
            "first EOS error remains authoritative"
        );
        let message = message.expect("completion refusal message");
        let gst::MessageView::Error(failure) = message.view() else {
            panic!("completion refusal must not post bus EOS");
        };
        assert!(failure
            .error()
            .message()
            .contains(&format!("rc={}", error.code())));
        assert_eq!(
            observed_drains, drains,
            "refused completion must not be drained as success"
        );
        assert_eq!(
            fake::log_count(Op::Complete),
            completes + 1,
            "no retry or duplicate completion"
        );
    }
}

#[test]
fn asynchronous_completion_error_survives_successful_output_drain() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let p = pad(&s, true);
    let bus = gst::Bus::new();
    s.set_bus(Some(&bus));
    control().on_drain = Some(Box::new(|| {
        control().stats.completion_error = Some(Error::NOMEM);
    }));
    let accepted = eos(&s, &p);
    let message = bus.pop_filtered(&[gst::MessageType::Error, gst::MessageType::Eos]);
    let drain_count = observe_control(|c| c.drains);
    control().on_drain = None;
    control().stats.completion_error = None;
    release(&s, p);
    s.set_bus(None);
    finish(s);
    assert!(!accepted, "local output flush is not broadcast completion");
    assert_eq!(drain_count, 1);
    let message = message.expect("sticky completion failure");
    let gst::MessageView::Error(failure) = message.view() else {
        panic!("asynchronous completion error must not post bus EOS");
    };
    assert!(failure
        .error()
        .message()
        .contains(&format!("rc={}", Error::NOMEM.code())));
}

#[test]
fn completion_drain_cannot_exceed_absolute_eos_deadline() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let p = pad(&s, true);
    let bus = gst::Bus::new();
    s.set_bus(Some(&bus));
    control().on_drain = Some(Box::new(|| {
        NOW.fetch_add(EOS_DRAIN_TIMEOUT_US, Ordering::SeqCst);
    }));
    let accepted = eos(&s, &p);
    let message = bus.pop_filtered(&[gst::MessageType::Error, gst::MessageType::Eos]);
    control().on_drain = None;
    release(&s, p);
    s.set_bus(None);
    finish(s);
    assert!(!accepted);
    assert_eq!(
        message.expect("expired EOS deadline").type_(),
        gst::MessageType::Error
    );
}

#[test]
fn broadcast_completion_belongs_to_each_restarted_sender() {
    use crate::service::fake;
    use crate::service::fake::Op;

    let _suite = suite();
    let (s, _null_sink) = make_sink();
    let _ = s.set_state(gst::State::Paused);
    let old = pad(&s, true);
    assert!(eos(&s, &old));
    assert_eq!(fake::log_count(Op::Complete), 1);
    assert!(s.set_state(gst::State::Ready).is_ok());
    assert!(s.set_state(gst::State::Paused).is_ok());
    let new = pad(&s, true);
    assert!(eos(&s, &new));
    assert_eq!(fake::log_count(Op::Complete), 2);
    assert_eq!(observe_control(|c| c.drains), 2);
    release(&s, old);
    release(&s, new);
    finish(s);
}

// -- Opaque SETUP credentials through actual deferred sender startup --------

#[test]
fn setup_auth_sink_conversion_flush_restart_and_instances() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    s.set_property("setup-auth-token", "AP8B");
    s.set_property("setup-auth-token-type", 16u64);
    let p = pad(&s, true);
    let captured = observe_control(|c| c.setup_auth.clone());
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].token_type, 16);
    assert_eq!(captured[0].value, [0, 255, 1]);
    flush(&s, &p, true);
    flush(&s, &p, false);
    assert_eq!(observe_control(|c| c.connects), 1);
    release(&s, p);
    assert!(s.set_state(gst::State::Null).is_ok());
    assert!(s.set_state(gst::State::Ready).is_ok());
    let p = pad(&s, true);
    assert_eq!(observe_control(|c| c.setup_auth.clone()), captured);
    release(&s, p);
    finish(s);
    let (other, _null_other) = make_sink();
    let p = pad(&other, true);
    assert!(observe_control(|c| c.setup_auth.is_empty()));
    release(&other, p);
    finish(other);
}

#[test]
fn setup_auth_sink_invalid_before_connect() {
    let _suite = suite();
    let s = gst::glib::Object::new::<MoqSink>();
    let _cleanup = crate::test_support::NullCleanup::new(&s);
    for value in ["", "AA", "base64:AA=="] {
        s.set_property("setup-auth-token", value);
        assert!(s.set_state(gst::State::Ready).is_err());
        assert_eq!(observe_control(|c| c.connects), 0);
        assert!(s.set_state(gst::State::Null).is_ok());
    }
}

#[test]
fn setup_auth_sink_refusal_no_fallback() {
    let _suite = suite();
    let (s, _null_sink) = make_sink();
    s.set_property("setup-auth-token", "AP8B");
    s.set_property("setup-auth-token-type", u64::MAX);
    let p = pad(&s, false);
    control().connect_result = Some(Error::INVAL);
    let outcome = {
        let mut ts = s.imp().track_lock();
        ts.preparing_tracks = Some(initial_snapshot(&p));
        let generation = s.imp().obj_lock().flush_generation;
        let outcome = s.imp().open_sender(&mut ts, generation);
        ts.preparing_tracks = None;
        outcome
    };
    assert_eq!(outcome, Err(gst::FlowError::Error));
    assert_eq!(observe_control(|c| c.connects), 1);
    let captured = observe_control(|c| c.setup_auth.clone());
    assert_eq!(captured[0].token_type, u64::MAX);
    assert_eq!(captured[0].value, [0, 255, 1]);
    release(&s, p);
    finish(s);
}

#[test]
fn setup_auth_sink_concurrent_admission() {
    use crate::auth_tests::{admission_interleave, transition_wins_interleave, Change};
    let _suite = suite();
    for change in [Change::Replace, Change::Type, Change::Clear] {
        let (s, _cleanup) = make_sink();
        s.set_property("setup-auth-token", "AP8B");
        s.set_property("setup-auth-token-type", 16u64);
        let p = pad(&s, false);
        let transitioning = s.clone();
        let prepared = p.clone();
        admission_interleave(
            s.upcast_ref(),
            change,
            || s.imp().settings.try_lock().is_err(),
            move || {
                assert!(transitioning.set_state(gst::State::Paused).is_ok());
                let mut ts = transitioning.imp().track_lock();
                ts.preparing_tracks = Some(initial_snapshot(&prepared));
                let generation = transitioning.imp().obj_lock().flush_generation;
                let result = transitioning.imp().open_sender(&mut ts, generation);
                ts.preparing_tracks = None;
                drop(ts);
                assert_eq!(result, Ok(()));
            },
        );
        change.assert_snapshot(&observe_control(|c| c.setup_auth.clone()));
        release(&s, p);
        finish(s);
    }
    let (s, _cleanup) = make_sink();
    s.set_property("setup-auth-token", "AP8B");
    s.set_property("setup-auth-token-type", 16u64);
    let transitioning = s.clone();
    transition_wins_interleave(s.upcast_ref(), move || {
        assert!(transitioning.set_state(gst::State::Paused).is_ok());
    });
    let p = pad(&s, true);
    let captured = observe_control(|c| c.setup_auth.clone());
    assert_eq!(captured[0].token_type, 16);
    assert_eq!(captured[0].value, [0, 255, 1]);
    release(&s, p);
    finish(s);
}

#[test]
fn draft_negotiation_sink_default_and_exact() {
    use moq5::{Version, VersionOffer};
    let _suite = suite();
    for selected in [None, Some(0u32), Some(22), Some(18), Some(16)] {
        let (s, _cleanup) = make_sink();
        assert_eq!(
            s.property::<u32>("draft"),
            0,
            "newest-first native AUTO is the default"
        );
        if let Some(draft) = selected {
            s.set_property("draft", draft);
        }
        s.set_property("setup-auth-token", "AP8B");
        s.set_property("setup-auth-token-type", 16u64);
        let before = observe_control(|c| c.connects);
        let p = pad(&s, true);
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
        release(&s, p);
        finish(s);
    }
}
