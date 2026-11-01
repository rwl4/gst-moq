//! The track barrier: when `moqsink` opens a session, and with which tracks.
//!
//! Port of tests/test_sink_barrier.c. The element is the real one; the libmoq
//! service is the double in [`crate::service::fake`], which records an ordered
//! log of what the element asked for, so "connected too early" is an ordering
//! violation rather than a race.

use gst::prelude::*;
use gst::subclass::prelude::ObjectSubclassIsExt;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::MoqSink;
use crate::service::fake::{
    control, log_add, log_count, log_find, release_attach, suite as fake_suite,
    wait_attach_entered, Op, Suite as FakeSuite,
};
use crate::test_support::{bytes, span};
use moq5::sys::{
    moq_cmaf_fragment_info_init, moq_cmaf_fragment_info_t, moq_cmaf_init_info_init_sized,
    moq_cmaf_init_info_t, moq_cmaf_parse_fragment, moq_cmaf_parse_init_sized, moq_cmaf_sample_t,
    MOQ_OK,
};

/// Read owned observations before an assertion can unwind the CONTROL mutex.
fn observe_control<T>(read: impl FnOnce(&crate::service::fake::Control) -> T) -> T {
    let state = control();
    read(&state)
}

// -- fixture -------------------------------------------------------------------

type Suite = FakeSuite;

fn suite() -> Suite {
    fake_suite()
}

/// The refIDs the element declared on the track called `name`, comma joined.
fn track_refs(name: &str) -> Option<String> {
    control()
        .added_cfgs
        .iter()
        .find(|cfg| cfg.name == name)
        .map(|cfg| cfg.cp_ref_ids.join(","))
}

/// Poll until `done` holds or a second passes.
fn wait_until(done: impl Fn() -> bool) -> bool {
    let limit = Instant::now() + Duration::from_secs(1);
    while !done() {
        if Instant::now() >= limit {
            return false;
        }
        std::thread::yield_now();
    }
    true
}

/// Set once the element logs that it is parked in the barrier.
static BARRIER_WAITING: AtomicBool = AtomicBool::new(false);

/// avcC: version 1, profile 0x42, compat 0xc0, level 0x1e. prepare_loc_track's
/// stream-format=avc path needs only these four bytes to be well-formed.
const AVCC: [u8; 6] = [1, 0x42, 0xc0, 0x1e, 0xff, 0xe1];
/// AudioSpecificConfig: AOT 2 (AAC-LC), 48 kHz, stereo.
const ASC: [u8; 2] = [0x11, 0x90];

fn video_caps() -> gst::Caps {
    gst::Caps::builder("video/x-h264")
        .field("stream-format", "avc")
        .field("alignment", "au")
        .field("width", 1280i32)
        .field("height", 720i32)
        .field("framerate", gst::Fraction::new(24, 1))
        .field("codec_data", gst::Buffer::from_slice(AVCC))
        .build()
}

fn audio_caps() -> gst::Caps {
    gst::Caps::builder("audio/mpeg")
        .field("mpegversion", 4i32)
        .field("stream-format", "raw")
        .field("rate", 48000i32)
        .field("channels", 2i32)
        .field("codec_data", gst::Buffer::from_slice(ASC))
        .build()
}

/// A src pad linked to `name` on `sink`, activated, with the sticky events a
/// chain function is entitled to assume.
fn open_src_pad(sink: &MoqSink, name: &str, stream_id: &str, caps: gst::Caps) -> gst::Pad {
    let src = gst::Pad::builder(gst::PadDirection::Src).build();
    let peer = sink.static_pad(name).expect("sink pad");
    src.link(&peer).unwrap();

    src.set_active(true).unwrap();
    assert!(src.push_event(gst::event::StreamStart::new(stream_id)));
    src.push_event(gst::event::Caps::new(&caps));
    // push_event returns TRUE for a caps event the peer REJECTED -- it only
    // marks the sticky event pending -- so check negotiation explicitly.
    // Without this a caps mismatch surfaces later as a confusing flow error
    // from the chain function instead of a failure here.
    assert!(peer.has_current_caps());

    let seg = gst::FormattedSegment::<gst::ClockTime>::new();
    assert!(src.push_event(gst::event::Segment::new(&seg)));
    src
}

fn close_source(src: gst::Pad) {
    let _ = src.set_active(false);
}

/// A keyframe (no DELTA_UNIT flag) at `pts`. Payload content is irrelevant on
/// the stream-format=avc path -- the avcC comes from the caps.
fn keyframe(pts: gst::ClockTime, len: usize) -> gst::Buffer {
    let mut b = gst::Buffer::from_slice(vec![0u8; len]);
    {
        let b = b.get_mut().unwrap();
        b.set_pts(pts);
        b.set_dts(pts);
    }
    b
}

fn new_sink() -> (MoqSink, crate::test_support::NullCleanup) {
    let sink = gst::glib::Object::builder::<MoqSink>()
        // Never dialled: moq_endpoint_connect is mocked.
        .property("host", "127.0.0.1")
        .property("port", 1u32)
        // No clock waits -- the barrier is what this test times, nothing else.
        .property("sync", false)
        .build();
    let cleanup = crate::test_support::NullCleanup::new(&sink);
    (sink, cleanup)
}

// -- a push on its own streaming thread ----------------------------------------

#[derive(Default)]
struct JobState {
    entered: bool,
    done: bool,
    ret: Option<Result<gst::FlowSuccess, gst::FlowError>>,
}

/// A case-owned worker: explicit join preserves the current pipeline, while
/// unwinding cancels the fixture before waiting for its worker to settle.
pub(super) struct PushThread<T = ()> {
    handle: Option<JoinHandle<T>>,
    target: gst::Element,
}
impl<T: Send + 'static> PushThread<T> {
    pub(super) fn spawn(
        target: &impl IsA<gst::Element>,
        job: impl FnOnce() -> T + Send + 'static,
    ) -> Self {
        Self {
            handle: Some(std::thread::spawn(job)),
            target: target.upcast_ref::<gst::Element>().clone(),
        }
    }
    fn thread(&self) -> &std::thread::Thread {
        self.handle.as_ref().expect("unjoined worker").thread()
    }
    pub(super) fn join(mut self) -> std::thread::Result<T> {
        self.handle.take().expect("unjoined worker").join()
    }
}
impl<T> Drop for PushThread<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            release_attach();
            let null = self.target.set_state(gst::State::Null);
            let joined = handle.join();
            if std::thread::panicking() {
                if null.is_err() || joined.is_err() {
                    eprintln!("WARNING: fixture worker cleanup failed during unwind");
                }
            } else {
                assert!(null.is_ok(), "fixture worker NULL transition failed");
                assert!(joined.is_ok(), "fixture worker panicked during cleanup");
            }
        }
    }
}

/// A callback can retain a slot clone; the case still settles its worker
/// before clock/serial fixture guards are released.
pub(super) struct PendingThread(Arc<Mutex<Option<PushThread>>>);
impl PendingThread {
    pub(super) fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }
    pub(super) fn slot(&self) -> Arc<Mutex<Option<PushThread>>> {
        self.0.clone()
    }
    pub(super) fn join(&self) -> std::thread::Result<()> {
        let worker = self.0.lock().unwrap().take();
        worker.expect("callback started its worker").join()
    }
}
impl Drop for PendingThread {
    fn drop(&mut self) {
        let worker = self.0.lock().unwrap().take();
        drop(worker);
    }
}

struct PushJob {
    state: Mutex<JobState>,
    cond: Condvar,
}

impl PushJob {
    fn start(src: &gst::Pad, buffer: gst::Buffer) -> (Arc<PushJob>, PushThread) {
        let job = Arc::new(PushJob {
            state: Mutex::new(JobState::default()),
            cond: Condvar::new(),
        });
        let target = src
            .peer()
            .and_then(|peer| peer.parent_element())
            .expect("linked fixture element");
        let (j, src) = (job.clone(), src.clone());
        let th = PushThread::spawn(&target, move || {
            {
                j.state.lock().unwrap().entered = true;
                j.cond.notify_all();
            }
            let ret = src.push(buffer);
            let mut s = j.state.lock().unwrap();
            s.ret = Some(ret);
            s.done = true;
            j.cond.notify_all();
        });
        (job, th)
    }

    fn wait_entered(&self) {
        let mut s = self.state.lock().unwrap();
        while !s.entered {
            s = self.cond.wait(s).unwrap();
        }
    }

    /// Wait up to `limit` for the push to finish; true if it did.
    fn wait_done(&self, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        let mut s = self.state.lock().unwrap();
        while !s.done {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            s = self.cond.wait_timeout(s, deadline - now).unwrap().0;
        }
        s.done
    }

    fn join(self: Arc<Self>, th: PushThread, expect: Result<gst::FlowSuccess, gst::FlowError>) {
        th.join().unwrap();
        let result = self.state.lock().unwrap().ret;
        assert_eq!(result, Some(expect));
    }
}

const OK: Result<gst::FlowSuccess, gst::FlowError> = Ok(gst::FlowSuccess::Ok);

fn ms(v: u64) -> gst::ClockTime {
    gst::ClockTime::from_mseconds(v)
}

// -- tests -----------------------------------------------------------------------

/// Audio's first buffer arrives first and must NOT open the session: the
/// catalog cannot be published until the video pad has produced its track too.
#[test]
fn initial_catalog_complete() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();

    // PAUSED first: the element activates its sink pads on READY->PAUSED, and
    // an inactive pad rejects the sticky events open_src_pad pushes.
    assert!(sink.set_state(gst::State::Paused).is_ok());

    let asrc = open_src_pad(&sink, "audio", "audio-stream", audio_caps());
    let vsrc = open_src_pad(&sink, "video", "video-stream", video_caps());

    // Audio first, on its own streaming thread: it must park in the barrier.
    let (job, th) = PushJob::start(&asrc, keyframe(gst::ClockTime::ZERO, 64));
    job.wait_entered();
    // SECONDARY, bounded: the push must still be in flight. A barrier that
    // released on one pad would have completed it by now.
    assert!(!job.wait_done(Duration::from_millis(250)));

    // PRIMARY: everything the element does from here on is logged after this
    // marker, so an early connect is an ordering violation, not a race.
    log_add(Op::Mark, Some("push-video"));
    assert_eq!(vsrc.push(keyframe(gst::ClockTime::ZERO, 64)), OK);

    job.join(th, OK);

    let mark = log_find(Op::Mark, Some("push-video"));
    let connect = log_find(Op::Connect, None);
    let attach = log_find(Op::Attach, None);
    assert!(mark >= 0);
    assert!(connect > mark);
    assert!(attach > connect);

    // One session, both tracks, and both added before anything is written:
    // the session cannot reach ESTABLISHED with a partial track set.
    assert_eq!(log_count(Op::Connect), 1);
    assert_eq!(log_count(Op::Attach), 1);
    assert_eq!(log_count(Op::AddTrack), 2);

    let audio_track = log_find(Op::AddTrack, Some("audio"));
    let video_track = log_find(Op::AddTrack, Some("video"));
    assert!(audio_track > attach);
    assert!(video_track > attach);

    let first_write = log_find(Op::Write, None);
    assert!(first_write > audio_track);
    assert!(first_write > video_track);

    let _ = sink.set_state(gst::State::Null);
    close_source(asrc);
    close_source(vsrc);
}

/// The always pads exist even when a pipeline feeds only one of them. An
/// unlinked pad must not hold the barrier shut -- and this is what keeps the
/// ordering assertion above honest: here CONNECT *does* land on the first
/// buffer, so the oracle can observe an early connect when one is correct.
#[test]
fn unlinked_pad_excluded() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();

    assert!(sink.set_state(gst::State::Paused).is_ok());

    // "audio" deliberately left unlinked.
    let vsrc = open_src_pad(&sink, "video", "video-stream", video_caps());

    log_add(Op::Mark, Some("push-video"));
    assert_eq!(vsrc.push(keyframe(gst::ClockTime::ZERO, 64)), OK);

    let mark = log_find(Op::Mark, Some("push-video"));
    let connect = log_find(Op::Connect, None);
    assert!(connect > mark);
    assert_eq!(log_count(Op::Connect), 1);
    assert_eq!(log_count(Op::AddTrack), 1);
    assert!(log_find(Op::AddTrack, Some("video")) >= 0);
    assert_eq!(log_find(Op::AddTrack, Some("audio")), -1);

    let _ = sink.set_state(gst::State::Null);
    close_source(vsrc);
}

#[test]
fn timeout_no_partial() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    sink.set_property("catalog-wait-ms", 10u32);
    let bus = gst::Bus::new();
    sink.set_bus(Some(&bus));
    let _ = sink.set_state(gst::State::Paused);
    let asrc = open_src_pad(&sink, "audio", "a", audio_caps());
    let vsrc = open_src_pad(&sink, "video", "v", video_caps());
    assert_eq!(
        asrc.push(keyframe(gst::ClockTime::ZERO, 64)),
        Err(gst::FlowError::Error)
    );
    assert_eq!(log_count(Op::Connect), 0);
    assert_eq!(log_count(Op::Write), 0);
    assert!(bus.pop_filtered(&[gst::MessageType::Error]).is_some());
    let _ = sink.set_state(gst::State::Null);
    close_source(asrc);
    close_source(vsrc);
    sink.set_bus(None);
}

#[test]
fn empty_eos() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    let bus = gst::Bus::new();
    sink.set_bus(Some(&bus));
    let _ = sink.set_state(gst::State::Paused);
    let src = open_src_pad(&sink, "video", "v", video_caps());
    assert!(src.push_event(gst::event::Eos::new()));
    assert_eq!(log_count(Op::Connect), 0);
    assert_eq!(log_count(Op::Complete), 0);
    let m = bus
        .pop_filtered(&[gst::MessageType::Eos, gst::MessageType::Error])
        .expect("a message");
    assert_eq!(m.type_(), gst::MessageType::Eos);
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
    sink.set_bus(None);
}

fn flush_cancels(state_change: bool) {
    let _suite = suite();
    BARRIER_WAITING.store(false, Ordering::SeqCst);
    let observe = gst::log::add_log_function(|_cat, _level, _file, _fn, _line, _obj, msg| {
        if msg
            .get()
            .is_some_and(|m| m.as_str() == "waiting for linked track set")
        {
            BARRIER_WAITING.store(true, Ordering::SeqCst);
        }
    });
    let cat = *super::imp::CAT;
    let previous = cat.threshold();
    cat.set_threshold(gst::DebugLevel::Debug);
    let log_cleanup = crate::test_support::LogCleanup::new(observe, cat, previous);

    let (sink, _null_sink) = new_sink();
    sink.set_property("catalog-wait-ms", 5000u32);
    let _ = sink.set_state(gst::State::Paused);
    let asrc = open_src_pad(&sink, "audio", "a", audio_caps());
    let vsrc = open_src_pad(&sink, "video", "v", video_caps());
    let (job, th) = PushJob::start(&asrc, keyframe(gst::ClockTime::ZERO, 64));
    assert!(wait_until(|| BARRIER_WAITING.load(Ordering::SeqCst)));
    if state_change {
        assert!(sink.set_state(gst::State::Ready).is_ok());
    } else {
        assert!(asrc.push_event(gst::event::FlushStart::new()));
    }
    assert!(job.wait_done(Duration::from_secs(1)));
    job.join(th, Err(gst::FlowError::Flushing));
    assert_eq!(log_count(Op::Connect), 0);
    let _ = sink.set_state(gst::State::Null);
    close_source(asrc);
    close_source(vsrc);
    drop(log_cleanup);
}

#[test]
fn flush_cancels_barrier() {
    flush_cancels(false);
}

#[test]
fn state_cancels_barrier() {
    flush_cancels(true);
}

/// A startup that fails at the service: `attach` refused, or the first
/// `add_track` refused. Either way no media may be written.
///
fn start_failure(attach: bool) {
    let _suite = suite();
    {
        let mut c = control();
        c.fail_attach = attach;
        c.fail_add = if attach { 0 } else { 1 };
    }
    let (sink, _null_sink) = new_sink();
    let _ = sink.set_state(gst::State::Paused);
    let src = open_src_pad(&sink, "video", "v", video_caps());
    assert_eq!(
        src.push(keyframe(gst::ClockTime::ZERO, 64)),
        Err(gst::FlowError::Error)
    );
    assert_eq!(log_count(Op::Write), 0);
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
}

#[test]
fn attach_failed() {
    start_failure(true);
}

#[test]
fn add_failed() {
    start_failure(false);
}

#[test]
fn partial_add_failed() {
    let _suite = suite();
    control().fail_add = 2;
    let (sink, _null_sink) = new_sink();
    let _ = sink.set_state(gst::State::Paused);
    let asrc = open_src_pad(&sink, "audio", "a", audio_caps());
    let vsrc = open_src_pad(&sink, "video", "v", video_caps());
    let (job, th) = PushJob::start(&asrc, keyframe(gst::ClockTime::ZERO, 64));
    assert_eq!(
        vsrc.push(keyframe(gst::ClockTime::ZERO, 64)),
        Err(gst::FlowError::Error)
    );
    job.join(th, Err(gst::FlowError::Error));
    assert_eq!(observe_control(|c| c.added_tracks), 2);
    assert_eq!(log_count(Op::Write), 0);
    let _ = sink.set_state(gst::State::Null);
    close_source(asrc);
    close_source(vsrc);
}

#[test]
fn running_start_cancel() {
    let _suite = suite();
    control().hold_attach = true;
    let (sink, _null_sink) = new_sink();
    let bus = gst::Bus::new();
    sink.set_bus(Some(&bus));
    let _ = sink.set_state(gst::State::Paused);
    let src = open_src_pad(&sink, "video", "v", video_caps());
    let (job, th) = PushJob::start(&src, keyframe(gst::ClockTime::ZERO, 64));
    wait_attach_entered();
    let sinkpad = sink.static_pad("video").unwrap();
    // The pad's public flushing flag is set by FLUSH_START before the handler
    // runs; release the held attach only once it is.
    let release = PushThread::spawn(&sink, move || {
        let limit = Instant::now() + Duration::from_secs(1);
        while !sinkpad.pad_flags().contains(gst::PadFlags::FLUSHING) && Instant::now() < limit {
            std::thread::yield_now();
        }
        assert!(sinkpad.pad_flags().contains(gst::PadFlags::FLUSHING));
        release_attach();
    });
    assert!(src.push_event(gst::event::FlushStart::new()));
    release.join().unwrap();
    job.join(th, Err(gst::FlowError::Flushing));
    assert_eq!(log_count(Op::Write), 0);
    assert!(bus.pop_filtered(&[gst::MessageType::Error]).is_none());
    let observed = !sink.imp().track_lock().tracks_failed;
    assert!(observed);
    assert!(src.push_event(gst::event::FlushStop::new(false)));
    let seg = gst::FormattedSegment::<gst::ClockTime>::new();
    assert!(src.push_event(gst::event::Segment::new(&seg)));
    assert_eq!(src.push(keyframe(gst::ClockTime::ZERO, 64)), OK);
    assert_eq!(log_count(Op::Write), 1);
    assert!(bus.pop_filtered(&[gst::MessageType::Error]).is_none());
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
}

/// mode 0: truncated ASC; 1: HE-AAC ASC; 2: caps rate disagrees with the ASC.
fn bad_audio_config(mode: u32) {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    let _ = sink.set_state(gst::State::Paused);
    let mut caps = audio_caps();
    {
        let config = [if mode == 1 { 0x2a } else { 0x11 }, 0x90];
        let len = if mode == 0 { 1 } else { 2 };
        let caps = caps.make_mut();
        caps.set(
            "codec_data",
            gst::Buffer::from_slice(config[..len].to_vec()),
        );
        if mode == 2 {
            caps.set("rate", 44100i32);
        }
    }
    let src = open_src_pad(&sink, "audio", "a", caps);
    assert_eq!(
        src.push(keyframe(gst::ClockTime::ZERO, 64)),
        Err(gst::FlowError::Error)
    );
    assert_eq!(log_count(Op::Connect), 0);
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
}

#[test]
fn audio_truncated() {
    bad_audio_config(0);
}

#[test]
fn audio_he_rejected() {
    bad_audio_config(1);
}

#[test]
fn audio_caps_mismatch() {
    bad_audio_config(2);
}

fn push_segment(src: &gst::Pad, offset: gst::ClockTime) {
    let mut seg = gst::FormattedSegment::<gst::ClockTime>::new();
    seg.set_start(offset);
    seg.set_position(offset);
    assert!(src.push_event(gst::event::Segment::new(&seg)));
}

#[test]
fn capture_and_groups() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    let _ = sink.set_state(gst::State::Paused);
    let asrc = open_src_pad(&sink, "audio", "a", audio_caps());
    let vsrc = open_src_pad(&sink, "video", "v", video_caps());
    // Common encoder offset: video PTS is large but running time is zero.
    let offset = gst::ClockTime::from_seconds(3_600_000);
    push_segment(&vsrc, offset);
    let (job, th) = PushJob::start(&asrc, keyframe(gst::ClockTime::ZERO, 64));
    assert_eq!(vsrc.push(keyframe(offset, 64)), OK);
    job.join(th, OK);
    {
        let (video, audio) = observe_control(|c| (c.capture_video, c.capture_audio));
        assert_eq!(video, audio);
    }
    let before = control().audio_groups;
    assert_eq!(asrc.push(keyframe(ms(20), 64)), OK);
    let after = control().audio_groups;
    assert_eq!(asrc.push(keyframe(ms(40), 64)), OK);
    assert_eq!(observe_control(|c| c.audio_groups), after); // no group for every AAC AU
    assert!(after <= before + 1);
    assert_eq!(vsrc.push(keyframe(offset + ms(100), 64)), OK);
    assert_eq!(asrc.push(keyframe(ms(120), 64)), OK);
    assert_eq!(observe_control(|c| c.audio_groups), after + 1);
    let _ = sink.set_state(gst::State::Null);
    close_source(asrc);
    close_source(vsrc);
}

// -- CMSF contentProtections (CMSF 4.1.1) ----------------------------------------

/// A CMAF init segment, optionally CENC-protected:
///   ftyp + moov(trak(tkhd mdia(mdhd minf(stbl(stsd(
///       <avc1|encv>(VisualSampleEntry avcC [sinf(frma schm schi(tenc))]))))))
/// Layout mirrors libmoq's own cmaf parser expectations; the first assertion
/// of every test below is that libmoq parses this back, so a wrong fixture
/// fails loudly rather than quietly weakening the test.
const FIXTURE_AVCC: [u8; 4] = [0x01, 0x42, 0xe0, 0x1e];

fn box_hdr(out: &mut Vec<u8>, size: usize, ty: &[u8; 4]) {
    out.extend_from_slice(&(size as u32).to_be_bytes());
    out.extend_from_slice(ty);
}

fn be32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn build_cmaf_init_entry(
    timescale: u32,
    w: u16,
    h: u16,
    protected: bool,
    scheme: &[u8; 4],
    kid: &[u8; 16],
    sample_entry: &[u8; 4],
) -> Vec<u8> {
    let avcc_box = 8 + FIXTURE_AVCC.len();
    // ISO/IEC 23001-7 8.2.2 TrackEncryptionBox body: version/flags(4) +
    // reserved(1) + reserved-or-pattern(1) + default_isProtected(1) +
    // default_Per_Sample_IV_Size(1) + default_KID(16) = 24. TWO bytes precede
    // isProtected, not one -- a fixture that writes 23 shifts the KID by a byte
    // and silently agrees with a parser that reads it one byte early.
    let tenc_box = if protected { 8 + 24 } else { 0 };
    let schi_box = if protected { 8 + tenc_box } else { 0 };
    let schm_box = if protected { 20 } else { 0 }; // hdr + v/f + 4cc + version
    let frma_box = if protected { 12 } else { 0 }; // hdr + 4cc
    let sinf_box = if protected {
        8 + frma_box + schm_box + schi_box
    } else {
        0
    };
    let entry_box = 8 + 78 + avcc_box + sinf_box;
    let stsd_box = 8 + 8 + entry_box;
    let stbl_box = 8 + stsd_box;
    let minf_box = 8 + stbl_box;
    let mdhd_box = 28;
    let mdia_box = 8 + mdhd_box + minf_box;
    let tkhd_box = 24;
    let trak_box = 8 + tkhd_box + mdia_box;
    let moov_box = 8 + trak_box;
    let mut o = Vec::new();

    box_hdr(&mut o, 20, b"ftyp");
    o.extend_from_slice(b"isom");
    be32(&mut o, 0);
    o.extend_from_slice(b"isom");

    box_hdr(&mut o, moov_box, b"moov");
    box_hdr(&mut o, trak_box, b"trak");
    box_hdr(&mut o, tkhd_box, b"tkhd");
    o.extend_from_slice(&[0; 12]); // v/f + creation + modification
    be32(&mut o, 1); // track_ID

    box_hdr(&mut o, mdia_box, b"mdia");
    box_hdr(&mut o, mdhd_box, b"mdhd");
    o.extend_from_slice(&[0; 12]); // v/f + creation + modification
    be32(&mut o, timescale);
    be32(&mut o, 0); // duration

    box_hdr(&mut o, minf_box, b"minf");
    box_hdr(&mut o, stbl_box, b"stbl");
    box_hdr(&mut o, stsd_box, b"stsd");
    be32(&mut o, 0); // v/f
    be32(&mut o, 1); // entry_count

    box_hdr(
        &mut o,
        entry_box,
        if protected { b"encv" } else { sample_entry },
    );
    let mut entry = [0u8; 78];
    entry[24..26].copy_from_slice(&w.to_be_bytes());
    entry[26..28].copy_from_slice(&h.to_be_bytes());
    o.extend_from_slice(&entry);

    box_hdr(&mut o, avcc_box, b"avcC");
    o.extend_from_slice(&FIXTURE_AVCC);

    if protected {
        box_hdr(&mut o, sinf_box, b"sinf");
        box_hdr(&mut o, frma_box, b"frma");
        o.extend_from_slice(sample_entry); // effective original format
        box_hdr(&mut o, schm_box, b"schm");
        be32(&mut o, 0); // v/f
        o.extend_from_slice(scheme); // scheme_type
        be32(&mut o, 0); // scheme_version
        box_hdr(&mut o, schi_box, b"schi");
        box_hdr(&mut o, tenc_box, b"tenc");
        be32(&mut o, 0); // v/f (version 0)
        o.push(0); // reserved
        o.push(0); // reserved (pattern in v1)
        o.push(1); // default_isProtected
        o.push(8); // default_Per_Sample_IV_Size
        o.extend_from_slice(kid); // default_KID
    }
    o
}

/// moof(mfhd traf(tfhd tfdt trun)) + mdat. The splitter only emits the INIT
/// unit once a moof arrives (an init segment alone is still accumulating), so
/// a CMAF pad needs one chunk before the barrier can see its track.
fn build_cmaf_chunk(track_id: u32, first_sample_flags: u32) -> Vec<u8> {
    // FullBox with a single payload word, as the CMAF fragment parser expects.
    let mut inner = Vec::new();
    box_hdr(&mut inner, 16, b"tfhd");
    be32(&mut inner, 0); // version + flags
    be32(&mut inner, track_id);
    box_hdr(&mut inner, 16, b"tfdt");
    be32(&mut inner, 0);
    be32(&mut inner, 0); // baseMediaDecodeTime
    box_hdr(&mut inner, 20, b"trun");
    be32(&mut inner, 0x0000_0400); // first-sample-flags
    be32(&mut inner, 1); // sample_count
    be32(&mut inner, first_sample_flags);

    let moof_body = 16 /* mfhd */ + 8 + inner.len();
    let mut o = Vec::new();
    box_hdr(&mut o, 8 + moof_body, b"moof");
    box_hdr(&mut o, 16, b"mfhd");
    be32(&mut o, 0);
    be32(&mut o, 1); // sequence_number
    box_hdr(&mut o, 8 + inner.len(), b"traf");
    o.extend_from_slice(&inner);
    box_hdr(&mut o, 9, b"mdat");
    o.push(0x00);
    o
}

const FIXTURE_KID: [u8; 16] = [
    0x10, 0x77, 0xef, 0xec, 0xc0, 0xb2, 0x4d, 0x02, 0xac, 0xe3, 0x3c, 0x1e, 0x52, 0xe2, 0xfb, 0x4b,
];
const FIXTURE_KID_UUID: &str = "1077efec-c0b2-4d02-ace3-3c1e52e2fb4b";
const WIDEVINE_UUID: &str = "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed";
const PLAYREADY_UUID: &str = "9a04f079-9840-4286-ab92-e65be0885f95";

/// Drive one CMAF request pad with an init segment and let the barrier commit.
fn push_cmaf_init(
    sink: &MoqSink,
    protected: bool,
    scheme: &[u8; 4],
) -> Result<gst::FlowSuccess, gst::FlowError> {
    push_cmaf_init_flags(sink, protected, scheme, 0)
}

fn push_cmaf_init_flags(
    sink: &MoqSink,
    protected: bool,
    scheme: &[u8; 4],
    first_flags: u32,
) -> Result<gst::FlowSuccess, gst::FlowError> {
    push_cmaf_init_entry(sink, protected, scheme, first_flags, b"avc1")
}

fn push_cmaf_init_entry(
    sink: &MoqSink,
    protected: bool,
    scheme: &[u8; 4],
    first_flags: u32,
    entry: &[u8; 4],
) -> Result<gst::FlowSuccess, gst::FlowError> {
    let init = build_cmaf_init_entry(90000, 1920, 1080, protected, scheme, &FIXTURE_KID, entry);
    let parsed = moq5::cmaf::CmafInit::parse(init.as_slice()).unwrap();
    assert_eq!(parsed.info().sample_entry(), Some(*entry));

    // The fixture must be what libmoq thinks it is, or nothing below means
    // anything.
    let mut probe = moq_cmaf_init_info_t::default();
    unsafe {
        moq_cmaf_init_info_init_sized(&mut probe, std::mem::size_of::<moq_cmaf_init_info_t>());
        assert_eq!(
            moq_cmaf_parse_init_sized(
                span(&init),
                &mut probe,
                std::mem::size_of::<moq_cmaf_init_info_t>()
            ),
            MOQ_OK
        );
    }
    assert_eq!(probe.has_cenc, protected);
    if protected {
        assert_eq!(probe.default_kid.len, 16);
        assert_eq!(unsafe { bytes(probe.default_kid) }, FIXTURE_KID);
        assert_eq!(&unsafe { bytes(probe.scheme) }[..4], scheme);
    }

    assert!(sink.set_state(gst::State::Paused).is_ok());

    // A video_%u request pad defaults its track-name to "video".
    let peer = sink.request_pad_simple("video_%u").expect("request pad");

    let src = gst::Pad::builder(gst::PadDirection::Src).build();
    src.link(&peer).unwrap();
    src.set_active(true).unwrap();
    assert!(src.push_event(gst::event::StreamStart::new("cmaf-stream")));
    src.push_event(gst::event::Caps::new(&gst::Caps::new_empty_simple(
        "video/quicktime",
    )));
    assert!(peer.has_current_caps());
    let seg = gst::FormattedSegment::<gst::ClockTime>::new();
    assert!(src.push_event(gst::event::Segment::new(&seg)));

    let mut b = gst::Buffer::from_slice(init);
    b.get_mut().unwrap().set_pts(gst::ClockTime::ZERO);
    assert_eq!(src.push(b), OK);

    // One sync chunk: this is what makes the splitter release the init, which
    // runs prepare -> barrier -> attach -> add_track.
    let chunk = build_cmaf_chunk(1, first_flags);
    let mut samples = [moq_cmaf_sample_t::default(); 8];
    let mut fi = moq_cmaf_fragment_info_t::default();
    unsafe {
        moq_cmaf_fragment_info_init(&mut fi, samples.as_mut_ptr(), samples.len());
        assert_eq!(moq_cmaf_parse_fragment(span(&chunk), &mut fi), MOQ_OK);
    }

    let mut cb = gst::Buffer::from_slice(chunk);
    cb.get_mut().unwrap().set_pts(gst::ClockTime::ZERO);
    let result = src.push(cb);

    let _ = sink.set_state(gst::State::Null);
    let _ = src.set_active(false);
    result
}

/// The contentProtections the element authored on the sender.
fn cps() -> Vec<moq5::ContentProtection> {
    control().attached_cps.clone()
}

/// The scheme and KID are never configured -- they come from the init
/// segment's own sinf/schm/schi(tenc). drm-systems supplies only what an fMP4
/// cannot.
#[test]
fn cp_authored_from_init() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    sink.set_property(
        "drm-systems",
        format!("{WIDEVINE_UUID},laurl=https://lic.example/wv,pssh=AAAAQ3Bzc2g="),
    );

    assert_eq!(push_cmaf_init(&sink, true, b"cenc"), OK);

    let cps = cps();
    assert_eq!(cps.len(), 1);
    assert_eq!(cps[0].ref_id, "cp0");
    // Derived from the tenc default_KID: raw 16 bytes -> CMSF UUID string.
    assert_eq!(cps[0].default_kids, [FIXTURE_KID_UUID]);
    assert_eq!(cps[0].scheme, "cenc");
    assert_eq!(cps[0].drm_system.system_id, WIDEVINE_UUID);
    assert_eq!(
        cps[0].drm_system.la_url.as_ref().map(|u| u.url.as_str()),
        Some("https://lic.example/wv")
    );
    // The '=' padding of a base64 PSSH must survive the key=value split.
    assert_eq!(cps[0].drm_system.pssh.as_deref(), Some("AAAAQ3Bzc2g="));

    assert_eq!(track_refs("video").as_deref(), Some("cp0"));
}

/// CMSF carries one drmSystem per entry, so two systems over one key means two
/// entries sharing the KID, and the track references both.
#[test]
fn cp_two_systems_one_key() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    sink.set_property(
        "drm-systems",
        format!(
            "{WIDEVINE_UUID},laurl=https://lic.example/wv;{PLAYREADY_UUID},laurl=https://lic.example/pr"
        ),
    );

    assert_eq!(push_cmaf_init(&sink, true, b"cbcs"), OK);

    let cps = cps();
    assert_eq!(cps.len(), 2);
    assert_eq!(cps[0].ref_id, "cp0");
    assert_eq!(cps[1].ref_id, "cp1");
    assert_eq!(cps[0].drm_system.system_id, WIDEVINE_UUID);
    assert_eq!(cps[1].drm_system.system_id, PLAYREADY_UUID);
    // One key, carried by both, and the scheme comes from schm, not config.
    assert_eq!(cps[0].default_kids, [FIXTURE_KID_UUID]);
    assert_eq!(cps[1].default_kids, [FIXTURE_KID_UUID]);
    assert_eq!(cps[0].scheme, "cbcs");
    assert_eq!(cps[1].scheme, "cbcs");

    assert_eq!(track_refs("video").as_deref(), Some("cp0,cp1"));
}

/// CMSF 4.1.2: absence of protection references means clear content. Refuse an
/// encrypted stream before connecting when its DRM system is unknown.
#[test]
fn cp_encrypted_without_systems() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();

    assert_eq!(
        push_cmaf_init(&sink, true, b"cenc"),
        Err(gst::FlowError::Error)
    );

    assert!(cps().is_empty());
    assert_eq!(log_count(Op::Connect), 0);
    assert_eq!(log_count(Op::Attach), 0);
    assert_eq!(log_count(Op::Write), 0);
    assert!(observe_control(|c| c.added_cfgs.is_empty()));
}

/// A clear track must never be described as protected, even with drm-systems
/// set: declaring refIDs on an unencrypted track would make libmoq reject the
/// add, and would lie to subscribers.
#[test]
fn cp_clear_track_declares_nothing() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    sink.set_property("drm-systems", WIDEVINE_UUID);

    assert_eq!(push_cmaf_init(&sink, false, b"cenc"), OK);

    assert!(cps().is_empty());
    assert_eq!(track_refs("video").as_deref(), Some(""));
}

#[test]
fn refused_startup_never_attaches_or_writes() {
    let _suite = suite();
    control().fail_post = true;
    let (sink, _null_sink) = new_sink();
    let _ = sink.set_state(gst::State::Paused);
    let src = open_src_pad(&sink, "video", "v", video_caps());
    assert_eq!(
        src.push(keyframe(gst::ClockTime::ZERO, 64)),
        Err(gst::FlowError::Error)
    );
    assert_eq!(log_count(Op::Post), 1);
    assert_eq!(log_count(Op::Attach), 0);
    assert_eq!(log_count(Op::Write), 0);
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
}

#[test]
fn queued_startup_is_cancelled_by_flush() {
    let _suite = suite();
    control().hold_startup = true;
    let (sink, _null_sink) = new_sink();
    let bus = gst::Bus::new();
    sink.set_bus(Some(&bus));
    let _ = sink.set_state(gst::State::Paused);
    let src = open_src_pad(&sink, "video", "v", video_caps());
    let (job, thread) = PushJob::start(&src, keyframe(gst::ClockTime::ZERO, 64));
    crate::service::fake::wait_startup_queued();
    assert!(src.push_event(gst::event::FlushStart::new()));
    job.join(thread, Err(gst::FlowError::Flushing));
    assert_eq!(log_count(Op::Attach), 0);
    assert_eq!(log_count(Op::Write), 0);
    assert!(bus.pop_filtered(&[gst::MessageType::Error]).is_none());
    let observed = !sink.imp().track_lock().tracks_failed;
    assert!(observed);
    control().hold_startup = false;
    assert!(src.push_event(gst::event::FlushStop::new(false)));
    let seg = gst::FormattedSegment::<gst::ClockTime>::new();
    assert!(src.push_event(gst::event::Segment::new(&seg)));
    assert_eq!(src.push(keyframe(gst::ClockTime::ZERO, 64)), OK);
    assert_eq!(log_count(Op::Attach), 1);
    assert_eq!(log_count(Op::Write), 1);
    assert!(bus.pop_filtered(&[gst::MessageType::Error]).is_none());
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
}

#[test]
fn cmaf_timeline_without_exact_sap_is_rejected_before_connect() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    sink.set_property("sap-timeline", true);
    assert_eq!(
        push_cmaf_init(&sink, false, b"cenc"),
        Err(gst::FlowError::Error)
    );
    assert_eq!(log_count(Op::Connect), 0);
    assert_eq!(log_count(Op::Write), 0);
}

#[test]
fn cmaf_sync_group_does_not_claim_exact_sap() {
    let _suite = suite();
    for (flags, sync) in [(0, true), (0x0001_0000, false)] {
        let (sink, _null_sink) = new_sink();
        let n = control().written_sap.len();
        assert_eq!(push_cmaf_init_flags(&sink, false, b"cenc", flags), OK);
        assert_eq!(
            observe_control(|c| c.written_sap.clone())[n],
            (sync, sync, None)
        );
    }
}

// Gst-owned application memory may invoke arbitrary safe application code on
// final release. Drive the real chain so no such destructor reaches native
// write/rejection, cancellation or endpoint teardown.
type MemoryReleases = Arc<Mutex<Vec<(std::thread::ThreadId, String)>>>;

struct ApplicationMemory {
    sink: MoqSink,
    releases: MemoryReleases,
    bytes: [u8; 64],
}

impl AsRef<[u8]> for ApplicationMemory {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for ApplicationMemory {
    fn drop(&mut self) {
        // Safe application reentry, deliberately acquires the sink settings
        // mutex rather than being restricted to a trivial byte deallocation.
        let host = self.sink.property::<String>("host");
        self.releases
            .lock()
            .unwrap()
            .push((std::thread::current().id(), host));
    }
}

fn application_memory_release(mode: u32) {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    assert!(sink.set_state(gst::State::Paused).is_ok());
    let src = open_src_pad(&sink, "video", "v", video_caps());
    let releases = Arc::new(Mutex::new(Vec::new()));
    let mut buffer = gst::Buffer::from_slice(ApplicationMemory {
        sink: sink.clone(),
        releases: releases.clone(),
        bytes: [0; 64],
    });
    buffer.get_mut().unwrap().set_pts(gst::ClockTime::ZERO);
    buffer.get_mut().unwrap().set_dts(gst::ClockTime::ZERO);
    let caller = if mode < 2 {
        if mode == 1 {
            control().refuse_writes = 1;
        }
        assert_eq!(src.push(buffer), OK);
        if mode == 0 {
            assert_eq!(log_count(Op::Write), 1);
        } else {
            assert_eq!(observe_control(|c| c.refusals), 1);
            assert_eq!(log_count(Op::Write), 0);
        }
        std::thread::current().id()
    } else {
        control().hold_startup = true;
        let (job, thread) = PushJob::start(&src, buffer);
        let caller = thread.thread().id();
        crate::service::fake::wait_startup_queued();
        let observed = releases.lock().unwrap().is_empty();
        assert!(observed);
        if mode == 2 {
            assert!(src.push_event(gst::event::FlushStart::new()));
        } else {
            assert!(sink.set_state(gst::State::Null).is_ok());
        }
        job.join(thread, Err(gst::FlowError::Flushing));
        assert_eq!(log_count(Op::Write), 0);
        caller
    };
    let actual_releases = releases.lock().unwrap().clone();
    assert_eq!(actual_releases, [(caller, "127.0.0.1".into())]);
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
    let observed = releases.lock().unwrap().len();
    assert_eq!(observed, 1);
}

#[test]
fn application_memory_accepted_release() {
    application_memory_release(0);
}

#[test]
fn application_memory_refused_release() {
    application_memory_release(1);
}

#[test]
fn application_memory_flush_release() {
    application_memory_release(2);
}

#[test]
fn application_memory_teardown_release() {
    application_memory_release(3);
}

// Flush handlers notify under the track lock. Hold that lock to force both
// START and STOP to update their object state before an old waiter can resume.
#[test]
fn completed_flush_cancels_old_track_waiter() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    sink.set_property("catalog-wait-ms", 1u32);
    assert!(sink.set_state(gst::State::Paused).is_ok());
    let bus = gst::Bus::new();
    sink.set_bus(Some(&bus));
    let src = open_src_pad(&sink, "video", "v", video_caps());
    let peer = sink
        .static_pad("video")
        .unwrap()
        .downcast::<super::MoqSinkPad>()
        .unwrap();
    let track_guard = sink.imp().track_lock();
    let old_generation = sink.imp().obj_lock().flush_generation;
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let waiter_sink = sink.clone();
    let waiter_pad = peer.clone();
    let waiter = PushThread::spawn(&sink, move || {
        // Snapshot the operation generation before the forced flush cycle.
        entered_tx.send(()).unwrap();
        waiter_sink
            .imp()
            .await_tracks_generation(&waiter_pad, old_generation)
    });
    let entered = entered_rx.recv_timeout(Duration::from_secs(1));
    let start_sink = sink.clone();
    let start_pad = peer.clone();
    let start = PushThread::spawn(&sink, move || {
        start_sink.imp().set_pad_flush(&start_pad, true)
    });
    let start_observed = wait_until(|| sink.imp().obj_lock().flush_count == 1);
    let stop_sink = sink.clone();
    let stop_pad = peer.clone();
    let stop = PushThread::spawn(&sink, move || {
        stop_sink.imp().set_pad_flush(&stop_pad, false)
    });
    let stop_observed = wait_until(|| sink.imp().obj_lock().flush_count == 0);
    drop(track_guard);
    start.join().unwrap();
    stop.join().unwrap();
    assert!(entered.is_ok());
    assert!(start_observed);
    assert!(stop_observed);
    assert_eq!(waiter.join().unwrap(), Err(gst::FlowError::Flushing));
    let observed = !sink.imp().track_lock().tracks_failed;
    assert!(observed);
    assert!(bus.pop_filtered(&[gst::MessageType::Error]).is_none());
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
}

fn interrupt_snapshot(
    sink: &MoqSink,
    pad: &super::MoqSinkPad,
    thread: &Arc<Mutex<Option<PushThread>>>,
) -> bool {
    let flushing_sink = sink.clone();
    let flushing_pad = pad.clone();
    *thread.lock().unwrap() = Some(PushThread::spawn(sink, move || {
        flushing_sink.imp().set_pad_flush(&flushing_pad, true)
    }));
    wait_until(|| sink.imp().obj_lock().flush_count == 1)
}

#[test]
fn flush_during_snapshot_preparation_is_retryable() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    assert!(sink.set_state(gst::State::Paused).is_ok());
    let src = open_src_pad(&sink, "video", "v", video_caps());
    let peer = sink
        .static_pad("video")
        .unwrap()
        .downcast::<super::MoqSinkPad>()
        .unwrap();
    let bus = gst::Bus::new();
    sink.set_bus(Some(&bus));
    let thread = PendingThread::new();
    let observer_sink = sink.clone();
    let observer_pad = peer.clone();
    let observer_thread = thread.slot();
    let flush_observed = Arc::new(AtomicBool::new(false));
    let callback_observed = flush_observed.clone();
    let once = AtomicBool::new(false);
    let cat = *super::imp::CAT;
    let previous = cat.threshold();
    cat.set_threshold(gst::DebugLevel::Debug);
    let observer = gst::log::add_log_function(move |_, _, _, _, _, _, msg| {
        if msg
            .get()
            .is_some_and(|m| m.as_str() == "preparing initial track snapshot")
            && !once.swap(true, Ordering::SeqCst)
        {
            callback_observed.store(
                interrupt_snapshot(&observer_sink, &observer_pad, &observer_thread),
                Ordering::SeqCst,
            );
        }
    });
    let log_cleanup = crate::test_support::LogCleanup::new(observer, cat, previous);
    assert_eq!(
        src.push(keyframe(gst::ClockTime::ZERO, 64)),
        Err(gst::FlowError::Flushing)
    );
    drop(log_cleanup);
    assert!(flush_observed.load(Ordering::SeqCst));
    thread.join().unwrap();
    let observed = !sink.imp().track_lock().tracks_failed;
    assert!(observed);
    assert!(bus.pop_filtered(&[gst::MessageType::Error]).is_none());
    assert_eq!(log_count(Op::Connect), 0);
    sink.imp().set_pad_flush(&peer, false);
    assert_eq!(src.push(keyframe(gst::ClockTime::ZERO, 64)), OK);
    assert_eq!(log_count(Op::Write), 1);
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
}

fn connect_error_with_flush(cancel: bool) {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    assert!(sink.set_state(gst::State::Paused).is_ok());
    let src = open_src_pad(&sink, "video", "v", video_caps());
    let peer = sink
        .static_pad("video")
        .unwrap()
        .downcast::<super::MoqSinkPad>()
        .unwrap();
    let bus = gst::Bus::new();
    sink.set_bus(Some(&bus));
    let thread = PendingThread::new();
    control().connect_result = Some(moq5::Error::CLOSED);
    let flush_observed = Arc::new(AtomicBool::new(false));
    if cancel {
        let callback_observed = flush_observed.clone();
        let callback_sink = sink.clone();
        let callback_pad = peer.clone();
        let callback_thread = thread.slot();
        control().on_connect = Some(Box::new(move || {
            callback_observed.store(
                interrupt_snapshot(&callback_sink, &callback_pad, &callback_thread),
                Ordering::SeqCst,
            );
        }));
    }
    assert_eq!(
        src.push(keyframe(gst::ClockTime::ZERO, 64)),
        Err(if cancel {
            gst::FlowError::Flushing
        } else {
            gst::FlowError::Error
        })
    );
    assert_eq!(
        bus.pop_filtered(&[gst::MessageType::Error]).is_none(),
        cancel
    );
    let observed = !sink.imp().track_lock().tracks_failed;
    assert_eq!(observed, cancel);
    let observed = sink.imp().namespace.lock().unwrap().is_some();
    assert_eq!(observed, cancel);
    if cancel {
        assert!(flush_observed.load(Ordering::SeqCst));
        thread.join().unwrap();
        sink.imp().set_pad_flush(&peer, false);
        control().connect_result = None;
        assert_eq!(src.push(keyframe(gst::ClockTime::ZERO, 64)), OK);
        assert_eq!(log_count(Op::Write), 1);
    }
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
}

#[test]
fn flush_racing_connect_error_is_retryable() {
    connect_error_with_flush(true);
}
#[test]
fn actual_connect_error_is_reported() {
    connect_error_with_flush(false);
}

#[test]
fn cmaf_avc_sample_entries_are_signaled_exactly() {
    let _suite = suite();
    for entry in [b"avc1", b"avc3"] {
        for protected in [false, true] {
            let (sink, _null_sink) = new_sink();
            if protected {
                sink.set_property("drm-systems", WIDEVINE_UUID);
            }
            let index = control().added_cfgs.len();
            assert_eq!(
                push_cmaf_init_entry(&sink, protected, b"cenc", 0, entry),
                OK
            );
            let cfg = observe_control(|c| c.added_cfgs.clone())[index].clone();
            let expected = format!("{}.42e01e", std::str::from_utf8(entry).unwrap());
            assert_eq!(cfg.codec, expected);
            assert_eq!(
                cfg.init_data.as_deref(),
                Some(
                    build_cmaf_init_entry(
                        90000,
                        1920,
                        1080,
                        protected,
                        b"cenc",
                        &FIXTURE_KID,
                        entry
                    )
                    .as_slice()
                )
            );
            assert_eq!(!cfg.cp_ref_ids.is_empty(), protected);
        }
    }
}

#[test]
fn protected_avc3_without_drm_is_refused_before_connect() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    assert_eq!(
        push_cmaf_init_entry(&sink, true, b"cenc", 0, b"avc3"),
        Err(gst::FlowError::Error)
    );
    assert_eq!(log_count(Op::Connect), 0);
    assert_eq!(log_count(Op::Write), 0);
}

fn null_cleanup_on_return_or_panic(panic: bool) {
    let _suite = suite();
    let saved_sink = Mutex::new(None);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (sink, _null_sink) = new_sink();
        assert!(sink.set_state(gst::State::Paused).is_ok());
        let src = open_src_pad(&sink, "video", "cleanup-proof", video_caps());
        assert_eq!(src.push(keyframe(gst::ClockTime::ZERO, 64)), OK);
        *saved_sink.lock().unwrap() = Some(sink.clone());
        close_source(src);
        // This panic occurs in ordinary Rust after all Gst/FFI calls return.
        if panic {
            panic!("controlled fixture NULL cleanup panic");
        }
    }));
    assert_eq!(result.is_err(), panic);
    if let Err(payload) = result {
        let message = payload.downcast_ref::<&str>().copied();
        assert_eq!(message, Some("controlled fixture NULL cleanup panic"));
    }
    let sink = saved_sink.lock().unwrap().take();
    let sink = sink.expect("fixture created");
    assert_eq!(sink.current_state(), gst::State::Null);
    assert_eq!(observe_control(|c| c.destroys), 1);
    assert_eq!(log_count(Op::DestroySender), 1);
}

#[test]
fn null_cleanup_precedes_serial_release_on_return() {
    null_cleanup_on_return_or_panic(false);
}

#[test]
fn null_cleanup_precedes_serial_release_on_panic() {
    null_cleanup_on_return_or_panic(true);
}

#[test]
fn held_push_worker_settles_before_serial_release_on_panic() {
    let _suite = suite();
    let (sink, _null_sink) = new_sink();
    assert!(sink.set_state(gst::State::Paused).is_ok());
    let src = open_src_pad(&sink, "video", "held-worker-cleanup", video_caps());
    control().hold_attach = true;
    let mut saved_job = None;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (job, _worker) = PushJob::start(&src, keyframe(gst::ClockTime::ZERO, 64));
        wait_attach_entered();
        saved_job = Some(job);
        // The real fixture worker is now blocked inside attach. Only this
        // ordinary Rust controller unwinds; no panic crosses a Gst callback.
        panic!("controlled held-worker cleanup panic");
    }));
    let payload = result.expect_err("controlled worker-owner panic");
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("controlled held-worker cleanup panic")
    );
    let job = saved_job.expect("worker entered attach");
    let (done, returned) = {
        let state = job.state.lock().unwrap();
        (state.done, state.ret.is_some())
    };
    assert!(done && returned);
    assert_eq!(sink.current_state(), gst::State::Null);
    assert_eq!(observe_control(|c| c.destroys), 1);
    assert_eq!(log_count(Op::DestroySender), 1);
    close_source(src);
}
