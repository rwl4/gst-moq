//! A programmable stand-in for the libmoq service, for the unit tests.
//!
//! It is a stub -- no relay, no network -- and a spy: every operation the
//! element performs is appended to an ordered log the tests assert on. The
//! element under test is the real one; only this boundary is replaced, which
//! is what lets `tests_barrier`, `tests_recovery` and `moqsrc::tests` check
//! the barrier, the sender state machine and track selection without a
//! server.
//!
//! The double is process-wide, like the clock seam in [`crate::clock`], so a
//! test takes [`suite`] first: it serialises against the other tests and
//! resets the control block.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use moq5::{
    ContentProtection, Error, MediaType, Packaging, Result, SendObject, SenderStats,
    SubscribeOptions, TerminalReason, TrackEventKind, WaitResult,
};

use super::{EndpointCfg, ReceiverCfg, SenderCfg, TerminalInfo, TrackCfg};

// -- handles ----------------------------------------------------------------

/// A send-track handle: an index into the control block's per-track state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SendTrack(pub(crate) usize);

/// A receive-track handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Track(pub(crate) usize);

// -- the call log -----------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    /// A marker the test places to order its own steps against the element's.
    Mark,
    Connect,
    Post,
    Attach,
    AddTrack,
    Write,
    EndTrack,
    Complete,
    Drain,
    Interrupt,
    Subscribe,
    DestroySender,
    DestroyReceiver,
}

static LOG: Mutex<Vec<(Op, Option<String>)>> = Mutex::new(Vec::new());

pub(crate) fn log_add(op: Op, detail: Option<&str>) {
    LOG.lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((op, detail.map(str::to_owned)));
}

/// The index of the first `op` (with `detail`, when given), or -1.
pub(crate) fn log_find(op: Op, detail: Option<&str>) -> i32 {
    LOG.lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .position(|(o, d)| *o == op && detail.is_none_or(|want| d.as_deref() == Some(want)))
        .map_or(-1, |i| i as i32)
}

pub(crate) fn log_count(op: Op) -> usize {
    LOG.lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|(o, _)| *o == op)
        .count()
}

// -- what a test can arrange ------------------------------------------------

/// A track event queued for `poll_track`.
#[derive(Clone, Debug)]
pub(crate) struct Event {
    pub(crate) kind: TrackEventKind,
    /// The code `raw_kind` reports, for the kinds this crate only logs.
    pub(crate) raw_kind: u32,
    pub(crate) local_result: Option<Error>,
    pub(crate) delivery_origin: Option<moq5::DeliveryOrigin>,
    pub(crate) track: Option<Track>,
    pub(crate) desc: Option<DescData>,
}

/// A catalog description a queued event or query hands back.
#[derive(Clone, Debug, Default)]
pub(crate) struct DescData {
    pub(crate) name: Option<Vec<u8>>,
    pub(crate) codec: Vec<u8>,
    pub(crate) packaging: Option<Packaging>,
    pub(crate) init_data: Vec<u8>,
    pub(crate) codec_config: Vec<u8>,
    pub(crate) samplerate: Option<u32>,
    pub(crate) channel_config: Vec<u8>,
}

/// A media object queued for `poll_object`.
#[derive(Clone, Debug, Default)]
pub(crate) struct ObjectData {
    pub(crate) track: Option<Track>,
    pub(crate) packaging: Option<Packaging>,
    pub(crate) is_keyframe: bool,
    pub(crate) presentation_time_us: u64,
    pub(crate) payload: Vec<u8>,
    pub(crate) fragment: Vec<u8>,
}

/// Everything the double does that a test may want to steer.
pub(crate) struct Control {
    // -- sender
    /// Refuse `Sender::attach` with NOMEM.
    pub(crate) fail_attach: bool,
    pub(crate) fail_post: bool,
    pub(crate) hold_startup: bool,
    pub(crate) startup_queued: bool,
    /// Refuse the Nth (1-based) `add_track`; 0 refuses none.
    pub(crate) fail_add: u32,
    pub(crate) added_tracks: u32,
    /// Hold inside `attach` until released, so a test can flush mid-attach.
    pub(crate) hold_attach: bool,
    pub(crate) attach_entered: bool,
    /// `is_ready` answers false for this many calls, then true.
    pub(crate) not_ready_for: u32,
    pub(crate) ready_calls: u32,
    pub(crate) sender_wait_error: Option<Error>,
    pub(crate) writes: u32,
    pub(crate) written_sap: Vec<(bool, bool, Option<moq5::SapType>)>,
    pub(crate) ends: u32,
    pub(crate) refusals: u32,
    /// Refuse the next `write` with WOULD_BLOCK this many times.
    pub(crate) refuse_writes: u32,
    /// Refuse the next `end_track` with WOULD_BLOCK this many times.
    pub(crate) refuse_ends: u32,
    pub(crate) complete_result: Option<Error>,
    pub(crate) ended: [bool; 16],
    pub(crate) stats: SenderStats,
    pub(crate) sender_fatal: Option<u64>,
    pub(crate) attached_cps: Vec<ContentProtection>,
    pub(crate) track_names: Vec<String>,
    pub(crate) added_cfgs: Vec<TrackCfg>,
    pub(crate) capture_video: u64,
    pub(crate) capture_audio: u64,
    pub(crate) audio_groups: u32,

    // -- endpoint
    pub(crate) connects: u32,
    /// What the element dialled, for the draft/verification assertions.
    pub(crate) dialled: Option<EndpointCfg>,
    pub(crate) version_offer: Option<moq5::VersionOffer>,
    pub(crate) setup_auth: Vec<moq5::AuthToken>,
    pub(crate) connect_result: Option<Error>,
    pub(crate) on_connect: Option<Box<dyn FnMut() + Send>>,
    pub(crate) receiver_cfg: Option<ReceiverCfg>,
    pub(crate) receiver_attach_result: Option<Error>,
    pub(crate) drains: u32,
    pub(crate) destroys: u32,
    pub(crate) interrupted: bool,
    /// What `drain` reports; `None` means it settled cleanly.
    pub(crate) drain_result: Option<std::result::Result<WaitResult, Error>>,
    /// The drain timeout the element asked for, in microseconds.
    pub(crate) budget: u64,
    /// Run when the element waits on the endpoint -- the recovery suite
    /// advances its clock here, so deadlines are deterministic.
    pub(crate) on_wait: Option<Box<dyn FnMut() + Send>>,
    /// Run when the element drains, for the cancel-during-drain case.
    pub(crate) on_drain: Option<Box<dyn FnMut() + Send>>,
    pub(crate) terminal_getter_fails: bool,
    pub(crate) terminal_reason: TerminalReason,
    pub(crate) terminal_detail: u64,

    // -- receiver
    pub(crate) events: Vec<Event>,
    pub(crate) event_index: usize,
    pub(crate) objects: Vec<ObjectData>,
    pub(crate) object_index: usize,
    pub(crate) waits: u32,
    /// An unexpected wait is a quick failure unless a test wants otherwise.
    pub(crate) wait_result: Option<Error>,
    pub(crate) track_poll_error: Option<Error>,
    pub(crate) description_error: Option<Error>,
    pub(crate) cleaned: u32,
    pub(crate) subscriptions: u32,
    pub(crate) subscribed_track: Option<Track>,
    pub(crate) subscribe_result: Option<Error>,
    /// What `poll_object` reports once the queue is empty: `None` is simply
    /// "nothing queued now".
    pub(crate) empty_result: Option<Error>,
    pub(crate) receiver_fatal: Option<u64>,
    pub(crate) negotiated_draft: u32,
    pub(crate) startup_calls: String,
    /// The terminal diagnostic the element logged, captured by the test.
    pub(crate) terminal_diagnostic: Option<String>,
}

impl Control {
    fn new() -> Control {
        Control {
            fail_attach: false,
            fail_post: false,
            hold_startup: false,
            startup_queued: false,
            fail_add: 0,
            added_tracks: 0,
            hold_attach: false,
            attach_entered: false,
            not_ready_for: 0,
            ready_calls: 0,
            sender_wait_error: None,
            writes: 0,
            written_sap: Vec::new(),
            ends: 0,
            refusals: 0,
            refuse_writes: 0,
            refuse_ends: 0,
            complete_result: None,
            ended: [false; 16],
            stats: SenderStats::default(),
            sender_fatal: None,
            attached_cps: Vec::new(),
            track_names: Vec::new(),
            added_cfgs: Vec::new(),
            capture_video: 0,
            capture_audio: 0,
            audio_groups: 0,
            budget: 0,
            connects: 0,
            dialled: None,
            version_offer: None,
            setup_auth: Vec::new(),
            connect_result: None,
            on_connect: None,
            receiver_cfg: None,
            receiver_attach_result: None,
            drains: 0,
            destroys: 0,
            interrupted: false,
            drain_result: None,
            on_wait: None,
            on_drain: None,
            terminal_getter_fails: false,
            terminal_reason: TerminalReason::None,
            terminal_detail: 0,
            events: Vec::new(),
            event_index: 0,
            objects: Vec::new(),
            object_index: 0,
            waits: 0,
            wait_result: Some(Error::INTERRUPTED),
            track_poll_error: None,
            description_error: None,
            cleaned: 0,
            subscriptions: 0,
            subscribed_track: None,
            subscribe_result: None,
            empty_result: None,
            receiver_fatal: None,
            negotiated_draft: 0,
            startup_calls: String::new(),
            terminal_diagnostic: None,
        }
    }
}

static CONTROL: Mutex<Option<Control>> = Mutex::new(None);
// Accounting only: SERIAL remains the sole fixture coordinator. A new suite
// cannot discard state while an endpoint/sender/receiver/object still exists.
static LIVE_OWNERS: AtomicUsize = AtomicUsize::new(0);
static COND: Condvar = Condvar::new();
static RELEASED: AtomicBool = AtomicBool::new(false);

/// The control block. Panics outside a [`suite`], which is the point: a test
/// that forgot to take one would otherwise share state with its neighbour.
pub(crate) fn control() -> MappedControl {
    MappedControl(CONTROL.lock().unwrap_or_else(|e| e.into_inner()))
}

/// A guard that derefs to the control block.
pub(crate) struct MappedControl(MutexGuard<'static, Option<Control>>);

impl std::ops::Deref for MappedControl {
    type Target = Control;
    fn deref(&self) -> &Control {
        self.0.as_ref().expect("fake::suite() was not taken")
    }
}

impl std::ops::DerefMut for MappedControl {
    fn deref_mut(&mut self) -> &mut Control {
        self.0.as_mut().expect("fake::suite() was not taken")
    }
}

/// Serialises the test and installs a fresh control block, which is torn down
/// when the guard drops.
#[must_use = "the double is uninstalled when the guard drops"]
pub(crate) struct Suite(
    /// Held, not read: it serialises this test against the others.
    #[allow(dead_code)]
    MutexGuard<'static, ()>,
    Option<Box<dyn FnOnce()>>,
);

impl Suite {
    /// Observe the actual fixture immediately before its serial guard releases.
    pub(crate) fn observe_before_release(&mut self, observe: impl FnOnce() + 'static) {
        self.1 = Some(Box::new(observe));
    }
}

impl Drop for Suite {
    fn drop(&mut self) {
        if LIVE_OWNERS.load(Ordering::SeqCst) == 0 {
            *CONTROL.lock().unwrap_or_else(|e| e.into_inner()) = None;
        } else {
            eprintln!("WARNING: fake suite ended with surviving service owners");
        }
        if let Some(observe) = self.1.take() {
            observe();
        }
    }
}

pub(crate) fn suite() -> Suite {
    // The shared serial lock: it also initialises GStreamer, and the clock
    // seam's users take the same one.
    let serial = crate::test_support::serial();
    assert_eq!(
        LIVE_OWNERS.load(Ordering::SeqCst),
        0,
        "prior fake owners did not settle"
    );
    {
        let mut log = LOG.lock().unwrap_or_else(|e| e.into_inner());
        log.clear();
        LOG.clear_poison();
    }
    RELEASED.store(false, Ordering::SeqCst);
    {
        let mut state = CONTROL.lock().unwrap_or_else(|e| e.into_inner());
        *state = Some(Control::new());
        // Recovery is restricted to this exclusive reset, after old owners
        // have settled and fresh fixture state has been installed.
        CONTROL.clear_poison();
    }
    Suite(serial, None)
}

/// Let an `attach` held by `hold_attach` proceed.
pub(crate) fn release_attach() {
    RELEASED.store(true, Ordering::SeqCst);
    COND.notify_all();
}

/// Block until the element is inside `attach`.
pub(crate) fn wait_attach_entered() {
    let mut c = CONTROL.lock().unwrap_or_else(|e| e.into_inner());
    while !c.as_ref().is_some_and(|c| c.attach_entered) {
        c = COND.wait(c).unwrap_or_else(|e| e.into_inner());
    }
}

// -- endpoint ---------------------------------------------------------------

/// A handle on the endpoint. `moq5::Endpoint` is refcounted, so cloning it
/// does not open a second one and only the last handle tears it down -- the
/// restart case counts exactly one teardown across the element's clones.
#[derive(Debug)]
pub(crate) struct Endpoint(Arc<EndpointOwner>);

impl Clone for Endpoint {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Debug)]
struct EndpointOwner;

impl Drop for EndpointOwner {
    /// `moq5::Endpoint`'s own drop stops and destroys the endpoint when the
    /// last handle goes; the startup tests assert that call order.
    fn drop(&mut self) {
        // Keep accounting during controlled Rust unwinding without clearing
        // poison. Only suite() may reset poisoned fixture state.
        let mut guard = CONTROL.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = guard.as_mut() {
            c.destroys += 1;
            c.startup_calls.push_str("stop;destroy;");
        } else {
            eprintln!("WARNING: fake endpoint outlived its suite");
        }
        LIVE_OWNERS.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Endpoint {
    pub(crate) fn connect(config: &EndpointCfg) -> Result<Endpoint> {
        let mut c = control();
        c.connects += 1;
        c.dialled = Some(config.clone());
        c.version_offer = Some(super::real::version_offer(config.draft));
        c.setup_auth = super::real::setup_auth_tokens(config);
        c.startup_calls.push_str("connect;");
        log_add(Op::Connect, None);
        let result = c.connect_result;
        let hook = c.on_connect.take();
        drop(c);
        if let Some(mut hook) = hook {
            hook();
        }
        match result {
            Some(e) => Err(e),
            None => {
                LIVE_OWNERS.fetch_add(1, Ordering::SeqCst);
                Ok(Endpoint(Arc::new(EndpointOwner)))
            }
        }
    }

    pub(crate) fn drain(&self, timeout: Duration) -> Result<WaitResult> {
        let hook = {
            let mut c = control();
            c.drains += 1;
            c.budget = timeout.as_micros() as u64;
            log_add(Op::Drain, None);
            c.on_drain.take()
        };
        // Outside the lock: the hook reaches back into the element, which
        // may call the double again.
        if let Some(mut hook) = hook {
            hook();
            control().on_drain = Some(hook);
        }
        let c = control();
        if c.interrupted {
            return Err(Error::INTERRUPTED);
        }
        c.drain_result.unwrap_or(Ok(WaitResult::Ready))
    }

    pub(crate) fn negotiated_draft(&self) -> u32 {
        control().negotiated_draft
    }

    pub(crate) fn set_interrupted(&self, interrupted: bool) {
        control().interrupted = interrupted;
        log_add(Op::Interrupt, None);
    }

    pub(crate) fn terminal(&self) -> Result<TerminalInfo> {
        let mut c = control();
        c.startup_calls.push_str("get;");
        if c.terminal_getter_fails {
            return Err(Error::INVAL);
        }
        Ok(TerminalInfo {
            reason: c.terminal_reason,
            detail_code: c.terminal_detail,
        })
    }

    pub(crate) fn wait(&self, _timeout: Duration) -> Result<WaitResult> {
        let hook = control().on_wait.take();
        if let Some(mut hook) = hook {
            hook();
            control().on_wait = Some(hook);
        }
        if control().interrupted {
            return Err(Error::INTERRUPTED);
        }
        Ok(WaitResult::Ready)
    }
}

// -- sender -----------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct Sender;

impl Drop for Sender {
    fn drop(&mut self) {
        log_add(Op::DestroySender, None);
        LIVE_OWNERS.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Sender {
    pub(crate) fn attach_with_tracks(
        endpoint: &Endpoint,
        config: &SenderCfg,
        tracks: &[TrackCfg],
    ) -> Result<SenderStartup> {
        if tracks.is_empty() {
            return Err(Error::INVAL);
        }
        log_add(Op::Post, None);
        if control().fail_post {
            return Err(Error::CLOSED);
        }
        control().startup_queued = true;
        COND.notify_all();
        Ok(SenderStartup {
            endpoint: endpoint.clone(),
            config: Some(config.clone()),
            tracks: tracks.to_vec(),
        })
    }

    pub(crate) fn attach(_endpoint: &Endpoint, config: &SenderCfg) -> Result<Sender> {
        {
            let mut c = control();
            c.attach_entered = true;
            c.attached_cps = config.content_protections.clone();
            COND.notify_all();
            if c.hold_attach {
                drop(c);
                let mut guard = CONTROL.lock().unwrap_or_else(|e| e.into_inner());
                while !RELEASED.load(Ordering::SeqCst) {
                    guard = COND.wait(guard).unwrap_or_else(|e| e.into_inner());
                }
            }
        }
        log_add(Op::Attach, None);
        if control().fail_attach {
            return Err(Error::NOMEM);
        }
        LIVE_OWNERS.fetch_add(1, Ordering::SeqCst);
        Ok(Sender)
    }

    pub(crate) fn add_track(&self, config: &TrackCfg) -> Result<SendTrack> {
        let mut c = control();
        c.added_tracks += 1;
        let n = c.added_tracks;
        let name = config.name.clone();
        log_add(Op::AddTrack, Some(&name));
        if c.fail_add == n {
            return Err(Error::NOMEM);
        }
        let index = c.track_names.len();
        c.track_names.push(name);
        c.added_cfgs.push(config.clone());
        Ok(SendTrack(index))
    }

    pub(crate) fn write(&self, track: SendTrack, object: SendObject) -> Result<()> {
        let mut c = control();
        if c.interrupted {
            return Err(Error::INTERRUPTED);
        }
        if c.refuse_writes > 0 {
            c.refuse_writes -= 1;
            c.refusals += 1;
            return Err(Error::WOULD_BLOCK);
        }
        // Writing an ended track is the state error libmoq reports.
        if c.ended.get(track.0).copied().unwrap_or(false) {
            return Err(Error::WRONG_STATE);
        }
        if track.0 >= c.track_names.len() {
            return Err(Error::INVAL);
        }
        c.writes += 1;
        c.written_sap
            .push((object.is_sync, object.starts_group, object.sap_type));
        // The LOC timing the barrier suite asserts: the capture timestamp per
        // media kind, and how often an audio object opened a group.
        let audio = c
            .added_cfgs
            .get(track.0)
            .is_some_and(|cfg| cfg.media_type == MediaType::Audio);
        if audio {
            c.capture_audio = object.capture_time_us.unwrap_or(0);
            if object.starts_group {
                c.audio_groups += 1;
            }
        } else {
            c.capture_video = object.capture_time_us.unwrap_or(0);
        }
        let name = c.track_names.get(track.0).cloned().unwrap_or_default();
        log_add(Op::Write, Some(&name));
        Ok(())
    }

    pub(crate) fn end_track(&self, track: SendTrack) -> Result<()> {
        let mut c = control();
        c.ends += 1;
        if c.interrupted {
            return Err(Error::INTERRUPTED);
        }
        if c.refuse_ends > 0 {
            c.refuse_ends -= 1;
            return Err(Error::WOULD_BLOCK);
        }
        if track.0 >= c.track_names.len() {
            return Err(Error::INVAL);
        }
        if let Some(slot) = c.ended.get_mut(track.0) {
            *slot = true;
        }
        let name = c.track_names.get(track.0).cloned().unwrap_or_default();
        log_add(Op::EndTrack, Some(&name));
        Ok(())
    }

    pub(crate) fn complete(&self) -> Result<()> {
        let c = control();
        log_add(Op::Complete, None);
        if c.interrupted {
            return Err(Error::INTERRUPTED);
        }
        match c.complete_result {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) fn stats(&self) -> Result<SenderStats> {
        Ok(control().stats)
    }

    pub(crate) fn is_ready(&self) -> bool {
        let mut c = control();
        c.ready_calls += 1;
        c.ready_calls > c.not_ready_for
    }

    pub(crate) fn is_fatal(&self) -> bool {
        control().sender_fatal.is_some()
    }

    pub(crate) fn fatal_code(&self) -> u64 {
        control().sender_fatal.unwrap_or(0)
    }

    pub(crate) fn wait(&self, _timeout: Duration) -> Result<WaitResult> {
        let hook = control().on_wait.take();
        if let Some(mut hook) = hook {
            hook();
            control().on_wait = Some(hook);
        }
        let c = control();
        if c.interrupted {
            return Err(Error::INTERRUPTED);
        }
        if let Some(error) = c.sender_wait_error {
            return Err(error);
        }
        Ok(WaitResult::Ready)
    }
}

pub(crate) struct SenderStartup {
    endpoint: Endpoint,
    config: Option<SenderCfg>,
    tracks: Vec<TrackCfg>,
}

impl SenderStartup {
    pub(crate) fn poll(&mut self) -> Result<Option<(Sender, Vec<SendTrack>)>> {
        if control().hold_startup {
            return Ok(None);
        }
        let config = self.config.take().ok_or(Error::WRONG_STATE)?;
        let sender = Sender::attach(&self.endpoint, &config)?;
        let handles = self
            .tracks
            .iter()
            .map(|track| sender.add_track(track))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some((sender, handles)))
    }
}

pub(crate) fn wait_startup_queued() {
    let mut guard = CONTROL.lock().unwrap();
    while !guard.as_ref().is_some_and(|c| c.startup_queued) {
        guard = COND.wait(guard).unwrap();
    }
}

// -- receiver ---------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct Receiver {
    _owner: (),
}

impl Drop for Receiver {
    fn drop(&mut self) {
        log_add(Op::DestroyReceiver, None);
        LIVE_OWNERS.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Receiver {
    /// A receiver installed directly by source fixtures, with normal ownership.
    pub(crate) fn fixture() -> Self {
        LIVE_OWNERS.fetch_add(1, Ordering::SeqCst);
        Receiver { _owner: () }
    }

    pub(crate) fn attach(_endpoint: &Endpoint, config: &ReceiverCfg) -> Result<Receiver> {
        let mut c = control();
        c.receiver_cfg = Some(config.clone());
        c.startup_calls.push_str("attach;");
        log_add(Op::Attach, None);
        match c.receiver_attach_result {
            Some(e) => Err(e),
            None => Ok(Receiver::fixture()),
        }
    }

    pub(crate) fn subscribe_track(
        &self,
        track: Track,
        _options: Option<SubscribeOptions>,
    ) -> Result<()> {
        let mut c = control();
        c.subscriptions += 1;
        c.subscribed_track = Some(track);
        c.startup_calls.push_str("subscribe;");
        log_add(Op::Subscribe, None);
        match c.subscribe_result {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub(crate) fn poll_track(&self) -> Result<Option<TrackEvt<'_>>> {
        let mut c = control();
        if let Some(error) = c.track_poll_error {
            return Err(error);
        }
        if c.event_index >= c.events.len() {
            return Ok(None);
        }
        let ev = c.events[c.event_index].clone();
        c.event_index += 1;
        Ok(Some(TrackEvt {
            event: ev,
            _receiver: std::marker::PhantomData,
        }))
    }

    pub(crate) fn poll_object(&self) -> Result<Option<Object>> {
        let mut c = control();
        if c.object_index >= c.objects.len() {
            return match c.empty_result {
                Some(e) => Err(e),
                None => Ok(None),
            };
        }
        let o = c.objects[c.object_index].clone();
        c.object_index += 1;
        LIVE_OWNERS.fetch_add(1, Ordering::SeqCst);
        Ok(Some(Object(o, std::marker::PhantomData)))
    }

    pub(crate) fn wait(&self, _timeout: Duration) -> Result<WaitResult> {
        let mut c = control();
        c.waits += 1;
        match c.wait_result {
            Some(e) => Err(e),
            None => Ok(WaitResult::Ready),
        }
    }

    pub(crate) fn is_fatal(&self) -> bool {
        control().receiver_fatal.is_some()
    }

    pub(crate) fn fatal_code(&self) -> u64 {
        control().receiver_fatal.unwrap_or(0)
    }

    pub(crate) fn negotiated_draft(&self) -> u32 {
        control().negotiated_draft
    }
}

// -- what the receiver hands back -------------------------------------------

pub(crate) struct TrackEvt<'r> {
    event: Event,
    _receiver: std::marker::PhantomData<&'r Receiver>,
}

impl<'r> TrackEvt<'r> {
    pub(crate) fn local_result(&self) -> Option<Error> {
        self.event.local_result
    }
    pub(crate) fn delivery_origin(&self) -> Option<moq5::DeliveryOrigin> {
        self.event.delivery_origin
    }
    pub(crate) fn kind(&self) -> TrackEventKind {
        self.event.kind
    }
    pub(crate) fn track(&self) -> Option<Track> {
        self.event.track
    }
    pub(crate) fn description(&self) -> Result<Option<Desc<'r>>> {
        if let Some(error) = control().description_error {
            return Err(error);
        }
        Ok(self.event.desc.clone().map(|d| Desc {
            data: d,
            _receiver: std::marker::PhantomData,
        }))
    }
    pub(crate) fn raw_kind(&self) -> u32 {
        self.event.raw_kind
    }
}

pub(crate) struct Desc<'r> {
    data: DescData,
    _receiver: std::marker::PhantomData<&'r Receiver>,
}

impl<'r> Desc<'r> {
    pub(crate) fn name(&self) -> &[u8] {
        self.data.name.as_deref().unwrap_or(&[])
    }
    pub(crate) fn codec(&self) -> &[u8] {
        &self.data.codec
    }
    pub(crate) fn packaging(&self) -> Option<Packaging> {
        self.data.packaging
    }
    pub(crate) fn init_data(&self) -> &[u8] {
        &self.data.init_data
    }
    pub(crate) fn codec_config(&self) -> &[u8] {
        &self.data.codec_config
    }
    pub(crate) fn samplerate(&self) -> Option<u32> {
        self.data.samplerate
    }
    pub(crate) fn channel_config(&self) -> &[u8] {
        &self.data.channel_config
    }
    pub(crate) fn has_name(&self) -> bool {
        self.data.name.is_some()
    }
}

pub(crate) struct Object(ObjectData, std::marker::PhantomData<std::rc::Rc<()>>);

impl Drop for Object {
    fn drop(&mut self) {
        let mut guard = CONTROL.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = guard.as_mut() {
            c.cleaned += 1;
        } else {
            eprintln!("WARNING: fake object outlived its suite");
        }
        LIVE_OWNERS.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Object {
    pub(crate) fn track(&self) -> Option<Track> {
        self.0.track
    }
    pub(crate) fn packaging(&self) -> Option<Packaging> {
        self.0.packaging
    }
    pub(crate) fn is_keyframe(&self) -> bool {
        self.0.is_keyframe
    }
    pub(crate) fn presentation_time_us(&self) -> u64 {
        self.0.presentation_time_us
    }
    pub(crate) fn payload(&self) -> &[u8] {
        &self.0.payload
    }
    pub(crate) fn fragment(&self) -> &[u8] {
        &self.0.fragment
    }
}

#[cfg(test)]
mod isolation_tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    fn owners() -> (Endpoint, Receiver, Object) {
        let endpoint = Endpoint::connect(&EndpointCfg {
            setup_auth: None,
            url: "https://example.invalid/moq".into(),
            insecure: false,
            draft: 16,
        })
        .unwrap();
        let receiver = Receiver::attach(
            &endpoint,
            &ReceiverCfg::live(moq5::Namespace::parse("fixture").unwrap()),
        )
        .unwrap();
        control().objects.push(ObjectData::default());
        let object = receiver.poll_object().unwrap().unwrap();
        (endpoint, receiver, object)
    }

    fn cleanup_and_reset(poison: bool) {
        {
            let _suite = suite();
            let (endpoint, receiver, object) = owners();
            if poison {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let _control = control();
                    panic!("controlled CONTROL poison");
                }));
                assert!(result.is_err());
                assert!(CONTROL.is_poisoned());
            }
            let other = endpoint.clone();
            drop(endpoint);
            let early = control().destroys;
            assert_eq!(early, 0);
            drop(object);
            drop(receiver);
            drop(other);
            let observed = {
                let c = control();
                (c.cleaned, c.destroys, c.startup_calls.clone())
            };
            assert_eq!(observed, (1, 1, "connect;attach;stop;destroy;".into()));
            assert_eq!(LIVE_OWNERS.load(Ordering::SeqCst), 0);
            assert_eq!(
                CONTROL.is_poisoned(),
                poison,
                "cleanup must not clear poison"
            );
        }
        let _next = suite();
        assert!(
            !CONTROL.is_poisoned(),
            "only the exclusive reset clears poison"
        );
        let empty = {
            let c = control();
            (c.cleaned, c.destroys)
        };
        assert_eq!(empty, (0, 0));
        let (endpoint, receiver, object) = owners();
        drop(object);
        drop(receiver);
        drop(endpoint);
        let observed = {
            let c = control();
            (c.cleaned, c.destroys, c.startup_calls.clone())
        };
        assert_eq!(observed, (1, 1, "connect;attach;stop;destroy;".into()));
        assert_eq!(LIVE_OWNERS.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn healthy_cleanup_and_next_suite_reset() {
        cleanup_and_reset(false);
    }
    #[test]
    fn poisoned_control_cleanup_and_next_suite_reset() {
        cleanup_and_reset(true);
    }
}
