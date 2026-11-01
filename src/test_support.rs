//! Shared plumbing for the element suites.
//!
//! Each C suite ran as its own process with global mock state. Here the
//! suites share one test binary, so every test that installs the service
//! double, the clock hooks or a GStreamer log function holds [`serial`] for
//! its whole run.

use std::sync::{Mutex, MutexGuard, Once};

static SERIAL: Mutex<()> = Mutex::new(());

/// Serialize a mock-driven test and make sure GStreamer is initialized.
pub fn serial() -> MutexGuard<'static, ()> {
    static INIT: Once = Once::new();
    INIT.call_once(|| gst::init().unwrap());
    // A failed test poisons the lock; the next one still runs on fresh mocks.
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// Borrow `s` as a libmoq byte span (null when empty), for fake objects.
pub fn span(s: &[u8]) -> moq5::sys::moq_bytes_t {
    if s.is_empty() {
        moq5::sys::moq_bytes_t::default()
    } else {
        moq5::sys::moq_bytes_t {
            data: s.as_ptr(),
            len: s.len(),
        }
    }
}

/// View a libmoq byte span.
///
/// # Safety
/// `b` must describe memory valid for `'a`.
pub unsafe fn bytes<'a>(b: moq5::sys::moq_bytes_t) -> &'a [u8] {
    if b.data.is_null() || b.len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(b.data, b.len)
    }
}

/// Return a fixture-owned element to NULL before releasing serial ownership.
#[must_use = "keep cleanup alive until all fixture activity has settled"]
pub(crate) struct NullCleanup(gst::Element);

impl NullCleanup {
    pub(crate) fn new(element: &impl gst::prelude::IsA<gst::Element>) -> Self {
        use gst::prelude::*;
        Self(element.clone().upcast())
    }
}

impl Drop for NullCleanup {
    fn drop(&mut self) {
        use gst::prelude::*;
        // A fixture may be unwinding while its streaming worker is parked
        // inside the private attach hold and owns the track lock. Release
        // that test-only hold before NULL waits for streaming to settle.
        crate::service::fake::release_attach();
        if let Err(error) = self.0.set_state(gst::State::Null) {
            if std::thread::panicking() {
                eprintln!("WARNING: fixture NULL cleanup failed while unwinding: {error}");
            } else {
                panic!("fixture NULL cleanup failed: {error}");
            }
        }
    }
}

/// Remove a fixture log callback and restore its threshold during unwinding.
#[must_use = "keep log cleanup alive until the fixture has finished logging"]
pub(crate) struct LogCleanup {
    handle: Option<gst::log::DebugLogFunction>,
    category: gst::DebugCategory,
    previous: gst::DebugLevel,
}

impl LogCleanup {
    pub(crate) fn new(
        handle: gst::log::DebugLogFunction,
        category: gst::DebugCategory,
        previous: gst::DebugLevel,
    ) -> Self {
        Self {
            handle: Some(handle),
            category,
            previous,
        }
    }
}

impl Drop for LogCleanup {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            gst::log::remove_log_function(handle);
        }
        self.category.set_threshold(self.previous);
    }
}
