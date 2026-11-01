//! Monotonic time and short sleeps, behind a test seam.
//!
//! The sink's EOS retry loop is driven by `monotonic_us()`/`sleep_us()`.
//! Release builds use GLib's monotonic clock and a real sleep; unit tests can
//! substitute both (the C suite did the same with macro overrides of
//! `g_get_monotonic_time` and `g_usleep`).

/// Microseconds on the monotonic clock (`g_get_monotonic_time`).
#[cfg(not(test))]
#[inline]
pub fn monotonic_us() -> i64 {
    gst::glib::monotonic_time()
}

/// Sleep for `us` microseconds (`g_usleep`).
#[cfg(not(test))]
#[inline]
pub fn sleep_us(us: u64) {
    std::thread::sleep(std::time::Duration::from_micros(us));
}

#[cfg(test)]
pub use self::test_seam::{install, monotonic_us, sleep_us, ClockGuard};

#[cfg(test)]
mod test_seam {
    use std::sync::RwLock;

    #[derive(Clone, Copy)]
    pub struct Hooks {
        pub monotonic_us: fn() -> i64,
        pub sleep_us: fn(u64),
    }

    fn real_monotonic() -> i64 {
        gst::glib::monotonic_time()
    }
    fn real_sleep(us: u64) {
        std::thread::sleep(std::time::Duration::from_micros(us));
    }

    static HOOKS: RwLock<Hooks> = RwLock::new(Hooks {
        monotonic_us: real_monotonic,
        sleep_us: real_sleep,
    });

    pub fn monotonic_us() -> i64 {
        let f = HOOKS.read().unwrap().monotonic_us;
        f()
    }

    pub fn sleep_us(us: u64) {
        let f = HOOKS.read().unwrap().sleep_us;
        f(us)
    }

    /// Restores the previous hooks when dropped.
    pub struct ClockGuard(Hooks);

    impl ClockGuard {
        /// Observe restoration without retaining a borrow of the guard.
        pub fn restoration_probe(&self) -> impl Fn() -> bool + Send + 'static {
            let previous = self.0;
            move || {
                let current = *HOOKS.read().unwrap();
                std::ptr::fn_addr_eq(current.monotonic_us, previous.monotonic_us)
                    && std::ptr::fn_addr_eq(current.sleep_us, previous.sleep_us)
            }
        }
    }

    impl Drop for ClockGuard {
        fn drop(&mut self) {
            *HOOKS.write().unwrap() = self.0;
        }
    }

    /// Replace both hooks until the returned guard drops.
    pub fn install(monotonic_us: fn() -> i64, sleep_us: fn(u64)) -> ClockGuard {
        ClockGuard(std::mem::replace(
            &mut *HOOKS.write().unwrap(),
            Hooks {
                monotonic_us,
                sleep_us,
            },
        ))
    }
    #[cfg(test)]
    mod isolation_tests {
        use super::*;
        use std::panic::{catch_unwind, AssertUnwindSafe};
        use std::sync::{mpsc, Arc, Mutex, MutexGuard};
        use std::time::Duration;

        fn predecessor_clock() -> i64 {
            101
        }
        fn successor_clock() -> i64 {
            202
        }
        fn no_sleep(_: u64) {}

        struct ReleaseAndWait<'a> {
            serial: Option<MutexGuard<'a, ()>>,
            installed: mpsc::Receiver<()>,
        }
        impl Drop for ReleaseAndWait<'_> {
            fn drop(&mut self) {
                drop(self.serial.take());
                self.installed.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        // Actual Rust field destruction order, including during unwinding.
        struct OldOrder<'a> {
            _serial: ReleaseAndWait<'a>,
            _clock: ClockGuard,
        }
        struct ClockFirst<'a> {
            _clock: ClockGuard,
            _serial: ReleaseAndWait<'a>,
        }

        fn handoff(old_order: bool, panicking: bool) {
            // Keep the actual global fixture serial guard for this entire
            // deliberately broken experiment. Its local handoff lock models
            // the field order without exposing other parallel tests to it.
            let _global = crate::service::fake::suite();
            let serial = Arc::new(Mutex::new(()));
            let previous = *HOOKS.read().unwrap();
            let guard = serial.lock().unwrap();
            let clock = install(predecessor_clock, no_sleep);
            let (installed_tx, installed_rx) = mpsc::channel();
            let (probe_tx, probe_rx) = mpsc::channel();
            let successor_serial = serial.clone();
            let successor = std::thread::spawn(move || {
                let _serial = successor_serial.lock().unwrap_or_else(|e| e.into_inner());
                let clock = install(successor_clock, no_sleep);
                installed_tx.send(()).unwrap();
                probe_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                let observed = HOOKS.read().unwrap().monotonic_us;
                let is_successor = std::ptr::fn_addr_eq(observed, successor_clock as fn() -> i64);
                let is_previous = std::ptr::fn_addr_eq(observed, previous.monotonic_us);
                drop(clock);
                // The old-order mutant restored a stale predecessor value.
                // Repair ONLY its experiment while still holding both serial
                // boundaries; leave no contaminated clock for another test.
                *HOOKS.write().unwrap() = previous;
                (is_successor, is_previous)
            });
            let release = ReleaseAndWait {
                serial: Some(guard),
                installed: installed_rx,
            };
            let result = catch_unwind(AssertUnwindSafe(|| {
                if old_order {
                    let _suite = OldOrder {
                        _serial: release,
                        _clock: clock,
                    };
                    if panicking {
                        panic!("controlled clock teardown panic");
                    }
                } else {
                    let _suite = ClockFirst {
                        _clock: clock,
                        _serial: release,
                    };
                    if panicking {
                        panic!("controlled clock teardown panic");
                    }
                }
            }));
            probe_tx.send(()).unwrap();
            let observed = successor.join().unwrap();
            eprintln!("clock handoff old_order={old_order} panicking={panicking} successor_preserved={} previous_restored={}", observed.0, observed.1);
            assert_eq!(result.is_err(), panicking);
            assert_eq!(
                observed,
                if old_order {
                    (false, true)
                } else {
                    (true, false)
                }
            );
        }

        #[test]
        fn old_order_normal_loses_successor_clock() {
            handoff(true, false);
        }
        #[test]
        fn old_order_panic_loses_successor_clock() {
            handoff(true, true);
        }
        #[test]
        fn clock_first_normal_preserves_successor_clock() {
            handoff(false, false);
        }
        #[test]
        fn clock_first_panic_preserves_successor_clock() {
            handoff(false, true);
        }
    }
}
