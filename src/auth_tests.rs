//! Shared public property contract; synthetic bytes only.
use gst::prelude::*;

#[test]
fn setup_auth_properties() {
    gst::init().unwrap();
    let objects: Vec<gst::Element> = vec![
        gst::glib::Object::new::<crate::moqsrc::MoqSrc>().upcast(),
        gst::glib::Object::new::<crate::moqsink::MoqSink>().upcast(),
    ];
    for obj in objects {
        let token = obj.find_property("setup-auth-token").expect("token input");
        assert!(token.flags().contains(gst::glib::ParamFlags::WRITABLE));
        assert!(!token.flags().contains(gst::glib::ParamFlags::READABLE));
        assert_eq!(obj.property::<u64>("setup-auth-token-type"), 1);
        obj.set_property("setup-auth-token-type", u64::MAX);
        assert_eq!(obj.property::<u64>("setup-auth-token-type"), u64::MAX);
        obj.set_property("setup-auth-token", "AP8B");
        obj.set_property("setup-auth-token", None::<String>);
    }
}

#[test]
fn setup_auth_mutation_requires_ready() {
    let _suite = crate::service::fake::suite();
    let objects: Vec<gst::Element> = vec![
        gst::glib::Object::new::<crate::moqsrc::MoqSrc>().upcast(),
        gst::glib::Object::new::<crate::moqsink::MoqSink>().upcast(),
    ];
    for obj in objects {
        let _cleanup = crate::test_support::NullCleanup::new(&obj);
        obj.set_property("setup-auth-token", "AP8B");
        obj.set_property("setup-auth-token-type", 16u64);
        assert!(obj.set_state(gst::State::Paused).is_ok());
        obj.set_property("setup-auth-token-type", 1u64);
        obj.set_property("setup-auth-token", "invalid");
        assert_eq!(obj.property::<u64>("setup-auth-token-type"), 16);
        assert!(obj.set_state(gst::State::Ready).is_ok());
        // The rejected malformed replacement must not poison the next startup.
        assert!(obj.set_state(gst::State::Paused).is_ok());
        assert!(obj.set_state(gst::State::Ready).is_ok());
        obj.set_property("setup-auth-token-type", 0u64);
        obj.set_property("setup-auth-token", None::<String>);
        assert_eq!(obj.property::<u64>("setup-auth-token-type"), 0);
        assert!(obj.set_state(gst::State::Null).is_ok());
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Change {
    Replace,
    Type,
    Clear,
}

impl Change {
    pub(crate) fn apply(self, obj: &gst::Element) {
        match self {
            Self::Replace => obj.set_property("setup-auth-token", "AQID"),
            Self::Type => obj.set_property("setup-auth-token-type", u64::MAX),
            Self::Clear => obj.set_property("setup-auth-token", None::<String>),
        }
    }

    pub(crate) fn assert_snapshot(self, actual: &[moq5::AuthToken]) {
        match self {
            Self::Clear => assert!(actual.is_empty(), "clear must precede startup snapshot"),
            _ => {
                assert_eq!(actual.len(), 1);
                let (token_type, bytes): (u64, &[u8]) = match self {
                    Self::Replace => (16, &[1, 2, 3]),
                    Self::Type => (u64::MAX, &[0, 255, 1]),
                    Self::Clear => unreachable!(),
                };
                assert_eq!(
                    actual[0].token_type, token_type,
                    "admitted type must reach startup"
                );
                assert_eq!(actual[0].value, bytes, "admitted bytes must reach startup");
            }
        }
    }
}

/// Park the actual setter at its state read, then enter actual READY->PAUSED.
/// With the old unlocked admission we can finish startup before allowing the
/// setter to read VOID_PENDING and replace the already captured credential.
/// With coordinated admission startup waits for the settings owner instead.
/// All waits are bounded; joins follow explicit completion receipts.
pub(crate) fn admission_interleave(
    obj: &gst::Element,
    change: Change,
    settings_locked: impl Fn() -> bool,
    transition_and_snapshot: impl FnOnce() + Send + 'static,
) {
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;
    let bound = Duration::from_secs(5);
    let (read_tx, read_rx) = sync_channel(1);
    let (resume_tx, resume_rx) = sync_channel(1);
    let (setter_done_tx, setter_done_rx) = sync_channel(1);
    let setter_obj = obj.clone();
    let setter = std::thread::spawn(move || {
        crate::auth::interleave::on_state_read(move || {
            read_tx.send(()).unwrap();
            resume_rx
                .recv_timeout(bound)
                .expect("bounded setter release");
        });
        change.apply(&setter_obj);
        setter_done_tx.send(()).unwrap();
    });
    read_rx.recv_timeout(bound).expect("setter read reached");
    // Release any successful try_lock immediately. This schedules the old
    // interleave without deadlocking the corrected settings owner.
    let owned = settings_locked();
    let (start_tx, start_rx) = sync_channel(1);
    let (done_tx, done_rx) = sync_channel(1);
    let transition = std::thread::spawn(move || {
        crate::auth::interleave::on_start(move || {
            start_tx.send(()).unwrap();
        });
        transition_and_snapshot();
        done_tx.send(()).unwrap();
    });
    start_rx
        .recv_timeout(bound)
        .expect("transition published pending PAUSED");
    if !owned {
        done_rx
            .recv_timeout(bound)
            .expect("old startup completes before late setter");
    }
    resume_tx.send(()).unwrap();
    setter_done_rx.recv_timeout(bound).expect("setter settled");
    if owned {
        done_rx
            .recv_timeout(bound)
            .expect("startup after admitted mutation");
    }
    setter.join().unwrap();
    transition.join().unwrap();
}

/// Hold actual startup after the framework publishes pending PAUSED, before
/// taking the settings snapshot. Token/type/NULL mutations must all be refused.
pub(crate) fn transition_wins_interleave(
    obj: &gst::Element,
    transition_and_snapshot: impl FnOnce() + Send + 'static,
) {
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;
    let bound = Duration::from_secs(5);
    let (start_tx, start_rx) = sync_channel(1);
    let (resume_tx, resume_rx) = sync_channel(1);
    let (done_tx, done_rx) = sync_channel(1);
    let transition = std::thread::spawn(move || {
        crate::auth::interleave::on_start(move || {
            start_tx.send(()).unwrap();
            resume_rx
                .recv_timeout(bound)
                .expect("bounded startup release");
        });
        transition_and_snapshot();
        done_tx.send(()).unwrap();
    });
    start_rx
        .recv_timeout(bound)
        .expect("pending PAUSED published");
    let (_, current, pending) = obj.state(Some(gst::ClockTime::ZERO));
    assert_eq!(current, gst::State::Ready);
    assert_eq!(pending, gst::State::Paused);
    for change in [Change::Replace, Change::Type, Change::Clear] {
        change.apply(obj);
    }
    resume_tx.send(()).unwrap();
    done_rx.recv_timeout(bound).expect("transition settled");
    transition.join().unwrap();
}
