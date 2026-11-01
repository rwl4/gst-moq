//! Opaque SETUP credentials. No token claims or wire framing live here.
use std::fmt;

use gst::glib;
use gst::prelude::*;

const MAX_TOKEN_BYTES: usize = 65535;
const MAX_ENCODED_BYTES: usize = 87380;

#[derive(Clone)]
pub(crate) struct Settings {
    pub(crate) encoded: Option<String>,
    pub(crate) token_type: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            encoded: None,
            token_type: 1,
        }
    }
}

impl fmt::Debug for Settings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SetupAuth")
            .field("configured", &self.encoded.is_some())
            .field("token_type", &self.token_type)
            .finish()
    }
}

/// Owned bytes never acquire the safe binding's byte-revealing Debug output.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Token {
    token_type: u64,
    value: Vec<u8>,
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SetupToken([redacted])")
    }
}

impl Token {
    pub(crate) fn to_service(&self) -> moq5::AuthToken {
        moq5::AuthToken {
            token_type: self.token_type,
            value: self.value.clone(),
        }
    }
}

impl Settings {
    pub(crate) fn decode(&self) -> Result<Option<Token>, &'static str> {
        let Some(encoded) = &self.encoded else {
            return Ok(None);
        };
        if encoded.is_empty() {
            return Err("SETUP token must not be empty");
        }
        if encoded.len() > MAX_ENCODED_BYTES {
            return Err("SETUP token exceeds the encoded size limit");
        }
        // GLib's decoder is permissive and accepts a C string. Reject NUL first,
        // then require an exact canonical round trip, including padding.
        if encoded.contains('\0') {
            return Err("SETUP token is not canonical Base64");
        }
        let value = glib::base64_decode(encoded);
        if value.len() > MAX_TOKEN_BYTES {
            return Err("SETUP token exceeds the decoded size limit");
        }
        if glib::base64_encode(&value).as_str() != encoded {
            return Err("SETUP token is not canonical Base64");
        }
        Ok(Some(Token {
            token_type: self.token_type,
            value,
        }))
    }
}

pub(crate) fn properties() -> [glib::ParamSpec; 2] {
    [
        glib::ParamSpecString::builder("setup-auth-token")
            .nick("SETUP authorization token")
            .blurb("Opaque SETUP token as canonical padded standard Base64; NULL is anonymous. Inject programmatically to keep secrets out of command lines.")
            .write_only()
            .mutable_ready()
            .build(),
        glib::ParamSpecUInt64::builder("setup-auth-token-type")
            .nick("SETUP authorization token type")
            .blurb("Token type: standard CAT is 1; choose other types explicitly. No token is sent without setup-auth-token.")
            .default_value(1)
            .mutable_ready()
            .build(),
    ]
}

/// Caller holds the element's settings mutex through this check and mutation.
/// The one framework snapshot cannot combine READY with a later VOID_PENDING.
/// Startup takes the same settings mutex: an admitted mutation precedes its
/// snapshot, or observes the already published transition and is refused.
pub(crate) fn can_mutate(obj: &impl IsA<gst::Element>) -> bool {
    let (_, current, pending) = obj.state(Some(gst::ClockTime::ZERO));
    #[cfg(test)]
    interleave::after_state_read();
    current <= gst::State::Ready && pending <= gst::State::Ready
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(encoded: Option<&str>, token_type: u64) -> Settings {
        Settings {
            encoded: encoded.map(str::to_owned),
            token_type,
        }
    }

    #[test]
    fn setup_auth_canonical_binary_and_type() {
        for token_type in [0, 1, 16, (1 << 62) - 1, 1 << 62, u64::MAX] {
            let token = settings(Some("AP8B"), token_type)
                .decode()
                .unwrap()
                .unwrap();
            let actual = token.to_service();
            assert_eq!(actual.token_type, token_type);
            assert_eq!(actual.value, [0, 255, 1]);
        }
        assert!(settings(None, u64::MAX).decode().unwrap().is_none());
    }

    #[test]
    fn setup_auth_malformed_never_becomes_absent() {
        for value in [
            "",
            "AA",
            "AA=",
            "AA===",
            "AB==",
            "AA==\n",
            " AA==",
            "_w==",
            "-w==",
            "base64:AA==",
            "A\0A==",
            "!!!!",
        ] {
            assert!(settings(Some(value), 1).decode().is_err());
        }
        assert_eq!(
            settings(Some("AA=="), 1)
                .decode()
                .unwrap()
                .unwrap()
                .to_service()
                .value,
            [0]
        );
    }

    #[test]
    fn setup_auth_size_boundaries() {
        for size in [65534, 65535, 65536] {
            let encoded = glib::base64_encode(&vec![42; size]);
            let decoded = settings(Some(&encoded), 1).decode();
            if size <= MAX_TOKEN_BYTES {
                assert_eq!(decoded.unwrap().unwrap().to_service().value.len(), size);
            } else {
                assert!(decoded.is_err());
            }
        }
        assert!(settings(Some(&"A".repeat(MAX_ENCODED_BYTES + 1)), 1)
            .decode()
            .is_err());
    }

    #[test]
    fn setup_auth_ownership_and_redaction() {
        let mut original = settings(Some("AP8B"), 16);
        let snapshot = original.clone();
        let decoded = original.decode().unwrap().unwrap();
        original.encoded = None;
        original.token_type = 1;
        assert_eq!(snapshot.decode().unwrap().unwrap(), decoded);
        assert!(original.decode().unwrap().is_none());
        assert_eq!(format!("{decoded:?}"), "SetupToken([redacted])");
        assert!(!format!("{snapshot:?}").contains("AP8B"));
        let mut cfg = crate::moq::endpoint_config("https://example.invalid", false, 18);
        cfg.setup_auth = Some(decoded);
        assert!(!format!("{cfg:?}").contains("AP8B"));
        assert!(!format!("{cfg:?}").contains("255"));
    }
}

/// Thread-local one-shot gates for deterministic element tests only. No
/// production callbacks, shared hook registry or exported API.
#[cfg(test)]
pub(crate) mod interleave {
    use std::cell::RefCell;

    type Hook = Option<Box<dyn FnOnce()>>;
    thread_local! {
        static AFTER_READ: RefCell<Hook> = const { RefCell::new(None) };
        static BEFORE_START: RefCell<Hook> = const { RefCell::new(None) };
    }

    pub(crate) fn on_state_read(hook: impl FnOnce() + 'static) {
        AFTER_READ.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    }

    pub(crate) fn on_start(hook: impl FnOnce() + 'static) {
        BEFORE_START.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    }

    pub(crate) fn after_state_read() {
        let hook = AFTER_READ.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(crate) fn before_start() {
        let hook = BEFORE_START.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
    }
}
