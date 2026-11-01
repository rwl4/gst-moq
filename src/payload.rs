//! Copy application-owned media into binding-owned storage before handoff.
//!
//! Gst memory may run an application destructor when its last mapping drops.
//! It must be released here, never from the native network-thread callback.

use moq5::{Buffer, Error, Result};
use std::ops::Range;

pub(crate) fn copy_owner<T: AsRef<[u8]>>(owner: T, range: Range<usize>) -> Result<Buffer> {
    let bytes = owner.as_ref().get(range).ok_or(Error::INVAL)?;
    Buffer::copy_from(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    struct Owner(Arc<Mutex<Vec<std::thread::ThreadId>>>, [u8; 4]);
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.1
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            // Models application reentry on release; no native callback may run it.
            self.0.lock().unwrap().push(std::thread::current().id());
        }
    }
    #[test]
    fn gst_owner_released_before_native_handoff() {
        gst::init().unwrap();
        for remote_release in [false, true] {
            let drops = Arc::new(Mutex::new(Vec::new()));
            let buffer = gst::Buffer::from_slice(Owner(drops.clone(), [1, 2, 3, 4]));
            let mapped = buffer.into_mapped_buffer_readable().unwrap();
            let copied = copy_owner(mapped, 1..3).unwrap();
            assert_eq!(&*drops.lock().unwrap(), &[std::thread::current().id()]);
            assert_eq!(copied.as_slice(), &[2, 3]);
            if remote_release {
                std::thread::spawn(move || drop(copied)).join().unwrap();
            } else {
                drop(copied);
            }
            assert_eq!(drops.lock().unwrap().len(), 1);
        }
    }
}
