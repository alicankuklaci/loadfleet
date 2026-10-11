use std::io::{self, ErrorKind};
use std::sync::atomic::{AtomicU64, Ordering};

static CLIENT_ERRORS: AtomicU64 = AtomicU64::new(0);

const EMFILE: i32 = 24;
const ENFILE: i32 = 23;
#[cfg(target_os = "macos")]
const EADDRNOTAVAIL: i32 = 49;
#[cfg(not(target_os = "macos"))]
const EADDRNOTAVAIL: i32 = 99;

/// Maps a socket error to an error code; `client:*` means the load generator itself ran out of something.
pub fn classify_io_error(e: &io::Error) -> &'static str {
    let code = match e.raw_os_error() {
        Some(EMFILE) | Some(ENFILE) => "client:too_many_files",
        Some(EADDRNOTAVAIL) => "client:addr_not_available",
        _ => match e.kind() {
            ErrorKind::AddrNotAvailable => "client:addr_not_available",
            ErrorKind::ConnectionRefused => "net:refused",
            ErrorKind::ConnectionReset => "net:reset",
            ErrorKind::ConnectionAborted => "net:aborted",
            ErrorKind::TimedOut => "net:timeout",
            _ => "net:io",
        },
    };
    if code.starts_with("client:") {
        CLIENT_ERRORS.fetch_add(1, Ordering::Relaxed);
    }
    code
}

/// [`classify_io_error`] for a failed `connect()`: there EAGAIN means no free ephemeral port
/// (macOS returns it for IPv6 instead of EADDRNOTAVAIL), so it is a client error too.
pub fn classify_connect_error(e: &io::Error) -> &'static str {
    if e.kind() == ErrorKind::WouldBlock {
        CLIENT_ERRORS.fetch_add(1, Ordering::Relaxed);
        return "client:addr_not_available";
    }
    classify_io_error(e)
}

pub fn client_error_count() -> u64 {
    CLIENT_ERRORS.load(Ordering::Relaxed)
}
