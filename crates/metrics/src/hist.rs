use crate::MetricsError;
use base64::{engine::general_purpose::STANDARD, Engine};
use flate2::read::ZlibDecoder;
use hdrhistogram::serialization::{Deserializer, Serializer, V2DeflateSerializer};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::time::Duration;

/// 1 hour in microseconds: the largest latency we keep.
pub const HIST_MAX_US: u64 = 3_600_000_000;

pub fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, HIST_MAX_US, 3).expect("valid histogram bounds")
}

pub fn record(h: &mut Histogram<u64>, d: Duration) {
    h.saturating_record((d.as_micros() as u64).clamp(1, HIST_MAX_US));
}

pub fn encode_hist(h: &Histogram<u64>) -> String {
    let mut buf = Vec::new();
    V2DeflateSerializer::new()
        .serialize(h, &mut buf)
        .expect("serializing to a Vec cannot fail");
    STANDARD.encode(buf)
}

/// Largest base64 histogram payload accepted from a peer (a populated 3-digit histogram
/// is a few KiB compressed); larger inputs are refused before any decoding.
pub const MAX_HIST_B64_BYTES: usize = 256 * 1024;

const OUTER_COOKIE: u32 = 0x1c84_9314; // V2 compressed
const INNER_COOKIE: u32 = 0x1c84_9313; // V2 uncompressed
const INNER_HEADER: usize = 40;
/// A `new_hist()` histogram has ~24k counts of at most 10 varint bytes each; 1 MiB is generous.
const MAX_INNER_PAYLOAD: usize = 1 << 20;

fn be32(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}
fn be64(b: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(b[at..at + 8].try_into().expect("8 bytes"))
}

fn bad(msg: &str) -> MetricsError {
    MetricsError::Decode(msg.to_string())
}

/// Decode a histogram received from a peer. Safe against crafted input: the compressed size is
/// capped, only the 40-byte inner header is inflated first and must match `new_hist()`'s bounds
/// with a sane payload length, and the total inflated size is limited to header + that length
/// (a zip bomb is refused) before the hdrhistogram decoder runs on the bounded bytes.
pub fn decode_hist(s: &str) -> Result<Histogram<u64>, MetricsError> {
    if s.len() > MAX_HIST_B64_BYTES {
        return Err(MetricsError::Decode(format!(
            "histogram payload too large: {} bytes (max {MAX_HIST_B64_BYTES})",
            s.len()
        )));
    }
    let bytes = STANDARD
        .decode(s)
        .map_err(|e| MetricsError::Decode(e.to_string()))?;
    if bytes.len() < 8 || be32(&bytes, 0) != OUTER_COOKIE {
        return Err(bad("not a compressed V2 histogram"));
    }
    if be32(&bytes, 4) as usize != bytes.len() - 8 {
        return Err(bad("compressed length mismatch"));
    }
    let comp = &bytes[8..];

    // Pass 1: inflate only the inner header.
    let mut head = [0u8; INNER_HEADER];
    ZlibDecoder::new(comp)
        .read_exact(&mut head)
        .map_err(|e| MetricsError::Decode(format!("header: {e}")))?;
    let reference = new_hist();
    let payload_len = be32(&head, 4) as usize;
    if be32(&head, 0) != INNER_COOKIE
        || be32(&head, 8) != 0
        || be32(&head, 12) != u32::from(reference.sigfig())
        || be64(&head, 16) != reference.low()
        || be64(&head, 24) != reference.high()
        || f64::from_bits(be64(&head, 32)) != 1.0
    {
        return Err(bad("histogram bounds differ from the expected ones"));
    }
    if payload_len > MAX_INNER_PAYLOAD {
        return Err(bad("histogram payload length out of range"));
    }

    // Pass 2: inflate at most header + payload_len, and require the stream to end there.
    let total = INNER_HEADER + payload_len;
    let mut dec = ZlibDecoder::new(comp);
    let mut inner = Vec::with_capacity(total.min(64 * 1024));
    (&mut dec)
        .take(total as u64)
        .read_to_end(&mut inner)
        .map_err(|e| MetricsError::Decode(format!("inflate: {e}")))?;
    if inner.len() != total {
        return Err(bad("histogram payload truncated"));
    }
    let mut extra = [0u8; 1];
    if dec
        .read(&mut extra)
        .map_err(|e| MetricsError::Decode(e.to_string()))?
        != 0
    {
        return Err(bad("histogram has trailing data"));
    }

    // A crafted varint (FF x9 = i64::MIN) overflows inside hdrhistogram's
    // zero-run handling and panics in builds with overflow checks; contain it as an error.
    // (Release keeps overflow-checks off, where it is caught by the length checks instead.)
    let decoded = std::panic::catch_unwind(move || {
        Deserializer::new().deserialize::<u64, _>(&mut std::io::Cursor::new(inner))
    })
    .map_err(|_| bad("histogram payload is malformed"))?;
    let h: Histogram<u64> = decoded.map_err(|e| MetricsError::Decode(format!("{e:?}")))?;
    let mut out = new_hist();
    out.add(&h)
        .map_err(|e| MetricsError::Merge(format!("{e:?}")))?;
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LatSummary {
    pub n: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

impl LatSummary {
    pub fn of(h: &Histogram<u64>) -> Option<LatSummary> {
        if h.is_empty() {
            return None;
        }
        let ms = |v: u64| (v as f64 / 100.0).round() / 10.0; // µs -> ms, 0.1 ms resolution
        Some(LatSummary {
            n: h.len(),
            p50_ms: ms(h.value_at_quantile(0.50)),
            p95_ms: ms(h.value_at_quantile(0.95)),
            p99_ms: ms(h.value_at_quantile(0.99)),
            max_ms: ms(h.max()),
        })
    }
}
