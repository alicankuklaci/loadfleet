use base64::{engine::general_purpose::STANDARD, Engine};
use flate2::{write::ZlibEncoder, Compression};
use loadfleet_metrics::*;
use std::io::Write;

fn craft(payload_len: u32, digits: u32, low: u64, high: u64, body: &[u8]) -> String {
    let mut inner = Vec::new();
    inner.extend(0x1c84_9313u32.to_be_bytes());
    inner.extend(payload_len.to_be_bytes());
    inner.extend(0u32.to_be_bytes());
    inner.extend(digits.to_be_bytes());
    inner.extend(low.to_be_bytes());
    inner.extend(high.to_be_bytes());
    inner.extend(1.0f64.to_be_bytes());
    inner.extend(body);
    let mut z = ZlibEncoder::new(Vec::new(), Compression::best());
    z.write_all(&inner).unwrap();
    let comp = z.finish().unwrap();
    let mut out = Vec::new();
    out.extend(0x1c84_9314u32.to_be_bytes());
    out.extend((comp.len() as u32).to_be_bytes());
    out.extend(comp);
    STANDARD.encode(out)
}

#[test]
fn huge_payload_len_is_rejected_without_allocating() {
    let s = craft(0xFFFF_FFF0, 3, 1, HIST_MAX_US_FOR_TEST, &[0; 8]);
    assert!(s.len() < 200);
    assert!(decode_hist(&s).is_err());
}

const HIST_MAX_US_FOR_TEST: u64 = 3_600_000_000;

#[test]
fn mismatched_bounds_are_rejected() {
    assert!(decode_hist(&craft(1, 5, 1, u64::MAX / 2, &[0])).is_err());
    assert!(decode_hist(&craft(1, 3, 1, HIST_MAX_US_FOR_TEST - 1, &[0])).is_err());
    assert!(decode_hist(&craft(1, 3, 2, HIST_MAX_US_FOR_TEST, &[0])).is_err());
}

#[test]
fn zip_bomb_is_rejected() {
    // valid header, tiny declared payload, then ~64 MiB of zeros that deflate to ~64 KiB
    let s = craft(4, 3, 1, HIST_MAX_US_FOR_TEST, &vec![0u8; 64 << 20]);
    assert!(
        s.len() < MAX_HIST_B64_BYTES,
        "bomb must slip past the size cap: {}",
        s.len()
    );
    assert!(decode_hist(&s).is_err());
}

#[test]
fn honest_histograms_still_roundtrip() {
    let mut h = new_hist();
    for i in 1..5000u64 {
        h.saturating_record(i * 37);
    }
    let d = decode_hist(&encode_hist(&h)).unwrap();
    assert_eq!(d.len(), h.len());
    assert_eq!(d.value_at_quantile(0.99), h.value_at_quantile(0.99));
    assert!(decode_hist(&encode_hist(&new_hist())).unwrap().is_empty());
}

#[test]
fn crafted_min_varint_is_an_error_not_a_panic() {
    // a 9-byte varint FF x9 decodes to i64::MIN, which overflows inside
    // hdrhistogram's zero-run handling in debug builds
    let s = craft(9, 3, 1, HIST_MAX_US_FOR_TEST, &[0xFF; 9]);
    assert!(decode_hist(&s).is_err());
}
