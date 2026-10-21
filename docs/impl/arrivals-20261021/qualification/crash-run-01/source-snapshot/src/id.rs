//! Identifier and timestamp helpers.
//!
//! Message IDs are ULIDs generated from `/dev/urandom` and the current wall
//! clock, avoiding a dependency on a randomness or UUID crate. ULIDs are
//! lexicographically sortable by creation time and fit in 26 Crockford base32
//! characters.

use std::fs::File;
use std::io::{self, Read};
use std::time::{SystemTime, UNIX_EPOCH};

const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Returns the current Unix time in milliseconds.
pub fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Reads `N` bytes from the operating system CSPRNG (`/dev/urandom`).
pub fn random_bytes<const N: usize>() -> io::Result<[u8; N]> {
    let mut buffer = [0u8; N];
    File::open("/dev/urandom")?.read_exact(&mut buffer)?;
    Ok(buffer)
}

/// Generates a ULID using the current time and 80 random bits.
pub fn new_ulid() -> io::Result<String> {
    let random = random_bytes::<10>()?;
    Ok(ulid_from_parts(unix_millis(), random))
}

/// Builds a ULID from an explicit millisecond timestamp and 80 random bits.
///
/// This is primarily useful for deterministic tests of the encoding.
pub fn ulid_from_parts(unix_ms: u64, random: [u8; 10]) -> String {
    let mut ulid = encode_time(unix_ms);
    ulid.push_str(&encode_random(&random));
    ulid
}

/// Returns true when `value` is a syntactically valid 26-character ULID.
pub fn is_ulid(value: &str) -> bool {
    value.len() == 26 && value.bytes().all(|byte| ALPHABET.contains(&byte))
}

/// Formats the current time as an RFC 3339 UTC timestamp with millisecond
/// precision, for example `2026-10-18T12:34:56.789Z`.
pub fn now_rfc3339() -> String {
    format_rfc3339_millis(unix_millis())
}

/// Formats a Unix millisecond timestamp as RFC 3339 UTC.
pub fn format_rfc3339_millis(unix_ms: u64) -> String {
    let seconds = unix_ms / 1_000;
    let millis = unix_ms % 1_000;
    let days = (seconds / 86_400) as i64;
    let second_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = second_of_day / 3_600;
    let minute = (second_of_day % 3_600) / 60;
    let second = second_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// Converts days since 1970-01-01 into a civil date (Howard Hinnant's
/// `civil_from_days` algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = (z - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (year + i64::from(month <= 2), month, day)
}

fn encode_time(unix_ms: u64) -> String {
    let mut encoded = String::with_capacity(10);
    for index in 0..10 {
        let shift = 45 - 5 * index;
        let value = ((unix_ms >> shift) & 0x1f) as usize;
        encoded.push(ALPHABET[value] as char);
    }
    encoded
}

fn encode_random(random: &[u8; 10]) -> String {
    let mut value: u128 = 0;
    for byte in random {
        value = (value << 8) | u128::from(*byte);
    }
    let mut encoded = String::with_capacity(16);
    for index in 0..16 {
        let shift = 75 - 5 * index;
        let symbol = ((value >> shift) & 0x1f) as usize;
        encoded.push(ALPHABET[symbol] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulid_time_encoding_matches_spec_vector() {
        assert_eq!(
            ulid_from_parts(1_469_918_176_385, [0; 10]),
            "01ARYZ6S410000000000000000"
        );
    }

    #[test]
    fn generated_ulids_are_valid() {
        let ulid = new_ulid().expect("read /dev/urandom");
        assert!(is_ulid(&ulid), "invalid ULID: {ulid}");
    }

    #[test]
    fn rfc3339_format_is_stable() {
        assert_eq!(format_rfc3339_millis(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_rfc3339_millis(1_700_000_000_123),
            "2023-11-14T22:13:20.123Z"
        );
    }
}
