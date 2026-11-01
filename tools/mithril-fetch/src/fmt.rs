//! Presentation-only formatting. No plan arithmetic and no rendering.

use std::time::{SystemTime, UNIX_EPOCH};

pub use mithril_snapshot::ingest::human_bytes as bytes;

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

/// `2026-10-29 14:03:12Z`, from a unix timestamp.
///
/// Mainnet slots are one second, so this is also the wall clock at a slot.
pub fn utc(unix: i64) -> String {
    let days = unix.div_euclid(86_400);
    let seconds = unix.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}Z",
        seconds / 3_600,
        (seconds % 3_600) / 60,
        seconds % 60
    )
}

/// Howard Hinnant's `civil_from_days`, with the epoch shifted to 0000-03-01.
fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };

    (year + i64::from(month <= 2), month as u32, day as u32)
}

pub fn duration(seconds: u64) -> String {
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let rest = seconds % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m")
    } else if minutes > 0 {
        format!("{minutes}m {rest:02}s")
    } else {
        format!("{rest}s")
    }
}

/// Thousands separators, for counts a reader compares at a glance.
pub fn count(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(character);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_and_a_few_known_instants_are_right() {
        assert_eq!(utc(0), "1970-01-01 00:00:00Z");
        assert_eq!(utc(1_704_067_200), "2024-01-01 00:00:00Z");
        assert_eq!(utc(1_596_059_091), "2020-07-29 21:44:51Z");
        assert_eq!(utc(1_600_000_000), "2020-09-13 12:26:40Z");
        // A leap day, which is where a hand-rolled calendar usually breaks.
        assert_eq!(utc(1_709_164_800), "2024-02-29 00:00:00Z");
    }

    #[test]
    fn durations_read_as_durations() {
        assert_eq!(duration(9), "9s");
        assert_eq!(duration(90), "1m 30s");
        assert_eq!(duration(3_600 + 120), "1h 02m");
    }

    #[test]
    fn counts_get_thousands_separators() {
        assert_eq!(count(0), "0");
        assert_eq!(count(999), "999");
        assert_eq!(count(1_000), "1,000");
        assert_eq!(count(1_234_567), "1,234,567");
    }
}
