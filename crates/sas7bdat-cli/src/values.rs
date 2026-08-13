//! Number formatting for terminal output.
//!
//! Rendering SAS *values* moved to `sas7bdat_convert::values` with the rest of
//! conversion; what is left here formats counts and sizes for a human reading a
//! report, which is the CLI's own concern.

/// Group a number with thousands separators (e.g. `70044` -> `70,044`).
#[must_use]
pub fn thousands(n: u64) -> String {
    let digits = n.to_string();
    let len = digits.len();
    let mut out = String::with_capacity(len + len / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

/// Format a byte count as a compact human-readable size (e.g. `1.2 KB`).
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::{human_bytes, thousands};

    #[test]
    fn thousands_groups_digits() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(42), "42");
        assert_eq!(thousands(4041), "4,041");
        assert_eq!(thousands(70044), "70,044");
        assert_eq!(thousands(1_234_567), "1,234,567");
    }

    #[test]
    fn human_bytes_scales_units() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1024), "1.0 KB");
        assert_eq!(human_bytes(1536), "1.5 KB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MB");
    }
}
