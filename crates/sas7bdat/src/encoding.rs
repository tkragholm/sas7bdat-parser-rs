use encoding_rs::{Encoding, UTF_8};

pub fn resolve_encoding(label: Option<&str>) -> &'static Encoding {
    label.and_then(resolve_label).unwrap_or(UTF_8)
}

fn resolve_label(name: &str) -> Option<&'static Encoding> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }

    try_encoding_label(trimmed).or_else(|| {
        let lower = trimmed.to_ascii_lowercase();
        try_encoding_label(&lower)
            .or_else(|| try_encoding_label(&lower.replace('_', "-")))
            .or_else(|| mac_compat_encoding(&lower))
    })
}

fn try_encoding_label(label: &str) -> Option<&'static Encoding> {
    Encoding::for_label(label.as_bytes())
}

fn mac_compat_encoding(lower_label: &str) -> Option<&'static Encoding> {
    match lower_label {
        "macroman" => Encoding::for_label(b"macintosh"),
        "macarabic" => Encoding::for_label(b"x-mac-arabic"),
        "machebrew" => Encoding::for_label(b"x-mac-hebrew"),
        "macgreek" => Encoding::for_label(b"x-mac-greek"),
        "macthai" => Encoding::for_label(b"x-mac-thai"),
        "macturkish" => Encoding::for_label(b"x-mac-turkish"),
        "macukraine" => Encoding::for_label(b"x-mac-ukrainian"),
        "maciceland" => Encoding::for_label(b"x-mac-icelandic"),
        "maccroatian" => Encoding::for_label(b"x-mac-croatian"),
        "maccyrillic" => Encoding::for_label(b"x-mac-cyrillic"),
        "macromania" => Encoding::for_label(b"x-mac-romanian"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_encoding;

    #[test]
    fn missing_empty_and_blank_labels_fall_back_to_utf8() {
        assert_eq!(resolve_encoding(None).name(), "UTF-8");
        assert_eq!(resolve_encoding(Some("")).name(), "UTF-8");
        assert_eq!(resolve_encoding(Some("   ")).name(), "UTF-8");
    }

    #[test]
    fn direct_labels_resolve_case_insensitively() {
        // Exact label, then the to-ascii-lowercase fallback branch.
        assert_eq!(
            resolve_encoding(Some("windows-1252")).name(),
            "windows-1252"
        );
        assert_eq!(
            resolve_encoding(Some("WINDOWS-1252")).name(),
            "windows-1252"
        );
        assert_eq!(resolve_encoding(Some("UTF-8")).name(), "UTF-8");
    }

    #[test]
    fn underscores_are_normalized_to_hyphens() {
        // Exercises the `lower.replace('_', "-")` branch: WHATWG folds iso-8859-1
        // onto windows-1252.
        assert_eq!(resolve_encoding(Some("ISO_8859_1")).name(), "windows-1252");
    }

    #[test]
    fn mac_aliases_in_the_standard_resolve() {
        // SAS mac aliases that exist in the WHATWG Encoding Standard. macroman maps to
        // the `macintosh` encoding; maccyrillic and macukraine both fold onto the
        // Cyrillic Mac encoding (Ukrainian shares it).
        assert_eq!(resolve_encoding(Some("MacRoman")).name(), "macintosh");
        assert_eq!(
            resolve_encoding(Some("maccyrillic")).name(),
            "x-mac-cyrillic"
        );
        assert_eq!(
            resolve_encoding(Some("macukraine")).name(),
            "x-mac-cyrillic"
        );
    }

    #[test]
    fn mac_aliases_outside_the_standard_exercise_their_arm_and_fall_back() {
        // Not part of the WHATWG Encoding Standard, so `for_label` yields None and
        // the resolver falls back to UTF-8 — but each match arm is still executed.
        for label in [
            "macarabic",
            "machebrew",
            "macgreek",
            "macthai",
            "macturkish",
            "maciceland",
            "maccroatian",
            "macromania",
        ] {
            assert_eq!(
                resolve_encoding(Some(label)).name(),
                "UTF-8",
                "{label} should fall back to UTF-8",
            );
        }
    }

    #[test]
    fn unknown_label_falls_back_to_utf8() {
        assert_eq!(
            resolve_encoding(Some("definitely-not-an-encoding")).name(),
            "UTF-8"
        );
    }
}
