use encoding_rs::{Encoding, UTF_8};

pub fn resolve_encoding(label: Option<&str>) -> &'static Encoding {
    label
        .and_then(resolve_label)
        .unwrap_or(UTF_8)
}

pub fn trim_trailing(bytes: &[u8]) -> &[u8] {
    match bytes.iter().rposition(|b| *b != 0 && *b != b' ') {
        Some(last) => &bytes[..=last],
        None => &[],
    }
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
