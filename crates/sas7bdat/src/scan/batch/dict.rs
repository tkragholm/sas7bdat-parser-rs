//! In-scan string dictionary staging.
//!
//! SAS character columns are fixed-width and often low-cardinality, so the same bytes recur
//! across rows. This table deduplicates them *within a batch* while the scan runs: repeated
//! values are stored once in the column's data buffer and referenced again, which cuts the
//! bytes a string-heavy batch allocates.
//!
//! It is not the same thing as [`crate::dictionary`], and the two do not overlap. This is a
//! per-batch allocation optimisation with no output of its own; that module is a post-scan,
//! whole-column encoder that emits `{codes, levels}` for a categorical (an R `factor`).
//!
//! Cost control is the whole design. A free-text or ID column has nothing to deduplicate, so
//! the table gives up on one: a value is stored raw on first sight and only interned on the
//! second (`insert_seen_once` / promotion), and once enough lookups have gone by with too few
//! promotions the table disables itself for the rest of the batch.

const DICT_CAPACITY: usize = 512;
const DICT_SLOT_MASK: usize = DICT_CAPACITY - 1;
const MAX_DICT_ENTRIES: usize = 200;
pub(super) const MAX_STAGED_STRING_WIDTH: u32 = 20;
const INLINE_DICT_KEY_BYTES: usize = 16;
pub(super) const DICT_ID_NONE: u32 = u32::MAX;
const DICT_DISABLE_MIN_LOOKUPS: u32 = 512;
const DICT_DISABLE_MIN_LOOKUPS_ZERO_PROMOTIONS: u32 = 256;
const DICT_DISABLE_MIN_PROMOTION_BPS: u32 = 800; // 8%

#[derive(Debug, Clone, Copy)]
struct DictSlot {
    fingerprint: u16, // 0 = empty
    entry_idx: u16,
}

impl DictSlot {
    const EMPTY: Self = Self {
        fingerprint: 0,
        entry_idx: 0,
    };
}

#[derive(Debug, Clone, Copy)]
enum DictEntryState {
    SeenOnce,
    Interned,
}

#[derive(Debug, Clone)]
struct DictEntry {
    raw_start: u32,
    raw_end: u32,
    inline_len: u8,
    inline_key: [u8; INLINE_DICT_KEY_BYTES],
    state: DictEntryState,
    utf8_is_raw: bool,
    utf8_start: u32,
    utf8_end: u32,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum StageLookupHit {
    SeenOnce(u16),
    Interned(u16),
}

/// Per-column open-address hash table mapping semantic string bytes -> staged entry.
/// Entries are first staged as `SeenOnce`, then promoted to `Interned` on second sighting.
#[derive(Debug)]
pub(super) struct StagedStringLookup {
    slots: Vec<DictSlot>,
    entries: Vec<DictEntry>,
    /// Concatenated raw bytes for staged dictionary keys.
    raw_data: Vec<u8>,
    /// Concatenated UTF-8 bytes for each unique entry.
    utf8_data: Vec<u8>,
    seen: u32,
    promoted: u32,
    disabled: bool,
    recent_hashes: [u32; 4],
    recent_entries: [u16; 4],
    recent_valid: [u8; 4],
}

impl StagedStringLookup {
    pub(super) fn new() -> Self {
        Self {
            slots: vec![DictSlot::EMPTY; DICT_CAPACITY],
            entries: Vec::new(),
            raw_data: Vec::new(),
            utf8_data: Vec::new(),
            seen: 0,
            promoted: 0,
            disabled: false,
            recent_hashes: [0; 4],
            recent_entries: [0; 4],
            recent_valid: [0; 4],
        }
    }

    #[inline]
    pub(super) const fn should_use(&self) -> bool {
        !self.disabled
    }

    #[inline]
    pub(super) const fn observe_lookup(&mut self) {
        self.seen = self.seen.saturating_add(1);
        if self.promoted == 0 && self.seen >= DICT_DISABLE_MIN_LOOKUPS_ZERO_PROMOTIONS {
            self.disabled = true;
            return;
        }
        if self.seen >= DICT_DISABLE_MIN_LOOKUPS
            && self.promoted.saturating_mul(10_000)
                < self.seen.saturating_mul(DICT_DISABLE_MIN_PROMOTION_BPS)
        {
            self.disabled = true;
        }
    }

    #[inline]
    fn fnv1a(bytes: &[u8]) -> u32 {
        let mut h = 0x811c_9dc5_u32;
        for &b in bytes {
            h ^= u32::from(b);
            h = h.wrapping_mul(0x0100_0193);
        }
        h
    }

    #[inline]
    fn key_hash_and_fingerprint(key: &[u8]) -> (u32, u16) {
        let h = Self::fnv1a(key);
        let fp = (h >> 16) as u16 | 1; // force non-zero
        (h, fp)
    }

    #[inline]
    const fn remember_recent(&mut self, hash: u32, entry_idx: u16) {
        let slot = (hash as usize) & 3;
        self.recent_hashes[slot] = hash;
        self.recent_entries[slot] = entry_idx;
        self.recent_valid[slot] = 1;
    }

    /// Returns staged state for `key` if present.
    #[inline]
    pub(super) fn lookup(&mut self, key: &[u8]) -> Option<StageLookupHit> {
        if self.disabled {
            return None;
        }
        let (h, fp) = Self::key_hash_and_fingerprint(key);
        let recent_slot = (h as usize) & 3;
        if self.recent_valid[recent_slot] != 0 && self.recent_hashes[recent_slot] == h {
            let entry_idx = self.recent_entries[recent_slot];
            let (matched, state) = {
                let entry = &self.entries[entry_idx as usize];
                (self.entry_key_matches(entry, key), entry.state)
            };
            if matched {
                return match state {
                    DictEntryState::SeenOnce => Some(StageLookupHit::SeenOnce(entry_idx)),
                    DictEntryState::Interned => Some(StageLookupHit::Interned(entry_idx)),
                };
            }
        }

        let mut slot = (h as usize) & DICT_SLOT_MASK;
        loop {
            let s = self.slots[slot];
            if s.fingerprint == 0 {
                return None; // empty slot -> not in table
            }
            if s.fingerprint == fp {
                let (matched, state) = {
                    let entry = &self.entries[s.entry_idx as usize];
                    (self.entry_key_matches(entry, key), entry.state)
                };
                if matched {
                    self.remember_recent(h, s.entry_idx);
                    return match state {
                        DictEntryState::SeenOnce => Some(StageLookupHit::SeenOnce(s.entry_idx)),
                        DictEntryState::Interned => Some(StageLookupHit::Interned(s.entry_idx)),
                    };
                }
            }
            slot = (slot + 1) & DICT_SLOT_MASK;
        }
    }

    #[inline]
    fn entry_raw_bytes<'a>(&'a self, entry: &DictEntry) -> &'a [u8] {
        let start = entry.raw_start as usize;
        let end = entry.raw_end as usize;
        &self.raw_data[start..end]
    }

    #[inline]
    fn entry_key_matches(&self, entry: &DictEntry, key: &[u8]) -> bool {
        if key.len() <= INLINE_DICT_KEY_BYTES && usize::from(entry.inline_len) == key.len() {
            return entry.inline_key[..key.len()] == *key;
        }
        self.entry_raw_bytes(entry) == key
    }

    pub(super) fn interned_utf8(&self, entry_idx: u16) -> &[u8] {
        let entry = &self.entries[entry_idx as usize];
        if entry.utf8_is_raw {
            return self.entry_raw_bytes(entry);
        }
        let start = entry.utf8_start as usize;
        let end = entry.utf8_end as usize;
        &self.utf8_data[start..end]
    }

    pub(super) fn insert_seen_once(&mut self, key: &[u8]) -> Option<u16> {
        if self.disabled {
            return None;
        }
        if key.len() > MAX_STAGED_STRING_WIDTH as usize {
            return None;
        }
        if self.entries.len() >= MAX_DICT_ENTRIES {
            return None;
        }
        let (h, fp) = Self::key_hash_and_fingerprint(key);
        let mut slot = (h as usize) & DICT_SLOT_MASK;
        loop {
            if self.slots[slot].fingerprint == 0 {
                break;
            }
            slot = (slot + 1) & DICT_SLOT_MASK;
        }
        let entry_idx = u16::try_from(self.entries.len()).ok()?;
        let raw_start = u32::try_from(self.raw_data.len()).ok()?;
        self.raw_data.extend_from_slice(key);
        let raw_end = u32::try_from(self.raw_data.len()).ok()?;
        let mut inline_key = [0_u8; INLINE_DICT_KEY_BYTES];
        let inline_len = if key.len() <= INLINE_DICT_KEY_BYTES {
            inline_key[..key.len()].copy_from_slice(key);
            u8::try_from(key.len()).expect("inline key length is capped")
        } else {
            0
        };
        self.entries.push(DictEntry {
            raw_start,
            raw_end,
            inline_len,
            inline_key,
            state: DictEntryState::SeenOnce,
            utf8_is_raw: false,
            utf8_start: 0,
            utf8_end: 0,
        });
        self.slots[slot] = DictSlot {
            fingerprint: fp,
            entry_idx,
        };
        self.remember_recent(h, entry_idx);
        Some(entry_idx)
    }

    #[inline]
    pub(super) fn promote_interned(&mut self, entry_idx: u16, utf8: &[u8], utf8_is_raw: bool) {
        let (start, end) = if utf8_is_raw {
            (0, 0)
        } else {
            let start = u32::try_from(self.utf8_data.len()).expect("dict utf8 data within u32");
            self.utf8_data.extend_from_slice(utf8);
            let end = u32::try_from(self.utf8_data.len()).expect("dict utf8 data within u32");
            (start, end)
        };
        let entry = &mut self.entries[entry_idx as usize];
        entry.state = DictEntryState::Interned;
        entry.utf8_is_raw = utf8_is_raw;
        entry.utf8_start = start;
        entry.utf8_end = end;
        self.promoted = self.promoted.saturating_add(1);
    }
}

pub(super) fn push_dictionary_id(dictionary_ids: &mut Option<Vec<u32>>, dictionary_id: u32) {
    if let Some(ids) = dictionary_ids.as_mut() {
        ids.push(dictionary_id);
    }
}

#[inline]
pub(super) fn staged_entry_to_dictionary_id(entry_idx: u16) -> u32 {
    u32::from(entry_idx).saturating_add(1)
}

#[cfg(test)]
mod staged_string_lookup_tests {
    use super::{MAX_STAGED_STRING_WIDTH, StageLookupHit, StagedStringLookup};

    #[test]
    fn empty_table_misses() {
        let mut dict = StagedStringLookup::new();
        assert!(dict.lookup(b"alpha").is_none());
    }

    #[test]
    fn stage_then_promote_resolves_through_recent_and_slot_scan() {
        let mut dict = StagedStringLookup::new();

        // First sighting stages the key and primes the recent cache.
        let alpha = dict.insert_seen_once(b"alpha").expect("alpha staged");
        assert!(matches!(
            dict.lookup(b"alpha"),
            Some(StageLookupHit::SeenOnce(idx)) if idx == alpha
        ));

        // A key longer than the inline window (16 bytes), still within the staged-width
        // cap (20), forces the raw-bytes key comparison path.
        let long_key = b"0123456789abcdefghij"; // 20 bytes
        let long = dict.insert_seen_once(long_key).expect("long key staged");
        assert!(matches!(
            dict.lookup(long_key),
            Some(StageLookupHit::SeenOnce(idx)) if idx == long
        ));

        // Promote to interned; the stored UTF-8 should come back from interned_utf8.
        dict.promote_interned(alpha, b"ALPHA", false);
        assert!(matches!(
            dict.lookup(b"alpha"),
            Some(StageLookupHit::Interned(idx)) if idx == alpha
        ));
        assert_eq!(dict.interned_utf8(alpha), b"ALPHA");

        // Invalidate the recent cache to force the open-addressed slot scan, which must
        // still find the (now interned) entry.
        dict.recent_valid = [0; 4];
        assert!(matches!(
            dict.lookup(b"alpha"),
            Some(StageLookupHit::Interned(idx)) if idx == alpha
        ));

        // A key that was never staged misses.
        assert!(dict.lookup(b"never-staged").is_none());
    }

    #[test]
    fn keys_wider_than_the_cap_are_not_staged() {
        let mut dict = StagedStringLookup::new();
        let too_wide = vec![b'x'; MAX_STAGED_STRING_WIDTH as usize + 1];
        assert!(dict.insert_seen_once(&too_wide).is_none());
    }

    #[test]
    fn lookup_short_circuits_once_disabled() {
        let mut dict = StagedStringLookup::new();
        let alpha = dict.insert_seen_once(b"alpha").expect("staged");
        dict.promote_interned(alpha, b"alpha", true);

        // Drive lookups until the adaptive heuristic disables the table.
        while dict.should_use() {
            dict.observe_lookup();
        }
        assert!(
            dict.lookup(b"alpha").is_none(),
            "a disabled dictionary reports no hits regardless of contents",
        );
    }
}
