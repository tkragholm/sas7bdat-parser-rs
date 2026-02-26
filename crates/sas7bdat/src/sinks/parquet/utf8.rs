use super::constants::UTF8_DICTIONARY_LIMIT;
use ahash::RandomState;
use bytes::Bytes;
use hashbrown::{HashMap, hash_map::RawEntryMut};
use parquet::data_type::ByteArray;

pub(super) struct Utf8Scratch {
    pub ryu: ryu::Buffer,
    pub itoa: itoa::Buffer,
    /// Keyed by `Bytes` (Arc-backed) so all clones are cheap reference-count bumps.
    dictionary: HashMap<Bytes, ByteArray, RandomState>,
    dictionary_enabled: bool,
    last_short: Option<(Bytes, ByteArray)>,
}

impl Utf8Scratch {
    pub(crate) fn new() -> Self {
        Self {
            ryu: ryu::Buffer::new(),
            itoa: itoa::Buffer::new(),
            dictionary: HashMap::with_capacity_and_hasher(
                UTF8_DICTIONARY_LIMIT,
                RandomState::new(),
            ),
            dictionary_enabled: true,
            last_short: None,
        }
    }

    pub(crate) fn intern_slice(&mut self, data: &[u8]) -> ByteArray {
        if data.len() <= 32
            && let Some((ref previous, ref handle)) = self.last_short
            && previous.as_ref() == data
        {
            return handle.clone();
        }
        if self.dictionary_enabled && self.dictionary.len() >= UTF8_DICTIONARY_LIMIT {
            self.dictionary.clear();
            self.dictionary_enabled = false;
        }
        if !self.dictionary_enabled {
            let stored = ByteArray::from(Bytes::copy_from_slice(data));
            if data.len() <= 32 {
                // ByteArray wraps Bytes; extract it back for the cache key (cheap Arc clone).
                let key = Bytes::copy_from_slice(data);
                self.last_short = Some((key, stored.clone()));
            }
            return stored;
        }
        match self.dictionary.raw_entry_mut().from_key(data) {
            RawEntryMut::Occupied(entry) => {
                let cloned = entry.get().clone();
                if data.len() <= 32 {
                    // Reuse the existing Bytes key already stored in the hashmap (cheap clone).
                    self.last_short = Some((entry.key().clone(), cloned.clone()));
                }
                cloned
            }
            RawEntryMut::Vacant(vacant) => {
                // One allocation for the bytes; all subsequent uses are cheap Arc clones.
                let key = Bytes::copy_from_slice(data);
                let stored = ByteArray::from(key.clone());
                let (inserted_key, _) = vacant.insert(key, stored.clone());
                if data.len() <= 32 {
                    self.last_short = Some((inserted_key.clone(), stored.clone()));
                }
                stored
            }
        }
    }

    pub(crate) fn intern_str(&mut self, text: &str) -> ByteArray {
        self.intern_slice(text.as_bytes())
    }
}
