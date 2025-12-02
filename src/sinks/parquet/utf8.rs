use bytes::Bytes;
use ahash::RandomState;
use hashbrown::{HashMap, hash_map::RawEntryMut};
use parquet::data_type::ByteArray;

use super::constants::UTF8_DICTIONARY_LIMIT;

pub(super) struct Utf8Scratch {
    pub ryu: ryu::Buffer,
    pub itoa: itoa::Buffer,
    dictionary: HashMap<Vec<u8>, ByteArray, RandomState>,
    dictionary_enabled: bool,
    last_short: Option<(Vec<u8>, ByteArray)>,
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
                && previous.as_slice() == data {
                    return handle.clone();
                }
        if self.dictionary_enabled && self.dictionary.len() >= UTF8_DICTIONARY_LIMIT {
            self.dictionary.clear();
            self.dictionary_enabled = false;
        }
        if !self.dictionary_enabled {
            let stored = ByteArray::from(Bytes::copy_from_slice(data));
            if data.len() <= 32 {
                self.last_short = Some((data.to_vec(), stored.clone()));
            }
            return stored;
        }
        match self.dictionary.raw_entry_mut().from_key(data) {
            RawEntryMut::Occupied(entry) => {
                let cloned = entry.get().clone();
                if data.len() <= 32 {
                    self.last_short = Some((data.to_vec(), cloned.clone()));
                }
                cloned
            }
            RawEntryMut::Vacant(vacant) => {
                let stored = ByteArray::from(Bytes::copy_from_slice(data));
                vacant.insert(data.to_vec(), stored.clone());
                if data.len() <= 32 {
                    self.last_short = Some((data.to_vec(), stored.clone()));
                }
                stored
            }
        }
    }

    pub(crate) fn intern_str(&mut self, text: &str) -> ByteArray {
        self.intern_slice(text.as_bytes())
    }
}
