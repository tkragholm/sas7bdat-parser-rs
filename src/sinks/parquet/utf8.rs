use bytes::Bytes;
use hashbrown::{HashMap, hash_map::RawEntryMut};
use parquet::data_type::ByteArray;

use super::constants::UTF8_DICTIONARY_LIMIT;

pub(super) struct Utf8Scratch {
    pub ryu: ryu::Buffer,
    pub itoa: itoa::Buffer,
    dictionary: HashMap<Vec<u8>, ByteArray>,
    dictionary_enabled: bool,
}

impl Utf8Scratch {
    pub(crate) fn new() -> Self {
        Self {
            ryu: ryu::Buffer::new(),
            itoa: itoa::Buffer::new(),
            dictionary: HashMap::new(),
            dictionary_enabled: true,
        }
    }

    pub(crate) fn intern_slice(&mut self, data: &[u8]) -> ByteArray {
        if self.dictionary_enabled && self.dictionary.len() >= UTF8_DICTIONARY_LIMIT {
            self.dictionary.clear();
            self.dictionary_enabled = false;
        }
        if !self.dictionary_enabled {
            return ByteArray::from(Bytes::copy_from_slice(data));
        }
        match self.dictionary.raw_entry_mut().from_key(data) {
            RawEntryMut::Occupied(entry) => entry.get().clone(),
            RawEntryMut::Vacant(vacant) => {
                let stored = ByteArray::from(Bytes::copy_from_slice(data));
                vacant.insert(data.to_vec(), stored.clone());
                stored
            }
        }
    }

    pub(crate) fn intern_str(&mut self, text: &str) -> ByteArray {
        self.intern_slice(text.as_bytes())
    }
}
