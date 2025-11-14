use std::convert::TryInto;

use crate::metadata::Endianness;

#[inline]
#[must_use]
pub fn read_u16(endian: Endianness, bytes: &[u8]) -> u16 {
    match endian {
        Endianness::Little => u16::from_le_bytes([bytes[0], bytes[1]]),
        Endianness::Big => u16::from_be_bytes([bytes[0], bytes[1]]),
    }
}

#[inline]
#[must_use]
pub fn read_i16(endian: Endianness, bytes: &[u8]) -> i16 {
    match endian {
        Endianness::Little => i16::from_le_bytes([bytes[0], bytes[1]]),
        Endianness::Big => i16::from_be_bytes([bytes[0], bytes[1]]),
    }
}

/// Reads a `u32` from `bytes` using the provided `endian` ordering.
///
/// # Panics
///
/// Panics if `bytes` has fewer than four elements.
#[inline]
#[must_use]
pub fn read_u32(endian: Endianness, bytes: &[u8]) -> u32 {
    match endian {
        Endianness::Little => u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
        Endianness::Big => u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
    }
}

/// Reads a `u64` from `bytes` using the provided `endian` ordering.
///
/// # Panics
///
/// Panics if `bytes` has fewer than eight elements.
#[inline]
#[must_use]
pub fn read_u64(endian: Endianness, bytes: &[u8]) -> u64 {
    match endian {
        Endianness::Little => u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        Endianness::Big => u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
    }
}

/// Reads a big-endian `u64` from `bytes`.
///
/// # Panics
///
/// Panics if `bytes` has fewer than eight elements.
#[inline]
#[must_use]
pub fn read_u64_be(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes[0..8].try_into().unwrap())
}
