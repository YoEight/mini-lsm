#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buffer = BytesMut::new();

        buffer.put(self.data.as_slice());

        let num = self.offsets.len();
        for offset in self.offsets.iter().copied() {
            buffer.put_u16_le(offset);
        }

        buffer.put_u16_le(num as u16);
        buffer.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        if data.is_empty() {
            return Self {
                data: vec![],
                offsets: vec![],
            };
        }

        let len = data.len();
        let last_idx = len - 1;
        let num_of_entries_start_idx = last_idx - 1;
        let mut num_of_entries = &data[num_of_entries_start_idx..];
        let num_of_entries = num_of_entries.get_u16_le() as usize;
        let offsets_start_idx = num_of_entries_start_idx - (num_of_entries * 2);
        let mut offsets_slice = &data[offsets_start_idx..];
        let mut offsets = Vec::with_capacity(num_of_entries);

        for _ in 0..num_of_entries {
            offsets.push(offsets_slice.get_u16_le());
        }

        Self {
            data: Vec::from(&data[..offsets_start_idx]),
            offsets,
        }
    }
}

#[cfg(test)]
mod tests;
