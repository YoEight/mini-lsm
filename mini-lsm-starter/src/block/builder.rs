#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::Block;
use bytes::BufMut;

/// Builds a block.
pub struct BlockBuilder {
    block_size: usize,
    offset: u16,
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            offset: 0,
            data: vec![],
            offsets: vec![],
        }
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let entry_size = 2 + key.len() + 2 + value.len();

        // The last 2 represents the key offset in the offsets section.
        if self.current_size() + entry_size + 2 > self.block_size {
            return false;
        }

        let offset = self.offset;

        self.data.put_u16_le(key.len() as u16);
        self.data.put_slice(key);
        self.data.put_u16_le(value.len() as u16);
        self.data.put_slice(value);
        self.offsets.push(offset);
        self.offset += entry_size as u16;

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    fn current_size(&self) -> usize {
        if self.is_empty() {
            return 0;
        }

        // The last 2 is the size in bytes of on many entries we got in the block.
        self.data.len() + self.offsets.len() * 2 + 2
    }
}
