#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::Ordering;
use std::sync::Arc;
use bytes::Buf;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut this = BlockIterator::new(block);

        this.seek_to_first();

        this
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut it = BlockIterator::create_and_seek_to_first(block);

        it.seek_to_key(key);

        it
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        self.key.as_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        self.value.as_slice()
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !(self.key.is_empty() && self.value.is_empty())
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    fn seek_to(&mut self, idx: usize) {
        let offset = self.block.offsets[idx];
        let mut bytes = self.block.data.as_slice();
        bytes.advance(offset as usize);
        let key_len = bytes.get_u16_le();
        self.key.clear();
        self.key.extend(bytes.copy_to_bytes(key_len as usize));
        let value_len = bytes.get_u16_le();
        self.value.clear();
        self.value.extend(bytes.copy_to_bytes(value_len as usize));
        self.idx = idx;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.block.offsets.get(self.idx + 1).is_some() {
            self.seek_to(self.idx + 1);
            return;
        }

        self.key.clear();
        self.value.clear();
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut low = 0usize;
        let mut high = self.block.offsets.len() - 1;

        while low <= high {
            let mid = (low + high) / 2;

            self.seek_to(mid);
            match self.key().cmp(key) {
                Ordering::Less => {
                    if low == self.block.offsets.len() - 1 {
                        break;
                    }

                    low = mid + 1;
                },
                Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }

                    high = mid - 1
                },
                Ordering::Equal => return,
            }
        }

        self.seek_to(low);
    }
}
