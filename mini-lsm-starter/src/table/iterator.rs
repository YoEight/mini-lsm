#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::block::BlockIterator;
use anyhow::Result;

use super::SsTable;
use crate::iterators::StorageIterator;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block: BlockIterator,
    idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let idx = 0usize;
        let block = BlockIterator::create_and_seek_to_first(table.read_block(idx)?);

        Ok(Self { table, block, idx })
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.seek_to(0)
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let block_idx = table.find_block_idx(key);
        let block = table.read_block(block_idx)?;

        Ok(Self {
            table,
            block: BlockIterator::create_and_seek_to_first(block),
            idx: block_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let block_idx = self.table.find_block_idx(key);
        self.seek_to(block_idx)?;
        self.block.seek_to_key(key);

        Ok(())
    }

    fn seek_to(&mut self, idx: usize) -> Result<()> {
        let block = self.table.read_block(idx)?;
        self.block = BlockIterator::create_and_seek_to_first(block);
        self.idx = idx;

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    fn value(&self) -> &[u8] {
        self.block.value()
    }

    fn key(&self) -> &[u8] {
        self.block.key()
    }

    fn is_valid(&self) -> bool {
        self.block.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block.next();

        if !self.is_valid() {
            if self.table.block_metas.get(self.idx + 1).is_some() {
                self.seek_to(self.idx + 1)?;
            }
        }

        Ok(())
    }
}
