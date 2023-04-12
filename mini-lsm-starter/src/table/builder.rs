#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crate::block::{Block, BlockBuilder, BlockIterator};

use super::{BlockMeta, SsTable};
use crate::lsm_storage::BlockCache;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    pub blocks: Vec<Block>,
    pub current: BlockBuilder,
    // Add other fields you need.
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: vec![],
            blocks: vec![],
            current: BlockBuilder::new(block_size),
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.current.add(key, value) {
            return;
        }

        let mut old_builder = BlockBuilder::new(self.current.block_size());

        std::mem::swap(&mut old_builder, &mut self.current);

        let block = old_builder.build();
        let block = Arc::new(block);

        let first_key = {
            let it = BlockIterator::create_and_seek_to_first(block.clone());
            Bytes::from(it.key().to_vec())
        };

        // Terrible code right there.
        let block = Arc::try_unwrap(block).unwrap();

        let meta = BlockMeta {
            offset: self.blocks.len(),
            first_key,
        };

        self.blocks.push(block);
        self.meta.push(meta);

        if !self.current.add(key, value) {
            panic!("You probably have a too small block size if your key doesn't fit into an empty block");
        }
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        unimplemented!()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        unimplemented!()
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
