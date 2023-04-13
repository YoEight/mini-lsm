#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use crate::block::BlockBuilder;
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

use super::{BlockMeta, SsTable};
use crate::lsm_storage::BlockCache;
use crate::table::FileObject;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    buffer: BytesMut,
    pub current: BlockBuilder,
    pub idx: usize,
    // Add other fields you need.
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: vec![],
            idx: 0,
            buffer: BytesMut::new(),
            current: BlockBuilder::new(block_size),
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let mut attempts = 1;

        loop {
            if self.current.add(key, value) {
                if self.meta.get(self.idx).is_none() {
                    self.meta.push(BlockMeta {
                        offset: self.buffer.len(),
                        first_key: Bytes::from(key.to_vec()),
                    });
                }

                return;
            }

            if attempts >= 2 {
                panic!("Your block size is probably too short for the key/value you want to store");
            }

            self.idx += 1;
            let mut old_builder = BlockBuilder::new(self.current.block_size());

            std::mem::swap(&mut old_builder, &mut self.current);
            let block = old_builder.build();

            self.buffer.put(block.encode());
            attempts += 1;
        }
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.buffer.len()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let block = self.current.build();

        self.buffer.put(block.encode());
        let block_meta_offset = self.buffer.len();

        for meta in self.meta.iter() {
            self.buffer.put_u32_le(meta.offset as u32);
            self.buffer.put_u16_le(meta.first_key.len() as u16);
            self.buffer.put(meta.first_key.clone());
        }

        self.buffer.put_u32_le(block_meta_offset as u32);

        Ok(SsTable {
            file: FileObject(self.buffer.freeze()),
            block_metas: self.meta,
            block_meta_offset,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
