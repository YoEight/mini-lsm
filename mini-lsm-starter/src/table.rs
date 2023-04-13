#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        unimplemented!()
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        unimplemented!()
    }
}

/// A file object.
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        unimplemented!()
    }

    pub fn open(path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

pub struct SsTable {
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let size = file.size();
        let meta_offset = file.read(size-4, 4)?;
        let block_meta_offset = meta_offset.as_slice().get_u32_le() as usize;
        let meta_size = size - block_meta_offset as u64 - 4;
        let metas_vec = file.read(block_meta_offset as u64, meta_size)?;
        let mut metas_bytes = metas_vec.as_slice();

        let mut block_metas = Vec::new();

        while metas_bytes.remaining() > 0 {
            let offset = metas_bytes.get_u32_le();
            let key_len = metas_bytes.get_u16_le();
            let key = metas_bytes.copy_to_bytes(key_len as usize);

            block_metas.push(BlockMeta {
                offset: offset as usize,
                first_key: key,
            });
        }

        Ok(Self {
            file,
            block_metas,
            block_meta_offset,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let meta = if let Some(meta) = self.block_metas.get(block_idx) {
            meta
        } else {
            anyhow::bail!("Block {} doesn't exist", block_idx)
        };

        let next_offset = if let Some(next) = self.block_metas.get(block_idx + 1) {
            next.offset
        } else {
            self.block_meta_offset
        };

        let block_bytes = self.file.read(meta.offset as u64, (next_offset - meta.offset) as u64)?;

        Ok(Arc::new(Block::decode(block_bytes.as_slice())))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        let mut low = 0usize;
        let mut high = self.block_metas.len() -1;

        while low <= high {
            let mid = (low + high) / 2;
            let meta = &self.block_metas[mid];

            match meta.first_key.as_ref().cmp(key) {
                Ordering::Less => {
                    // To be honest, in that case, it's most likely the table doesn't have the key
                    if low == self.block_metas.len() - 1 {
                        break;
                    }

                    low = mid + 1;
                }
                Ordering::Greater => {
                    // To be honest, in that case, it's most likely the table doesn't have the key
                    if mid == 0 {
                        break;
                    }

                    high = mid - 1;
                },
                Ordering::Equal => return mid,
            }
        }

        low
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
