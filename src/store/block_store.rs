use serde::{Serialize, Deserialize};
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use url::Url;

use crate::common::SharedByteView;
use crate::common::error::GenericError;
use crate::common::error::GResult;
use crate::common::error::IncompleteDataStoreFromMeta;
use crate::common::error::OutofCoverageError;
use crate::io::internal::ExternalStorage;
use crate::io::storage::Range;
use crate::meta::Context;
use crate::store::DataStore;
use crate::store::DataStoreMeta;
use crate::store::DataStoreMetaserde;
use crate::store::DataStoreReader;
use crate::store::DataStoreReaderIter;
use crate::store::DataStoreWriter;
use crate::store::KeyT;
use crate::store::key_buffer::KeyBuffer;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::PositionT;


/* Page format */

type FlagT = u32;  // TODO: smaller/larger flag?
const FLAG_LENGTH: usize = std::mem::size_of::<FlagT>();
const CONT_FLAG: FlagT = 0;

fn write_page(page: &mut [u8], flag: FlagT, kv_chunk: &[u8]) {
  // TODO: move CONT_FLAG < 0, then write only one byte
  let chunk_length = kv_chunk.len();
  page[..FLAG_LENGTH].clone_from_slice(&flag.to_le_bytes());
  page[FLAG_LENGTH..FLAG_LENGTH+chunk_length].clone_from_slice(kv_chunk);
}

fn read_page(page: &[u8]) -> (FlagT, &[u8]) {
  // TODO: if leading bit is 1 --> CONT_FLAG
  let mut flag_bytes = [0u8; FLAG_LENGTH];
  flag_bytes[..FLAG_LENGTH].clone_from_slice(&page[..FLAG_LENGTH]);
  (FlagT::from_le_bytes(flag_bytes), &page[FLAG_LENGTH..])
}


/* Main block store */

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BlockStoreConfig {
  block_name: String,
  block_size: usize,
  page_size: usize,
}

impl BlockStoreConfig {
  fn new(block_name: String) -> BlockStoreConfig {
    BlockStoreConfig {
        block_name,
        block_size: 1 << 32,  // 4GB
        page_size: 32,
    }
  }

  pub fn block_name(mut self, block_name: String) -> BlockStoreConfig {
    self.block_name = block_name;
    self
  }

  pub fn block_size(mut self, block_size: usize) -> BlockStoreConfig {
    self.block_size = block_size;
    self
  }

  pub fn page_size(mut self, page_size: usize) -> BlockStoreConfig {
    self.page_size = page_size;
    self
  }

  pub fn build(self, storage: &Rc<RefCell<ExternalStorage>>, prefix_url: Url) -> BlockStore {
    BlockStore::new(storage, prefix_url, self)
  }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BlockStoreState {
  cfg: BlockStoreConfig,
  total_pages: usize,
}

pub struct BlockStore {
  storage: Rc<RefCell<ExternalStorage>>,
  prefix_url: Url,
  state: BlockStoreState,
}

impl fmt::Debug for BlockStore {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "BlockStore {{ {:?} }}", self.state)
  }
}

impl BlockStore {
  fn new(storage: &Rc<RefCell<ExternalStorage>>, prefix_url: Url, cfg: BlockStoreConfig) -> BlockStore {
    BlockStore{
      storage: Rc::clone(storage),
      prefix_url,
      state: BlockStoreState {
        cfg,
        total_pages: 0,
      },
    }
  }

  pub fn builder(block_name: String) -> BlockStoreConfig {
    BlockStoreConfig::new(block_name)
  }

  fn end_write(&mut self, written_pages: usize) {
    self.state.total_pages += written_pages;
  }

  fn chunk_size(&self) -> usize {
    self.state.cfg.page_size - FLAG_LENGTH 
  }

  fn pages_per_block(&self) -> usize {
    self.state.cfg.block_size / self.state.cfg.page_size
  }

  fn block_path(&self, block_idx: usize) -> String {
    format!("{}_block_{}", self.state.cfg.block_name, block_idx)
  }

  fn block_url(&self, block_idx: usize) -> GResult<Url> {
    Ok(self.prefix_url.join(&self.block_path(block_idx))?)
  }

  fn write_block(&self, block_idx: usize, block_buffer: &[u8]) -> GResult<()> {
      let block_url = self.block_url(block_idx)?;
      self.storage.borrow().write_all(&block_url, block_buffer)
  }

  fn read_page_range(&self, offset: PositionT, length: PositionT) -> GResult<(Vec<FlagT>, Vec<u8>)> {
    // calculate first and last page indexes
    let end_offset = offset + length;
    let start_page_idx = offset / self.state.cfg.page_size + (offset % self.state.cfg.page_size != 0) as usize;
    let end_page_idx = std::cmp::min(end_offset / self.state.cfg.page_size, self.state.total_pages);

    // make read requests
    let section_buffers = self.read_page_range_section(start_page_idx, end_page_idx)?;
    let mut flags = Vec::new();
    let mut chunks_buffer = Vec::new();
    for section_buffer in section_buffers {
      assert_eq!(section_buffer.len() % self.state.cfg.page_size, 0);
      // TODO: remove clone_all
      for page in section_buffer.clone_all().chunks(self.state.cfg.page_size) {
        let (flag, chunk) = read_page(page);
        flags.push(flag);
        chunks_buffer.extend(chunk);
      }
    }
    Ok((flags, chunks_buffer))
  }

  fn read_page_range_section(&self, mut start_page_idx: usize, end_page_idx: usize) -> GResult<Vec<SharedByteView>> {
    let pages_per_block = self.state.cfg.block_size / self.state.cfg.page_size;
    let mut start_block_idx = start_page_idx / pages_per_block;
    let mut section_buffers = Vec::new();
    while start_page_idx < end_page_idx {
      // calculate current section boundaries
      let start_section_offset = (start_page_idx % pages_per_block) * self.state.cfg.page_size;
      let end_section_page_idx = if end_page_idx / pages_per_block == start_block_idx {
        // the end is in the same block
        end_page_idx
      } else {
        // more blocks to read... read til the end of this block for now
        (start_block_idx + 1) * pages_per_block
      };
      let section_length = (end_section_page_idx - start_page_idx) * self.state.cfg.page_size;

      // add read request for this section
      let section_buffer = self.storage.borrow().read_range(
        &self.block_url(start_block_idx)?,
        &Range{ offset: start_section_offset, length: section_length },
      )?;
      section_buffers.push(section_buffer);

      // step forward
      start_page_idx = end_section_page_idx;
      start_block_idx += 1;
    }
    Ok(section_buffers)
  }
}

impl DataStore for BlockStore {
  fn begin_write(&mut self) -> GResult<Box<dyn DataStoreWriter + '_>> {
    // since we require mutable borrow, there will only be one writer in a code block.
    // this would disallow readers while the writer's lifetime as well
    self.state.total_pages = 0;  // TODO: append write?
    Ok(Box::new(BlockStoreWriter::new(self)))
  }

  fn read_all(&self) -> GResult<Box<dyn DataStoreReader>> {
    self.read_within(0, self.state.total_pages * self.state.cfg.page_size)
  }

  fn read_within(&self, offset: PositionT, length: PositionT) -> GResult<Box<dyn DataStoreReader>> {
    // read and extract dbuffer than completely fits in the range 
    let (chunk_flags, chunks_buffer) = self.read_page_range(offset, length)?;
    let chunk_size = self.chunk_size();
    Ok(Box::new(BlockStoreReader::new(chunk_flags, chunks_buffer, chunk_size)))
  }

  fn relevant_paths(&self) -> GResult<Vec<String>> {
    let total_size = self.state.total_pages * self.state.cfg.page_size;
    let num_blocks = total_size / self.state.cfg.block_size + (total_size % self.state.cfg.block_size != 0) as usize;
    Ok((0..num_blocks).map(|block_idx| self.block_path(block_idx)).collect())
  }
}

impl DataStoreMetaserde for BlockStore {  // for Metaserde
  fn to_meta(&self, ctx: &mut Context) -> GResult<DataStoreMeta> {
    ctx.put_storage(&self.storage);
    ctx.put_store_prefix(&self.prefix_url);
    Ok(DataStoreMeta::BlockStore{ state: self.state.clone() })
  }
}

impl BlockStore {  // for Metaserde
  pub fn from_meta(meta: BlockStoreState, ctx: &Context) -> GResult<BlockStore> {
    let storage = Rc::clone(ctx.storage.as_ref().expect("BlockStore requires storage context"));
    let store_prefix = ctx.store_prefix.as_ref().ok_or_else(|| IncompleteDataStoreFromMeta::boxed("BlockStore requires store prefix url"))?;
    Ok(BlockStore{
      storage, 
      prefix_url: store_prefix.clone(),
      state: meta
    })
  }
}


/* Writer */

pub struct BlockStoreWriter<'a> {
  owner_store: &'a mut BlockStore,

  // writing state
  block_buffer: Vec<u8>,
  block_idx: usize,
  page_idx: usize,

  // shortcuts for calculation
  chunk_size: usize,
  pages_per_block: usize,

  // temporary full index
  key_positions: KeyPositionCollection,
}

impl<'a> BlockStoreWriter<'a> {
  fn new(owner_store: &mut BlockStore) -> BlockStoreWriter {
    let block_buffer = vec![0; owner_store.state.cfg.block_size];
    let chunk_size = owner_store.chunk_size();
    let pages_per_block = owner_store.pages_per_block();
    BlockStoreWriter{
      owner_store,
      block_buffer,
      block_idx: 0,
      page_idx: 0,
      chunk_size,
      pages_per_block,
      key_positions: KeyPositionCollection::new(),
    }
  }

  fn write_dbuffer(&mut self, dbuffer: &[u8]) -> GResult<PositionT> {
    let key_offset = self.page_idx * self.owner_store.state.cfg.page_size;
    let mut flag = FlagT::try_from(dbuffer.len()).ok().unwrap();
    for kv_chunk in dbuffer.chunks(self.chunk_size) {
      // write this chunk to current page
      let page_buffer = self.page_to_write()?;
      write_page(page_buffer, flag, kv_chunk);

      // next pages are continuation
      flag = CONT_FLAG;
    }
    Ok(key_offset)
  }

  fn page_to_write(&mut self) -> GResult<&mut [u8]> {
    let page_size = self.owner_store.state.cfg.page_size;

    // get the buffer
    let page_buffer = if self.page_idx < (self.block_idx + 1) * self.pages_per_block {
      // continue writing in current block
      let page_offset = (self.page_idx % self.pages_per_block) * page_size;
      &mut self.block_buffer[page_offset .. page_offset+page_size]
    } else {
      // next page is in the new block, flush first
      self.flush_current_block()?;
      &mut self.block_buffer[0..page_size]
    };

    // forward page_idx
    self.page_idx += 1;

    Ok(page_buffer)
    
    // return the next page slice
  }

  fn flush_current_block(&mut self) -> GResult<()> {
    // write up to written page
    let written_buffer = if self.page_idx < (self.block_idx + 1) * self.pages_per_block {
      let written_length = (self.page_idx % self.pages_per_block) * self.owner_store.state.cfg.page_size;
      &self.block_buffer[0 .. written_length]
    } else {
      &self.block_buffer
    };

    // write to storage and step block forward
    self.owner_store.write_block(self.block_idx, written_buffer)?;
    self.block_idx += 1;
    Ok(())
  }
}

impl<'a> DataStoreWriter for BlockStoreWriter<'a> {
  fn write(&mut self, kb: &KeyBuffer) -> GResult<()> {
    let key_offset = self.write_dbuffer(&kb.serialize())?;
    self.key_positions.push(kb.key, key_offset);
    Ok(())
  }

  fn commit(mut self: Box<Self>) -> GResult<KeyPositionCollection> {
    self.flush_current_block()?;
    self.owner_store.end_write(self.page_idx);
    self.key_positions.set_position_range(0, self.page_idx * self.owner_store.state.cfg.page_size);
    Ok(self.key_positions)
  }
}


/* Reader */

pub struct BlockStoreReader {
  chunk_flags: Vec<FlagT>,
  chunks_buffer: Vec<u8>,
  chunk_idx_first: usize,
  chunk_size: usize,
}

pub struct BlockStoreReaderIter<'a> {
  r: &'a BlockStoreReader,
  chunk_idx: usize,
}

impl BlockStoreReader {
  fn new(chunk_flags: Vec<FlagT>, chunks_buffer: Vec<u8>, chunk_size: usize) -> BlockStoreReader {
    // seek first valid page
    let mut chunk_idx = 0;
    while chunk_idx < chunk_flags.len() && chunk_flags[chunk_idx] == CONT_FLAG {
      chunk_idx += 1;
    }

    BlockStoreReader {
      chunk_flags,
      chunks_buffer,
      chunk_idx_first: chunk_idx,
      chunk_size,
    }
  }
}

impl DataStoreReader for BlockStoreReader {
  fn iter(&self) -> Box<dyn DataStoreReaderIter + '_> {
    Box::new(BlockStoreReaderIter{ r: self, chunk_idx: self.chunk_idx_first })
  }

  fn first_of(&self, key: KeyT) -> GResult<KeyBuffer> {
    self.iter()
      .take_while(|kb| kb.key <= key)
      .last()
      .ok_or_else(|| Box::new(OutofCoverageError) as GenericError)
  }
}

impl<'a> BlockStoreReaderIter<'a> {
  fn next_block(&mut self) -> Option<&[u8]> {
    if self.chunk_idx < self.r.chunk_flags.len() {
      // calculate boundary
      let dbuffer_offset = self.chunk_idx * self.r.chunk_size;
      let dbuffer_length = usize::try_from(self.r.chunk_flags[self.chunk_idx]).ok().unwrap();
      assert_ne!(dbuffer_length, 0);
      if dbuffer_offset + dbuffer_length < self.r.chunks_buffer.len() {
        // move chunk index
        self.chunk_idx += dbuffer_length / self.r.chunk_size + (dbuffer_length % self.r.chunk_size != 0) as usize;

        // return the kp buffer slice
        Some(&self.r.chunks_buffer[dbuffer_offset .. dbuffer_offset + dbuffer_length])
      } else {
        // didn't read the whole buffer
        None
      }
    } else {
      None
    }
  }
}

impl<'a> DataStoreReaderIter for BlockStoreReaderIter<'a> {}

impl<'a> Iterator for BlockStoreReaderIter<'a> {
  type Item = KeyBuffer;

  fn next(&mut self) -> Option<Self::Item> {
    self.next_block().map(|block| KeyBuffer::deserialize(block.to_vec()))
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::TempDir;
  use crate::io::storage::FileSystemAdaptor;
  use crate::io::storage::url_from_dir_path;
  use crate::store::key_position::KeyT;

  fn generate_simple_kv() -> ([KeyT; 14], [Box<[u8]>; 14]) {
    let test_keys: [KeyT; 14] = [
      50,
      100,
      200,
      1,
      2,
      4,
      8,
      16,
      32,
      64,
      128,
      256,
      512,
      1024,
    ];
    let test_buffers: [Box<[u8]>; 14] = [
      Box::new([255u8]),
      Box::new([1u8, 1u8, 2u8, 3u8, 5u8, 8u8, 13u8, 21u8]),
      Box::new([0u8; 256]),
      Box::new([0u8; 1]),
      Box::new([0u8; 2]),
      Box::new([0u8; 4]),
      Box::new([0u8; 8]),
      Box::new([0u8; 16]),
      Box::new([0u8; 32]),
      Box::new([0u8; 64]),
      Box::new([0u8; 128]),
      Box::new([0u8; 256]),
      Box::new([0u8; 512]),
      Box::new([0u8; 1024]),
    ];
    (test_keys, test_buffers)
  }

  #[test]
  fn read_write_full_test() -> GResult<()> {
    let (test_keys, test_buffers) = generate_simple_kv();

    // setup a block store
    let temp_dir = TempDir::new()?;
    let temp_dir_url = &url_from_dir_path(temp_dir.path())?;
    let fsa = FileSystemAdaptor::new();
    let es = Rc::new(RefCell::new(ExternalStorage::new().with("file".to_string(), Box::new(fsa))?));
    let mut bstore = BlockStore::builder("bstore".to_string())
      .block_size(128)  // tune down for unit testing
      .build(&es, temp_dir_url.clone());

    // write but never commit
    let _kps = {
      let mut bwriter = bstore.begin_write()?;
      for (key, value) in test_keys.iter().zip(test_buffers.iter()) {
        bwriter.write(&KeyBuffer::new(*key, value.to_vec()))?;
      }
    };
    assert_eq!(bstore.state.total_pages, 0, "Total pages should be zero without commit");

    // write some data
    let kps = {
      let mut bwriter = bstore.begin_write()?;
      for (key, value) in test_keys.iter().zip(test_buffers.iter()) {
        bwriter.write(&KeyBuffer::new(*key, value.to_vec()))?;
      }
      bwriter.commit()?
    };
    assert!(bstore.state.total_pages > 0, "Total pages should be updated after writing");

    // check monotonicity of the key-position pairs
    let mut prev_position = 0;  // position must be at least zero
    for (key, kp) in test_keys.iter().zip(kps.iter()) {
      assert_eq!(*key, kp.key, "Key must be written in order of insertions");
      assert!(prev_position <= kp.position, "Positions must be non-decreasing");
      prev_position = kp.position;
    }

    // check rereading from position
    for idx in 0..kps.len() {
      let kr = kps.range_at(idx)?;
      let cur_key = kr.key_l;
      let cur_offset = kr.offset;
      let cur_length = kr.length;
      let reader = bstore.read_within(cur_offset, cur_length)?;
      let mut reader_iter = reader.iter();

      // check correctness
      let kb = reader_iter.next().expect("Expect more data buffer");
      assert_eq!(kb.key, cur_key, "Read key does not match with the given map");
      assert_eq!(kb.key, test_keys[idx], "Read key does not match");
      assert_eq!(&kb.buffer[..], test_buffers[idx].to_vec(), "Read buffer does not match");

      // check completeness
      assert!(reader_iter.next().is_none(), "Expected no more data buffer")
    }

    // check reading partially, unaligned
    {
      // read in from between(1, 2) and between(7, 8)... should ignore 1 + 7
      let pos_1 = kps[1].position;
      let pos_2 = kps[2].position;
      let pos_1half = (pos_1 + pos_2) / 2;
      let pos_7 = kps[7].position;
      let pos_8 = kps[8].position;
      let pos_7half = (pos_7 + pos_8) / 2;
      let reader = bstore.read_within(pos_1half, pos_7half - pos_1half)?;
      let mut reader_iter = reader.iter();

      // should read 2, 3, 4, 5, 6 pairs
      for idx in 2..7 {  
        let kb = reader_iter.next().expect("Expect more data buffer");
        assert_eq!(kb.key, test_keys[idx], "Read key does not match (partial)");
        assert_eq!(&kb.buffer[..], test_buffers[idx].to_vec(), "Read buffer does not match (partial)");
      }
      assert!(reader_iter.next().is_none(), "Expected no more data buffer (partial)")
    }

    // check reading all
    {
      let reader = bstore.read_all()?;
      let mut reader_iter = reader.iter();
      for (cur_key, cur_value) in test_keys.iter().zip(test_buffers.iter()) {
        // get next and check correctness
        let kb = reader_iter.next().expect("Expect more data buffer");
        assert_eq!(kb.key, *cur_key, "Read key does not match");
        assert_eq!(&kb.buffer[..], cur_value.to_vec(), "Read buffer does not match");
      } 
      assert!(reader_iter.next().is_none(), "Expected no more data buffer (read all)")
    }

    Ok(())
  }
}
