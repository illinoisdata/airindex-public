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
use crate::store::key_buffer::KeyBuffer;
use crate::store::key_position::KEY_LENGTH;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::PositionT;
use crate::store::KeyT;


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ArrayStoreState {
  array_name: String,
  data_size: usize,
  offset: usize,  // in bytes, array file might contain some header
  length: usize,  // number of elements
}


pub struct ArrayStore {
  storage: Rc<RefCell<ExternalStorage>>,
  prefix_url: Url,
  state: ArrayStoreState,
  array_url: Url,
}

impl fmt::Debug for ArrayStore {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "ArrayStore {{ {:?} }}", self.state)
  }
}

impl ArrayStore {
  pub fn new_sized(storage: &Rc<RefCell<ExternalStorage>>, prefix_url: Url, array_name: String, data_size: usize) -> ArrayStore {
    let array_url = ArrayStore::array_url(&prefix_url, &array_name);
    ArrayStore{
      storage: Rc::clone(storage),
      prefix_url,
      state: ArrayStoreState {
        array_name,
        data_size,
        offset: 0,
        length: 0,
      },
      array_url,
    }
  }
  pub fn from_exact(storage: &Rc<RefCell<ExternalStorage>>, prefix_url: Url, array_name: String, data_size: usize, offset: usize, length: usize) -> ArrayStore {
    let array_url = ArrayStore::array_url(&prefix_url, &array_name);
    ArrayStore{
      storage: Rc::clone(storage),
      prefix_url,
      state: ArrayStoreState {
        array_name,
        data_size,
        offset,
        length,
      },
      array_url,
    }
  }

  pub fn read_array_within(&self, offset: PositionT, length: PositionT) -> GResult<ArrayStoreReader> {
    // read and extract dbuffer than completely fits in the range 
    let (array_buffer, start_rank) = self.read_page_range(offset, length)?;
    Ok(ArrayStoreReader::new(array_buffer, start_rank, self.state.data_size))
  }

  pub fn read_array_all(&self) -> GResult<ArrayStoreReader> {
    self.read_array_within(0, self.read_all_size())
  }

  pub fn data_size(&self) -> usize {
    self.state.data_size
  }

  pub fn read_all_size(&self) -> usize {
    self.state.length * self.state.data_size
  }

  fn end_write(&mut self, written_elements: usize) {
    self.state.length += written_elements;
  }

  fn write_array(&self, array_buffer: &[u8]) -> GResult<()> {
      self.storage.borrow().write_all(&self.array_url, array_buffer)
  }

  fn read_page_range(&self, offset: PositionT, length: PositionT) -> GResult<(SharedByteView, usize)> {
    // calculate first and last "page" indexes
    let end_offset = offset + length;
    let start_rank = std::cmp::min(
      offset / self.state.data_size + (offset % self.state.data_size != 0) as usize,
      self.state.length - 1,
    );
    let end_rank = std::cmp::min(
      end_offset / self.state.data_size + (end_offset % self.state.data_size != 0) as usize,
      self.state.length,
    );

    // make read requests
    let array_buffer = self.storage.borrow().read_range(
      &self.array_url,
      &Range{
        offset: start_rank * self.state.data_size + self.state.offset,
        length: (end_rank - start_rank) * self.state.data_size
      },
    )?;
    Ok((array_buffer, start_rank))
  }

  fn array_url(prefix_url: &Url, array_name: &str) -> Url {
    prefix_url.join(array_name).unwrap()
  }
}

impl DataStore for ArrayStore {
  fn begin_write(&mut self) -> GResult<Box<dyn DataStoreWriter + '_>> {
    // since we require mutable borrow, there will only be one writer in a code block.
    // this would disallow readers while the writer's lifetime as well
    self.state.length = 0;  // TODO: append write?
    Ok(Box::new(ArrayStoreWriter::new(self)))
  }

  fn read_all(&self) -> GResult<Box<dyn DataStoreReader>> {
    self.read_within(0, self.state.length * self.state.data_size)
  }

  fn read_within(&self, offset: PositionT, length: PositionT) -> GResult<Box<dyn DataStoreReader>> {
    // read and extract dbuffer than completely fits in the range 
    let (array_buffer, start_rank) = self.read_page_range(offset, length)?;
    Ok(Box::new(ArrayStoreReader::new(array_buffer, start_rank, self.state.data_size)))
  }

  fn relevant_paths(&self) -> GResult<Vec<String>> {
    Ok(vec![self.state.array_name.clone()])
  }
}

impl DataStoreMetaserde for ArrayStore {  // for Metaserde
  fn to_meta(&self, ctx: &mut Context) -> GResult<DataStoreMeta> {
    Ok(DataStoreMeta::ArrayStore{ state: self.to_meta_state(ctx)? })
  }
}

impl ArrayStore {  // for Metaserde
  pub fn to_meta_state(&self, ctx: &mut Context) -> GResult<ArrayStoreState> {
    ctx.put_storage(&self.storage);
    ctx.put_store_prefix(&self.prefix_url);
    Ok(self.state.clone())
  }

  pub fn from_meta(meta: ArrayStoreState, ctx: &Context) -> GResult<ArrayStore> {
    let storage = Rc::clone(ctx.storage.as_ref().expect("ArrayStore requires storage context"));
    let store_prefix = ctx.store_prefix.as_ref().ok_or_else(|| IncompleteDataStoreFromMeta::boxed("ArrayStore requires store prefix url"))?;
    let prefix_url = store_prefix.clone();
    let array_url = ArrayStore::array_url(&prefix_url, &meta.array_name);
    let array_store = ArrayStore {
      storage,
      prefix_url,
      state: meta,
      array_url,
    };
    // array_store.read_all()?;
    Ok(array_store)
  }
}

/* Writer */

pub struct ArrayStoreWriter<'a> {
  owner_store: &'a mut ArrayStore,

  // writing state
  array_buffer: Vec<u8>,

  // temporary full index
  key_positions: KeyPositionCollection,
}

impl<'a> ArrayStoreWriter<'a> {
  fn new(owner_store: &mut ArrayStore) -> ArrayStoreWriter {
    ArrayStoreWriter{
      owner_store,
      array_buffer: Vec::new(),
      key_positions: KeyPositionCollection::new(),
    }
  }

  fn write_dbuffer(&mut self, dbuffer: &[u8]) -> GResult<PositionT> {
    assert_eq!(dbuffer.len(), self.owner_store.state.data_size);
    let cur_position = self.array_buffer.len();
    self.array_buffer.extend_from_slice(dbuffer);
    Ok(cur_position)
  }

  fn flush_array_buffer(&mut self) -> GResult<()> {
    // write to storage and step block forward
    self.owner_store.write_array(&self.array_buffer)
  }
}

impl<'a> DataStoreWriter for ArrayStoreWriter<'a> {
  fn write(&mut self, kb: &KeyBuffer) -> GResult<()> {
    let key_offset = self.write_dbuffer(&kb.serialize())?;
    self.key_positions.push(kb.key, key_offset);
    Ok(())
  }

  fn commit(mut self: Box<Self>) -> GResult<KeyPositionCollection> {
    let length = self.key_positions.len();
    self.flush_array_buffer()?;
    self.owner_store.end_write(length);
    self.key_positions.set_position_range(0, length * self.owner_store.state.data_size);
    Ok(self.key_positions)
  }
}


/* Reader */

pub struct ArrayStoreReader {
  array_view: SharedByteView,
  start_rank: usize,
  data_size: usize,
}

pub struct ArrayStoreReaderIter<'a> {
  r: &'a ArrayStoreReader,
  current_offset: usize,
}

impl ArrayStoreReader {
  fn new(array_view: SharedByteView, start_rank: usize, data_size: usize) -> ArrayStoreReader {
    ArrayStoreReader {
      array_view,
      start_rank,
      data_size,
    }
  }

  pub fn clone_all(&self) -> Vec<u8> {
    self.array_view.clone_all()
  }

  pub fn key_at(&self, idx: usize) -> KeyT {
    let offset = idx * self.data_size;
    let key_bytes = self.array_view.clone_within(offset .. offset + KEY_LENGTH);
    KeyBuffer::deserialize_key(key_bytes.try_into().unwrap())
  }

  pub fn kb_at(&self, idx: usize) -> KeyBuffer {
    let offset = idx * self.data_size;
    KeyBuffer::deserialize(self.array_view.clone_within(offset .. offset + self.data_size))
  }

  pub fn first_of_with_rank(&self, key: KeyT) -> GResult<(KeyBuffer, usize)> {
    // binary search
    assert!(self.array_view.len() % self.data_size == 0);
    let mut l = 0;
    let mut r = self.array_view.len() / self.data_size;
    let mut mid;
    let mut mid_key;
    while l + 1 < r {
      mid = l + (r - l) / 2;
      mid_key = self.key_at(mid);
      match mid_key.cmp(&key) {  // smallest mid_key <= key
          std::cmp::Ordering::Less => { l = mid },
          std::cmp::Ordering::Equal => { r = mid },
          std::cmp::Ordering::Greater => { r = mid },
      }
    }
    let is_not_tail = r < self.array_view.len() / self.data_size;
    let idx = if is_not_tail && self.key_at(r) == key && self.key_at(l) != key { r } else { l };

    // deserialize and report back
    if idx < self.array_view.len() / self.data_size {
      let kb = self.kb_at(idx);
      return Ok((kb, idx + self.start_rank));
    }
    Err(Box::new(OutofCoverageError) as GenericError)
  }
}

impl DataStoreReader for ArrayStoreReader {
  fn iter(&self) -> Box<dyn DataStoreReaderIter + '_> {
    Box::new(ArrayStoreReaderIter{ r: self, current_offset: 0 })
  }

  fn first_of(&self, key: KeyT) -> GResult<KeyBuffer> {
    self.first_of_with_rank(key).map(|(kb, _rank)| kb)
  }
}

impl<'a> ArrayStoreReaderIter<'a> {
  fn next_block(&mut self) -> Option<Vec<u8>> {
    if self.current_offset < self.r.array_view.len() {
      let dbuffer = self.r.array_view.clone_within(self.current_offset .. self.current_offset + self.r.data_size);
      self.current_offset += self.r.data_size;
      Some(dbuffer)
    } else {
      None
    }
  }
}

impl<'a> DataStoreReaderIter for ArrayStoreReaderIter<'a> {}

impl<'a> Iterator for ArrayStoreReaderIter<'a> {
  type Item = KeyBuffer;
  
  fn next(&mut self) -> Option<Self::Item> {
    self.next_block().map(KeyBuffer::deserialize)
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::TempDir;
  use crate::io::storage::FileSystemAdaptor;
  use crate::io::storage::url_from_dir_path;
  use crate::store::key_position::KeyT;

  fn generate_simple_kv() -> ([KeyT; 10], [Vec<u8>; 10]) {
    let test_keys: [KeyT; 10] = [0, 2, 8, 21, 24, 666, 667, 669, 672, 679];
    let test_buffers: [Vec<u8>; 10] = [
      vec![0u8, 0u8, 0u8, 0u8],
      vec![2u8, 0u8, 0u8, 0u8],
      vec![8u8, 0u8, 0u8, 0u8],
      vec![21u8, 0u8, 0u8, 0u8],
      vec![24u8, 0u8, 0u8, 0u8],
      vec![154u8, 2u8, 0u8, 0u8],
      vec![155u8, 2u8, 0u8, 0u8],
      vec![157u8, 2u8, 0u8, 0u8],
      vec![160u8, 2u8, 0u8, 0u8],
      vec![167u8, 2u8, 0u8, 0u8],
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
    let mut arrstore = ArrayStore::new_sized(
      &es,
      temp_dir_url.clone(),
      "test_arrstore".to_string(),
      12
    );

    // write but never commit
    let _kps = {
      let mut bwriter = arrstore.begin_write()?;
      for (key, value) in test_keys.iter().zip(test_buffers.iter()) {
        bwriter.write(&KeyBuffer::new(*key, value.to_vec()))?;
      }
    };
    assert_eq!(arrstore.state.length, 0, "Total pages should be zero without commit");

    // write some data
    let kps = {
      let mut bwriter = arrstore.begin_write()?;
      for (key, value) in test_keys.iter().zip(test_buffers.iter()) {
        bwriter.write(&KeyBuffer::new(*key, value.to_vec()))?;
      }
      bwriter.commit()?
    };
    assert!(arrstore.state.length > 0, "Total pages should be updated after writing");

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
      let reader = arrstore.read_within(cur_offset, cur_length)?;
      let mut reader_iter = reader.iter();

      // check correctness
      let kb = reader_iter.next().expect("Expect more data buffer");
      assert_eq!(kb.key, cur_key, "Read key does not match with the given map");
      assert_eq!(kb.key, test_keys[idx], "Read key does not match");
      assert_eq!(&kb.buffer[..], test_buffers[idx], "Read buffer does not match");

      // check completeness
      assert!(reader_iter.next().is_none(), "Expected no more data buffer")
    }

    // check reading partially, unaligned
    {
      // read in from between(1, 2) and between(7, 8)... should include 1 + 7 to be safe
      let pos_1 = kps[1].position;
      let pos_2 = kps[2].position;
      let pos_1half = (pos_1 + pos_2) / 2;
      let pos_7 = kps[7].position;
      let pos_8 = kps[8].position;
      let pos_7half = (pos_7 + pos_8) / 2;
      let reader = arrstore.read_within(pos_1half, pos_7half - pos_1half)?;
      let mut reader_iter = reader.iter();

      // should read 2, 3, 4, 5, 6, 7 pairs
      for idx in 2..8 {  
        let kb = reader_iter.next().expect("Expect more data buffer");
        assert_eq!(kb.key, test_keys[idx], "Read key does not match (partial)");
        assert_eq!(&kb.buffer[..], test_buffers[idx], "Read buffer does not match (partial)");
      }
      assert!(reader_iter.next().is_none(), "Expected no more data buffer (partial)")
    }

    // check reading all
    {
      let reader = arrstore.read_all()?;
      let mut reader_iter = reader.iter();
      for (cur_key, cur_value) in test_keys.iter().zip(test_buffers.iter()) {
        // get next and check correctness
        let kb = reader_iter.next().expect("Expect more data buffer");
        assert_eq!(kb.key, *cur_key, "Read key does not match");
        assert_eq!(&kb.buffer[..], cur_value, "Read buffer does not match");
      } 
      assert!(reader_iter.next().is_none(), "Expected no more data buffer (read all)")
    }

    Ok(())
  }
}
