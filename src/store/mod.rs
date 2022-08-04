use serde::{Serialize, Deserialize};
use std::fmt::Debug;

use crate::common::error::GResult;
use crate::meta::Context;
use crate::store::key_buffer::KeyBuffer;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::KeyT;
use crate::store::key_position::PositionT;

pub trait DataStore: DataStoreMetaserde + Debug {
  fn begin_write(&mut self) -> GResult<Box<dyn DataStoreWriter + '_>>;
  fn read_all(&self) -> GResult<Box<dyn DataStoreReader>>;
  fn read_within(&self, offset: PositionT, length: PositionT) -> GResult<Box<dyn DataStoreReader>>;
  fn relevant_paths(&self) -> GResult<Vec<String>>;
}

pub trait DataStoreWriter {
  fn write(&mut self, kb: &KeyBuffer) -> GResult<()>;
  fn commit(self: Box<Self>) -> GResult<KeyPositionCollection>;
}

pub trait DataStoreReader {
  fn iter(&self) -> Box<dyn DataStoreReaderIter + '_>;
  fn first_of(&self, key: KeyT) -> GResult<KeyBuffer>;
}

pub trait DataStoreReaderIter: Iterator<Item = KeyBuffer> {}

pub mod key_position;
pub mod key_buffer;
pub mod complexity;
pub mod array_store;
pub mod block_store;
pub mod store_designer;


// FUTURE: extensible metaserde?
#[derive(Serialize, Deserialize)]
pub enum DataStoreMeta {
  BlockStore { state: block_store::BlockStoreState },
  ArrayStore { state: array_store::ArrayStoreState },
}

pub trait DataStoreMetaserde {
  fn to_meta(&self, ctx: &mut Context) -> GResult<DataStoreMeta>;
}

impl DataStoreMeta {
  pub fn from_meta(meta: DataStoreMeta, ctx: &Context) -> GResult<Box<dyn DataStore>> {
    let store = match meta {
      DataStoreMeta::BlockStore { state } => Box::new(block_store::BlockStore::from_meta(state, ctx)?) as Box<dyn DataStore>,
      DataStoreMeta::ArrayStore { state } => Box::new(array_store::ArrayStore::from_meta(state, ctx)?) as Box<dyn DataStore>,
    };
    Ok(store)
  }
}