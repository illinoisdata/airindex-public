use core::cell::RefCell;
use crate::store::DataStore;
use serde::{Serialize, Deserialize};
use std::rc::Rc;
use url::Url;

use crate::common::SharedBytes;
use crate::index::GResult;
use crate::index::Index;
use crate::index::IndexMeta;
use crate::index::IndexMetaserde;
use crate::index::KeyPositionRange;
use crate::index::KeyT;
use crate::index::LoadDistribution;
use crate::io::internal::ExternalStorage;
use crate::meta::Context;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::PositionT;

/* Index that stash everything in given store and warm up the cache on deserialize */

#[derive(Serialize, Deserialize, Clone)]
struct Stash {
  path: String,
  buffer: SharedBytes,
}

impl Stash {
  fn new(path: String, storage: &Rc<RefCell<ExternalStorage>>, prefix_url: &Url) -> GResult<Stash> {
    let url = prefix_url.join(&path)?;
    let buffer = storage.borrow().read_all(&url)?;
    Ok(Stash { path, buffer })
  }

  fn apply(&self, ctx: &Context) -> GResult<()> {
    if let Some(storage) = &ctx.storage {
      let url = ctx.store_prefix.as_ref()
        .expect("Applying stash require store_prefix")
        .join(&self.path)?;
      storage.borrow().warm_cache(&url, &self.buffer.slice_all());
    }
    Ok(())
  }

  fn size(&self) -> usize {
    self.buffer.len()
  }
}

impl std::fmt::Debug for Stash {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Stash")
      .field("path", &self.path)
      .field("buffer_len", &self.buffer.len())
      .finish()
  }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StashIndex {
  start_position: PositionT,
  end_position: PositionT,
  stashes: Vec<Stash>,
}

impl StashIndex {
  pub fn build(
    kps: &KeyPositionCollection, 
    data_store: Option<&dyn DataStore>,  // to be stashed
    storage: &Rc<RefCell<ExternalStorage>>,  // source of target data
    prefix_url: &Url,
  ) -> GResult<StashIndex> {
    let (start_position, end_position) = kps.whole_range();
    let stashes = StashIndex::stash(data_store, storage, prefix_url)?;
    Ok(StashIndex { start_position, end_position, stashes })
  }

  fn stash(
    data_store: Option<&dyn DataStore>,  // to be stashed
    storage: &Rc<RefCell<ExternalStorage>>,  // source of target data
    prefix_url: &Url,
  ) -> GResult<Vec<Stash>> {
    match data_store {
      Some(data_store) => {
        let stashes: Vec<Stash> = data_store.relevant_paths()?.into_iter().map(|url|
          Stash::new(url, storage, prefix_url)
        ).collect::<GResult<Vec<Stash>>>()?;
        Ok(stashes)
      },
      None => Ok(Vec::new())
    }
  }

  fn apply(&self, ctx: &Context) -> GResult<()> {
    self.stashes.iter().try_for_each(|stash| stash.apply(ctx))
  }
}

impl Index for StashIndex {
  fn predict(&self, key: &KeyT) -> GResult<KeyPositionRange> {
    Ok(KeyPositionRange::from_bound(*key, *key, self.start_position, self.end_position))
  }

  fn get_load(&self) -> Vec<LoadDistribution> {
    let stash_size = self.stashes.iter().map(|stash| stash.size()).sum();
    let position_range = self.end_position - self.start_position;
    vec![LoadDistribution::exact(std::cmp::max(stash_size, position_range))]
  }
}


pub type StashIndexMeta = StashIndex;

impl IndexMetaserde for StashIndex {  // for Metaserde
  fn to_meta(&self, _ctx: &mut Context) -> GResult<IndexMeta> {
    Ok(IndexMeta::Stash { meta: self.clone() })
  }
}

impl StashIndex {  // for Metaserde
  pub fn from_meta(stash_index: StashIndexMeta, ctx: &Context) -> GResult<StashIndex> {
    stash_index.apply(ctx)?;
    Ok(stash_index)
  }
}