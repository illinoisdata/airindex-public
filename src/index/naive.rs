use serde::{Serialize, Deserialize};

use crate::index::GResult;
use crate::index::Index;
use crate::index::IndexMeta;
use crate::index::IndexMetaserde;
use crate::index::KeyPositionRange;
use crate::index::KeyT;
use crate::index::LoadDistribution;
use crate::meta::Context;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::PositionT;

/* Index that predicts the whole data layer */

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NaiveIndex {
  start_position: PositionT,
  end_position: PositionT,
}

impl NaiveIndex {
  pub fn build(kps: &KeyPositionCollection) -> NaiveIndex {
    let (start_position, end_position) = kps.whole_range();
    NaiveIndex { start_position, end_position }
  }
}

impl Index for NaiveIndex {
  fn predict(&self, key: &KeyT) -> GResult<KeyPositionRange> {
    Ok(KeyPositionRange::from_bound(*key, *key, self.start_position, self.end_position))
  }

  fn get_load(&self) -> Vec<LoadDistribution> {
    vec![LoadDistribution::exact(self.end_position - self.start_position)]
  }
}


pub type NaiveIndexMeta = NaiveIndex;

impl IndexMetaserde for NaiveIndex {  // for Metaserde
  fn to_meta(&self, _ctx: &mut Context) -> GResult<IndexMeta> {
    Ok(IndexMeta::Naive { meta: self.clone() })
  }
}

impl NaiveIndex {  // for Metaserde
  pub fn from_meta(meta: NaiveIndexMeta, _ctx: &Context) -> GResult<NaiveIndex> {
    Ok(meta)
  }
}