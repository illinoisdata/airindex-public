use serde::{Serialize, Deserialize};
use std::fmt::Debug;

use crate::common::error::GResult;
use crate::meta::Context;
use crate::model::load::LoadDistribution;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::KeyPositionRange;
use crate::store::key_position::KeyT;


/* Index traits */

pub trait Index: IndexMetaserde + Debug {
  fn predict(&self, key: &KeyT) -> GResult<KeyPositionRange>;
  fn get_load(&self) -> Vec<LoadDistribution>;
}

pub trait PartialIndex: PartialIndexMetaserde + Index {
  fn predict_within(&self, kr: &KeyPositionRange) -> GResult<KeyPositionRange>;
}

pub trait IndexBuilder: Debug {
  fn build_index(&self, kps: &KeyPositionCollection) -> GResult<Box<dyn Index>>;
}

pub mod piecewise;
pub mod hierarchical;
pub mod naive;
pub mod stash;


// FUTURE: extensible metaserde?
#[derive(Serialize, Deserialize)]
pub enum IndexMeta {
  Piecewise { meta: piecewise::PiecewiseIndexMeta },
  Stack { meta: Box<hierarchical::StackIndexMeta> },
  Naive { meta: naive::NaiveIndex },
  Stash { meta: stash::StashIndex },
}

pub trait IndexMetaserde {
  fn to_meta(&self, ctx: &mut Context) -> GResult<IndexMeta>;
}

impl IndexMeta {
  pub fn from_meta(meta: IndexMeta, ctx: &Context) -> GResult<Box<dyn Index>> {
    let store = match meta {
      IndexMeta::Piecewise { meta } => Box::new(piecewise::PiecewiseIndex::from_meta(meta, ctx)?) as Box<dyn Index>,
      IndexMeta::Stack { meta } => Box::new(hierarchical::StackIndex::from_meta(*meta, ctx)?) as Box<dyn Index>,
      IndexMeta::Naive { meta } => Box::new(naive::NaiveIndex::from_meta(meta, ctx)?) as Box<dyn Index>,
      IndexMeta::Stash { meta } => Box::new(stash::StashIndex::from_meta(meta, ctx)?) as Box<dyn Index>,
    };
    Ok(store)
  }
}

#[derive(Serialize, Deserialize)]
pub enum PartialIndexMeta {
  Piecewise { meta: piecewise::PiecewiseIndexMeta },
}

pub trait PartialIndexMetaserde {
  fn to_meta_partial(&self, ctx: &mut Context) -> GResult<PartialIndexMeta>;
}

impl PartialIndexMeta {
  pub fn from_meta_partial(meta: PartialIndexMeta, ctx: &Context) -> GResult<Box<dyn PartialIndex>> {
    let store = match meta {
      PartialIndexMeta::Piecewise { meta } => Box::new(piecewise::PiecewiseIndex::from_meta_partial(meta, ctx)?) as Box<dyn PartialIndex>,
    };
    Ok(store)
  }
}