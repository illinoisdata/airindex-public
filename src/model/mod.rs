use serde::{Serialize, Deserialize};
use std::fmt::Debug;
use std::time::Duration;

use crate::common::error::GResult;
use crate::io::profile::StorageProfile;
use crate::meta::Context;
use crate::model::load::LoadDistribution;
use crate::store::key_buffer::KeyBuffer;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::KeyPositionRange;
use crate::store::key_position::KeyT;

type MaybeKeyBuffer = Option<KeyBuffer>;


/* Models */

pub trait Model: Debug {
  // predict position(s) for the key
  fn predict(&self, key: &KeyT) -> KeyPositionRange;
}


/* Model Deserializer */

pub trait ModelRecon: ModelReconMetaserde + Debug + Send {
  fn reconstruct(&self, buffer: &[u8]) -> GResult<Box<dyn Model>>;
  fn get_load(&self) -> Vec<LoadDistribution>;

  fn combine_with(&mut self, other: &dyn ModelRecon);
  fn to_typed(&self) -> ModelReconMeta;
}

#[derive(Serialize, Deserialize)]
pub enum ModelReconMeta {
  Step { meta: Box<step::StepModelReconMeta> },
  Band { meta: Box<band::BandModelReconMeta> },  // BandModelReconMeta is large
}

pub trait ModelReconMetaserde {
  fn to_meta(&self, ctx: &mut Context) -> GResult<ModelReconMeta>;
}

impl ModelReconMeta {
  pub fn from_meta(meta: ModelReconMeta, ctx: &Context) -> GResult<Box<dyn ModelRecon>> {
    let store = match meta {
      ModelReconMeta::Step { meta } => Box::new(step::StepModelRecon::from_meta(*meta, ctx)?) as Box<dyn ModelRecon>,
      ModelReconMeta::Band { meta } => Box::new(band::BandModelRecon::from_meta(*meta, ctx)?) as Box<dyn ModelRecon>,
    };
    Ok(store)
  }
}


/* Model (Incremental) Builders */

pub struct BuilderFinalReport {
  pub maybe_model_kb: MaybeKeyBuffer,  // last buffer if any
  pub serde: Box<dyn ModelRecon>,  // for future deserialization
}

pub trait ModelBuilder: Debug + Sync {
  fn consume(&mut self, kpr: &KeyPositionRange) -> GResult<MaybeKeyBuffer>;
  fn finalize(self: Box<Self>) -> GResult<BuilderFinalReport>;
}


/* Model Drafter */
// prefer this if the resulting model is not large

pub struct ModelDraft {
  pub key_buffers: Vec<KeyBuffer>,
  pub serde: Box<dyn ModelRecon>,
  pub cost: Duration,
}

impl std::fmt::Debug for ModelDraft {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ModelDraft")
      .field("num_kb", &self.key_buffers.len())
      .field("serde", &self.serde)
      .field("cost", &self.cost)
      .finish()
  }
}

unsafe impl Send for ModelDraft {}
unsafe impl Sync for ModelDraft {}

pub trait ModelDrafter: Sync + Debug {
  fn draft(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> GResult<ModelDraft>;
  fn draft_many(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> Vec<ModelDraft>;
}



pub mod toolkit;
pub mod load;
pub mod step;
pub mod band;
