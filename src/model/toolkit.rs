use rayon::prelude::*;

use crate::model::BuilderFinalReport;
use crate::model::GResult;
use crate::model::KeyBuffer;
use crate::model::KeyPositionCollection;
use crate::model::LoadDistribution;
use crate::model::ModelBuilder;
use crate::model::ModelDraft;
use crate::model::ModelDrafter;
use crate::model::ModelRecon;
use crate::model::StorageProfile;
use crate::store::complexity::StepComplexity;
use crate::store::key_position::KeyPositionRangeIterator;


/* Accumulating mulitple drafters into one that tries and picks the best one */

#[derive(Debug)]
pub struct MultipleDrafter {
  drafters: Vec<Box<dyn ModelDrafter>>,
  use_parallel: bool,
}

impl Default for MultipleDrafter {
    fn default() -> Self {
        Self::new()
    }
}

impl MultipleDrafter {
  pub fn new() -> MultipleDrafter {
    MultipleDrafter::from(Vec::new())
  }

  pub fn from(drafters: Vec<Box<dyn ModelDrafter>>) -> MultipleDrafter {
    MultipleDrafter{ drafters, use_parallel: true }
  }

  pub fn is_empty(&self) -> bool {
    self.drafters.is_empty()
  }

  pub fn push(&mut self, drafter: Box<dyn ModelDrafter>) {
    self.drafters.push(drafter)
  }

  pub fn extend(mut self, other: MultipleDrafter) -> Self {
    self.drafters.extend(other.drafters);
    self
  }

  pub fn to_serial(mut self) -> MultipleDrafter {
    self.use_parallel = false;
    self
  }

  pub fn to_parallel(mut self) -> MultipleDrafter {
    self.use_parallel = true;
    self
  }

  fn draft_par(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> Option<ModelDraft> {
    self.drafters.par_iter()
      .map(|drafter| drafter.draft(kps, profile)
          .unwrap_or_else(|_| panic!("Drafting failed at {:?}", drafter)))
      .min_by_key(|draft| draft.cost)
  }

  fn draft_ser(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> Option<ModelDraft> {
    self.drafters.iter()
      .map(|drafter| drafter.draft(kps, profile)
          .unwrap_or_else(|_| panic!("Drafting failed at {:?}", drafter)))
      .min_by_key(|draft| draft.cost)
  }
}

impl ModelDrafter for MultipleDrafter {
  fn draft(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> GResult<ModelDraft> {
    let best_draft = match self.use_parallel {
      true => self.draft_par(kps, profile),
      false => self.draft_ser(kps, profile),
    }.expect("No draft produced (possibly drafters list is empty?)");
    log::info!(
      "Best drafted model: {:?}, {} submodels, cost= {:?}",
      best_draft.serde,
      best_draft.key_buffers.len(),
      best_draft.cost,
    );
    Ok(best_draft)
  }

  fn draft_many(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> Vec<ModelDraft> {
    self.drafters.par_iter()
      .map(|drafter| drafter.draft(kps, profile)
          .unwrap_or_else(|_| panic!("Drafting failed at {:?}", drafter)))
      .collect()
  }
}


/* Builder --> Drafter adaptor */

pub type BuilerProducer = dyn Fn() -> Box<dyn ModelBuilder> + Sync;

type PreliminaryDraft = (Vec<KeyBuffer>, Box<dyn ModelRecon>, usize);
const KPS_CHUNK_SIZE: usize = 1_000_000;

pub struct BuilderAsDrafter {
  builder_producer: Box<BuilerProducer>,
}

impl std::fmt::Debug for BuilderAsDrafter {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("BuilderAsDrafter")
      .field("drafter", &(*self.builder_producer)())
      .finish()
  }
}

impl BuilderAsDrafter {
  pub fn wrap(builder_producer: Box<BuilerProducer>) -> BuilderAsDrafter {
    BuilderAsDrafter { builder_producer }
  }

  fn summarize_loads(&self, loads: &[LoadDistribution]) -> Vec<usize> {
    // TODO: configurable?
    loads.iter()
      // .map(|load| load.max())
      // .map(|load| load.average())
      .map(|load| load.percentile(50.0))
      .collect()
  }

  fn draft_inner(&self, kps_iter: &mut KeyPositionRangeIterator) -> GResult<PreliminaryDraft> {
    let mut model_builder = (*self.builder_producer)();
    let mut total_size = 0;
    let mut key_buffers = Vec::new();
    for kpr in kps_iter {
      if let Some(model_kb) = model_builder.consume(&kpr)? {
        total_size += model_kb.serialized_size();
        key_buffers.push(model_kb);
      }
    }

    // finalize last bits of model
    let BuilderFinalReport { maybe_model_kb, serde } = model_builder.finalize()?;
    if let Some(model_kb) = maybe_model_kb {
        total_size += model_kb.serialized_size();
        key_buffers.push(model_kb);
    }

    Ok((key_buffers, serde, total_size))
  }

  fn draft_prelim(&self, kps: &KeyPositionCollection) -> GResult<PreliminaryDraft> {
    // draft in each chunk on parallel
    let mut prelim_drafts: Vec<PreliminaryDraft> = kps.chunk_iter(KPS_CHUNK_SIZE)
      .par_iter_mut()
      .map(|kps_iter| self.draft_inner(kps_iter)
          .unwrap_or_else(|_| panic!("Drafting failed on a chunk of key-positions")))
      .collect();

    // combine all drafts
    let (mut key_buffers, mut serde, mut total_size) = prelim_drafts.remove(0);
    for (next_key_buffers, next_serde, next_total_size) in &mut prelim_drafts {
      key_buffers.append(next_key_buffers);
      serde.combine_with(next_serde.as_ref());
      total_size += *next_total_size;
    }
    Ok((key_buffers, serde, total_size))
  }
}

impl ModelDrafter for BuilderAsDrafter {
  fn draft(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> GResult<ModelDraft> {
    let (key_buffers, serde, total_size) = self.draft_prelim(kps)?;

    // estimate cost
    let model_load_summary = self.summarize_loads(&serde.get_load());
    let (est_complexity_loads, _) = StepComplexity::measure(profile, total_size);
    let complexity_cost = profile.sequential_cost(&est_complexity_loads);
    let model_cost = profile.sequential_cost(&model_load_summary);
    let total_loads = [est_complexity_loads, model_load_summary].concat();
    let cost = profile.sequential_cost(&total_loads);
    log::trace!(
      "{:?}: {} submodels, loads= {:?} with {:?}, cost= {:?} (c/m: {:?}/{:?})",
      self,
      key_buffers.len(),
      total_loads,
      serde.get_load(),
      cost,
      complexity_cost,
      model_cost,
    );
    Ok(ModelDraft{ key_buffers, serde, cost })
  }

  fn draft_many(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> Vec<ModelDraft> {
    vec!(self.draft(kps, profile).unwrap())
  }
}