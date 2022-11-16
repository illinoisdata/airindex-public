use serde::{Serialize, Deserialize};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use url::Url;

use crate::common::error::GResult;
use crate::index::Index;
use crate::index::IndexBuilder;
use crate::index::IndexMeta;
use crate::index::IndexMetaserde;
use crate::index::naive::NaiveIndex;
use crate::index::PartialIndex;
use crate::index::PartialIndexMeta;
use crate::index::piecewise::PiecewiseIndex;
use crate::index::stash::StashIndex;
use crate::io::internal::ExternalStorage;
use crate::io::profile::StorageProfile;
use crate::io::storage::DummyAdaptor;
use crate::meta::Context;
use crate::model::load::LoadDistribution;
use crate::model::ModelDraft;
use crate::model::ModelDrafter;
use crate::store::DataStore;
use crate::store::key_buffer::KeyBuffer;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::KeyPositionRange;
use crate::store::key_position::KeyT;
use crate::store::store_designer::StoreDesigner;


/* Stack index */

#[derive(Debug)]
pub struct StackIndex {
  upper_index: Box<dyn Index>,
  lower_index: Box<dyn PartialIndex>,
}

impl Index for StackIndex {
  fn predict(&self, key: &KeyT) -> GResult<KeyPositionRange> {
    let kr = self.upper_index.predict(key)?;
    self.lower_index.predict_within(&kr)
  }

  fn get_load(&self) -> Vec<LoadDistribution> {
    [self.upper_index.get_load(), self.lower_index.get_load()].concat()
  }
}

#[derive(Debug)]
pub struct BalanceStackIndexBuilder<'a> {
  storage: Rc<RefCell<ExternalStorage>>,
  drafter: Box<dyn ModelDrafter>,
  profile: &'a dyn StorageProfile,
  prefix_url: Url,
}

impl<'a> BalanceStackIndexBuilder<'a> {
  pub fn new(storage: &Rc<RefCell<ExternalStorage>>, drafter: Box<dyn ModelDrafter>, profile: &'a dyn StorageProfile, prefix_url: Url) -> BalanceStackIndexBuilder<'a> {
    BalanceStackIndexBuilder {
      storage: Rc::clone(storage),
      drafter,
      profile,
      prefix_url,
    }
  }
}

impl<'a> BalanceStackIndexBuilder<'a> {
  pub fn bns_at_layer(  // balance & stack, at layer
    &self,
    kps: &KeyPositionCollection,
    layer_idx: usize,
    lower_data_store: Option<&dyn DataStore>,
  ) -> GResult<Box<dyn Index>> {
    // if no index is built
    let no_index_cost = self.profile.cost(kps.total_bytes());

    // if index is built
    let model_draft = self.drafter.draft(kps, self.profile)?;

    // if this layer is profitable, stack and try next layer
    if model_draft.cost < no_index_cost {
      // persist
      let data_store = StoreDesigner::new(&self.storage)
        .design_for_kbs(&model_draft.key_buffers, self.prefix_url.clone(), self.layer_name(layer_idx));
      let (piecewise_index, lower_index_kps) = PiecewiseIndex::craft(model_draft, data_store)?;

      // try next
      let upper_index = self.bns_at_layer(
        &lower_index_kps,
        layer_idx + 1,
        Some(piecewise_index.borrow_data_store()),
      )?;
      let lower_index = Box::new(piecewise_index) as Box<dyn PartialIndex>;
      Ok(Box::new(StackIndex {
        upper_index,
        lower_index
      }))
    } else {
      // // fetching whole data layer is faster than building index
      // Ok(Box::new(NaiveIndex::build(kps)))
      Ok(Box::new(StashIndex::build(kps, lower_data_store, &self.storage, &self.prefix_url)?))
    }
  }

  fn layer_name(&self, layer_idx: usize) -> String {
    format!("layer_{}", layer_idx)
  }
}

impl<'a> IndexBuilder for BalanceStackIndexBuilder<'a> {
  fn build_index(&self, kps: &KeyPositionCollection) -> GResult<Box<dyn Index>> {
    self.bns_at_layer(kps, 1, None)
  }
}

#[derive(Debug)]
pub struct BoundedTopStackIndexBuilder<'a> {
  storage: Rc<RefCell<ExternalStorage>>,
  drafter: Box<dyn ModelDrafter>,
  profile: &'a dyn StorageProfile,
  top_load: usize,
  prefix_url: Url,
}

impl<'a> BoundedTopStackIndexBuilder<'a> {
  pub fn new(storage: &Rc<RefCell<ExternalStorage>>, drafter: Box<dyn ModelDrafter>, profile: &'a dyn StorageProfile, top_load: usize, prefix_url: Url) -> BoundedTopStackIndexBuilder<'a> {
    BoundedTopStackIndexBuilder {
      storage: Rc::clone(storage),
      drafter,
      profile,
      top_load,
      prefix_url,
    }
  }
}

impl<'a> BoundedTopStackIndexBuilder<'a> {
  pub fn bts_at_layer(  // balance & stack, at layer
    &self,
    kps: &KeyPositionCollection,
    layer_idx: usize,
    lower_data_store: Option<&dyn DataStore>,
  ) -> GResult<Box<dyn Index>> {
    log::info!("Check total bytes {} <==> {}", kps.total_bytes(), self.top_load);
    if kps.total_bytes() > self.top_load {
      // kps is still large, so build and stack more index
      let model_draft = self.drafter.draft(kps, self.profile)?;

      // persist
      let data_store = StoreDesigner::new(&self.storage)
        .design_for_kbs(&model_draft.key_buffers, self.prefix_url.clone(), self.layer_name(layer_idx));
      let (piecewise_index, lower_index_kps) = PiecewiseIndex::craft(model_draft, data_store)?;

      // try next
      let upper_index = self.bts_at_layer(
        &lower_index_kps,
        layer_idx + 1,
        Some(piecewise_index.borrow_data_store()),
      )?;
      let lower_index = Box::new(piecewise_index) as Box<dyn PartialIndex>;
      Ok(Box::new(StackIndex {
        upper_index,
        lower_index,
      }))
    } else {
      // fetching whole data layer is faster than building index
      if lower_data_store.is_some() {
        Ok(Box::new(StashIndex::build(kps, lower_data_store, &self.storage, &self.prefix_url)?))
      } else {
        Ok(Box::new(NaiveIndex::build(kps)))
      }
    }
  }

  fn layer_name(&self, layer_idx: usize) -> String {
    format!("layer_{}", layer_idx)
  }
}

impl<'a> IndexBuilder for BoundedTopStackIndexBuilder<'a> {
  fn build_index(&self, kps: &KeyPositionCollection) -> GResult<Box<dyn Index>> {
    self.bts_at_layer(kps, 1, None)
  }
}


#[derive(Serialize, Deserialize)]
pub struct StackIndexMeta {
  upper_index: IndexMeta,
  lower_index: PartialIndexMeta,
}

impl IndexMetaserde for StackIndex {  // for Metaserde
  fn to_meta(&self, ctx: &mut Context) -> GResult<IndexMeta> {
    Ok(IndexMeta::Stack {
      meta: Box::new(StackIndexMeta {
        upper_index: self.upper_index.to_meta(ctx)?,
        lower_index: self.lower_index.to_meta_partial(ctx)?,
      })
    })
  }
}

impl StackIndex {  // for Metaserde
  pub fn from_meta(meta: StackIndexMeta, ctx: &Context) -> GResult<StackIndex> {
    Ok(StackIndex{
      upper_index: IndexMeta::from_meta(meta.upper_index, ctx)?,
      lower_index: PartialIndexMeta::from_meta_partial(meta.lower_index, ctx)?,
    })
  }
}

#[derive(Debug)]
pub struct ExploreStackIndexBuilder<'a> {
  storage: Rc<RefCell<ExternalStorage>>,
  drafter: Box<dyn ModelDrafter>,
  profile: &'a dyn StorageProfile,
  prefix_url: Url,

  // For generating kps without actually writing to storage
  dummy_storage: Rc<RefCell<ExternalStorage>>,
  dummy_prefix_url: Url,

  target_layers: Option<usize>,  // if set, only build index with many layers

  top_k_candidates: usize,
}

impl<'a> ExploreStackIndexBuilder<'a> {
  // explore all model drafts in many layers
  pub fn new(
    storage: &Rc<RefCell<ExternalStorage>>,
    drafter: Box<dyn ModelDrafter>,
    profile: &'a dyn StorageProfile,
    prefix_url: Url
  ) -> ExploreStackIndexBuilder<'a> {
    let dummy_storage = Rc::new(RefCell::new(ExternalStorage::new()
      .with("dummy".to_string(), Box::new(DummyAdaptor::default()))
      .expect("Failed to initiate dummy storage")
    ));
    ExploreStackIndexBuilder {
      storage: Rc::clone(storage),
      drafter,
      profile,
      prefix_url,
      dummy_storage,
      dummy_prefix_url: Url::parse("dummy:///index").unwrap(),
      target_layers: None,
      top_k_candidates: 5,
    }
  }

  // build at an exactly target number of layers
  pub fn exact_layers(
    storage: &Rc<RefCell<ExternalStorage>>,
    drafter: Box<dyn ModelDrafter>,
    profile: &'a dyn StorageProfile,
    prefix_url: Url,
    target_layers: usize,
  ) -> ExploreStackIndexBuilder<'a> {
    let dummy_storage = Rc::new(RefCell::new(ExternalStorage::new()
      .with("dummy".to_string(), Box::new(DummyAdaptor::default()))
      .expect("Failed to initiate dummy storage")
    ));
    ExploreStackIndexBuilder {
      storage: Rc::clone(storage),
      drafter,
      profile,
      prefix_url,
      dummy_storage,
      dummy_prefix_url: Url::parse("dummy:///index").unwrap(),
      target_layers: Some(target_layers),
      top_k_candidates: 5,
    }
  }

  pub fn set_top_k_candidates(mut self, top_k_candidates: usize) -> Self {
    self.top_k_candidates = top_k_candidates;
    self
  }

  fn summarize_loads(&self, loads: &[LoadDistribution]) -> Vec<usize> {
    // TODO: configurable?
    loads.iter()
      // .map(|load| load.max())
      .map(|load| load.average() as usize)
      // .map(|load| load.percentile(50.0))
      .collect()
  }

  fn should_build(&self, no_index_cost: &Duration, ideal_index_cost: &Duration, layer_idx: usize) -> bool {
    if let Some(target_layers) = self.target_layers {
      // keep building until having target_layers (i.e. construct from exact_layers)
      layer_idx <= target_layers
    } else {
      // if there is a chance that an index is beneficial (i.e. construct from new)
      ideal_index_cost < no_index_cost
    }
  }
}

impl<'a> ExploreStackIndexBuilder<'a> {
  pub fn ens_at_layer(  // explore & stack, at layer
    &self,
    kps: &KeyPositionCollection,
    layer_idx: usize,
  ) -> GResult<(Vec<ModelDraft>, Duration)> {
    // decide whether to continue
    let no_index_cost = self.profile.cost(kps.total_bytes());
    let ideal_index_cost = self.profile.sequential_cost(&[1, 1]);

    if self.should_build(&no_index_cost, &ideal_index_cost, layer_idx) {
      let mut maybe_drafts = None;
      let mut drafts = self.drafter.draft_many(kps, self.profile);
      drafts.sort_by_key(|draft| draft.cost);
      for model_draft in drafts.into_iter().take(self.top_k_candidates) {
        // calculate cost at this layer
        let current_loads = self.summarize_loads(&model_draft.serde.get_load());
        let current_costs = self.profile.sequential_cost(&current_loads);
        let current_ideal_cost = self.profile.sequential_cost(&[vec![1], current_loads].concat());
        if !self.should_build(&no_index_cost, &current_ideal_cost, layer_idx) {
          continue;
        }

        // generate next kps
        let mut data_store = self.make_data_store_dummy(&model_draft.key_buffers, layer_idx);
        let mut data_writer = data_store.begin_write()?;
        for model_kb in &model_draft.key_buffers {
          data_writer.write(model_kb)?;
        }
        let current_kps = data_writer.commit()?;
        if current_kps.total_bytes() >= kps.total_bytes() / 2 {
          continue;
        }

        // try next layer
        if let Ok((mut model_drafts, upper_cost)) = self.ens_at_layer(&current_kps, layer_idx + 1) {
          model_drafts.push(model_draft);
          let total_cost = upper_cost + current_costs;

          // decide whether to use this draft
          if layer_idx == 1 {
            self.log_draft("Candidate", &model_drafts, &total_cost);
          }
          maybe_drafts = match maybe_drafts {
            Some((best_drafts, best_cost)) => if best_cost < total_cost {
              Some((best_drafts, best_cost))
            } else {
              Some((model_drafts, total_cost))
            },
            None => Some((model_drafts, total_cost))
          }
        }
      }

      // return if beneficial
      if let Some((model_drafts, best_index_cost)) = maybe_drafts {
        if self.should_build(&no_index_cost, &best_index_cost, layer_idx) {
          return Ok((model_drafts, best_index_cost))
        }
      }
    }

    // if layer not at target, return error
    if let Some(target_layers) = self.target_layers {
      if layer_idx <= target_layers {
        return Err("Target number of layers is not satisfied".into())
      }
    }

    // fetching whole data layer is faster than building index, no further index to build
    Ok((Vec::new(), no_index_cost))
    
  }

  fn craft_all(
    &self,
    mut model_drafts: Vec<ModelDraft>,
    layer_idx: usize,
    kps: &KeyPositionCollection,
    lower_data_store: Option<&dyn DataStore>,
  ) -> GResult<Box<dyn Index>> {
    if let Some(current_model_draft) = model_drafts.pop() {
      // write current draft to storage
      let current_data_store = self.make_data_store(&current_model_draft.key_buffers, layer_idx);
      let (current_index, current_kps) = PiecewiseIndex::craft(current_model_draft, current_data_store)?;

      // continue to write upper index
      let upper_index = self.craft_all(
        model_drafts,
        layer_idx + 1,
        &current_kps,
        Some(current_index.borrow_data_store()),
      )?;

      // compose upper layers with current layer
      Ok(Box::new(StackIndex {
          upper_index,
          lower_index: Box::new(current_index),
      }))
    } else {
      // no more layer, make the root (no index) layer
      if lower_data_store.is_some() {
        Ok(Box::new(StashIndex::build(kps, lower_data_store, &self.storage, &self.prefix_url)?))
      } else {
        Ok(Box::new(NaiveIndex::build(kps)))
      }
    }
  }

  fn make_data_store(&self, key_buffers: &[KeyBuffer], layer_idx: usize) -> Box<dyn DataStore> {
    StoreDesigner::new(&self.storage)
      .design_for_kbs(
        key_buffers,
        self.prefix_url.clone(),
        self.layer_name(layer_idx),
      )
  }

  fn make_data_store_dummy(&self, key_buffers: &[KeyBuffer], layer_idx: usize) -> Box<dyn DataStore> {
    StoreDesigner::new(&self.dummy_storage)
      .design_for_kbs(
        key_buffers,
        self.dummy_prefix_url.clone(),
        self.layer_name(layer_idx),
      )
  }

  fn layer_name(&self, layer_idx: usize) -> String {
    format!("layer_{}", layer_idx)
  }

  fn log_draft(&self, prefix: &str, model_drafts: &[ModelDraft], total_cost: &Duration) {
    log::info!(
      "{}\n\t{}\n\tcost= {:?}",
      prefix,
      model_drafts.iter().map(|md| format!("{:?}", md)).collect::<Vec<String>>().join("\n\t"),
      total_cost,
    );
  }
}

impl<'a> IndexBuilder for ExploreStackIndexBuilder<'a> {
  fn build_index(&self, kps: &KeyPositionCollection) -> GResult<Box<dyn Index>> {
    let (model_drafts, best_cost) = self.ens_at_layer(kps, 1)?;  // root, ..., layer 1
    self.log_draft("Best draft", &model_drafts, &best_cost);
    self.craft_all(model_drafts, 1, kps, None)
  }
}
