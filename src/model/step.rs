use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use serde::{Serialize, Deserialize};
use std::io;

use crate::common::error::GResult;
use crate::meta::Context;
use crate::model::BuilderFinalReport;
use crate::model::LoadDistribution;
use crate::model::MaybeKeyBuffer;
use crate::model::Model;
use crate::model::ModelBuilder;
use crate::model::ModelDrafter;
use crate::model::ModelRecon;
use crate::model::ModelReconMeta;
use crate::model::ModelReconMetaserde;
use crate::model::toolkit::BuilderAsDrafter;
use crate::model::toolkit::MultipleDrafter;
use crate::store::key_buffer::KeyBuffer;
use crate::store::key_position::KEY_LENGTH;
use crate::store::key_position::KeyPosition;
use crate::store::key_position::KeyPositionRange;
use crate::store::key_position::KeyT;
use crate::store::key_position::POSITION_LENGTH;
use crate::store::key_position::PositionT;


/* The Model */

#[derive(Debug)]
struct StepModel {
  anchors: Vec<KeyPosition>,
}

impl StepModel {
  fn new() -> StepModel {
    StepModel { anchors: Vec::new() }
  }

  fn push(&mut self, kp: KeyPosition) {
    self.anchors.push(kp)
  }

  fn push_kpr(&mut self, kpr: &KeyPositionRange) {
    self.anchors.push(KeyPosition {key: kpr.key_l, position: kpr.offset });
  }

  fn push_kpr_closing(&mut self, kpr: &KeyPositionRange) {
    self.anchors.push(KeyPosition {key: u64::MAX, position: kpr.offset + kpr.length });
  }

  fn len(&self) -> usize {
    self.anchors.len()
  }

  fn is_empty(&self) -> bool {
    self.anchors.is_empty()
  }

  fn left_anchor(&self) -> Option<&KeyPosition> {
    if self.is_empty() {
      None
    } else {
      Some(&self.anchors[0])
    }
  }

  fn load_at(&self, idx: usize) -> PositionT {
    assert!((0..self.anchors.len()-1).contains(&idx));
    self.anchors[idx + 1].position - self.anchors[idx].position
  }

  // fn right_anchor(&self) -> Option<&KeyPosition> {
  //   if self.is_empty() {
  //     None
  //   } else {
  //     Some(&self.anchors[self.anchors.len() - 1])
  //   }
  // }
}

impl Model for StepModel {
  fn predict(&self, key: &KeyT) -> KeyPositionRange {
    for anchor_pair in self.anchors.windows(2) {
      let left_anchor = &anchor_pair[0];
      let right_anchor = &anchor_pair[1];
      if left_anchor.key <= *key && *key < right_anchor.key {
        return KeyPositionRange::from_bound(*key, *key, left_anchor.position, right_anchor.position)
      }
    }
    panic!("Step model does not cover key {}", key)
  }
}


/* Serialization */

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StepModelRecon {
  load: LoadDistribution,
}

impl StepModelRecon {
  fn new() -> StepModelRecon {
    StepModelRecon { load: LoadDistribution::default() }
  }

  fn sketch(
    &mut self,
    stm: &StepModel,
    bundle_size: usize,
    num_samples: &[usize],
  ) -> io::Result<Vec<u8>> {
    // update load distribution
    assert_eq!(num_samples.len(), stm.anchors.len() - 1);
    for (idx, samples) in num_samples.iter().enumerate() {
      self.load.add(stm.load_at(idx) as f64, (*samples).try_into().unwrap());
    }

    // turn the model into a buffer
    let mut model_buffer = vec![];

    // bytes for the actual anchors
    for anchor in &stm.anchors {
      model_buffer.write_uint::<BigEndian>(anchor.key, KEY_LENGTH)?;
      model_buffer.write_uint::<BigEndian>(anchor.position as u64, POSITION_LENGTH)?;

    }

    // fill until constant size (for space efficiency)
    let fillin_anchor = &stm.anchors[stm.anchors.len() - 1];
    for _ in stm.anchors.len()..bundle_size {
      model_buffer.write_uint::<BigEndian>(fillin_anchor.key, KEY_LENGTH)?;
      model_buffer.write_uint::<BigEndian>(fillin_anchor.position as u64, POSITION_LENGTH)?;
    }
    Ok(model_buffer)
  }

  fn reconstruct_raw(&self, buffer: &[u8]) -> GResult<StepModel> {
    assert!(buffer.len() % ANCHOR_LENGTH == 0, "Unexpected buffer size for a step model");
    let mut stm = StepModel::new();
    for idx in 0..(buffer.len() / ANCHOR_LENGTH) {
      let offset = idx * ANCHOR_LENGTH;
      let key_buffer = &buffer[offset..offset+KEY_LENGTH];
      let position_buffer = &buffer[offset+KEY_LENGTH..offset+KEY_LENGTH+POSITION_LENGTH];
      stm.push(KeyPosition {
          key: BigEndian::read_uint(key_buffer, KEY_LENGTH),
          position: BigEndian::read_uint(position_buffer, POSITION_LENGTH) as PositionT,
      })
    }
    Ok(stm)
  }
}

const ANCHOR_LENGTH: usize = KEY_LENGTH + POSITION_LENGTH;

impl ModelRecon for StepModelRecon {
  fn reconstruct(&self, buffer: &[u8]) -> GResult<Box<dyn Model>> {
    let stm = self.reconstruct_raw(buffer)?;
    Ok(Box::new(stm))
  }

  fn get_load(&self) -> Vec<LoadDistribution> {
    vec![self.load.clone()]
  }

  fn combine_with(&mut self, other: &dyn ModelRecon) {
    match other.to_typed() {
      ModelReconMeta::Step { meta } => {
        self.load.extend(&meta.load);
      },
      _ => panic!("Cannot combine StepModelRecon with this {:?}", other),
    }
  }

  fn to_typed(&self) -> ModelReconMeta {
    // TODO: downcast directly without meta enum?
    ModelReconMeta::Step { meta: Box::new(self.clone()) }
  }
}


pub type StepModelReconMeta = StepModelRecon;

impl ModelReconMetaserde for StepModelRecon {  // for Metaserde
  fn to_meta(&self, _ctx: &mut Context) -> GResult<ModelReconMeta> {
    Ok(ModelReconMeta::Step { meta: Box::new(self.clone()) })
  }
}

impl StepModelRecon {  // for Metaserde
  pub fn from_meta(meta: StepModelReconMeta, _ctx: &Context) -> GResult<StepModelRecon> {
    Ok(meta)
  }
}


/* Builder */

pub struct StepGreedyBuilder {
  max_load: PositionT,  // max range between anchors
  bundle_size: usize,  // number of anchors per submodel
  serde: StepModelRecon,
  stm: StepModel,
  num_samples: Vec<usize>,
  cur_kpr: Option<KeyPositionRange>,  // current active range
}

impl std::fmt::Debug for StepGreedyBuilder {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("StepGB")
      .field("max_load", &self.max_load)
      .field("bundle_size", &self.bundle_size)
      .finish()
  }
}

impl StepGreedyBuilder {
  pub fn new(max_load: PositionT, bundle_size: usize) -> StepGreedyBuilder {
    assert!(bundle_size > 2, "Each step submodel requires at least two anchors");
    StepGreedyBuilder {
      max_load,
      bundle_size,
      serde: StepModelRecon::new(),
      stm: StepModel::new(),
      num_samples: Vec::new(),
      cur_kpr: None,
    }
  }

  fn generate_segment(&mut self) -> GResult<MaybeKeyBuffer> {
    assert!(self.stm.len() <= self.bundle_size);
    let result = match self.stm.left_anchor() {
      Some(left_anchor) => {
        let step_buffer = self.serde.sketch(&self.stm, self.bundle_size, &self.num_samples)?;
        Ok(Some(KeyBuffer::new(left_anchor.key, step_buffer)))
      },
      None => Ok(None),
    };
    self.stm = StepModel::new();
    self.num_samples = match self.cur_kpr {
      Some(_) => vec![1],
      None => Vec::new(),
    };
    result
  }
}

impl ModelBuilder for StepGreedyBuilder {
  fn consume(&mut self, kpr: &KeyPositionRange) -> GResult<MaybeKeyBuffer> {
    // self.max_load = std::cmp::max(self.max_load, kpr.length);
    match &mut self.cur_kpr {
      None => {
        self.cur_kpr = Some(kpr.clone());
        self.num_samples.push(1);
      },
      Some(the_cur_kpr) => {
        if the_cur_kpr.offset + self.max_load >= kpr.offset + kpr.length {
          // include in anchor
          the_cur_kpr.key_r = kpr.key_r;
          the_cur_kpr.length = kpr.offset + kpr.length - the_cur_kpr.offset;
          if let Some(last) = self.num_samples.last_mut() {
            *last += 1;
          }
        } else {
          // not inclide in anchor, starting new submodel
          self.stm.push_kpr(the_cur_kpr);
          self.cur_kpr = Some(kpr.clone());
          self.num_samples.push(1);
        }
      },
    };

    // check if saturated to write
    if self.stm.len() == self.bundle_size - 1 {
      self.stm.push_kpr(self.cur_kpr.as_ref().unwrap());
      self.num_samples.pop();
      Ok(self.generate_segment()?)
    } else {
      Ok(None)
    }
  }

  fn finalize(mut self: Box<Self>) -> GResult<BuilderFinalReport> {
    if let Some(the_cur_kpr) = &self.cur_kpr {
      self.stm.push_kpr(the_cur_kpr);
      self.stm.push_kpr_closing(the_cur_kpr);
    }
    Ok(BuilderFinalReport {
      maybe_model_kb: self.generate_segment()?,
      serde: Box::new(self.serde),
    })
  }
}

impl StepGreedyBuilder {
  fn drafter(max_error: usize, bundle_size: usize) -> Box<dyn ModelDrafter> {
    let stm_producer = Box::new(
      move || {
        Box::new(StepGreedyBuilder::new(max_error, bundle_size)) as Box<dyn ModelBuilder>
      });
    Box::new(BuilderAsDrafter::wrap(stm_producer))
  }
}


/* Drafter */

// drafter that tries models with all these max errors and picks cheapest one
// it offers different choices of linear builders
pub struct StepMultipleDrafter;

impl StepMultipleDrafter {
  pub fn exponentiation(low_error: PositionT, high_error: PositionT, exponent: f64, bundle_size: usize) -> MultipleDrafter {
    let mut stm_drafters = Vec::new();
    let mut current_error = low_error;
    while current_error < high_error {
      stm_drafters.push(StepGreedyBuilder::drafter(current_error, bundle_size));
      current_error = ((current_error as f64) * exponent) as PositionT;
    }
    stm_drafters.push(StepGreedyBuilder::drafter(high_error, bundle_size));
    MultipleDrafter::from(stm_drafters)
  }
}


/* Tests */

#[cfg(test)]
mod tests {
  use super::*;

  use crate::common::SharedByteSlice;


  fn test_same_model(model_1: &StepModel, model_2: &StepModel) {
    assert_eq!(model_1.anchors, model_2.anchors);
  }

  fn test_same_model_box(model_1: &Box<dyn Model>, model_2: &Box<StepModel>, key_left: KeyT, key_right: KeyT) {
    for test_key in key_left..key_right {
      assert_eq!(
        model_1.predict(&test_key),
        model_2.predict(&test_key),
        "Models predict differently {:#?} <--> {:#?}",
        model_1,
        model_2,
      ); 
    }
  }
  
  #[test]
  fn serde_test() -> GResult<()> {
    let mut stm_serde = StepModelRecon::new();
    let stm = Box::new(StepModel {
      anchors: vec![
        KeyPosition { key: 0, position: 0 },
        KeyPosition { key: 105, position: 30 },
        KeyPosition { key: 110, position: 50 },
      ],
    });
    let stm_fillin = Box::new(StepModel {
      anchors: vec![
        KeyPosition { key: 0, position: 0 },
        KeyPosition { key: 105, position: 30 },
        KeyPosition { key: 110, position: 50 },
        KeyPosition { key: 110, position: 50 },
        KeyPosition { key: 110, position: 50 },
      ],
    });
    let num_samples = vec![10, 20];
    let num_samples_fillin = vec![10, 20, 30, 10];

    // sketch this model
    let stm_buffer = stm_serde.sketch(&stm, 3, &num_samples)?;
    assert!(stm_buffer.len() > 0);

    // reconstruct
    let stm_recon = stm_serde.reconstruct_raw(&stm_buffer)?;
    test_same_model(&stm_recon, &stm);

    // sketch this model, higher bundle size
    let stm_buffer_fillin = stm_serde.sketch(&stm_fillin, 5, &num_samples_fillin)?;
    assert!(stm_buffer_fillin.len() > 0);

    // reconstruct
    let stm_recon_fillin = stm_serde.reconstruct_raw(&stm_buffer_fillin)?;
    test_same_model(&stm_recon_fillin, &stm_fillin);

    Ok(())
  }

  fn generate_test_kprs() -> [KeyPositionRange; 8] {
    [
      KeyPositionRange{ key_l: 0, key_r: 0, offset: 0, length: 7},  // 0
      KeyPositionRange{ key_l: 50, key_r: 50, offset: 7, length: 3},  // 1
      KeyPositionRange{ key_l: 100, key_r: 100, offset: 10, length: 20},  // 2
      KeyPositionRange{ key_l: 105, key_r: 105, offset: 30, length: 20},  // 3
      KeyPositionRange{ key_l: 110, key_r: 110, offset: 50, length: 20},  // 4
      KeyPositionRange{ key_l: 115, key_r: 115, offset: 70, length: 20},  // 5
      KeyPositionRange{ key_l: 120, key_r: 120, offset: 90, length: 910},  // 6: jump, should split here
      KeyPositionRange{ key_l: 131, key_r: 131, offset: 1000, length: 915},  // 7
    ]
  }

  fn assert_none_buffer(buffer: MaybeKeyBuffer) -> MaybeKeyBuffer {
    assert!(buffer.is_none());
    None
  }

  fn assert_some_buffer(buffer: MaybeKeyBuffer) -> SharedByteSlice {
    assert!(buffer.is_some());
    buffer.unwrap().buffer
  }
  
  #[test]
  fn greedy_corridor_test() -> GResult<()> {
    let kprs = generate_test_kprs();
    let mut stm_builder = Box::new(StepGreedyBuilder::new(30, 3));

    // start adding points
    let _model_kb_0 = assert_none_buffer(stm_builder.consume(&kprs[0])?);
    let _model_kb_1 = assert_none_buffer(stm_builder.consume(&kprs[1])?);
    let _model_kb_2 = assert_none_buffer(stm_builder.consume(&kprs[2])?);
    let _model_kb_3 = assert_none_buffer(stm_builder.consume(&kprs[3])?);
    let model_kb_4 = assert_some_buffer(stm_builder.consume(&kprs[4])?);
    let _model_kb_5 = assert_none_buffer(stm_builder.consume(&kprs[5])?);
    let model_kb_6 = assert_some_buffer(stm_builder.consume(&kprs[6])?);
    let _model_kb_7 = assert_none_buffer(stm_builder.consume(&kprs[7])?);

    // finalize the builder
    let BuilderFinalReport {
      maybe_model_kb: last_buffer,
      serde: stm_serde,
    } = stm_builder.finalize()?;
    let model_kb_8 = assert_some_buffer(last_buffer);
    let max_load: Vec<usize> = stm_serde.get_load().iter().map(|load| load.max()).collect();
    assert_eq!(max_load, vec![915]);

    // check buffers
    test_same_model_box(
      &stm_serde.reconstruct(&model_kb_4[..])?,
      &Box::new(StepModel {
        anchors: vec![
          KeyPosition { key: 0, position: 0 },
          KeyPosition { key: 105, position: 30 },
          KeyPosition { key: 110, position: 50 },
        ],
      }),
      0,
      110,
    );
    test_same_model_box(
      &stm_serde.reconstruct(&model_kb_6[..])?,
      &Box::new(StepModel {
        anchors: vec![
          KeyPosition { key: 110, position: 50 },
          KeyPosition { key: 115, position: 70 },
          KeyPosition { key: 120, position: 90 },
        ],
      }),
      110,
      120,
    );
    test_same_model_box(
      &stm_serde.reconstruct(&model_kb_8[..])?,
      &Box::new(StepModel {
        anchors: vec![
          KeyPosition { key: 120, position: 90 },
          KeyPosition { key: 131, position: 1000 },
          KeyPosition { key: u64::MAX, position: 1915 },
        ],
      }),
      120,
      132,
    );
    Ok(())
  }
  
  #[test]
  fn greedy_corridor_with_error_test() -> GResult<()> {
    let kprs = generate_test_kprs();
    let mut stm_builder = Box::new(StepGreedyBuilder::new(1000, 5));

    // start adding points
    let _model_kb_0 = assert_none_buffer(stm_builder.consume(&kprs[0])?);
    let _model_kb_1 = assert_none_buffer(stm_builder.consume(&kprs[1])?);
    let _model_kb_2 = assert_none_buffer(stm_builder.consume(&kprs[2])?);
    let _model_kb_3 = assert_none_buffer(stm_builder.consume(&kprs[3])?);
    let _model_kb_4 = assert_none_buffer(stm_builder.consume(&kprs[4])?);
    let _model_kb_5 = assert_none_buffer(stm_builder.consume(&kprs[5])?);
    let _model_kb_6 = assert_none_buffer(stm_builder.consume(&kprs[6])?);
    let _model_kb_7 = assert_none_buffer(stm_builder.consume(&kprs[7])?);

    // finalize the builder
    let BuilderFinalReport {
      maybe_model_kb: last_buffer,
      serde: stm_serde,
    } = stm_builder.finalize()?;
    let model_kb_8 = assert_some_buffer(last_buffer);
    let max_load: Vec<usize> = stm_serde.get_load().iter().map(|load| load.max()).collect();
    assert_eq!(max_load, vec![1000]);

    // check buffers
    test_same_model_box(
      &stm_serde.reconstruct(&model_kb_8[..])?,
      &Box::new(StepModel {
        anchors: vec![
          KeyPosition { key: 0, position: 0 },
          KeyPosition { key: 131, position: 1000 },
          KeyPosition { key: u64::MAX, position: 1915 },
          KeyPosition { key: u64::MAX, position: 1915 },
          KeyPosition { key: u64::MAX, position: 1915 },
        ],
      }),
      120,
      132,
    );
    Ok(())
  }
}
