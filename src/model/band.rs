use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
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
use crate::store::key_position::KeyPosition;
use crate::store::key_position::KPDirection;
use crate::store::key_position::KeyPositionRange;
use crate::store::key_position::KeyT;
use crate::store::key_position::POSITION_LENGTH;
use crate::store::key_position::PositionT;


/* Linear lower bound with max load width */

#[derive(Debug)]
pub struct BandModel {
  kp_1: KPDirection,
  kp_2: KPDirection,
  width: PositionT,  // max load, position
}

impl BandModel {
  fn width(&self) -> PositionT {
    self.width
  }
}

impl Model for BandModel {
  fn predict(&self, key: &KeyT) -> KeyPositionRange {
    let left_offset = std::cmp::max(self.kp_1.interpolate_with(&self.kp_2, key), 0) as PositionT;
    let right_offset = left_offset + self.width;
    KeyPositionRange::from_bound(*key, *key, left_offset, right_offset)
  }
}

#[derive(Debug)]
struct AnchoredBand {
  band: BandModel,
  anchor_key: KeyT,
}


/* Linear with max load width on both sides */

#[derive(Debug)]
struct DoubleBandModel {
  kp_1: KPDirection,
  kp_2: KPDirection,
  width_under: PositionT,  // max load
  width_over: PositionT,  // min load
}

impl DoubleBandModel {
  fn new(kp_1: &KeyPosition, kp_2: &KeyPosition) -> DoubleBandModel {
    DoubleBandModel {
      kp_1: KPDirection::from_kp(kp_1),
      kp_2: KPDirection::from_kp(kp_2),
      width_under: 0,
      width_over: 0,
    }
  } 

  fn update(&mut self, kp: &KeyPosition) {
    // shift kps down every update?
    let predict_offset = self.kp_1.interpolate_with(&self.kp_2, &kp.key);
    let deviation = kp.position as i64 - predict_offset as i64;
    if deviation > 0 {
      // underestimate
      self.width_under = std::cmp::max(self.width_under, deviation as PositionT);
    } else {
      // overestimate
      self.width_over = std::cmp::max(self.width_over, (-deviation) as PositionT);
    }
  }

  fn into_band(self) -> BandModel {
    // shift anchor points down and adjust the width
    BandModel {
      kp_1: self.kp_1.subtract_y(self.width_over),
      kp_2: self.kp_2.subtract_y(self.width_over),
      width: self.width_under + self.width_over,
    }
  }

  fn width(&self) -> PositionT {
    self.width_under + self.width_over
  }
}


/* Convex hull capturing all given points */

// check whether the angle at 2 on [1 --> 2 --> 3] is convex
fn is_convex(kp_1: &KeyPosition, kp_2: &KeyPosition, kp_3: &KeyPosition) -> bool {
  KPDirection::from_pair(kp_1, kp_2).is_lower_than(&KPDirection::from_pair(kp_2, kp_3))
}

fn is_concave(kp_1: &KeyPosition, kp_2: &KeyPosition, kp_3: &KeyPosition) -> bool {
  KPDirection::from_pair(kp_2, kp_3).is_lower_than(&KPDirection::from_pair(kp_1, kp_2))
}

fn find_critical_lower(kpd: &KPDirection, kps: &[KeyPosition]) -> usize {
  // binary search for slope_L < kpd_slope <= slope_R
  // assuming slopes are increasing in order of kps
  let n = kps.len();
  assert!(n > 0);
  if n == 1 {
    0
  } else {
    let mid = (n - 1) / 2;
    let cur_kpd = KPDirection::from_pair(&kps[mid], &kps[mid + 1]);
    if cur_kpd.is_lower_than(kpd) {
      find_critical_lower(kpd, &kps[mid+1..]) + mid + 1
    } else {
      find_critical_lower(kpd, &kps[..mid+1])
    }
  } 
}

fn find_critical_upper(kpd: &KPDirection, kps: &[KeyPosition]) -> usize {
  // binary search for slope_L > kpd_slope >= slope_R
  // assuming slopes are decreasing in order of kps
  let n = kps.len();
  assert!(n > 0);
  if n == 1 {
    0
  } else {
    let mid = (n - 1) / 2;
    let cur_kpd = KPDirection::from_pair(&kps[mid], &kps[mid + 1]);
    if kpd.is_lower_than(&cur_kpd) {
      find_critical_upper(kpd, &kps[mid+1..]) + mid + 1
    } else {
      find_critical_upper(kpd, &kps[..mid+1])
    }
  } 
}

// create band line (from endpoints in lower_kps) and test its width on covered points (point_kps)
fn pick_one_band_from(lower_kps: &[KeyPosition], upper_kps: &[KeyPosition]) -> Option<BandModel> {
  if lower_kps.len() <= 1 || upper_kps.is_empty() {
    None
  } else {
    // use only endpoints: fast, but inaccurate
    let mut double_band = DoubleBandModel::new(&lower_kps[0], &lower_kps[lower_kps.len() - 1]);
    let kpd = KPDirection::from_pair(&lower_kps[0], &lower_kps[lower_kps.len() - 1]);
    let lower_crit_idx = find_critical_lower(&kpd, lower_kps);
    let upper_crit_idx = find_critical_upper(&kpd, upper_kps);
    assert!(lower_crit_idx == 0 || KPDirection::from_pair(&lower_kps[lower_crit_idx - 1], &lower_kps[lower_crit_idx]).is_lower_than(&kpd), "{:?}, {:?}", kpd, lower_kps);
    assert!(lower_crit_idx == lower_kps.len() - 1 || !KPDirection::from_pair(&lower_kps[lower_crit_idx], &lower_kps[lower_crit_idx + 1]).is_lower_than(&kpd), "{:?}, {:?}", kpd, lower_kps);
    assert!(upper_crit_idx == 0 || kpd.is_lower_than(&KPDirection::from_pair(&upper_kps[upper_crit_idx - 1], &upper_kps[upper_crit_idx])), "{:?}, {:?}", kpd, upper_kps);
    assert!(upper_crit_idx == upper_kps.len() - 1 || !kpd.is_lower_than(&KPDirection::from_pair(&upper_kps[upper_crit_idx], &upper_kps[upper_crit_idx + 1])), "{:?}, {:?}", kpd, upper_kps);
    double_band.update(&lower_kps[lower_crit_idx]);
    double_band.update(&upper_kps[upper_crit_idx]);
    if lower_crit_idx < lower_kps.len() - 1 {
      double_band.update(&lower_kps[lower_crit_idx + 1]);
    }
    if upper_crit_idx < upper_kps.len() - 1 {
      double_band.update(&upper_kps[upper_crit_idx + 1]);
    }
    Some(double_band.into_band())
  }
}

// create band line (from endpoints in lower_kps) and test its width on covered points (point_kps)
fn pick_best_band_from(lower_kps: &[KeyPosition], upper_kps: &[KeyPosition]) -> Option<BandModel> {
  if lower_kps.is_empty() || upper_kps.is_empty() {
    None
  } else {
    let mut best_double_band: Option<DoubleBandModel> = None;

    // try create from lower
    for idx in 0 .. lower_kps.len() - 1 {
      let mut double_band = DoubleBandModel::new(&lower_kps[idx], &lower_kps[idx + 1]);
      let kpd = KPDirection::from_pair(&lower_kps[idx], &lower_kps[idx + 1]);
      let upper_crit_idx = find_critical_upper(&kpd, upper_kps);
      assert!(upper_crit_idx == 0 || kpd.is_lower_than(&KPDirection::from_pair(&upper_kps[upper_crit_idx - 1], &upper_kps[upper_crit_idx])), "{:?}, {:?}", kpd, upper_kps);
      assert!(upper_crit_idx == upper_kps.len() - 1 || !kpd.is_lower_than(&KPDirection::from_pair(&upper_kps[upper_crit_idx], &upper_kps[upper_crit_idx + 1])), "{:?}, {:?}", kpd, upper_kps);
      double_band.update(&lower_kps[idx]);
      double_band.update(&upper_kps[upper_crit_idx]);
      if idx < lower_kps.len() - 1 {
        double_band.update(&lower_kps[idx + 1]);
      }
      if upper_crit_idx < upper_kps.len() - 1 {
        double_band.update(&upper_kps[upper_crit_idx + 1]);
      }

      // pick best
      best_double_band = match best_double_band {
        Some(best_db) => if best_db.width() <= double_band.width() {
          Some(best_db)
        } else {
          Some(double_band)
        },
        None => Some(double_band),
      };
    }

    // try create from upper
    for idx in 0 .. upper_kps.len() - 1 {
      let mut double_band = DoubleBandModel::new(&upper_kps[idx], &upper_kps[idx + 1]);
      let kpd = KPDirection::from_pair(&upper_kps[idx], &upper_kps[idx + 1]);
      let lower_crit_idx = find_critical_lower(&kpd, lower_kps);
      assert!(lower_crit_idx == 0 || KPDirection::from_pair(&lower_kps[lower_crit_idx - 1], &lower_kps[lower_crit_idx]).is_lower_than(&kpd), "{:?}, {:?}", kpd, lower_kps);
      assert!(lower_crit_idx == lower_kps.len() - 1 || !KPDirection::from_pair(&lower_kps[lower_crit_idx], &lower_kps[lower_crit_idx + 1]).is_lower_than(&kpd), "{:?}, {:?}", kpd, lower_kps);
      double_band.update(&lower_kps[lower_crit_idx]);
      double_band.update(&upper_kps[idx]);
      if lower_crit_idx < lower_kps.len() - 1 {
        double_band.update(&lower_kps[lower_crit_idx + 1]);
      }
      if idx < upper_kps.len() - 1 {
        double_band.update(&upper_kps[idx + 1]);
      }

      // pick best
      best_double_band = match best_double_band {
        Some(best_db) => if best_db.width() <= double_band.width() {
          Some(best_db)
        } else {
          Some(double_band)
        },
        None => Some(double_band),
      };
    }

    best_double_band.map(|db| db.into_band())
  }
}

#[derive(Debug)]
struct ConvexHull {
  lower_kps: Vec<KeyPosition>,  // convex lower curve
  upper_kps: Vec<KeyPosition>,  // concave upper curve
}

impl ConvexHull {
  pub fn new() -> ConvexHull {
    ConvexHull {
      lower_kps: Vec::new(),
      upper_kps: Vec::new(),
    }
  }

  pub fn is_empty(&self) -> bool {
    self.lower_kps.is_empty() && self.upper_kps.is_empty()
  }

  // create linear model 
  pub fn make_band(&self) -> Option<AnchoredBand> {
    assert_eq!(self.lower_kps[0], self.upper_kps[0], "Convex hull should align on its left end");
    pick_one_band_from(&self.lower_kps, &self.upper_kps).map(|band| AnchoredBand { 
      band,
      anchor_key: self.lower_kps[0].key,
    })
  }

  // create linear model 
  pub fn make_best_band(&self) -> Option<AnchoredBand> {
    assert_eq!(self.lower_kps[0], self.upper_kps[0], "Convex hull should align on its left end");
    pick_best_band_from(&self.lower_kps, &self.upper_kps).map(|band| AnchoredBand { 
      band,
      anchor_key: self.lower_kps[0].key,
    })
  }

  pub fn lowest_offset(&self) -> PositionT {
    assert_eq!(self.lower_kps.first(), self.upper_kps.first(), "Convex hull should align on its left end");
    self.lower_kps.first().unwrap().position
  }

  fn push_right_lower(&mut self, kp: KeyPosition) {
    // pop violating segment from back to front
    if !self.lower_kps.is_empty() {
      assert!(self.lower_kps[self.lower_kps.len() - 1].key <= kp.key);
    }
    while self.lower_kps.len() >= 2 {
      let n = self.lower_kps.len();
      if !is_convex(&self.lower_kps[n - 2], &self.lower_kps[n - 1], &kp) {
        self.lower_kps.pop();
      } else {
        break;
      }
    }
    self.lower_kps.push(kp);
  }

  fn push_right_upper(&mut self, kp: KeyPosition) {
    // pop violating segment from back to front
    if !self.upper_kps.is_empty() {
      assert!(self.upper_kps[self.upper_kps.len() - 1].key <= kp.key);
    }
    while self.upper_kps.len() >= 2 {
      let n = self.upper_kps.len();
      if !is_concave(&self.upper_kps[n - 2], &self.upper_kps[n - 1], &kp) {
        self.upper_kps.pop();
      } else {
        break;
      }
    }
    self.upper_kps.push(kp);
  }
}


/* Serialization */

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BandModelRecon {
  load: LoadDistribution,
}

impl BandModelRecon {
  fn new() -> BandModelRecon {
    BandModelRecon { load: LoadDistribution::default() }
  }

  fn sketch(&mut self, bm: &BandModel, num_samples: usize) -> io::Result<Vec<u8>> {
    // update load distribution
    self.load.add(bm.width() as f64, num_samples.try_into().unwrap());

    // turn the model into a buffer
    let mut model_buffer = vec![];
    model_buffer.write_u64::<BigEndian>(bm.kp_1.x.try_into().unwrap())?;
    model_buffer.write_i64::<BigEndian>(bm.kp_1.y.try_into().unwrap())?;
    model_buffer.write_u64::<BigEndian>(bm.kp_2.x.try_into().unwrap())?;
    model_buffer.write_i64::<BigEndian>(bm.kp_2.y.try_into().unwrap())?;
    model_buffer.write_uint::<BigEndian>(bm.width as u64, POSITION_LENGTH)?;
    Ok(model_buffer)  // expect 5 * 8 = 40 bytes
  }

  fn reconstruct_raw(&self, buffer: &[u8]) -> GResult<BandModel> {
    let mut model_buffer = io::Cursor::new(buffer);
    Ok(BandModel {
      kp_1: KPDirection {
        x: model_buffer.read_u64::<BigEndian>()?.into(),
        y: model_buffer.read_i64::<BigEndian>()?.into(),
      },
      kp_2: KPDirection {
        x: model_buffer.read_u64::<BigEndian>()?.into(),
        y: model_buffer.read_i64::<BigEndian>()?.into(),
      },
      width: model_buffer.read_uint::<BigEndian>(POSITION_LENGTH)? as PositionT,
    })
  }
}

pub type BandModelReconMeta = BandModelRecon;

impl ModelRecon for BandModelRecon {
  fn reconstruct(&self, buffer: &[u8]) -> GResult<Box<dyn Model>> {
    let model = self.reconstruct_raw(buffer)?;
    Ok(Box::new(model))
  }

  fn get_load(&self) -> Vec<LoadDistribution> {
    vec![self.load.clone()]
  }

  fn combine_with(&mut self, other: &dyn ModelRecon) {
    match other.to_typed() {
      ModelReconMeta::Band { meta } => {
        self.load.extend(&meta.load);
      },
      _ => panic!("Cannot combine StepModelRecon with this {:?}", other),
    }
  }

  fn to_typed(&self) -> ModelReconMeta {
    ModelReconMeta::Band { meta: Box::new(self.clone()) }
  }
}

impl ModelReconMetaserde for BandModelRecon {  // for Metaserde
  fn to_meta(&self, _ctx: &mut Context) -> GResult<ModelReconMeta> {
    Ok(ModelReconMeta::Band { meta: Box::new(self.clone()) })
  }
}

impl BandModelRecon {  // for Metaserde
  pub fn from_meta(meta: BandModelReconMeta, _ctx: &Context) -> GResult<BandModelRecon> {
    Ok(meta)
  }
}

/* Builder */

pub struct BandConvexHullGreedyBuilder {
  max_load: PositionT,
  serde: BandModelRecon,
  hull: ConvexHull,
  feasible_band: Option<AnchoredBand>,
  current_samples: usize,
}

impl std::fmt::Debug for BandConvexHullGreedyBuilder {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("BandConvexHullGB")
      .field("max_load", &self.max_load)
      .finish()
  }
}

impl BandConvexHullGreedyBuilder {
  pub fn new(max_load: PositionT) -> BandConvexHullGreedyBuilder {
    BandConvexHullGreedyBuilder {
      max_load,
      serde: BandModelRecon::new(),
      hull: ConvexHull::new(),
      feasible_band: None,
      current_samples: 0,
    }
  }

  fn push_to_hull(&mut self, kpr: &KeyPositionRange) {
    self.hull.push_right_lower(KeyPosition { key: kpr.key_l, position: kpr.offset });
    self.hull.push_right_lower(KeyPosition { key: kpr.key_r, position: kpr.offset });
    self.hull.push_right_lower(KeyPosition { key: kpr.key_r, position: kpr.offset + kpr.length });
    self.hull.push_right_upper(KeyPosition { key: kpr.key_l, position: kpr.offset });
    self.hull.push_right_upper(KeyPosition { key: kpr.key_l, position: kpr.offset + kpr.length });
    self.hull.push_right_upper(KeyPosition { key: kpr.key_r, position: kpr.offset + kpr.length });
  }

  fn start_hull_with(&mut self, kpr: &KeyPositionRange) {
    // log::info!("Hull size at full {} / {}", self.hull.lower_kps.len(), self.hull.upper_kps.len());
    self.hull = ConvexHull::new();
    self.push_to_hull(kpr);
    let new_band = self.hull.make_band()
      .expect("Convex hull should produce a band after adding a kpr"); 
    self.feasible_band = Some(new_band);
    self.current_samples = 1;
  }

  fn continue_hull_with(&mut self, band: AnchoredBand) {
    self.feasible_band = Some(band);
    self.current_samples += 1;
  }

  fn consume_produce_feasible(&mut self, kpr: &KeyPositionRange) -> Option<(AnchoredBand, usize)> {
    // try adding points to convex hull and get a band model
    self.push_to_hull(kpr);
    let current_band = self.hull.make_band()
      .expect("Convex hull should produce a band after adding a kpr");

    // check whether hull is full
    if current_band.band.width() > self.max_load {
      match self.feasible_band.take() {
        Some(the_feasible_band) => {
          // adding this kpr breaks the hull capacity
          // repack kpr to the new hull and ship previously feasible band
          let num_samples = self.current_samples;
          self.start_hull_with(kpr);
          Some((the_feasible_band, num_samples))
        },
        None => {
          // this hapeens when the only kpr is too large to fit in max_load
          self.continue_hull_with(current_band);
          None  // not writing this yet...
        }
      }
    } else {
      // band's width is still within max_load
      self.continue_hull_with(current_band);
      None
    }
  }

  fn generate_segment(&mut self, band: AnchoredBand, num_samples: usize) -> GResult<MaybeKeyBuffer> {
    let band_buffer = self.serde.sketch(&band.band, num_samples)?;
    Ok(Some(KeyBuffer::new(band.anchor_key, band_buffer)))
  }
}

impl ModelBuilder for BandConvexHullGreedyBuilder {
  fn consume(&mut self, kpr: &KeyPositionRange) -> GResult<MaybeKeyBuffer> {
    if let Some((band, num_samples)) = self.consume_produce_feasible(kpr) {
      self.generate_segment(band, num_samples)
    } else {
      Ok(None)
    }
  }

  fn finalize(mut self: Box<Self>) -> GResult<BuilderFinalReport> {
    // make last band if needed
    let maybe_last_kb = if let Some(band) = self.hull.make_band() {
      self.generate_segment(band, self.current_samples)?
    } else {
      None
    };
    Ok(BuilderFinalReport {
      maybe_model_kb: maybe_last_kb,
      serde: Box::new(self.serde),
    })
  }
}

impl BandConvexHullGreedyBuilder {
  fn drafter(max_load: usize) -> Box<dyn ModelDrafter> {
    let bm_producer = Box::new(
      move || {
        Box::new(BandConvexHullGreedyBuilder::new(max_load)) as Box<dyn ModelBuilder>
      });
    Box::new(BuilderAsDrafter::wrap(bm_producer))
  }
}


/* Build with bounded offset range */

pub struct BandConvexHullEqualBuilder {
  max_range: PositionT,
  serde: BandModelRecon,
  hull: ConvexHull,
  current_samples: usize,
}

impl std::fmt::Debug for BandConvexHullEqualBuilder {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("BandConvexHullEB")
      .field("max_range", &self.max_range)
      .finish()
  }
}

impl BandConvexHullEqualBuilder {
  pub fn new(max_range: PositionT) -> BandConvexHullEqualBuilder {
    BandConvexHullEqualBuilder {
      max_range,
      serde: BandModelRecon::new(),
      hull: ConvexHull::new(),
      current_samples: 0,
    }
  }

  fn push_to_hull(&mut self, kpr: &KeyPositionRange) {
    self.hull.push_right_lower(KeyPosition { key: kpr.key_l, position: kpr.offset });
    self.hull.push_right_lower(KeyPosition { key: kpr.key_r, position: kpr.offset });
    self.hull.push_right_lower(KeyPosition { key: kpr.key_r, position: kpr.offset + kpr.length });
    self.hull.push_right_upper(KeyPosition { key: kpr.key_l, position: kpr.offset });
    self.hull.push_right_upper(KeyPosition { key: kpr.key_l, position: kpr.offset + kpr.length });
    self.hull.push_right_upper(KeyPosition { key: kpr.key_r, position: kpr.offset + kpr.length });
    self.current_samples += 1;
  }

  fn consume_produce_feasible(&mut self, kpr: &KeyPositionRange) -> Option<(AnchoredBand, usize)> {
    // check whether adding next point is valid
    if self.hull.is_empty() || kpr.offset + kpr.length - self.hull.lowest_offset() <= self.max_range {
      // add this next point
      self.push_to_hull(kpr);
      None
    } else {
      let band = self.hull.make_best_band().unwrap();
      let band_samples = self.current_samples;
      self.hull = ConvexHull::new();
      self.current_samples = 0;
      self.push_to_hull(kpr);
      Some((band, band_samples))
    }
  }

  fn generate_segment(&mut self, band: AnchoredBand, num_samples: usize) -> GResult<MaybeKeyBuffer> {
    let band_buffer = self.serde.sketch(&band.band, num_samples)?;
    Ok(Some(KeyBuffer::new(band.anchor_key, band_buffer)))
  }
}

impl ModelBuilder for BandConvexHullEqualBuilder {
  fn consume(&mut self, kpr: &KeyPositionRange) -> GResult<MaybeKeyBuffer> {
    if let Some((band, num_samples)) = self.consume_produce_feasible(kpr) {
      self.generate_segment(band, num_samples)
    } else {
      Ok(None)
    }
  }

  fn finalize(mut self: Box<Self>) -> GResult<BuilderFinalReport> {
    // make last band if needed
    let maybe_last_kb = if let Some(band) = self.hull.make_best_band() {
      self.generate_segment(band, self.current_samples)?
    } else {
      None
    };
    Ok(BuilderFinalReport {
      maybe_model_kb: maybe_last_kb,
      serde: Box::new(self.serde),
    })
  }
}

impl BandConvexHullEqualBuilder {
  fn drafter(max_range: usize) -> Box<dyn ModelDrafter> {
    let bm_producer = Box::new(
      move || {
        Box::new(BandConvexHullEqualBuilder::new(max_range)) as Box<dyn ModelBuilder>
      });
    Box::new(BuilderAsDrafter::wrap(bm_producer))
  }
}


/* Drafter */

pub struct BandMultipleDrafter;

impl BandMultipleDrafter {
  pub fn greedy_exp(low_load: PositionT, high_load: PositionT, exponent: f64) -> MultipleDrafter {
    let mut bm_drafters = Vec::new();
    let mut current_load = low_load;
    while current_load < high_load {
      bm_drafters.push(BandConvexHullGreedyBuilder::drafter(current_load));
      current_load = ((current_load as f64) * exponent) as PositionT;
    }
    bm_drafters.push(BandConvexHullGreedyBuilder::drafter(high_load));
    MultipleDrafter::from(bm_drafters)
  }

  pub fn equal_exp(low_load: PositionT, high_load: PositionT, exponent: f64) -> MultipleDrafter {
    let mut bm_drafters = Vec::new();
    let mut current_load = low_load;
    while current_load < high_load {
      bm_drafters.push(BandConvexHullEqualBuilder::drafter(current_load));
      current_load = ((current_load as f64) * exponent) as PositionT;
    }
    bm_drafters.push(BandConvexHullEqualBuilder::drafter(high_load));
    MultipleDrafter::from(bm_drafters)
  }
}


/* Tests */

#[cfg(test)]
mod tests {
  use super::*;

  use crate::common::SharedByteSlice;


  fn test_same_model(model_1: &BandModel, model_2: &BandModel) {
    assert_eq!(model_1.kp_1, model_2.kp_1);
    assert_eq!(model_1.kp_2, model_2.kp_2);
    assert_eq!(model_1.width, model_2.width);
  }
  
  #[test]
  fn serde_test() -> GResult<()> {
    let mut bm_serde = BandModelRecon::new();
    let bm = Box::new(BandModel {
      kp_1: KPDirection { x: 0, y: 0 },
      kp_2: KPDirection { x: 105, y: 30 },
      width: 123,
    });

    // sketch this model
    let bm_buffer = bm_serde.sketch(&bm, 1  /* num_samples */)?;
    assert!(bm_buffer.len() > 0);

    // reconstruct
    let bm_recon = bm_serde.reconstruct_raw(&bm_buffer)?;
    test_same_model(&bm_recon, &bm);

    Ok(())
  }

  fn test_same_model_box(model_1: &Box<dyn Model>, model_2: &Box<BandModel>, key_left: KeyT, key_right: KeyT) {
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
  fn greedy_test() -> GResult<()> {
    let kprs = generate_test_kprs();
    let mut bm_builder = Box::new(BandConvexHullGreedyBuilder::new(40));

    // start adding points
    let _model_kb_0 = assert_none_buffer(bm_builder.consume(&kprs[0])?);
    let _model_kb_1 = assert_none_buffer(bm_builder.consume(&kprs[1])?);
    let _model_kb_2 = assert_none_buffer(bm_builder.consume(&kprs[2])?);
    let model_kb_3 = assert_some_buffer(bm_builder.consume(&kprs[3])?);
    let _model_kb_4 = assert_none_buffer(bm_builder.consume(&kprs[4])?);
    let _model_kb_5 = assert_none_buffer(bm_builder.consume(&kprs[5])?);
    let model_kb_6 = assert_some_buffer(bm_builder.consume(&kprs[6])?);
    let model_kb_7 = assert_some_buffer(bm_builder.consume(&kprs[7])?);

    // finalize the builder
    let BuilderFinalReport {
      maybe_model_kb: last_buffer,
      serde: bm_serde,
    } = bm_builder.finalize()?;
    let model_kb_8 = assert_some_buffer(last_buffer);
    let max_load: Vec<usize> = bm_serde.get_load().iter().map(|load| load.max()).collect();
    assert_eq!(max_load, vec![915]);

    // check buffers
    test_same_model_box(
      &bm_serde.reconstruct(&model_kb_3[..])?,
      &Box::new(BandModel {
        kp_1: KPDirection { x: 0, y: -20 },
        kp_2: KPDirection { x: 100, y: 10 },
        width: 27,
      }),
      0,
      101,
    );
    test_same_model_box(
      &bm_serde.reconstruct(&model_kb_6[..])?,
      &Box::new(BandModel {
        kp_1: KPDirection { x: 105, y: 10 },
        kp_2: KPDirection { x: 115, y: 70 },
        width: 40,
      }),
      105,
      116,
    );
    test_same_model_box(
      &bm_serde.reconstruct(&model_kb_7[..])?,
      &Box::new(BandModel {
        kp_1: KPDirection { x: 120, y: 90 },
        kp_2: KPDirection { x: 120, y: 1000 },
        width: 910,
      }),
      120,
      121,
    );
    test_same_model_box(
      &bm_serde.reconstruct(&model_kb_8[..])?,
      &Box::new(BandModel {
        kp_1: KPDirection { x: 131, y: 1000 },
        kp_2: KPDirection { x: 131, y: 1915 },
        width: 915,
      }),
      121,
      132,
    );
    Ok(())
  }
  
  #[test]
  fn greedy_with_error_test() -> GResult<()> {
    let kprs = generate_test_kprs();
    let mut bm_builder = Box::new(BandConvexHullGreedyBuilder::new(1500));

    // start adding points
    let _model_kb_0 = assert_none_buffer(bm_builder.consume(&kprs[0])?);
    let _model_kb_1 = assert_none_buffer(bm_builder.consume(&kprs[1])?);
    let _model_kb_2 = assert_none_buffer(bm_builder.consume(&kprs[2])?);
    let _model_kb_3 = assert_none_buffer(bm_builder.consume(&kprs[3])?);
    let _model_kb_4 = assert_none_buffer(bm_builder.consume(&kprs[4])?);
    let _model_kb_5 = assert_none_buffer(bm_builder.consume(&kprs[5])?);
    let _model_kb_6 = assert_none_buffer(bm_builder.consume(&kprs[6])?);
    let model_kb_7 = assert_some_buffer(bm_builder.consume(&kprs[7])?);

    // finalize the builder
    let BuilderFinalReport {
      maybe_model_kb: last_buffer,
      serde: bm_serde,
    } = bm_builder.finalize()?;
    let model_kb_8 = assert_some_buffer(last_buffer);
    let max_load: Vec<usize> = bm_serde.get_load().iter().map(|load| load.max()).collect();
    assert_eq!(max_load, vec![917]);

    // check buffers
    test_same_model_box(
      &bm_serde.reconstruct(&model_kb_7[..])?,
      &Box::new(BandModel {
        kp_1: KPDirection { x: 0, y: -910 },
        kp_2: KPDirection { x: 120, y: 90 },
        width: 917,
      }),
      0,
      121,
    );
    test_same_model_box(
      &bm_serde.reconstruct(&model_kb_8[..])?,
      &Box::new(BandModel {
        kp_1: KPDirection { x: 131, y: 1000 },
        kp_2: KPDirection { x: 131, y: 1915 },
        width: 915,
      }),
      131,
      132,
    );
    Ok(())
  }
}
