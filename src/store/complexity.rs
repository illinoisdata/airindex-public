use std::cmp;
use std::time::Duration;

use crate::io::profile::StorageProfile;
use crate::store::key_position::KEY_LENGTH;
use crate::store::key_position::POSITION_LENGTH;


// estimate complexity based on step functions
// FUTURE: may extract trait interface if there is another way to estimate this 
pub struct StepComplexity;
const STEP_SIZE: usize = KEY_LENGTH + POSITION_LENGTH;  // size of a step function in bytes, typically 8 + 8 = 16 bytes
const MAX_LAYERS: usize = 16;  // with 16-byte window, this handles up to 2^64 bytes ~ 18 exabytes of data 

impl StepComplexity {

  // // FUTURE: this is more generic interface... in case if there is more accurate complexity measurement
  // pub fn measure_kps(&self, kps: &KeyPositionCollection) -> Duration {
  //   self.measure(kps.total_bytes())
  // }

  pub fn measure(profile: &dyn StorageProfile, data_size: usize) -> (Vec<usize>, Duration) {
    // assume we can put a step anchor at any position
    // this will underestimate if some key-positions are relatively larger than the rest
    let mut best_loads = vec![data_size];  // no index, download whole
    let mut best_cost = profile.sequential_cost(&best_loads);
    for num_layers in 1..MAX_LAYERS {
      // compression ratio, i.e. size of responsibility window per step function
      let cratio = (data_size as f64).powf(1.0 / (num_layers + 1) as f64)
                   * (STEP_SIZE as f64).powf(num_layers as f64 / (num_layers + 1) as f64);

      // try compress this (to account for ceiling)
      // maybe remove this if insignificant
      let mut current_size = data_size;
      for _layer in 0..num_layers {
        let num_steps = (current_size as f64 / cratio).ceil() as usize;
        current_size = num_steps * STEP_SIZE;
      } 

      // compute cost (fetch whole top layer and loads on intermediate layers)
      let loads = [vec![current_size], vec![cratio as usize; num_layers]].concat();
      let cost = profile.sequential_cost(&loads);
      // log::debug!("L= {}: cratio= {}  -->  loads= {:?}, cost= {:?}  <==>  best_cost= {:?}", num_layers, cratio, loads, cost, best_cost);
      if best_cost > cost {
        best_loads = loads;
        best_cost = cmp::min(best_cost, cost);
      }
    }
    (best_loads, best_cost)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::io::profile::AffineStorageProfile;
  use crate::io::profile::Bandwidth;
  use crate::io::profile::Latency;

  fn assert_measure(result: (Vec<usize>, Duration), expected_loads: Vec<usize>, profile: &Box<dyn StorageProfile>) {
    assert_eq!(result.0, expected_loads);
    assert_eq!(result.1, profile.sequential_cost(&expected_loads));
  }

  #[test]
  fn test_step_measure() {
    let profile = Box::new(AffineStorageProfile::new(
      Latency::from_millis(20),
      Bandwidth::from_mbps(20.0)
    )) as Box<dyn StorageProfile>;
    assert_measure(StepComplexity::measure(profile.as_ref(), 320_000), vec![320_000], &profile);
    assert_measure(StepComplexity::measure(profile.as_ref(), 32_000_000), vec![22_640, 22_627], &profile);
  }
}
