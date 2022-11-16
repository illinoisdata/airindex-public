use std::any::Any;
use std::fmt::Debug;
use std::time::Duration;

pub trait StorageProfile: Sync + Debug {
  // estimate cost for a read of size (read_size in bytes), output in nanoseconds
  fn cost(&self, read_size: usize) -> Duration;

  fn clone_box(&self) -> Box<dyn StorageProfile>;
  fn eq_box(&self, other: &dyn Any) -> bool;

  fn sequential_cost(&self, read_sizes: &[usize]) -> Duration {
    read_sizes.iter().map(|read_size| self.cost(*read_size)).sum()
  }
}


/* Latency (constant) */

pub type Latency = Duration;

impl StorageProfile for Latency {
  fn cost(&self, _read_size: usize) -> Duration {
    *self
  }

  fn clone_box(&self) -> Box<dyn StorageProfile> {
    Box::new(*self)
  }
  fn eq_box(&self, other: &dyn Any) -> bool {
    other.downcast_ref::<Self>().map_or(false, |other| self == other)
  }
}


/* Bandwidth (linear) */

#[derive(PartialEq, Clone, Debug)]
pub struct Bandwidth {
  pub nspb: f64,  // in ns per byte
}

impl Bandwidth {
  pub fn from_mbps(bandwidth_mbps: f64) -> Bandwidth {
    Bandwidth{ nspb: 1e3 / bandwidth_mbps }  // 1e9 / (1e6 * bandwidth_mbps)
  }
}

impl StorageProfile for Bandwidth {
  fn cost(&self, read_size: usize) -> Duration {
    Duration::from_nanos(((read_size as f64) * self.nspb) as u64)
  }

  fn clone_box(&self) -> Box<dyn StorageProfile> {
    Box::new(self.clone())
  }
  fn eq_box(&self, other: &dyn Any) -> bool {
    other.downcast_ref::<Self>().map_or(false, |other| self == other)
  }
}


/* Latency (constant) */

#[derive(PartialEq, Clone, Debug)]
pub struct AffineStorageProfile {
  latency: Duration,
  bandwidth: Bandwidth,
}

impl AffineStorageProfile {
  pub fn new(latency: Latency, bandwidth: Bandwidth) -> AffineStorageProfile {
    AffineStorageProfile{ latency, bandwidth }
  }
}

impl StorageProfile for AffineStorageProfile {
  fn cost(&self, read_size: usize) -> Duration {
    self.latency.cost(read_size) + self.bandwidth.cost(read_size)
  }

  fn clone_box(&self) -> Box<dyn StorageProfile> {
    Box::new(self.clone())
  }
  fn eq_box(&self, other: &dyn Any) -> bool {
    other.downcast_ref::<Self>().map_or(false, |other| self == other)
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  
  #[test]
  fn latency_test() {
    assert_eq!(Latency::from_secs(1).cost(1000), Duration::from_secs(1));
    assert_eq!(Latency::from_millis(1).cost(1000), Duration::from_millis(1));
    assert_eq!(Latency::from_micros(1).cost(1000), Duration::from_micros(1));
    assert_eq!(Latency::from_nanos(1).cost(1000), Duration::from_nanos(1));
  }
  
  #[test]
  fn bandwidth_test() {
    assert_eq!(Bandwidth::from_mbps(1.0).cost(1000000), Duration::from_secs(1));
    assert_eq!(Bandwidth::from_mbps(1.0).cost(1000), Duration::from_millis(1));
    assert_eq!(Bandwidth::from_mbps(1.0).cost(1), Duration::from_micros(1));

    assert_eq!(Bandwidth::from_mbps(100.0).cost(100000000), Duration::from_secs(1));
    assert_eq!(Bandwidth::from_mbps(100.0).cost(100000), Duration::from_millis(1));
    assert_eq!(Bandwidth::from_mbps(100.0).cost(100), Duration::from_micros(1));
  }
  
  #[test]
  fn affine_test() {
    let profile = AffineStorageProfile::new(
      Latency::from_secs(1),
      Bandwidth::from_mbps(1.0)
    );
    assert_eq!(profile.cost(1000000), Duration::from_micros(1000000 + 1000000));
    assert_eq!(profile.cost(1000), Duration::from_micros(1000000 + 1000));
    assert_eq!(profile.cost(1), Duration::from_micros(1000000 + 1));
  }
  
  #[test]
  fn affine_seq_test() {
    let profile = AffineStorageProfile::new(
      Latency::from_secs(1),
      Bandwidth::from_mbps(1.0)
    );
    assert_eq!(profile.sequential_cost(&[1000000, 1000, 1]), Duration::from_micros(4001001));
  }
}