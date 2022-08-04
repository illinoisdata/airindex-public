use serde::{Serialize, Deserialize};


#[derive(Default, Serialize, Deserialize, Clone)]
pub struct LoadDistribution {
  load_counts: [u64; 32],  // counts of keys whose load <= 2^(idx+1), last bracket > 2^28
  total_counts: u64,
  max_load: usize,
}

impl std::fmt::Debug for LoadDistribution {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("LD")
      // .field("total_counts", &self.total_counts)
      // .field("p25", &self.percentile(25.0))
      .field("p50", &self.percentile(50.0))
      // .field("p75", &self.percentile(75.0))
      .field("p90", &self.percentile(90.0))
      // .field("p99", &self.percentile(99.0))
      .field("max", &self.max())
      .field("average", &self.average())
      // .field("histogram", &format!("{:?}", &self.load_counts))
      .finish()
  }
}

impl LoadDistribution {

  // deterministic distribution of load
  pub fn exact(load: usize) -> LoadDistribution {
    let mut ld = LoadDistribution::default();
    ld.add(load as f64, 1);
    ld
  }

  // deterministic distribution of many loads
  pub fn exacts(loads: Vec<usize>) -> Vec<LoadDistribution> {
    loads.into_iter().map(LoadDistribution::exact).collect()
  }

  pub fn add(&mut self, load: f64, count: u64) {
    let bracket: usize = if load <= 1.0 {
      0
    } else {
      std::cmp::min((load - 1.0).log2() as usize + 1, 31)
    };
    self.load_counts[bracket] += count;
    self.total_counts += count;
    self.max_load = std::cmp::max(self.max_load, load as usize);
  }

  pub fn extend(&mut self, other: &LoadDistribution) {
    for idx in 0 .. self.load_counts.len() {
      self.load_counts[idx] += other.load_counts[idx];
    }
    self.total_counts += other.total_counts;
    self.max_load = std::cmp::max(self.max_load, other.max_load);
  }

  pub fn average(&self) -> f64 {
    let mut avg = 0.0;
    let mut mul = 1.0;
    for idx in 0 .. self.load_counts.len() - 1 {
      avg += mul * (self.load_counts[idx] as f64) / (self.total_counts as f64);
      mul *= 2.0;
    }
    avg += (self.max_load as f64) * (*self.load_counts.last().unwrap() as f64) / (self.total_counts as f64);
    avg
  }

  pub fn percentile(&self, p: f64) -> usize {
    assert!((0.0..=100.0).contains(&p));
    if self.total_counts == 1 {
      // useful for deterministic (exact)
      return self.max_load
    }
    let mut acc_mass = 0;
    let mut mul = 1;
    for idx in 0 .. self.load_counts.len() - 1 {
      acc_mass += self.load_counts[idx];
      if (acc_mass as f64) / (self.total_counts as f64) * 100.0 >= p {
        return mul;
      }
      mul *= 2;
    }
    self.max_load
  }

  pub fn max(&self) -> usize {
    self.max_load
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_average() {
    let mut ld = LoadDistribution::default();
    ld.add(1.0, 1);
    ld.add(2.0, 8);
    ld.add(16.0, 1);
    assert!((ld.average() - (1.0 + 2.0 * 8.0 + 16.0) / 10.0).abs() < 1e-4);
  }

  #[test]
  fn test_percentiles() {
    let mut ld = LoadDistribution::default();
    ld.add(2.0, 1);
    ld.add(4.0, 1);
    ld.add(8.0, 1);
    ld.add(16.0, 1);
    ld.add(32.0, 1);
    ld.add(64.0, 1);
    ld.add(128.0, 1);
    ld.add(256.0, 1);
    ld.add(512.0, 1);
    ld.add(1024.0, 1);

    // percentiles
    assert_eq!(ld.percentile(10.0), 2);
    assert_eq!(ld.percentile(20.0), 4);
    assert_eq!(ld.percentile(30.0), 8);
    assert_eq!(ld.percentile(40.0), 16);
    assert_eq!(ld.percentile(50.0), 32);
    assert_eq!(ld.percentile(60.0), 64);
    assert_eq!(ld.percentile(70.0), 128);
    assert_eq!(ld.percentile(80.0), 256);
    assert_eq!(ld.percentile(90.0), 512);
    assert_eq!(ld.percentile(100.0), 1024);

    // max
    assert_eq!(ld.max(), 1024);
  }
}