use bitvec::prelude::*;


pub type Interval = (usize, usize);

pub struct Intervals {
  flags: BitVec,
}

impl Intervals {
  pub fn empty(size: usize) -> Intervals {
    Intervals {
      flags: BitVec::<usize, Lsb0>::repeat(false, size),
    }
  }

  pub fn full(size: usize) -> Intervals {
    Intervals {
      flags: BitVec::<usize, Lsb0>::repeat(true, size),
    }
  }

  pub fn missing(&self, interval: &Interval) -> Option<Interval> {
    let slice = &self.flags[interval.0 .. interval.1];
    slice.first_zero()
      .map(|missing_left| {
        let missing_right = slice.last_zero().unwrap() + 1;
        (missing_left + interval.0, missing_right + interval.0)
      })
  }

  pub fn fill(&mut self, interval: &Interval) {
    self.flags.get_mut(interval.0 .. interval.1).unwrap().fill(true);
  }
}