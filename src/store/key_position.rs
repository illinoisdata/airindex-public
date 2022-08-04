use std::cmp;
use std::cmp::Ordering;
use std::ops::Index;
use std::ops::Sub;


/* Key-position */

pub type KeyT = u64;
pub type PositionT = usize;
pub const KEY_LENGTH: usize = std::mem::size_of::<KeyT>();
pub const POSITION_LENGTH: usize = std::mem::size_of::<PositionT>();

#[derive(Clone, PartialEq, Debug)]
pub struct KeyPosition {
  pub key: KeyT,  // TODO: generic Num + PartialOrd type
  pub position: PositionT,
}

#[derive(PartialEq, Debug)]
pub struct KPDirection {
  pub x: i128,
  pub y: i128,
}

impl KPDirection {
  pub fn from_pair(kp_1: &KeyPosition, kp_2: &KeyPosition) -> KPDirection {
    KPDirection {
      x: kp_2.key as i128 - kp_1.key as i128,
      y: kp_2.position as i128 - kp_1.position as i128,
    }
  }

  pub fn from_kp(kp: &KeyPosition) -> KPDirection {
    KPDirection {
      x: kp.key as i128,
      y: kp.position as i128,
    }
  }

  pub fn subtract_y(mut self, position: PositionT) -> KPDirection {
    self.y -= position as i128;
    self
  }

  pub fn is_lower_than(&self, other: &KPDirection) -> bool {
    // need to square the calculation space to multiple
    self.y * other.x < self.x * other.y
  }

  pub fn interpolate_with(&self, other: &KPDirection, key: &KeyT) -> i128 {
    if self.x == other.x {
      self.y
    } else {
      // need to square the calculation space to multiple
      self.y + (((*key as i128 - self.x) * (other.y - self.y)) / (other.x - self.x))
    }
  }
}

impl KeyPosition {
  pub fn interpolate_with(&self, other: &KeyPosition, key: &KeyT) -> PositionT {
    if self.key == other.key {
      self.position
    } else {
      self.position + (
        ((*key as f64 - self.key as f64)
        / (other.key as f64 - self.key as f64))
        * (other.position as f64 - self.position as f64)
      ).floor() as PositionT 
    }
  }

  pub fn is_lower_slope_than(&self, other: &KeyPosition, pov: &KeyPosition) -> bool {
    KPDirection::from_pair(self, pov).is_lower_than(&KPDirection::from_pair(other, pov))
  }
}

impl<'a, 'b> Sub<&'b KeyPosition> for &'a KeyPosition {
    type Output = KeyPosition;

    fn sub(self, other: &'b KeyPosition) -> Self::Output {
        KeyPosition {
            key: self.key - other.key,
            position: self.position - other.position,
        }
    }
}


/* Key-position-length */

#[derive(Clone, Debug, PartialEq)]
pub struct KeyPositionRange {
  pub key_l: KeyT,
  pub key_r: KeyT,
  pub offset: PositionT,
  pub length: PositionT,
}

impl KeyPositionRange {
  pub fn from_bound(key_l: KeyT, key_r: KeyT, left_offset: PositionT, right_offset: PositionT) -> KeyPositionRange {
    KeyPositionRange {
      key_l,
      key_r,
      offset: left_offset,
      length: right_offset.saturating_sub(left_offset),
    }
  }
}


/* Key interval */

#[derive(Debug, PartialEq)]
pub struct KeyInterval {
  pub left_key: KeyT,
  pub right_key: KeyT,
}

impl KeyInterval {
  pub fn greater_than(&self, key: &KeyT) -> bool {
    *key < self.left_key
  }

  pub fn less_than(&self, key: &KeyT) -> bool {
    self.right_key < *key
  }

  pub fn cover(&self, key: &KeyT) -> bool {
    self.left_key <= *key && *key <= self.right_key
  }

  pub fn intersect(&self, other: &KeyInterval) -> KeyInterval {
    // "empty" interval represented by criss-crossing boundary keys
    KeyInterval {
      left_key: cmp::max(self.left_key, other.left_key),
      right_key: cmp::min(self.right_key, other.right_key),
    }
  }
}


/* Key-position Collections */

pub struct KeyPositionCollection {
  kps: Vec<KeyPosition>,
  // start_key: KeyT,
  end_key: KeyT,
  start_position: PositionT,
  end_position: PositionT,
}

impl Default for KeyPositionCollection {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyPositionCollection {
  pub fn new() -> KeyPositionCollection {
    KeyPositionCollection{
      kps: Vec::new(),
      // start_key: 0,
      end_key: 0,
      start_position: 0,
      end_position: 0,
    }
  }

  pub fn push(&mut self, key: KeyT, position: PositionT) {
    // self.start_key = cmp::min(self.start_key, key);
    self.end_key = cmp::max(self.end_key, key);
    self.kps.push(KeyPosition{ key, position })
  }

  pub fn set_position_range(&mut self, start_position: PositionT, end_position: PositionT) {
    self.start_position = start_position;
    self.end_position = end_position;
  }

  pub fn len(&self) -> usize {
    self.kps.len()
  } 

  pub fn total_bytes(&self) -> usize {
    self.end_position - self.start_position
  }

  pub fn is_empty(&self) -> bool {
    self.kps.is_empty()
  }

  pub fn whole_range(&self) -> (PositionT, PositionT) {
    (self.start_position, self.end_position)
  }

  pub fn position_for(&self, key: KeyT) -> Result<usize, &str> {
    for kp in &self.kps {
      if kp.key == key {
        return Ok(kp.position);
      }
    }
    Err("Key not contained in this key-position collection")
  }

  pub fn range_at(&self, idx: usize) -> Result<KeyPositionRange, &str> {
    match idx.cmp(&(self.len() - 1)) {
      Ordering::Less => Ok(KeyPositionRange{
        key_l: self.kps[idx].key,
        key_r: self.kps[idx+1].key,
        offset: self.kps[idx].position,
        length: self.kps[idx+1].position - self.kps[idx].position,
      }),
      Ordering::Equal => Ok(KeyPositionRange{
        key_l: self.kps[idx].key,
        key_r: self.end_key,
        offset: self.kps[idx].position,
        length: self.end_position - self.kps[idx].position,
      }),
      Ordering::Greater => Err("Index out of range"),
    }
  }

  pub fn iter(&self) -> std::slice::Iter<KeyPosition> {
    self.kps.iter()
  }

  pub fn range_iter(&self) -> KeyPositionRangeIterator {
    KeyPositionRangeIterator::new(self)
  }

  pub fn range_slice_iter(&self, range: std::ops::Range<usize>) -> KeyPositionRangeIterator {
    KeyPositionRangeIterator::slice(self, range.start, range.end)
  }

  pub fn chunk_iter(&self, chunk_size: usize) -> Vec<KeyPositionRangeIterator> {
    let num_chunks = self.len() / chunk_size + (self.len() % chunk_size != 0) as usize;
    (0 .. num_chunks).map(|chunk_idx| {
      self.range_slice_iter(chunk_idx * chunk_size .. (chunk_idx + 1) * chunk_size)
    }).collect()
  }
}

impl Index<usize> for KeyPositionCollection {
  type Output = KeyPosition;
  fn index(&self, idx: usize) -> &Self::Output {
    &self.kps[idx]
  }
}


/* Range iterator */
pub struct KeyPositionRangeIterator<'a> {
  kps: &'a KeyPositionCollection,
  current_idx: usize,
  upper_idx: usize,
}

impl<'a> KeyPositionRangeIterator<'a> {
  fn new(kps: &'a KeyPositionCollection) -> KeyPositionRangeIterator {
    KeyPositionRangeIterator::slice(kps, 0, kps.len())
  }

  fn slice(kps: &'a KeyPositionCollection, start: usize, end: usize) -> KeyPositionRangeIterator {
    KeyPositionRangeIterator{
      kps,
      current_idx: start,
      upper_idx: end,
    }
  }
}

impl<'a> Iterator for KeyPositionRangeIterator<'a> {
  type Item = KeyPositionRange;

  fn next(&mut self) -> Option<Self::Item> {
    if self.current_idx < self.upper_idx {
      match self.kps.range_at(self.current_idx) {
        Ok(kr) => {
          self.current_idx += 1;
          Some(kr)
        },
        Err(_) => None,
      } 
    } else {
      None
    }
  }
}