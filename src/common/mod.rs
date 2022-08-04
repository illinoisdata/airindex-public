use serde::{Serialize, Deserialize};
use std::ops::Index;
use std::slice::Chunks;
use std::sync::Arc;

/*
 * Structures around byte array
 *   SharedBytes: shared immutable contiguous byte array
 *   SharedByteSlice: shared immutable contiguous byte slice
 *   SharedByteView: shared immutable possibly-non-contiguous byte slice
 */

#[derive(Serialize, Deserialize)]
pub struct SharedBytes {
  buffer: Arc<Vec<u8>>,
}

impl SharedBytes {
  pub fn len(&self) -> usize {
    self.buffer.len()
  }

  pub fn is_empty(&self) -> bool {
    self.buffer.is_empty()
  }

  pub fn chunks(&self, chunk_size: usize) -> Chunks<'_, u8> {
    self.buffer.chunks(chunk_size)
  }

  pub fn slice(&self, offset: usize, length: usize) -> SharedByteSlice {
    SharedByteSlice {
      buffer: Arc::clone(&self.buffer),
      offset,
      length,
    }
  }

  pub fn slice_all(&self) -> SharedByteSlice {
    SharedByteSlice {
      buffer: Arc::clone(&self.buffer),
      offset: 0,
      length: self.len(),
    }
  }
}

impl Clone for SharedBytes {
  fn clone(&self) -> Self {
    SharedBytes { buffer: Arc::clone(&self.buffer) }
  }
}

impl<Idx: std::slice::SliceIndex<[u8]>> Index<Idx> for SharedBytes {
  type Output = Idx::Output;

  fn index(&self, index: Idx) -> &Self::Output {
    &self.buffer[index]
  }
}

impl From<Arc<Vec<u8>>> for SharedBytes {
  fn from(buffer: Arc<Vec<u8>>) -> Self {
    SharedBytes { buffer }
  }
}

impl From<Vec<u8>> for SharedBytes {
  fn from(buffer: Vec<u8>) -> Self {
    SharedBytes { buffer: Arc::new(buffer) }
  }
}


/* Slice of one shared bytes */

#[derive(Clone)]
pub struct SharedByteSlice {
  buffer: Arc<Vec<u8>>,
  offset: usize,
  length: usize,
}

impl SharedByteSlice {
  pub fn len(&self) -> usize {
    self.length
  }

  pub fn is_empty(&self) -> bool {
    self.length == 0
  }

  pub fn slice(&self, offset: usize, length: usize) -> SharedByteSlice {
    assert!(offset + length <= self.length);
    SharedByteSlice {
      buffer: Arc::clone(&self.buffer),
      offset: self.offset + offset,
      length,
    }
  }
}

impl Index<std::ops::Range<usize>> for SharedByteSlice {
  type Output = [u8];

  fn index(&self, range: std::ops::Range<usize>) -> &Self::Output {
    assert!(range.end - range.start <= self.length);
    let new_range = std::ops::Range {
      start: range.start + self.offset,
      end: range.end + self.offset
    };
    &self.buffer[new_range]
  }
}

impl Index<std::ops::RangeFull> for SharedByteSlice {
  type Output = [u8];

  fn index(&self, _range: std::ops::RangeFull) -> &Self::Output {
    &self.buffer[self.offset .. self.offset + self.length]
  }
}


/* Contiguous view of non-continuous slices */

#[derive(Default)]
pub struct SharedByteView {  
  slices: Vec<SharedByteSlice>,
  acc_lengths: Vec<usize>,
  total_length: usize, 
}

impl SharedByteView {
  pub fn len(&self) -> usize {
    self.total_length
  }

  pub fn is_empty(&self) -> bool {
    self.total_length == 0
  }

  pub fn push(&mut self, slice: SharedByteSlice) {
    self.total_length += slice.len();
    self.slices.push(slice);
    self.acc_lengths.push(self.total_length);
  }

  pub fn clone_within(&self, range: std::ops::Range<usize>) -> Vec<u8> {
    assert!(range.start < self.total_length && range.end <= self.total_length);

    // find first relevant slice
    let mut slice_idx = self.acc_lengths.binary_search(&range.start).unwrap_or_else(|idx| idx);
    let mut slice_offset = self.acc_lengths[slice_idx] - self.slices[slice_idx].len();

    // copy relevant part(s)
    let length = range.end - range.start;
    let mut buffer = Vec::with_capacity(length);
    while slice_offset < range.end {
      let shift_offset = range.start.saturating_sub(slice_offset);
      let part_length = std::cmp::min(
        self.slices[slice_idx].len() - shift_offset,
        length - buffer.len()
      );
      let part_slice = &self.slices[slice_idx][shift_offset .. shift_offset + part_length];
      buffer.extend_from_slice(part_slice);
      slice_offset += self.slices[slice_idx].len();
      slice_idx += 1;
    }
    buffer
  }

  pub fn clone_all(&self) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(self.total_length);
    for slice in &self.slices {
      buffer.extend_from_slice(&slice[..]);
    }
    buffer
  }
}

impl From<Vec<SharedByteSlice>> for SharedByteView {
  fn from(slices: Vec<SharedByteSlice>) -> Self {
    let mut view = SharedByteView::default();
    for slice in slices {
      view.push(slice)
    }
    view
  }
}

impl From<SharedBytes> for SharedByteView {
  fn from(buffer: SharedBytes) -> Self {
    SharedByteView::from(vec![buffer.slice_all()])
  }
}

impl From<SharedByteSlice> for SharedByteView {
  fn from(buffer: SharedByteSlice) -> Self {
    SharedByteView::from(vec![buffer])
  }
}

pub mod error;
