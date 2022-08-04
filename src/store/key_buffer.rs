use std::fmt;

use crate::common::SharedBytes;
use crate::common::SharedByteSlice;
use crate::store::key_position::KEY_LENGTH;
use crate::store::key_position::KeyT;


/* key-value struct */

// const KEY_LENGTH: usize = std::mem::size_of::<KeyT>();
pub struct KeyBuffer {
  pub key: KeyT,  // TODO: generic Num + PartialOrd type
  pub buffer: SharedByteSlice,  // TODO: copy-on-write?
}

impl fmt::Debug for KeyBuffer {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("KeyBuffer")
      .field("key", &self.key)
      .field("buffer_bytes", &self.buffer.len())
      .finish()
  }
}

impl KeyBuffer {  // maybe implement in Serializer, Deserializer instead?
  pub fn new(key: KeyT, buffer: Vec<u8>) -> KeyBuffer {
    KeyBuffer {
      key,
      buffer: SharedBytes::from(buffer).slice_all(),
    }
  }

  pub fn serialize(&self) -> Vec<u8> {
    // TODO: return reference by concat slices
    let mut serialized_buffer = Vec::with_capacity(KEY_LENGTH + self.buffer.len());
    serialized_buffer.extend_from_slice(&self.key.to_le_bytes());
    serialized_buffer.extend_from_slice(&self.buffer[..]);
    serialized_buffer
  }

  pub fn deserialize(serialized_buffer: Vec<u8>) -> KeyBuffer {
    let buffer_length = serialized_buffer.len() - KEY_LENGTH;
    KeyBuffer {
      key: KeyT::from_le_bytes(serialized_buffer[..KEY_LENGTH].try_into().unwrap()),
      buffer: SharedBytes::from(serialized_buffer).slice(KEY_LENGTH, buffer_length),
    }
  }

  pub fn deserialize_key(serialized_buffer: [u8; KEY_LENGTH]) -> KeyT {
    KeyT::from_le_bytes(serialized_buffer)
  }

  pub fn serialized_size(&self) -> usize {
    KEY_LENGTH + self.buffer.len()
  }
}
