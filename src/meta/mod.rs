use serde::{Serialize, Deserialize};
use std::cell::RefCell;
use std::rc::Rc;
use url::Url;

use crate::common::error::GResult;
use crate::io::internal::ExternalStorage;


pub struct Context {
  pub storage: Option<Rc<RefCell<ExternalStorage>>>,
  pub store_prefix: Option<Url>,
}

impl std::fmt::Debug for Context {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let store_prefix_string = match &self.store_prefix {
      Some(url) => url.to_string(),
      None => "None".to_string(),
    };
    f.debug_struct("Context")
      .field("store_prefix", &store_prefix_string)
      .finish()
  }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
  pub fn new() -> Context {
    Context {
      storage: None,
      store_prefix: None,
    }
  }

  pub fn put_storage(&mut self, storage: &Rc<RefCell<ExternalStorage>>) {
    if let Some(storage) = &self.storage {
      // if exists, check same object
      assert!(Rc::ptr_eq(storage, storage));
    } else {
      // if not, update
      self.storage = Some(Rc::clone(storage));
    }
  }

  pub fn put_store_prefix(&mut self, store_prefix: &Url) {
    if let Some(current_store_prefix) = &self.store_prefix {
      // if exists, check same object
      assert_eq!(store_prefix, current_store_prefix);
    } else {
      // if not, update
      self.store_prefix = Some(store_prefix.clone());
    }
  }
}


// // default serializer, for convenience (JSON)
// pub fn serialize<T: Serialize>(meta: &T) -> GResult<Vec<u8>> {
//   Ok(serde_json::to_vec(meta)?)
// }

// pub fn deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> GResult<T> {
//   Ok(serde_json::from_slice(bytes)?)
// }


// // default serializer, for convenience (BSON)
// pub fn serialize<T: Serialize>(meta: &T) -> GResult<Vec<u8>> {
//   Ok(bson::to_vec(meta)?)
// }

// pub fn deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> GResult<T> {
//   Ok(bson::from_slice(bytes)?)
// }


// default serializer, for convenience (Postcard)
pub fn serialize<T: Serialize>(meta: &T) -> GResult<Vec<u8>> {
  Ok(postcard::to_stdvec(meta)?)
}

pub fn deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> GResult<T> {
  Ok(postcard::from_bytes(bytes)?)
}


/* Serializable to Metadata */

// TODO: make proper trait?
// pub trait Metaserde {
//   fn to_meta(&self: Self, ctx: &mut Context) -> GResult<Deserializable>;
//   fn from_meta(meta: &Deserializable, ctx: &Context) -> GResult<Self>;
// }