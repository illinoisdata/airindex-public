use std::cell::RefCell;
use std::rc::Rc;
use url::Url;

use crate::io::internal::ExternalStorage;
use crate::store::array_store::ArrayStore;
use crate::store::block_store::BlockStore;
use crate::store::DataStore;
use crate::store::KeyBuffer;


pub struct StoreDesigner {
  storage: Rc<RefCell<ExternalStorage>>,
}

impl StoreDesigner {
  pub fn new(storage: &Rc<RefCell<ExternalStorage>>) -> StoreDesigner {
    StoreDesigner { storage: Rc::clone(storage) }
  }

  pub fn design_for_kbs(&self, key_buffers: &[KeyBuffer], prefix_url: Url, store_name: String) -> Box<dyn DataStore> {
    match StoreDesigner::data_size_if_sized(key_buffers) {
      Some(data_size) => {
        log::trace!("Using ArrayStore with data_size= {}", data_size);
        Box::new(ArrayStore::new_sized(
          &self.storage,
          prefix_url,
          store_name,
          data_size,
        ))
      },
      None => {
        let page_size = 36;
        log::trace!("Using BlockStore with page_size= {}", page_size);
        Box::new(BlockStore::builder(store_name)
          .page_size(page_size)  // TODO: pick better page size?
          .build(&self.storage, prefix_url))
      },
    }
  }

  fn data_size_if_sized(key_buffers: &[KeyBuffer]) -> Option<usize> {
    assert!(!key_buffers.is_empty(), "Expect non-empty key-buffers");
    let data_size = key_buffers[0].serialized_size();
    for key_buffer in key_buffers {
      if key_buffer.serialized_size() != data_size {
        return None;
      }
    }
    Some(data_size)
  }
}