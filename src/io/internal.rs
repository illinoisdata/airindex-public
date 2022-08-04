// use lru::LruCache;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::rc::Rc;
use url::Url;

use crate::common::SharedBytes;
use crate::common::SharedByteSlice;
use crate::common::SharedByteView;
use crate::common::error::ConflictingStorageScheme;
use crate::common::error::GResult;
use crate::common::error::UnavailableStorageScheme;
use crate::io::storage::Adaptor;
use crate::io::storage::Range;


/* In-memory cache */

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct PageKey {
  pub url: Url,
  pub page_idx: usize
}

impl std::fmt::Debug for PageKey {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("PageKey")
      .field("url", &self.url.to_string())
      .field("page_idx", &self.page_idx)
      .finish()
  }
}

impl PageKey {
  fn new(url: Url, page_idx: usize) -> PageKey {
    PageKey { url, page_idx }
  }

  fn set_page(&mut self, page_idx: usize) {
    self.page_idx = page_idx
  }
}

struct KeyRef<K> {
  k: *const K,
}

impl<K> KeyRef<K> {
  fn borrow(&self) -> &K {
    unsafe { &*self.k }
  }
}


struct Cache<K, V> {
  total_size: usize,
  pages: BTreeMap<K, V>,
  fifo: VecDeque<KeyRef<K>>,
}

impl<K: Ord, V> Cache<K, V> {
  fn new(total_size: usize) -> Cache<K, V> {
    Cache {
      total_size,
      pages: BTreeMap::new(),
      fifo: VecDeque::with_capacity(total_size),
    }
  }

  fn get(&self, key: &K) -> Option<&V> {
    self.pages.get(key)
  }

  fn contains(&self, key: &K) -> bool {
    self.pages.contains_key(key)
  }

  fn put(&mut self, key: K, value: V) {
    if self.fifo.len() >= self.total_size {
      if let Some(pop_key) = self.fifo.pop_front() {
        self.pages.remove(pop_key.borrow());
      }
    }
    self.fifo.push_back(KeyRef { k: &key });
    self.pages.insert(key, value);
  }

  fn clear(&mut self) {
    self.fifo.clear();
    self.pages.clear();
  }
}


/* Common io interface */

pub struct ExternalStorage {
  adaptors: HashMap<String, Rc<Box<dyn Adaptor>>>,
  schemes: Vec<String>,  // HACK: for error reporting
  // page_cache: RefCell<LruCache<PageKey, SharedByteSlice>>,
  page_cache: RefCell<Cache<PageKey, SharedByteSlice>>,
  page_size: usize,
  total_page: usize,
}

impl std::fmt::Debug for ExternalStorage {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ExternalStorage")
      .field("adaptors", &self.adaptors)
      .field("schemes", &self.schemes)
      .field("page_size", &self.page_size)
      .field("total_page", &self.total_page)
      .finish()
  }
}

impl Default for ExternalStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl ExternalStorage {
  pub fn new() -> ExternalStorage {
    // ExternalStorage::new_with_cache(1 << 33 /* 8 GB */, 1 << 12 /* 1024 */)
    // ExternalStorage::new_with_cache(1 << 33 /* 8 GB */, 1 << 12 /* 2048 */)
    ExternalStorage::new_with_cache(1 << 33 /* 8 GB */, 1 << 12 /* 4096 */)
    // ExternalStorage::new_with_cache(1 << 33 /* 8 GB */, 1 << 13 /* 8192 */)
  }

  pub fn new_with_cache(cache_size: usize, page_size: usize) -> ExternalStorage {
    let total_page = cache_size / page_size;
    ExternalStorage{
      adaptors: HashMap::new(),
      schemes: Vec::new(),
      // page_cache: RefCell::new(LruCache::new(total_page)),
      page_cache: RefCell::new(Cache::new(total_page)),
      page_size,
      total_page,
    }
  }

  pub fn with(mut self, scheme: String, adaptor: Box<dyn Adaptor>) -> GResult<Self> {
    self.register(scheme, adaptor)?;
    Ok(self)
  }

  pub fn register(&mut self, scheme: String, adaptor: Box<dyn Adaptor>) -> GResult<()> {
    if self.adaptors.contains_key(&scheme) {
      // existing scheme
      return Err(ConflictingStorageScheme::boxed(&scheme));
    }

    // new scheme
    self.adaptors.insert(scheme.clone(), Rc::new(adaptor));
    self.schemes.push(scheme);
    Ok(())
  }

  fn select_adaptor(&self, url: &Url) -> GResult<Rc<Box<dyn Adaptor>>> {
    let scheme = url.scheme();
    match self.adaptors.get(scheme) {
      Some(entry) => Ok(entry.clone()),
      None => Err(UnavailableStorageScheme::boxed(scheme.to_string(), self.schemes.clone())),
    }
  }
}

impl ExternalStorage {

  pub fn warm_cache(&self, url: &Url, url_buffer: &SharedByteSlice) {
    self.warm_cache_at(url, url_buffer, 0);
    log::debug!("Warmed up cache for {:?}", url.to_string());
  }

  fn warm_cache_at(&self, url: &Url, buffer: &SharedByteSlice, offset: usize) {
    assert!(url.query().is_none());
    assert_eq!(offset % self.page_size, 0);
    let length = buffer.len();
    let buffer_range = Range { offset, length };
    self.range_to_pages(&buffer_range)
      // .into_par_iter()
      .for_each(|page_idx| {
        let page_key = PageKey::new(url.clone(), page_idx);
        let page_range = self.page_to_range(page_idx);
        let offset_l = page_range.offset - offset;  // underflow if offset not align
        let offset_r = std::cmp::min(length, page_range.offset + page_range.length - offset);
        let page_bytes = buffer.slice(offset_l, offset_r - offset_l);
        self.page_cache.borrow_mut().put(
          page_key,
          page_bytes,
        );
      });
  }

  fn prepare_cache(&self, page_key: &mut PageKey, range: &Range) -> GResult<()> {
    if let Some(missing_range) = self.missing_cache_range(page_key, range) {
      let cache_bytes = self.read_range_raw(page_key, &missing_range)?;
      log::trace!("Read missing cache of length {} bytes", cache_bytes.len());
      self.warm_cache_at(&page_key.url, &cache_bytes, missing_range.offset);
      log::trace!("Warmed up missing cache");
    }
    Ok(())
  }

  fn missing_cache_range(&self, page_key: &mut PageKey, range: &Range) -> Option<Range> {
    let mut missing_iter = self.range_to_pages(range)
      .filter(|page_idx| {
        page_key.set_page(*page_idx);
        self.miss_cache(page_key)
      });
    if let Some(missing_left) = missing_iter.next() {
      let missing_right = match missing_iter.rev().next() {
        Some(missing) => missing,
        None => missing_left,
      };
      let first_range = self.page_to_range(missing_left);
      let last_range = self.page_to_range(missing_right);
      let offset_l = first_range.offset;
      let offset_r = last_range.offset + last_range.length;
      return Some(Range { offset: offset_l, length: offset_r - offset_l })
    }
    None
  }

  fn miss_cache(&self, page_key: &PageKey) -> bool {
    !self.page_cache.borrow_mut().contains(page_key)
  }

  fn read_through_page(&self, page_key: &PageKey) -> GResult<SharedByteSlice> {
    // check in cache
    if let Some(cache_line) = self.page_cache.borrow_mut().get(page_key) {
      // cache hit
      Ok(cache_line.clone())
    } else {
      // cache miss even after prepare (can happen if eviction occurs in between)
      log::warn!("Cache missing after prepare {:?}", page_key);
      self.read_range_raw(
        page_key,
        &Range { offset: page_key.page_idx * self.page_size, length: self.page_size },
      )
    }
  }

  fn read_range_raw(&self, page_key: &PageKey, range: &Range) -> GResult<SharedByteSlice> {
    Ok(self.select_adaptor(&page_key.url)?.read_range(&page_key.url, range)?.slice_all())
  }

  fn range_to_pages(&self, range: &Range) -> std::ops::Range<usize> {
    let last_offset = range.offset + range.length;
    range.offset / self.page_size .. last_offset / self.page_size + (last_offset % self.page_size != 0) as usize
  }

  fn page_to_range(&self, page_idx: usize) -> Range {
    Range { offset: page_idx * self.page_size, length: self.page_size }
  }
}

impl ExternalStorage {
  pub fn read_all(&self, url: &Url) -> GResult<SharedBytes> {
    self.select_adaptor(url)?.read_all(url)
  }

  pub fn read_range(&self, url: &Url, range: &Range) -> GResult<SharedByteView> {
    let mut page_key = PageKey::new(url.clone(), 0);
    if range.length <= self.total_page * self.page_size {
      // warm up cache
      self.prepare_cache(&mut page_key, range)?;
      // tracing::trace!("internal_preparecache");

      // collect page bytes
      let mut view = SharedByteView::default();
      for page_idx in self.range_to_pages(range) {
        page_key.set_page(page_idx);
        let page_cache = self.read_through_page(&page_key)?;
        let page_range = self.page_to_range(page_idx);
        let page_l = range.offset.saturating_sub(page_range.offset);
        let page_r = std::cmp::min(page_cache.len(), (range.offset + range.length).saturating_sub(page_range.offset));
        view.push(page_cache.slice(page_l, page_r - page_l))
      }
      // tracing::trace!("internal_compileview");
      Ok(view)
    } else {
      // range too large for the cache
      self.read_range_raw(&page_key, range).map(SharedByteView::from)
    }
  }

  pub fn create(&self, url: &Url) -> GResult<()> {
    // TODO: use invalidate_entries_if and support_invalidation_closures to invalid some url
    self.page_cache.borrow_mut().clear();
    self.select_adaptor(url)?.create(url)
  }

  pub fn write_all(&self, url: &Url, buf: &[u8]) -> GResult<()> {
    // TODO: use invalidate_entries_if and support_invalidation_closures to invalid some url
    self.page_cache.borrow_mut().clear();
    self.select_adaptor(url)?.write_all(url, buf)
  }

  pub fn remove(&self, url: &Url) -> GResult<()> {
    // TODO: use invalidate_entries_if and support_invalidation_closures to invalid some url
    self.page_cache.borrow_mut().clear();
    self.select_adaptor(url)?.remove(url)
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use itertools::izip;
  use rand::Rng;

  use crate::io::storage::adaptor_test::fsa_resources_setup;
  use crate::io::storage::adaptor_test::fsa_tempdir_setup;
  use crate::io::storage::ReadRequest;
  use crate::io::storage::url_from_dir_path;

  /* generic unit tests */

  pub fn write_all_zero_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
    let test_path = base_url.join("test.bin")?;
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;
    Ok(())
  }

  pub fn write_read_all_zero_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;

    // read and check
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");
    Ok(())
  }

  pub fn write_read_all_random_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(&test_path, &test_data)?;

    // read and check
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");
    Ok(())
  }

  pub fn write_twice_read_all_random_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let test_data_old = [1u8; 256];
    adaptor.write_all(&test_path, &test_data_old)?;

    // write more, this should completely replace previous result
    let test_data_actual = [2u8; 128];
    adaptor.write_all(&test_path, &test_data_actual)?;

    // read and check
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_ne!(&test_data_old[..], &test_data_reread[..], "Old data should be removed");
    assert_eq!(
        &test_data_actual[..],
        &test_data_reread[..],
        "Reread data not matched with original one, possibly containing old data");
    Ok(())
  }

  pub fn write_read_range_random_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(&test_path, &test_data)?;

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      let test_data_range = adaptor.read_range(&test_path, &Range{ offset, length })?;
      let test_data_expected = &test_data[offset..offset+length];
      assert_eq!(test_data_expected, test_data_range.clone_all(), "Reread data not matched with original one"); 
    }
    Ok(())
  }

  pub fn write_read_generic_random_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(&test_path, &test_data)?;

    // read all
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      let test_data_reread = adaptor.read_range(&test_path, &Range{ offset, length })?;
      let test_data_expected = &test_data[offset..offset+length];
      assert_eq!(test_data_expected, test_data_reread.clone_all(), "Reread data not matched with original one"); 
    }
    Ok(())
  }


  /* ExternalStorage tests */

  #[test]
  fn es_write_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_all_zero_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_read_all_zero_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_read_all_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_twice_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_twice_read_all_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_range_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_read_range_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_generic_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_read_generic_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_read_all_ok() -> GResult<()> {
    let (resource_dir, fsa) = fsa_resources_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    let buf = es.read_all(&resource_dir.join("small.txt")?)?;
    let read_string = match std::str::from_utf8(&buf[..]) {
      Ok(v) => v,
      Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    assert_eq!("text for testing", read_string, "Retrieved string mismatched");
    Ok(())
  }

  #[test]
  fn es_read_batch_sequential() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let temp_dir_url = &url_from_dir_path(temp_dir.path())?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;

    // write some data
    let test_path = temp_dir_url.join("test.bin")?;
    let mut test_data = [0u8; 4096];
    rand::thread_rng().fill(&mut test_data[..]);
    es.write_all(&test_path, &test_data)?;

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    let requests: Vec<ReadRequest> = (1..100).map(|_i| {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      ReadRequest::Range { 
          url: test_path.clone(),
          range: Range{ offset, length },
      }
    }).collect();
    let responses = requests.iter()
      .map(|request| match request {
        ReadRequest::Range { url, range } => es.read_range(url, range),
        _ => panic!("Unexpected read request type"),
      })
      .collect::<GResult<Vec<SharedByteView>>>()?;

    // check correctness
    for (request, response) in izip!(&requests, &responses) {
      match request {
        ReadRequest::Range { url: _, range } => {
          let offset = range.offset;
          let length = range.length;
          let test_data_expected = &test_data[offset..offset+length];
          assert_eq!(test_data_expected, response.clone_all(), "Reread data not matched with original one");   
        },
        _ => panic!("This test should only has range requests"),
      };
    }

    Ok(())
  }
}