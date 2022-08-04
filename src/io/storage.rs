use azure_core::prelude::Range as AzureRange;
use azure_storage::core::prelude::StorageAccountClient;
use azure_storage_blobs::prelude::AsBlobClient;
use azure_storage_blobs::prelude::AsContainerClient;
use azure_storage_blobs::prelude::BlobClient;
use bytes::Bytes;
use itertools::Itertools;
use memmap2::Mmap;
use memmap2::MmapOptions;
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use tokio::runtime::Runtime;
use url::Url;

use crate::common::SharedBytes;
use crate::common::error::GenericError;
use crate::common::error::GResult;
use crate::common::error::InvalidAzureStorageUrl;
use crate::common::error::MissingAzureAuthetication;
use crate::common::error::OpenUrlError;
use crate::common::error::UrlParseFilePathError;

/* Data structs */

#[derive(Debug)]
pub struct Range {
  pub offset: usize,
  pub length: usize,
}

pub enum ReadRequest {
  All {
    url: Url,
  },
  Range {
    url: Url,
    range: Range,
  },
}

/* Adaptor */

pub trait Adaptor: std::fmt::Debug {
  // read whole blob specified in path
  fn read_all(&self, url: &Url) -> GResult<SharedBytes>;
  // read range starting at offset for length bytes
  fn read_range(&self, url: &Url, range: &Range) -> GResult<SharedBytes>;
  // read range starting at offset for length bytes
  fn read_in_place(&self, url: &Url, range: &Range, buffer: &mut [u8]) -> GResult<()>;
  // generic read for supported request type
  fn read(&self, request: &ReadRequest) -> GResult<SharedBytes> {
    match request {
      ReadRequest::All { url } => self.read_all(url),
      ReadRequest::Range { url, range } => self.read_range(url, range),
    }
  }

  // create empty file at url
  fn create(&self, url: &Url) -> GResult<()>;
  // write whole byte array to blob
  fn write_all(&self, url: &Url, buf: &[u8]) -> GResult<()>;
  // write whole byte array to blob
  fn remove(&self, url: &Url) -> GResult<()>;
}


/* File system */

fn open_rfile(url: &Url) -> GResult<File> {
  assert!(url.scheme() == "file" || url.scheme() == "mmap");
  match OpenOptions::new().read(true).open(url.path()) {
    Ok(file) => {
      // let suffix = url.path_segments().unwrap().last().unwrap_or("");
      // tracing::trace!("storage_openfile_{}", suffix);
      Ok(file)
    },
    Err(e) => Err(OpenUrlError::boxed(url.to_string(), e.to_string())),
  }
}

#[derive(Debug)]
pub struct FileSystemAdaptor {
  rfile_dict: Rc<RefCell<HashMap<Url, Rc<RefCell<File>>>>>,
}

impl Default for FileSystemAdaptor {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystemAdaptor {
  pub fn new() -> FileSystemAdaptor {
    FileSystemAdaptor { rfile_dict: Rc::new(RefCell::new(HashMap::new())) }
  }

  fn read_range_from_file(f: &File, range: &Range, buf: &mut [u8], trace_suffix: &str) -> GResult<()> {
    // File::read_at might return fewer bytes than requested (e.g. 2GB at a time)
    // To read whole range, we request until the buffer is filled
    assert_eq!(buf.len(), range.length);
    let mut buf_offset = 0;
    while buf_offset < range.length {
      let read_bytes = f.read_at(&mut buf[buf_offset..], (buf_offset + range.offset).try_into().unwrap())?; 
      buf_offset += read_bytes;
      if read_bytes == 0 {
        // try to read more beyond file size, return the truncated buffer
        log::debug!("Stopped filling buffer of {} bytes with only {} bytes", range.length, buf_offset);
        break;
      }
    }
    tracing::trace!("storage_readrange_{}", trace_suffix);
    Ok(())
  }

  fn create_directory(&self, path: &Path) -> GResult<()> {
    Ok(std::fs::create_dir_all(path)?)
  }

  fn open(&self, url: &Url) -> GResult<Rc<RefCell<File>>> {
    // this is or_insert_with_key with fallible insertion
    Ok(match self.rfile_dict.borrow_mut().entry(url.clone()) {
      Entry::Occupied(entry) => entry.get().clone(),
      Entry::Vacant(entry) => entry.insert(Rc::new(RefCell::new(open_rfile(url)?))).clone(),
    })
  }
}

impl Adaptor for FileSystemAdaptor {
  fn read_all(&self, url: &Url) -> GResult<SharedBytes> {
    let f = self.open(url)?;
    let mut buffer = Vec::new();
    f.borrow_mut().read_to_end(&mut buffer)?;
    Ok(SharedBytes::from(buffer))
  }

  fn read_range(&self, url: &Url, range: &Range) -> GResult<SharedBytes> {
    self.open(url).map(|f| {
      let mut buffer = vec![0u8; range.length];
      FileSystemAdaptor::read_range_from_file(
        &f.borrow(),
        range,
        &mut buffer,
        url.path_segments().unwrap().last().unwrap_or(""),
      ).map(|_| SharedBytes::from(buffer))
    })?
  }

  fn read_in_place(&self, url: &Url, range: &Range, buffer: &mut [u8]) -> GResult<()> {
    self.open(url).map(|f| {
      FileSystemAdaptor::read_range_from_file(
        &f.borrow(),
        range,
        buffer,
        url.path_segments().unwrap().last().unwrap_or(""),
      )
    })?
  }

  fn create(&self, url: &Url) -> GResult<()> {
    assert!(url.scheme() == "file" || url.scheme() == "mmap");
    std::fs::File::create(url.path())?;
    Ok(())
  }

  fn write_all(&self, url: &Url, buf: &[u8]) -> GResult<()> {
    assert!(url.scheme() == "file" || url.scheme() == "mmap");
    let url_path = url.path();
    self.create_directory(PathBuf::from(url_path).parent().unwrap())?;
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(url_path)?;
    Ok(f.write_all(buf.as_ref())?)
  }

  fn remove(&self, url: &Url) -> GResult<()> {
    assert!(url.scheme() == "file" || url.scheme() == "mmap");
    std::fs::remove_file(Path::new(url.path()))?;
    Ok(())
  }
}

// pub fn url_from_file_path(path: &Path) -> GResult<Url> {
//    url_from_file_str(path.to_str().expect("Unable to stringify path"))
// }

// pub fn url_from_file_str(path: &str) -> GResult<Url> {
//    Url::from_file_path(path).map_err(|_| Box::new(UrlParseFilePathError) as GenericError)
// }

pub fn url_from_dir_path(path: &Path) -> GResult<Url> {
   url_from_dir_str(path.to_str().expect("Unable to stringify path"))
}

pub fn url_from_dir_str(path: &str) -> GResult<Url> {
   Url::from_directory_path(path).map_err(|_| Box::new(UrlParseFilePathError) as GenericError)
}

/* File system adaptor with mmap as cache/buffer pool layer */

#[derive(Debug)]
pub struct MmapAdaptor {
  mmap_dict: Rc<RefCell<HashMap<Url, Rc<Mmap>>>>,
  fs_adaptor: FileSystemAdaptor,
}

fn new_mmap(url: &Url) -> GResult<Mmap> {
  assert_eq!(url.scheme(), "mmap");
  let file = File::open(url.path())?;
  let mmap = unsafe {
    MmapOptions::new()
      // .populate()
      .map(&file)?
  };
  log::debug!("Mmaped {:?}", url.to_string());
  Ok(mmap)
}

impl Default for MmapAdaptor {
    fn default() -> Self {
        Self::new()
    }
}

impl MmapAdaptor {
  pub fn new() -> MmapAdaptor {
    MmapAdaptor {
      mmap_dict: Rc::new(RefCell::new(HashMap::new())),
      fs_adaptor: FileSystemAdaptor::new(),
    }
  }

  fn map(&self, url: &Url) -> GResult<Rc<Mmap>> {
    // this is or_insert_with_key with fallible insertion
    Ok(match self.mmap_dict.borrow_mut().entry(url.clone()) {
      Entry::Occupied(entry) => entry.get().clone(),
      Entry::Vacant(entry) => entry.insert(Rc::new(new_mmap(url)?)).clone(),
    })
  }

  fn try_map(&self, url: &Url) -> Option<Rc<Mmap>> {
    match self.map(url) {
      Ok(mmap) => Some(mmap),  // TODO: avoid copy?
      Err(e) => {
        log::warn!("MmapAdaptor failed to mmap {:?} with {}", url, e);
        None
      }
    }
  }

  fn unmap(&self, url: &Url) -> GResult<()> {
    self.mmap_dict.borrow_mut().remove(url);
    Ok(())
  }
}

impl Adaptor for MmapAdaptor {
  fn read_all(&self, url: &Url) -> GResult<SharedBytes> {
    match self.try_map(url) {
      Some(mmap) => Ok(SharedBytes::from(mmap.to_vec())),  // TODO: avoid copy?
      None => self.fs_adaptor.read_all(url),
    }
  }

  fn read_range(&self, url: &Url, range: &Range) -> GResult<SharedBytes> {
    match self.try_map(url) {
      Some(mmap) => {
        let offset_r = std::cmp::min(mmap.len(), range.offset+range.length);
        Ok(SharedBytes::from(mmap[range.offset..offset_r].to_vec()))  // TODO: avoid copy?
      }
      None => self.fs_adaptor.read_range(url, range),
    }
  }

  fn read_in_place(&self, url: &Url, range: &Range, buffer: &mut [u8]) -> GResult<()> {
    assert_eq!(buffer.len(), range.length);
    match self.try_map(url) {
      Some(mmap) => {
        let offset_r = std::cmp::min(mmap.len(), range.offset+range.length);
        buffer.clone_from_slice(&mmap[range.offset..offset_r]);
        Ok(())
      }
      None => self.fs_adaptor.read_in_place(url, range, buffer),
    }
  }

  fn create(&self, url: &Url) -> GResult<()> {
    self.unmap(url)?;
    self.fs_adaptor.create(url)
  }

  fn write_all(&self, url: &Url, buf: &[u8]) -> GResult<()> {
    self.unmap(url)?;
    self.fs_adaptor.write_all(url, buf)
  }

  fn remove(&self, url: &Url) -> GResult<()> {
    self.unmap(url)?;
    self.fs_adaptor.remove(url)
  }
}


/* Azure storage adaptor (per storage account/key) */

// https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
#[derive(Deserialize, Debug)]
pub enum AzureBlobType {  // control only at blob creation time
  BlockBlob,  // fast read/write large block(s) of data
  AppendBlob,  // fast append
  PageBlob,  // fast random read/write, basis of azure virtual disk
}

pub struct AzureStorageAdaptor {
  storage_client: Arc<StorageAccountClient>,
  blob_type: AzureBlobType,

  rt: Runtime,  // TODO: move out? static/global variable?
}

impl std::fmt::Debug for AzureStorageAdaptor {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("AzureStorageAdaptor")
      .field("blob_type", &self.blob_type)
      .finish()
  }
}

impl AzureStorageAdaptor {
  pub fn new_block() -> GResult<AzureStorageAdaptor> {
    AzureStorageAdaptor::new(AzureBlobType::BlockBlob)
  }

  pub fn new_append() -> GResult<AzureStorageAdaptor> {
    AzureStorageAdaptor::new(AzureBlobType::AppendBlob)
  }

  pub fn new_page() -> GResult<AzureStorageAdaptor> {
    AzureStorageAdaptor::new(AzureBlobType::PageBlob)
  }

  fn new(blob_type: AzureBlobType) -> GResult<AzureStorageAdaptor> {
    // TODO: static storage account client?
    let account = std::env::var("AZURE_STORAGE_ACCOUNT")
      .map_err(|_| MissingAzureAuthetication::boxed("Set env variable AZURE_STORAGE_ACCOUNT"))?;
    let key = std::env::var("AZURE_STORAGE_KEY")
      .map_err(|_| MissingAzureAuthetication::boxed("Set env variable AZURE_STORAGE_KEY first!"))?;
    let http_client = azure_core::new_http_client();
    let storage_client = StorageAccountClient::new_access_key(http_client, &account, &key);
    Ok(AzureStorageAdaptor {
      storage_client,
      blob_type,
      rt: Runtime::new().expect("Failed to initialize tokio runtim"),
    })
  }

  fn parse_url(&self, url: &Url) -> GResult<(String, String)> {  // container name, blob path
    let mut path_segments = url.path_segments().ok_or_else(|| InvalidAzureStorageUrl::new("Failed to segment url"))?;
    let container = path_segments.next().ok_or_else(|| InvalidAzureStorageUrl::new("Require container name"))?;
    let blob_path = Itertools::intersperse(path_segments, "/").collect();
    Ok((container.to_string(), blob_path))
  }

  fn blob_client(&self, url: &Url) -> GResult<Arc<BlobClient>> {
    let (container_name, blob_name) = self.parse_url(url)?;
    Ok(self.storage_client.as_container_client(container_name).as_blob_client(&blob_name))
  }

  async fn read_all_async(&self, url: &Url) -> GResult<SharedBytes> {
    let blob_response = self.blob_client(url)?
      .get()
      .execute()
      .await?;
    Ok(SharedBytes::from(blob_response.data.to_vec()))
  }

  async fn read_range_async(&self, url: &Url, range: &Range) -> GResult<SharedBytes> {
    let blob_response = self.blob_client(url)?
      .get()
      .range(AzureRange::new(range.offset.try_into().unwrap(), (range.offset + range.length).try_into().unwrap()))
      .execute()
      .await?;
    Ok(SharedBytes::from(blob_response.data.to_vec()))
  }

  async fn write_all_async(&self, url: &Url, buf: &[u8]) -> GResult<()> {
    let blob_client = self.blob_client(url)?;
    match &self.blob_type {
      AzureBlobType::BlockBlob => {
        // TODO: avoid copy?
        let response = blob_client.put_block_blob(Bytes::copy_from_slice(buf)).execute().await?;
        log::debug!("{:?}", response);
        Ok(())
      }
      AzureBlobType::AppendBlob => {
        let response = blob_client.put_append_blob().execute().await?;
        log::debug!("{:?}", response);
        todo!()  // TODO: best way to write to append blob?
      }
      AzureBlobType::PageBlob => {
        let response = blob_client.put_page_blob(buf.len().try_into().unwrap()).execute().await?;
        log::debug!("{:?}", response);
        todo!()  // TODO: write in 512-byte pages
      }
    }
  }

  async fn remove_async(&self, url: &Url) -> GResult<()> {
    self.blob_client(url)?
      .delete()
      .execute()
      .await?;
    Ok(())
  }
}

impl Adaptor for AzureStorageAdaptor {
  fn read_all(&self, url: &Url) -> GResult<SharedBytes> {
    self.rt.block_on(self.read_all_async(url))
  }

  fn read_range(&self, url: &Url, range: &Range) -> GResult<SharedBytes> {
    self.rt.block_on(self.read_range_async(url, range))
  }

  fn read_in_place(&self, url: &Url, range: &Range, buffer: &mut [u8]) -> GResult<()> {
    let read_bytes = self.rt.block_on(self.read_range_async(url, range))?;
    buffer.clone_from_slice(&read_bytes[..]);
    Ok(())
  }

  fn create(&self, _url: &Url) -> GResult<()> {
    Ok(())  // do nothing, azure blob creates hierarchy on blob creation
  }

  fn write_all(&self, url: &Url, buf: &[u8]) -> GResult<()> {
    self.rt.block_on(self.write_all_async(url, buf))
  }

  fn remove(&self, url: &Url) -> GResult<()> {
    self.rt.block_on(self.remove_async(url))
  }
}


/* Dummy adaptor with no-op */

#[derive(Default, Debug)]
pub struct DummyAdaptor;

impl Adaptor for DummyAdaptor {
  fn read_all(&self, _url: &Url) -> GResult<SharedBytes> {
    Ok(SharedBytes::from(Vec::new()))
  }

  fn read_range(&self, _url: &Url, _range: &Range) -> GResult<SharedBytes> {
    Ok(SharedBytes::from(Vec::new()))
  }

  fn read_in_place(&self, _url: &Url, _range: &Range, _buffer: &mut [u8]) -> GResult<()> {
    Ok(())
  }

  fn create(&self, _url: &Url) -> GResult<()> {
    Ok(())
  }

  fn write_all(&self, _url: &Url, _buf: &[u8]) -> GResult<()> {
    Ok(())
  }

  fn remove(&self, _url: &Url) -> GResult<()> {
    Ok(())
  }
}


#[cfg(test)]
pub mod adaptor_test {
  use super::*;
  use rand::Rng;
  use rand;
  use tempfile::TempDir;

  /* generic Adaptor unit tests */

  pub fn write_all_zero_ok(adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
    let test_path = base_url.join("test.bin")?;
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;
    Ok(())
  }

  pub fn write_all_inside_dir_ok(adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
    let test_path = base_url.join("test_dir/test.bin")?;
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;
    Ok(())
  }

  pub fn write_read_all_zero_ok(adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;

    // read and check
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");
    Ok(())
  }

  pub fn write_read_all_random_ok(adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
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

  pub fn write_twice_read_all_random_ok(adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
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

  pub fn write_read_range_random_ok(adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
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
      assert_eq!(test_data_expected, &test_data_range[..], "Reread data not matched with original one"); 
    }
    Ok(())
  }

  pub fn write_read_generic_random_ok(adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(&test_path, &test_data)?;

    // read all
    let test_data_reread = adaptor.read(&ReadRequest::All { url: test_path.clone() })?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      let test_data_reread = adaptor.read(&ReadRequest::Range { 
          url: test_path.clone(),
          range: Range{ offset, length },
      })?;
      let test_data_expected = &test_data[offset..offset+length];
      assert_eq!(test_data_expected, &test_data_reread[..], "Reread data not matched with original one"); 
    }
    Ok(())
  }

  pub fn fsa_resources_setup() -> GResult<(Url, FileSystemAdaptor)> {
    let resource_dir = url_from_dir_str(env!("CARGO_MANIFEST_DIR"))?.join("resources/test/")?;
    Ok((resource_dir, FileSystemAdaptor::new()))
  }

  pub fn fsa_tempdir_setup() -> GResult<(TempDir, FileSystemAdaptor)> {
    let temp_dir = TempDir::new()?;
    let mfsa = FileSystemAdaptor::new();
    Ok((temp_dir, mfsa))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::TempDir;

  use crate::io::storage::adaptor_test::fsa_resources_setup;
  use crate::io::storage::adaptor_test::fsa_tempdir_setup;
  use crate::io::storage::adaptor_test::write_all_inside_dir_ok;
  use crate::io::storage::adaptor_test::write_all_zero_ok;
  use crate::io::storage::adaptor_test::write_read_all_random_ok;
  use crate::io::storage::adaptor_test::write_read_all_zero_ok;
  use crate::io::storage::adaptor_test::write_read_generic_random_ok;
  use crate::io::storage::adaptor_test::write_read_range_random_ok;
  use crate::io::storage::adaptor_test::write_twice_read_all_random_ok;

  /* FileSystemAdaptor-specific tests */

  #[test]
  fn fsa_write_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_all_zero_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_all_inside_dir_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_all_inside_dir_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_read_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_all_zero_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_all_random_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_twice_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_twice_read_all_random_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_read_range_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_range_random_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_read_generic_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_generic_random_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_read_all_ok() -> GResult<()> {
    let (resource_dir, fsa) = fsa_resources_setup()?;
    let buf = fsa.read_all(&resource_dir.join("small.txt")?)?;
    let read_string = match std::str::from_utf8(&buf[..]) {
      Ok(v) => v,
      Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    assert_eq!("text for testing", read_string, "Retrieved string mismatched");
    Ok(())
  }

  /* MmapAdaptor-specific tests */

  fn dir_to_mmap_url(resource_dir: &str) -> GResult<Url> {
    let file_url = url_from_dir_str(resource_dir)?;
    let url = Url::parse(&format!("mmap://{:?}", file_url.path()))?;
    log::error!("url: {:?}", url);
    Ok(url)
  }

  fn mfsa_resources_setup() -> GResult<(Url, MmapAdaptor)> {
    let resource_dir = dir_to_mmap_url(env!("CARGO_MANIFEST_DIR"))?.join("resources/test/")?;
    Ok((resource_dir, MmapAdaptor::new()))
  }

  fn mfsa_tempdir_setup() -> GResult<(TempDir, Url, MmapAdaptor)> {
    let temp_dir = TempDir::new()?;
    let temp_url = dir_to_mmap_url(temp_dir.path()
      .to_str()
      .expect("Failed to write tempdir as string")
    )?;
    Ok((temp_dir, temp_url, MmapAdaptor::new()))
  }

  #[test]
  fn mfsa_write_all_zero_ok() -> GResult<()> {
    let (_temp_dir, temp_url, mfsa) = mfsa_tempdir_setup()?;
    write_all_zero_ok(mfsa, &temp_url)
  }

  #[test]
  fn mfsa_write_all_inside_dir_ok() -> GResult<()> {
    let (_temp_dir, temp_url, mfsa) = mfsa_tempdir_setup()?;
    write_all_inside_dir_ok(mfsa, &temp_url)
  }

  #[test]
  fn mfsa_write_read_all_zero_ok() -> GResult<()> {
    let (_temp_dir, temp_url, mfsa) = mfsa_tempdir_setup()?;
    write_read_all_zero_ok(mfsa, &temp_url)
  }

  #[test]
  fn mfsa_write_read_all_random_ok() -> GResult<()> {
    let (_temp_dir, temp_url, mfsa) = mfsa_tempdir_setup()?;
    write_read_all_random_ok(mfsa, &temp_url)
  }

  #[test]
  fn mfsa_write_twice_read_all_random_ok() -> GResult<()> {
    let (_temp_dir, temp_url, mfsa) = mfsa_tempdir_setup()?;
    write_twice_read_all_random_ok(mfsa, &temp_url)
  }

  #[test]
  fn mfsa_write_read_range_random_ok() -> GResult<()> {
    let (_temp_dir, temp_url, mfsa) = mfsa_tempdir_setup()?;
    write_read_range_random_ok(mfsa, &temp_url)
  }

  #[test]
  fn mfsa_write_read_generic_random_ok() -> GResult<()> {
    let (_temp_dir, temp_url, mfsa) = mfsa_tempdir_setup()?;
    write_read_generic_random_ok(mfsa, &temp_url)
  }

  #[test]
  fn mfsa_read_all_ok() -> GResult<()> {
    let (resource_dir, mfsa) = mfsa_resources_setup()?;
    let buf = mfsa.read_all(&resource_dir.join("small.txt")?)?;
    let read_string = match std::str::from_utf8(&buf[..]) {
      Ok(v) => v,
      Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    assert_eq!("text for testing", read_string, "Retrieved string mismatched");
    Ok(())
  }
}