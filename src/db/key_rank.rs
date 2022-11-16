use byteorder::ByteOrder;
use byteorder::LittleEndian;
use rand::distributions::Distribution;
use rand::Rng;
use rand::SeedableRng;
use rand_pcg::Pcg64;
use serde::{Serialize, Deserialize};
use sscanf::scanf;
use std::collections::hash_map::DefaultHasher;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::Write;
use std::str::from_utf8;
use zipf::ZipfDistribution;

use crate::common::error::GResult;
use crate::index::Index;
use crate::index::IndexBuilder;
use crate::index::IndexMeta;
use crate::meta::Context;
use crate::model::load::LoadDistribution;
use crate::store::array_store::ArrayStore;
use crate::store::array_store::ArrayStoreState;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::KeyT;


#[derive(PartialEq, Debug)]
pub struct KeyRank {
  pub key: KeyT,
  pub rank: usize,  // from 0 to n-1
}

fn deserialize_key(dbuffer: &[u8]) -> KeyT {
  LittleEndian::read_uint(dbuffer, dbuffer.len())
}

fn shuffle_idx(t: usize, n: usize) -> usize {
  let mut s = DefaultHasher::new();
  t.hash(&mut s);
  (s.finish() as usize) % n
}


/* DB that manages key and compute their ranks */

#[derive(Debug)]
pub struct SOSDRankDB {
  array_store: ArrayStore,
  index: Option<Box<dyn Index>>,
}

impl SOSDRankDB {

  pub fn new(array_store: ArrayStore) -> SOSDRankDB {
    SOSDRankDB { array_store, index: None }
  }

  pub fn build_index(&mut self, index_builder: Box<dyn IndexBuilder>) -> GResult<()> {
    let kps = self.reconstruct_key_positions()?;
    self.attach_index(index_builder.build_index(&kps)?);
    Ok(())
  }

  pub fn attach_index(&mut self, index: Box<dyn Index>) {
    self.index = Some(index)
  }

  pub fn rank_of(&self, key: KeyT) -> GResult<Option<KeyRank>> {
    let kpr = self.index
      .as_ref()
      .expect("Index missing, trying to accessing empty data store")
      .predict(&key)?;
    // tracing::trace!("keyrank_index");
    let reader = self.array_store.read_array_within(kpr.offset, kpr.length)?;
    // tracing::trace!("keyrank_buffer");
    log::trace!("received rank buffer in {:?}", kpr);
    let (kb, rank) = reader.first_of_with_rank(key)?;
    // tracing::trace!("keyrank_find");
    if kb.key == key {
      Ok(Some(KeyRank{ key: kb.key, rank }))
    } else {
      Ok(None)  // no entry with matching key
    }
  }

  pub fn reconstruct_key_positions(&self) -> GResult<KeyPositionCollection> {
    // SOSD blob contains uint32/uint64s written next to each other
    // We can reconstruct the kps by multiplying the rank with data size

    // parse all keys (TODO: in parallel?)
    let data_size = self.array_store.data_size();
    let all_keys: Vec<KeyT> = self.array_store
      .read_array_all()?
      .clone_all()
      .chunks(data_size)
      .map(deserialize_key)
      .collect();

    // build key-position collection without duplicates
    let mut kps = KeyPositionCollection::new();  // goal is to fill this
    if !all_keys.is_empty() {
      // push first key
      kps.push(all_keys[0], 0);

      // push non-duplicated keys
      let mut duplicate_count = 0;
      for idx in 1 .. all_keys.len() {
        if all_keys[idx] == all_keys[idx - 1] {
          duplicate_count += 1;
        } else {
          kps.push(all_keys[idx], idx * data_size);
        }
      }
      log::debug!("{} duplicated key pairs", duplicate_count);
      kps.set_position_range(0, all_keys.len() * data_size); 
    }
    Ok(kps)
  }

  pub fn generate_uniform_keyset(
    &self,
    kps: &KeyPositionCollection,
    keyset_path: String,
    num_keyset: usize,
    seed: u64,
  ) -> GResult<()> {
    let mut keyset_file = OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open(keyset_path.as_str())?;
    let mut rng = Pcg64::seed_from_u64(seed);  // "random" seed via cat typing asdasd

    for _ in 0..num_keyset {
      let idx = rng.gen_range(0..kps.len());
      let kp = &kps[idx];  // assume key-position is sorted by key
      writeln!(&mut keyset_file, "{} {}", kp.key, kp.position / self.array_store.data_size())?;
    }
    Ok(())
  }

  pub fn generate_zipf_keyset(
    &self,
    kps: &KeyPositionCollection,
    keyset_path: String,
    num_keyset: usize,
    seed: u64,
    power: f64,
  ) -> GResult<()> {
    let mut keyset_file = OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open(keyset_path.as_str())?;
    let mut rng = Pcg64::seed_from_u64(seed);  // "random" seed via cat typing asdasd
    let zipf = ZipfDistribution::new(kps.len(), power)
      .unwrap_or_else(|_| panic!("Failed to create ZipfDistribution({}, {})", kps.len(), power));

    for _ in 0..num_keyset {
      let idx = shuffle_idx(zipf.sample(&mut rng) - 1, kps.len());
      let kp = &kps[idx];  // assume key-position is sorted by key
      writeln!(&mut keyset_file, "{} {}", kp.key, kp.position / self.array_store.data_size())?;
    }
    Ok(())
  }

  pub fn get_load(&self) -> Vec<LoadDistribution> {
    match &self.index {
      Some(index) => index.get_load(),
      None => vec![LoadDistribution::exact(self.array_store.read_all_size())],
    }
  }
}


#[derive(Serialize, Deserialize)]
pub struct SOSDRankDBMeta {
  array_store_state: ArrayStoreState,
  index: Option<IndexMeta>,
}

impl SOSDRankDB {  // for Metaserde
  pub fn to_meta(self, data_ctx: &mut Context, index_ctx: &mut Context) -> GResult<SOSDRankDBMeta> {
    Ok(SOSDRankDBMeta {
      array_store_state: self.array_store.to_meta_state(data_ctx)?,
      index: match self.index {
        Some(index) => Some(index.to_meta(index_ctx)?),
        None => None,
      }
    })
  }

  pub fn from_meta(meta: SOSDRankDBMeta, data_ctx: &Context, index_ctx: &Context) -> GResult<SOSDRankDB> {
    Ok(SOSDRankDB {
      array_store: ArrayStore::from_meta(meta.array_store_state, data_ctx)?,
      index: match meta.index {
        Some(index_meta) => Some(IndexMeta::from_meta(index_meta, index_ctx)?),
        None => None,
      },
    })
  }
}

pub fn read_keyset(keyset_bytes: &[u8]) -> GResult<Vec<KeyRank>> {
  Ok(from_utf8(keyset_bytes)?.lines().map(|line| {
    let (key, rank) = scanf!(line, "{} {}", KeyT, usize).unwrap();
    KeyRank { key, rank }
  }).collect())
}
