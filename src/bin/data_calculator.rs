/*
 * Reimplementation of Data Calculator's auto-completion compatible with AirIndex structure
 *   - B+Tree layout with variable fanout/partition/capacity
 *   - Fixed fanout or partition functions (radix, range)
 *   - Autocompletion iterates through fanouts and numbers of layers in parallel
 *   - Assume external storage I/O dominates search time: SortedSearch = RandomProbe = Read(size)
 */

use std::io::Write;
use std::fs::OpenOptions;
use serde::Serialize;
use std::cell::RefCell;
use std::fmt::Debug;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;
use structopt::StructOpt;
use rayon::prelude::*;
use url::Url;

use airindex::common::error::GResult;
use airindex::db::key_rank::SOSDRankDB;
use airindex::io::internal::ExternalStorage;
use airindex::io::profile::AffineStorageProfile;
use airindex::io::profile::Bandwidth;
use airindex::io::profile::Latency;
use airindex::io::profile::StorageProfile;
use airindex::io::storage::Adaptor;
use airindex::io::storage::AzureStorageAdaptor;
use airindex::io::storage::FileSystemAdaptor;
use airindex::io::storage::MmapAdaptor;
use airindex::meta::Context;
use airindex::store::array_store::ArrayStore;
use airindex::store::key_position::KeyPositionCollection;
use airindex::store::key_position::KeyT;


const POINTER_SIZE: usize = 8;


trait PartitionFunction: Debug + Send {
  fn partition<'a>(&self, keys: &'a [KeyT]) -> Vec<&'a [KeyT]>;
  fn step(&mut self);
  fn size(&self) -> usize;
  fn clone_boxed(&self) -> Box<dyn PartitionFunction>;
}

#[derive(Clone)]
struct FixedFanoutPF {
  fanout: usize,
}

impl std::fmt::Debug for FixedFanoutPF {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("FixedFanoutPF")
      .field("fanout", &self.fanout)
      .field("node_size", &self.size())
      .finish()
  }
}

impl FixedFanoutPF {
  fn new_boxed(fanout: usize) -> Box<dyn PartitionFunction> {
    Box::new(FixedFanoutPF { fanout })
  }
}

impl PartitionFunction for FixedFanoutPF {
  fn partition<'a>(&self, keys: &'a [KeyT]) -> Vec<&'a [KeyT]> {
    // Data Calculator's process 4 (P4) with faster windowing
    let capacity = (keys.len() - 1) / self.fanout + 1;
    let mut idx = 0;
    let mut subblocks = Vec::new();
    while idx < keys.len() {
      subblocks.push(&keys[idx .. std::cmp::min(idx + capacity, keys.len())]);
      idx += capacity;
    }
    subblocks
  }

  fn step(&mut self) { /* no-op */ }

  fn size(&self) -> usize {
    self.fanout * (POINTER_SIZE + (KeyT::BITS / 8) as usize)
  }

  fn clone_boxed(&self) -> Box<dyn PartitionFunction> {
    Box::new(self.clone())
  }
}


#[derive(Debug)]
struct DataLayout {
  pf: Box<dyn PartitionFunction>,
  layers: usize,
}

impl DataLayout {
  fn new(pf: Box<dyn PartitionFunction>, layers: usize) -> DataLayout {
    DataLayout { pf, layers }
  }

  fn clone(&self) -> DataLayout {
    DataLayout::new(self.pf.clone_boxed(), self.layers)
  }
}

fn data_calculator_cost(
  dl: &mut DataLayout,
  profile: &dyn StorageProfile,
  keys: &[KeyT]
) -> Duration {
  // evaluate cost of uniform gets from hardware profile primitive
  let key_size = keys.len();
  let mut cost = Duration::ZERO;
  let mut blocks = vec![keys];
  for _ in 0 .. dl.layers {
    // partition into sub-blocks
    let mut all_subblocks = Vec::new();
    for block in blocks {
      let subblocks = dl.pf.partition(block);
      // for sb in &subblocks {
      //   // TODO: if all size() is constant, this can be one line.
      //   cost += profile.cost(dl.pf.size()).mul_f64(sb.len() as f64 / key_size as f64);
      // }
      all_subblocks.extend(subblocks);
    }
    cost += profile.cost(dl.pf.size());

    // step to next layer
    dl.pf.step();
    blocks = all_subblocks;
  }
  log::debug!("index cost= {:?}", cost);
  let mut total_size = 0;
  let mut data_cost_ns = 0.0;
  for sb in &blocks {
    data_cost_ns += profile.cost(sb.len() * (KeyT::BITS / 8) as usize).as_nanos() as f64 
                    * (sb.len() as f64 / key_size as f64);
    total_size += sb.len() * (KeyT::BITS / 8) as usize 
  }
  cost += Duration::from_nanos(data_cost_ns as u64);
  log::debug!("total cost= {:?}, total_size= {}, avg_size= {}", cost, total_size, total_size as f64 / blocks.len() as f64);
  cost
}

fn data_calculator_select(
  mut dls: Vec<DataLayout>,
  profile: &dyn StorageProfile,
  keys: &[KeyT],
) -> (DataLayout, Duration) {
  assert!(dls.len() > 0);
  let (best_dl, best_cost) = dls.par_iter_mut()
    .map(|dl| {
      let cost = data_calculator_cost(dl, profile, keys);
      log::info!("cost= {:>9.2?} by {:?}", cost, dl);
      (dl, cost)
    })
    .min_by_key(|(_dl, cost)| cost.clone())
    .expect("Empty list of data layouts");
  (best_dl.clone(), best_cost)
}

fn data_calculator_generate_layouts(
  fanout_min: usize, 
  fanout_max: usize, 
  fanout_multiplier: f64, 
  layers_max: usize,
) -> Vec<DataLayout> {
  // construct different layouts
  let mut dls = Vec::new();
  dls.push(DataLayout::new(FixedFanoutPF::new_boxed(0), 0));  // empty index
  for layers in 1 .. layers_max + 1 {
    let mut fanout = fanout_min;
    while fanout <= fanout_max {
      dls.push(DataLayout::new(FixedFanoutPF::new_boxed(fanout), layers));
      fanout = (fanout as f64 * fanout_multiplier) as usize;
    }
  }
  dls
}

/* Parsed arguments */

#[derive(Debug, Serialize, StructOpt)]
pub struct Cli {
  /// output path to log experiment results in append mode
  #[structopt(long)]
  out_path: String,

  /// dataset name [blob]
  #[structopt(long)]
  dataset_name: String,


  /* SOSD params */

  /// url to the sosd data blob
  #[structopt(long)]
  sosd_blob_url: String,
  /// data type in the blob [uint32, uint64]
  #[structopt(long)]
  sosd_dtype: String,
  /// number of elements, in millions (typically 200, 400, 500, 800)
  #[structopt(long)]
  sosd_size: usize,


  /* db params */

  /// manual storage profile's latency in nanoseconds (affine)
  #[structopt(long, default_value = "10000000")]  // 10 ms
  affine_latency_ns: u64,
  /// manual storage profile's bandwidth in MB/s (affine)
  #[structopt(long, default_value = "100.0")]  // 100 MB/s
  affine_bandwidth_mbps: f64,
  /// lowerbound to fanout hyperparameters
  #[structopt(long, default_value = "16")]  // 256 / 16
  fanout_min: usize,
  /// upperbound to fanout hyperparameters
  #[structopt(long, default_value = "65536")]  // 1048576 / 16
  fanout_max: usize,
  /// exponentiation step for fanout hyperparameters
  #[structopt(long, default_value = "2.0")]
  fanout_multiplier: f64,
  /// maximum number of layers
  #[structopt(long, default_value = "4")]
  layers_max: usize,
}


/* Serializable result */

#[derive(Serialize)]
pub struct DataCalculatorResult<'a> {
  setting: &'a Cli,
  dl: &'a str,
  cost: &'a Duration,
}


/* Experiment scope */

struct Experiment {
  storage: Rc<RefCell<ExternalStorage>>,
  sosd_context: Context,
  sosd_blob_name: String,
}

impl std::fmt::Debug for Experiment {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Context")
      .field("storage", &self.storage)
      .field("sosd_context", &self.sosd_context)
      .field("sosd_blob_name", &self.sosd_blob_name)
      .finish()
  }
}

impl Experiment {
  pub fn from(args: &Cli) -> GResult<Experiment> {
    // common external storage
    let es = Rc::new(RefCell::new(Experiment::load_io()?));

    // create context for sosd dataset
    let sosd_blob_url = Url::parse(&args.sosd_blob_url)?;
    let mut sosd_context = Context::new();
    sosd_context.put_storage(&es);
    sosd_context.put_store_prefix(&sosd_blob_url.join(".")?);

    Ok(Experiment {
      storage: es,
      sosd_context,
      sosd_blob_name: PathBuf::from(sosd_blob_url.path()).file_name().unwrap().to_str().unwrap().to_string(),
    })
  }

  fn load_io() -> GResult<ExternalStorage> {
    let mut es = ExternalStorage::new();

    // file system
    let fsa = Box::new(FileSystemAdaptor::new()) as Box<dyn Adaptor>;
    es = es.with("file".to_string(), fsa)?;

    // file system, via mmap
    let mfsa = Box::new(MmapAdaptor::new()) as Box<dyn Adaptor>;
    es = es.with("mmap".to_string(), mfsa)?;

    // azure storage
    let aza = AzureStorageAdaptor::new_block();
    match aza {
      Ok(aza) => es = es.with("az".to_string(), Box::new(aza))?,
      Err(e) => log::warn!("Failed to initialize azure storage, {:?}", e),
    }

    Ok(es)
      
  }

  pub fn build(&mut self, args: &Cli) -> GResult<(DataLayout, Duration)> {
    // load storage profile
    let profile = self.load_profile(args);

    // load dataset and generate the first key-position pairs
    let sosd_db = self.load_new_sosd(args)?;
    let data_kps = sosd_db.reconstruct_key_positions()?;
    self.observe_kps(&data_kps, 10);

    // build index
    let keys: Vec<KeyT> = data_kps.iter().map(|kp| kp.key).collect();
    let (best_dl, best_cost) = self.build_index_from_keys(args, &keys, profile.as_ref());

    Ok((best_dl, best_cost))
  }

  fn load_new_sosd(&self, args: &Cli) -> GResult<SOSDRankDB> {
    match args.dataset_name.as_str() {
      "blob" => self.load_blob(args),
      _ => panic!("Invalid dataset name \"{}\"", args.dataset_name),
    }
  }

  fn load_blob(&self, args: &Cli) -> GResult<SOSDRankDB> {
    let array_store = ArrayStore::from_exact(
      self.sosd_context.storage.as_ref().unwrap(),
      self.sosd_context.store_prefix.as_ref().unwrap().clone(),
      self.sosd_blob_name.clone(),
      match args.sosd_dtype.as_str() {
        "uint32" => 4,
        "uint64" => 8,
        _ => panic!("Invalid sosd dtype \"{}\"", args.sosd_dtype),
      },
      8,  // SOSD array leads with 8-byte encoding of the length
      args.sosd_size * 1_000_000,
    );
    Ok(SOSDRankDB::new(array_store))
  }

  fn load_profile(&self, args: &Cli) -> Box<dyn StorageProfile> {
    Box::new(AffineStorageProfile::new(
      Latency::from_nanos(args.affine_latency_ns),
      Bandwidth::from_mbps(args.affine_bandwidth_mbps)
    ))
  }

  fn build_index_from_keys(&self, args: &Cli, data_kps: &[KeyT], profile: &dyn StorageProfile) -> (DataLayout, Duration) {
    let dls = data_calculator_generate_layouts(
      args.fanout_min,
      args.fanout_max,
      args.fanout_multiplier,
      args.layers_max,
    );
    log::info!("Generated {} data layouts", dls.len());
    data_calculator_select(dls, profile, data_kps)
  }

  fn observe_kps(&self, kps: &KeyPositionCollection, num_print_kps: usize) {
    println!("Head:");
    for idx in 0..num_print_kps {
      println!("\t{}: {:?}", idx, kps[idx]);
    }
    println!("Intermediate:");
    let step = kps.len() / num_print_kps;
    for idx in 0..num_print_kps {
      println!("\t{}: {:?}", idx * step, kps[idx * step]);
    }
    println!("Length= {}, where last kp: {:?}", kps.len(), kps[kps.len() - 1]);
  }
}

fn main_guarded() -> GResult<()> {
  // execution init
  env_logger::Builder::from_default_env()
    .format_timestamp_micros()
    .init();

  // parse args
  let args = Cli::from_args();
  log::info!("{:?}", args);

  // create experiment
  let mut exp = Experiment::from(&args)?;
  log::info!("{:?}", exp);

  // build index
  let (best_dl, best_cost) = exp.build(&args)?;
  log::info!("Best data layout {:#?}, with cost= {:>9.2?}", best_dl, best_cost);

  // save the index layout
  log_result(&args, &best_dl, &best_cost)
}

fn log_result(args: &Cli, dl: &DataLayout, cost: &Duration) -> GResult<()> {
  // compose json result
  let result_json = serde_json::to_string(&DataCalculatorResult {
    setting: args,
    dl: &format!("{:?}", dl),
    cost,
  })?;
  write_json(args, result_json)
}

fn write_json(args: &Cli, result_json: String) -> GResult<()> {
  let mut log_file = OpenOptions::new()
    .create(true)
    .write(true)
    .append(true)
    .open(args.out_path.as_str())?;
  log_file.write_all(result_json.as_bytes())?;
  log_file.write_all(b"\n")?;
  log::info!("Log result {} characters to {}", result_json.len(), args.out_path.as_str());
  Ok(())
}

fn main() {
  main_guarded().expect("Error occur during sosd experiment");
}
