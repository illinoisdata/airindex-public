use serde::Serialize;
use std::cell::RefCell;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;
use std::time::Instant;
use structopt::StructOpt;
use tracing::Dispatch;
use tracing_timing::Builder;
use tracing_timing::Histogram;
use url::Url;

use airindex::common::error::GResult;
use airindex::db::key_rank::KeyRank;
use airindex::db::key_rank::read_keyset;
use airindex::db::key_rank::SOSDRankDB;
use airindex::index::hierarchical::BalanceStackIndexBuilder;
use airindex::index::hierarchical::BoundedTopStackIndexBuilder;
use airindex::index::hierarchical::ExploreStackIndexBuilder;
use airindex::index::Index;
use airindex::index::IndexBuilder;
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
use airindex::meta;
use airindex::model::band::BandMultipleDrafter;
use airindex::model::ModelDrafter;
use airindex::model::step::StepMultipleDrafter;
use airindex::model::toolkit::MultipleDrafter;
use airindex::store::array_store::ArrayStore;
use airindex::store::key_position::KeyPositionCollection;


/* Parsed arguments */

#[derive(Debug, Serialize, StructOpt)]
pub struct Cli {
  /// output path to log experiment results in append mode
  #[structopt(long)]
  out_path: String,

  /// action: build index
  #[structopt(long)]
  do_build: bool,
  /// action: benchmark
  #[structopt(long)]
  do_benchmark: bool,
  /// action: inspect index
  #[structopt(long)]
  do_inspect: bool,
  /// action: breakdown latency
  #[structopt(long)]
  do_breakdown: bool,

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
  /// url to the sosd data blob
  #[structopt(long)]
  keyset_url: String,


  /* db params */

  /// url to directory with index/db data
  #[structopt(long)]
  db_url: String,
  /// index builder type [bns, enb, btree]
  #[structopt(long)]
  index_builder: String,
  /// index drafter types [step, band_greedy, band_equal]
  #[structopt(long, use_delimiter = true)]
  index_drafters: Vec<String>,
  /// manual storage profile's latency in nanoseconds (affine)
  #[structopt(long, default_value = "10000000.0")]  // 10 ms
  affine_latency_ns: u64,
  /// manual storage profile's bandwidth in MB/s (affine)
  #[structopt(long, default_value = "100.0")]  // 100 MB/s
  affine_bandwidth_mbps: f64,
  /// lowerbound to load hyperparameters
  #[structopt(long, default_value = "256")]
  low_load: usize,
  /// upperbound to load hyperparameters
  #[structopt(long, default_value = "1048576")]
  high_load: usize,
  /// btree load hyperparameters
  #[structopt(long, default_value = "4096")]
  btree_load: usize,
  /// exponentiation step for load hyperparameters
  #[structopt(long, default_value = "2.0")]
  step_load: f64,
  /// target number of layers (enb index only)
  #[structopt(long)]
  target_layers: Option<usize>,
  /// top-k candidates to select at each branching
  #[structopt(long)]
  top_k_candidates: Option<usize>,


  /* For testing/debugging */

  /// disable cache to storage IO interface
  #[structopt(long)]
  no_cache: bool,
  /// disable parallel index building
  #[structopt(long)]
  no_parallel: bool,
  /// number of queries to test
  #[structopt(long)]
  num_samples: Option<usize>,
}


/* Serializable result */

#[derive(Serialize)]
pub struct BenchmarkResult<'a> {
  setting: &'a Cli,
  time_measures: &'a [u128],
  query_counts: &'a [usize],
}

#[derive(Serialize)]
pub struct BreakdownResult<'a> {
  setting: &'a Cli,
  event_names: &'a [String],
  time_measures: &'a [u128],
}


/* Experiment scope */

struct Experiment {
  storage: Rc<RefCell<ExternalStorage>>,
  sosd_context: Context,
  db_context: Context,
  sosd_blob_name: String,
  keyset_url: Url,
}

impl std::fmt::Debug for Experiment {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Context")
      .field("storage", &self.storage)
      .field("sosd_context", &self.sosd_context)
      .field("db_context", &self.db_context)
      .field("sosd_blob_name", &self.sosd_blob_name)
      .field("keyset_url", &self.keyset_url.to_string())
      .finish()
  }
}

impl Experiment {
  pub fn from(args: &Cli) -> GResult<Experiment> {
    // common external storage
    let es = Rc::new(RefCell::new(Experiment::load_io(args)?));

    // create context for sosd dataset
    let sosd_blob_url = Url::parse(&args.sosd_blob_url)?;
    let mut sosd_context = Context::new();
    sosd_context.put_storage(&es);
    sosd_context.put_store_prefix(&sosd_blob_url.join(".")?);

    // create data context for sosd rank db
    let db_url = Url::parse(&(args.db_url.clone() + "/"))?;  // enforce directory
    let mut db_context = Context::new();
    db_context.put_storage(&es);
    db_context.put_store_prefix(&db_url);

    Ok(Experiment {
      storage: es,
      sosd_context,
      db_context,
      sosd_blob_name: PathBuf::from(sosd_blob_url.path()).file_name().unwrap().to_str().unwrap().to_string(),
      keyset_url: Url::parse(&args.keyset_url)?,
    })
  }

  fn load_io(args: &Cli) -> GResult<ExternalStorage> {
    let mut es = if args.no_cache {
      ExternalStorage::new_with_cache(0, 4096)  // cache of size 0 byte
    } else {
      ExternalStorage::new()
    };

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

  pub fn build(&mut self, args: &Cli) -> GResult<()> {
    // load storage profile
    let profile = self.load_profile(args);

    // load dataset and generate the first key-position pairs
    let mut sosd_db = self.load_new_sosd(args)?;
    let data_kps = sosd_db.reconstruct_key_positions()?;
    self.observe_kps(&data_kps, 10);

    // build index
    let index = self.build_index_from_kps(args, &data_kps, profile.as_ref())?;
    sosd_db.attach_index(index);

    // turn into serializable form
    let mut new_data_ctx = Context::new();
    let mut new_index_ctx = Context::new();
    let meta = sosd_db.to_meta(&mut new_data_ctx, &mut new_index_ctx)?;
    let meta_bytes = meta::serialize(&meta)?;
    log::info!("Extracted data_ctx= {:?}", new_data_ctx);
    log::info!("Extracted index_ctx= {:?}", new_index_ctx);

    // write metadata
    self.db_context.storage.as_ref().unwrap()
      .borrow()
      .write_all(&self.db_meta()?, &meta_bytes)?;

    Ok(())
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

  fn build_index_from_kps(&self, args: &Cli, data_kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> GResult<Box<dyn Index>> {
    let model_drafter = self.make_drafter(args);
    let index_builder = self.make_index_builder(args, model_drafter, profile);
    log::debug!("Building with {:?}", index_builder);
    let index = index_builder.build_index(data_kps)?;
    log::info!("Built index at {}: {:#?}", self.db_context.store_prefix.as_ref().unwrap().as_str(), index);
    Ok(index)
  }

  fn make_drafter(&self, args: &Cli) -> Box<dyn ModelDrafter> {
    let low_load = args.low_load;
    let high_load = args.high_load;
    let step_load = args.step_load;
    let btree_load = args.btree_load;
    let mut model_drafter = MultipleDrafter::new();
    for index_drafter in &args.index_drafters {
      let sub_drafter = match index_drafter.as_str() {
        "step" => StepMultipleDrafter::exponentiation(low_load, high_load, step_load, 16),
        "band_greedy" => BandMultipleDrafter::greedy_exp(low_load, high_load, step_load),
        "band_equal" => BandMultipleDrafter::equal_exp(low_load, high_load, step_load),
        "btree" => StepMultipleDrafter::exponentiation(btree_load, btree_load, 2.0, btree_load / 16 - 1),
        _ => panic!("Invalid index_drafter= {}", index_drafter),
      };
      model_drafter = model_drafter.extend(sub_drafter);
    };

    // serial or parallel drafting
    let model_drafter = if args.no_parallel {
      model_drafter.to_serial()
    } else {
      model_drafter.to_parallel()
    };
    Box::new(model_drafter)
  }

  fn make_index_builder<'a>(&'a self, args: &Cli, model_drafter: Box<dyn ModelDrafter>, profile: &'a (dyn StorageProfile + 'a)) -> Box<dyn IndexBuilder + 'a> {
    match args.index_builder.as_str() {
      "bns" => {
        Box::new(BalanceStackIndexBuilder::new(
          self.db_context.storage.as_ref().unwrap(),
          model_drafter,
          profile,
          self.db_context.store_prefix.as_ref().unwrap().clone(),
        ))
      },
      "enb" => {
        let mut enb = ExploreStackIndexBuilder::new(
          self.db_context.storage.as_ref().unwrap(),
          model_drafter,
          profile,
          self.db_context.store_prefix.as_ref().unwrap().clone(),
        );
        if let Some(top_k_candidates) = args.top_k_candidates {
          enb = enb.set_top_k_candidates(top_k_candidates);
        }
        Box::new(enb)
      },
      "enb_layers" => {
        let target_layers = args.target_layers.expect("enb_layer requires target_layers");
        let mut enb = ExploreStackIndexBuilder::exact_layers(
          self.db_context.storage.as_ref().unwrap(),
          model_drafter,
          profile,
          self.db_context.store_prefix.as_ref().unwrap().clone(),
          target_layers,
        );
        if let Some(top_k_candidates) = args.top_k_candidates {
          enb = enb.set_top_k_candidates(top_k_candidates);
        }
        Box::new(enb)
      },
      "btree" => {
        Box::new(BoundedTopStackIndexBuilder::new(
          self.db_context.storage.as_ref().unwrap(),
          model_drafter,
          profile,
          args.btree_load,
          self.db_context.store_prefix.as_ref().unwrap().clone(),
        ))
      },
      _ => panic!("Invalid index type \"{}\"", args.index_builder),
    }
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

  // TODO: multiple time?

  pub fn benchmark(&self, args: &Cli, test_keyset: Vec<KeyRank>) -> GResult<(Vec<u128>, Vec<usize>)> {
    // select keyset
    let num_samples = match args.num_samples {
      Some(num_samples) => num_samples,
      None => test_keyset.len(),
    };

    // start the clock
    let mut time_measures = Vec::new();
    let mut query_counts = Vec::new();
    let mut last_count_milestone = 0;
    let mut count_milestone = 1;
    let mut last_elasped = Duration::ZERO;
    let freq_mul: f64 = 1.1;
    let start_time = Instant::now();
    tracing::trace!("sosd_setup");
    log::debug!("Benchmark started");

    // reload data structure
    let sosd_db = self.reload()?;
    tracing::trace!("sosd_reload");
    log::debug!("Reloaded rank db");
    for (idx, test_kr) in test_keyset.iter().enumerate().take(num_samples) {
      let rcv_kr = sosd_db.rank_of(test_kr.key)?
        .unwrap_or_else(|| panic!("Existing key {} not found", test_kr.key));
      assert_eq!(rcv_kr, *test_kr, "Mismatch rank rcv: {:?}, actual: {:?}", rcv_kr, test_kr);
      if idx + 1 == count_milestone || idx + 1 == num_samples {
        let count_processed = idx + 1;
        let time_elapsed = start_time.elapsed();
        time_measures.push(time_elapsed.as_nanos());
        query_counts.push(count_processed);
        log::info!(
          "t= {:>9.2?}: {:7} counts, tot {:>9.2?}/op, seg {:>9.2?}/op",
          time_elapsed,
          count_processed,
          time_elapsed / count_processed.try_into().unwrap(),
          (time_elapsed - last_elasped) / (count_processed - last_count_milestone).try_into().unwrap() 
        );
        last_elasped = time_elapsed;
        last_count_milestone = count_processed;
        count_milestone = (count_milestone as f64 * freq_mul).ceil() as usize;
      }
      tracing::trace!("complete_query");
    }
    log::info!("Benchmarked {:#?}", sosd_db);
    Ok((time_measures, query_counts))
  }

  pub fn inspect(&self) -> GResult<()> {
    let sosd_db = self.reload()?;
    let load = sosd_db.get_load();
    let sum_load: f64 = load.iter().map(|ld| ld.average()).sum();
    log::debug!("Index structure {:#?}", sosd_db);
    log::debug!("Load {:#?}", load);
    log::info!(
      "prefix, num_layers, sum_load: {:?}, {}, {}",
      self.db_context.store_prefix.as_ref().unwrap().to_string(),
      load.len(),
      sum_load,
    );
    Ok(())
  }

  pub fn breakdown(&self, args: &Cli) -> GResult<(Vec<String>, Vec<u128>)> {
    // setup timing subscriber
    let timing_subscriber = Builder::default()
      .build(|| Histogram::new_with_max(10_000_000_000, 2).unwrap());
    let downcaster = timing_subscriber.downcaster();
    let dispatch = Dispatch::new(timing_subscriber);

    // do the benchmark
    let test_keyset = self.load_keyset()?;
    tracing::dispatcher::with_default(&dispatch, || {
      tracing::trace_span!("benchmark").in_scope(|| {
        self.benchmark(args, test_keyset)
      })
    })?;

    // inspect the measurements
    let mut event_names = Vec::new();
    let mut time_measures = Vec::new();
    let mut checksum = Duration::ZERO;
    let sub = downcaster.downcast(&dispatch).unwrap();
    sub.force_synchronize();
    sub.with_histograms(|hs| {
      for (span_group, hs) in hs {
        for (event_group, h) in hs {
          h.refresh();
          let sum = Duration::from_nanos((h.mean() * h.len() as f64) as u64);
          let mean = Duration::from_nanos(h.mean() as u64);
          let p50 = Duration::from_nanos(h.value_at_quantile(0.5));
          let min = Duration::from_nanos(h.min());
          let max = Duration::from_nanos(h.max());
          let count = h.len();
          let stdev = Duration::from_nanos(h.stdev() as u64);
          println!(
            "{} -> {:40}: sum= {:>9.2?}, mean= {:>9.2?}, p50= {:>9.2?}, min= {:>9.2?}, max= {:>9.2?}, count= {:>9.2?}, stdev= {:>9.2?}",
            span_group,
            event_group,
            sum,
            mean,
            p50,
            min,
            max,
            count,
            stdev,
          );
          event_names.push(event_group.clone());
          time_measures.push(sum.as_nanos());
          checksum += sum;
        }
      }
    });
    println!("checksum: {:>9.2?}", checksum);
    Ok((event_names, time_measures))
  }

  fn load_keyset(&self) -> GResult<Vec<KeyRank>> {
    let keyset_bytes = self.storage.borrow().read_all(&self.keyset_url)?;
    read_keyset(&keyset_bytes[..])
  }

  fn reload(&self) -> GResult<SOSDRankDB> {
    let meta_bytes = self.db_context.storage.as_ref().unwrap()
      .borrow()
      .read_all(&self.db_meta()?)?;
    tracing::trace!("sosd_readmeta");
    log::trace!("Loaded metadata of {} bytes", meta_bytes.len());
    let meta = meta::deserialize(&meta_bytes[..])?;
    // tracing::trace!("sosd_deserialize");
    log::trace!("Deserialized metadata");
    SOSDRankDB::from_meta(meta, &self.sosd_context, &self.db_context)
  }

  fn db_meta(&self) -> GResult<Url> {
    Ok(self.db_context.store_prefix.as_ref().unwrap().join("metadata")?)
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
  if args.do_build {
    exp.build(&args)?;
    log::info!("Built index"); 
  }

  // run benchmark
  if args.do_benchmark {
    let test_keyset = exp.load_keyset()?;
    let (time_measures, query_counts) = exp.benchmark(&args, test_keyset)?;
    log::info!("Collected {} measurements", time_measures.len()); 
    assert_eq!(time_measures.len(), query_counts.len());
    log_result(&args, &time_measures, &query_counts)?;
  };

  // inspect
  if args.do_inspect {
    exp.inspect()?;
  }

  // trace for latency breakdown
  if args.do_breakdown {
    let (event_names, time_measures) = exp.breakdown(&args)?;
    log::info!("Collected {} measurements", time_measures.len()); 
    assert_eq!(event_names.len(), time_measures.len());
    log_result_breakdown(&args, &event_names, &time_measures)?;
  }

  Ok(())
}

fn log_result(args: &Cli, time_measures: &[u128], query_counts: &[usize]) -> GResult<()> {
  // compose json result
  let result_json = serde_json::to_string(&BenchmarkResult {
    setting: args,
    time_measures,
    query_counts,
  })?;
  write_json(args, result_json)
}

fn log_result_breakdown(args: &Cli, event_names: &[String], time_measures: &[u128]) -> GResult<()> {
  // compose json result
  let result_json = serde_json::to_string(&BreakdownResult {
    setting: args,
    event_names,
    time_measures,
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
