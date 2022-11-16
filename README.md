# AirIndex: Versatile Index Tuning Through Data and Storage

This is an instruction to benchmark AirIndex Manual and AirIndex (auto-tuned index) for experiments in AirIndex: Versatile Index Tuning Through Data and Storage.

Please follow [dataset](https://github.com/illinoisdata/airindex-public/blob/main/dataset_setup.md) and [query key set](https://github.com/illinoisdata/airindex-public/blob/main/keyset_setup.md) instructions to setup the benchmarking environment. These are examples of environment [reset scripts](https://github.com/illinoisdata/airindex-public/blob/main/reload_examples.md). The following assumes that the dataset are under `/path/to/data/` and key sets are under `/path/to/keyset/`.

## Building the Binaries

```bash
cargo build --release
```

Optionally, you can run the unit tests to check compatibility.
```bash
cargo test
```


## End-to-end Search Performance (6.2)

For each storage (e.g., NFS) you would like benchmark on, tune and build indexes for all datasets.
```bash
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/manual btree btree build 1 ~/reload_nfs.sh nfs
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_nfs enb step,band_greedy,band_equal build 1 ~/reload_nfs.sh nfs
```

Afterwards, benchmark over 40 key set of 1M keys.
```bash
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/manual btree btree benchmark 40 ~/reload_nfs.sh nfs
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_nfs enb step,band_greedy,band_equal benchmark 40 ~/reload_nfs.sh nfs
```

The measurements will be recorded in `sosd_benchmark_out.jsons`.


## Latency Breakdown (6.3)

Inspect a breakdown of the latency from existing built indexes by following commands.
```bash
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/manual btree btree breakdown 40 ~/reload_nfs.sh nfs
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_nfs enb step,band_greedy,band_equal breakdown 40 ~/reload_nfs.sh nfs
```

The measurements will be recorded in `sosd_breakdown_out.jsons`.


## Skewed Workload (6.4)

Generate skewed Zipfian keysets by following the [instruction](https://github.com/illinoisdata/airindex-public/blob/main/keyset_setup.md).

Then use the benchmark script by pointing to the skewed keysets.
```bash
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset/skew file:///path/to/airindex_nfs enb step,band_greedy,band_equal benchmark 40 ~/reload_nfs.sh nfs
```


## Auto-tuning Accuracy (6.5)

Similarly to 5.2, build the AirIndex variants.
```bash
bash scripts/sosd_variants.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_variants_index build 1 ~/reload_nfs.sh nfs
```

Then, benchmark all of them
```bash
bash scripts/sosd_variants.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_variants_index benchmark 40 ~/reload_nfs.sh nfs
```


## Storage Impact on Index and Search

Let AirIndex tune indexes over a variety of affine storage profiles. Highly recommend executing on a CPU-rich machine; otherwise, this will take a considerable time.
```bash
bash scripts/storage_explore.sh file:///path/to/data file:///path/to/keyset file:///path/to/storage_explore enb
```

Then, read the index structures.
```bash
bash scripts/inspect.sh file:///path/to/data file:///path/to/keyset file:///path/to/storage_explore enb
```


## Build Scalability (6.6)

To measure the build time, run the build script.
```bash
bash scripts/scale.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_scalability enb scalability.jsons
```


## Top-k Candidate Parameter Sweep (6.7)

Build indexes with varying hyperparameter k by using a different action `buildtopk`.
```bash
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_nfs enb step,band_greedy,band_equal buildtopk 1 ~/reload_nfs.sh nfs
````


## Data Calculator (6.2 & 6.3 & 6.6)

To execute Data Calculator's auto-completion.
```bash
bash scripts/data_calculator_sosd.sh file:///path/to/data file:///path/to/keyset file:///path/to/data_calc autocomplete 1 ~/reload_nfs.sh nfs
```

Then copy the suggested structure at the end (load and number of layers) to insert to `scripts/data_calculator_sosd.sh` (lines 51-69).

Build and benchmark similarly to AirIndex
```bash
bash scripts/data_calculator_sosd.sh file:///path/to/data file:///path/to/keyset file:///path/to/data_calc build 1 ~/reload_nfs.sh nfs
bash scripts/data_calculator_sosd.sh file:///path/to/data file:///path/to/keyset file:///path/to/data_calc benchmark 40 ~/reload_nfs.sh nfs
```

To benchmark on skewed workload (6.3), generate skewed keysets and change the keyset path accordingly.


## Instructions for Other Baselines

- LMDB: https://github.com/illinoisdata/lmdb/tree/mdb.master/libraries/liblmdb
- RMI: https://github.com/illinoisdata/RMI/tree/master/tests/kv_test
- PGM-index: https://github.com/illinoisdata/PGM-index/tree/master/kv_test
- ALEX: https://github.com/illinoisdata/ALEX_ext
