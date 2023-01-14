# AirIndex: Versatile Index Tuning Through Data and Storage

This is an instruction to benchmark B-tree, Data Calculator, and AirIndex (auto-tuned index) for experiments in AirIndex: Versatile Index Tuning Through Data and Storage.

Please follow [dataset](dataset_setup.md) (`dataset_setup.md`) and [query key set](keyset_setup.md) (`keyset_setup.md`) instructions to setup the benchmarking environment. These are examples of environment [reset scripts](reload_examples.md) (`reload_examples.md`). The following assumes that the dataset are under `/path/to/data/` and key sets are under `/path/to/keyset/`.

## Building the Binaries

```bash
cargo build --release
```

Optionally, you can run the unit tests to check compatibility.
```bash
cargo test
```


## End-to-end Search Performance

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


## Latency Breakdown

Inspect a breakdown of the latency from existing built indexes by following commands.
```bash
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/manual btree btree breakdown 40 ~/reload_nfs.sh nfs
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_nfs enb step,band_greedy,band_equal breakdown 40 ~/reload_nfs.sh nfs
```

The measurements will be recorded in `sosd_breakdown_out.jsons`.


## Skewed Workload

Generate skewed Zipfian keysets by following the [instruction](keyset_setup.md) (`keyset_setup.md`).

Then use the benchmark script by pointing to the skewed keysets.
```bash
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset/skew file:///path/to/airindex_nfs enb step,band_greedy,band_equal benchmark 40 ~/reload_nfs.sh nfs
```


## Auto-tuning Accuracy

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


## Build Scalability

To measure the build time, run the build script.
```bash
bash scripts/scale.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_scalability enb scalability.jsons
```


## Top-k Candidate Parameter Sweep

Build indexes with varying hyperparameter k by using a different action `buildtopk`.
```bash
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_nfs enb step,band_greedy,band_equal buildtopk 1 ~/reload_nfs.sh nfs
````


## Data Calculator

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

To benchmark on skewed workload, generate skewed keysets and change the keyset path accordingly.


## Instructions for Other Baselines

- LMDB: https://anonymous.4open.science/r/lmdb-47F2/libraries/liblmdb/README.md
- RMI: https://anonymous.4open.science/r/RMI-95C2/tests/kv_test/README.md
- PGM-index: https://anonymous.4open.science/r/PGM-index-7626/kv_test/README.md
- ALEX: https://anonymous.4open.science/r/ALEX_ext-8F68/README.md
