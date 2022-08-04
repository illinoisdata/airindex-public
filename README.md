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


## End-to-end Search Performance (5.2)

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


## Latency Breakdown (5.3)

Inspect a breakdown of the latency from existing built indexes by following commands.
```bash
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/manual btree btree breakdown 40 ~/reload_nfs.sh nfs
bash scripts/sosd_experiment.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_nfs enb step,band_greedy,band_equal breakdown 40 ~/reload_nfs.sh nfs
```

The measurements will be recorded in `sosd_breakdown_out.jsons`.


## Auto-tuning Accuracy (5.4)

Similarly to 5.2, build the AirIndex variants.
```bash
bash scripts/sosd_variants.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_variants_index build 1 ~/reload_nfs.sh nfs
```

Then, benchmark all of them
```bash
bash scripts/sosd_variants.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_variants_index benchmark 40 ~/reload_nfs.sh nfs
```


## Storage Impact on Index and Search (5.5)

Let AirIndex tune indexes over a variety of affine storage profiles. Highly recommend executing on a CPU-rich machine; otherwise, this will take a considerable time.
```bash
bash scripts/storage_explore.sh file:///path/to/data file:///path/to/keyset file:///path/to/storage_explore enb
```

Then, read the index structures.
```bash
bash scripts/inspect.sh file:///path/to/data file:///path/to/keyset file:///path/to/storage_explore enb
```


## Build Scalability (5.6)

To measure the build time, run the build script.
```bash
bash scripts/scale.sh file:///path/to/data file:///path/to/keyset file:///path/to/airindex_scalability enb scalability.jsons
```
