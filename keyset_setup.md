# Setup Keyset

After setting up the [datasets](dataset_setup.md) (`dataset_setup.md`), clone AirIndex repository.
```bash
git clone https://github.com/illinoisdata/airindex-public.git
cd airindex-public
```

Generate 40 keysets for the 5 datasets by running the following command. Be sure to use absolute paths.
```bash
cargo build --release
./scripts/generate_sosd_keysets.sh file:///path/to/data file:///path/to/keyset 1000000 40
```

There should be 5 * 40 = 200 files under `/path/to/keyset` representing 40 different key sets per dataset.
```
/path/to/keyset/books_800M_uint64_ks_0
/path/to/keyset/books_800M_uint64_ks_1
/path/to/keyset/books_800M_uint64_ks_2
...
/path/to/keyset/books_800M_uint64_ks_39
/path/to/keyset/fb_1M_uint64_ks_0
...
```


## Zipfian Workload

Generate 3 sets of Zipfian keysets (with parameter 0.5, 1.0, and 2.0) for the 5 datasets by running the following command. Be sure to use absolute paths.
```bash
cargo build --release
./scripts/generate_sosd_keysets_zipf.sh file:///path/to/data file:///path/to/keyset 1000000 40
```

There should be 5 * 3 * 40 = 600 files under `/path/to/keyset` representing 3 * 40 different key sets per dataset.
```
/path/to/keyset/0.5/books_800M_uint64_ks_0
/path/to/keyset/0.5/books_800M_uint64_ks_1
/path/to/keyset/0.5/books_800M_uint64_ks_2
...
/path/to/keyset/0.5/books_800M_uint64_ks_39
/path/to/keyset/0.5/fb_1M_uint64_ks_0
/path/to/keyset/0.5/books_800M_uint64_ks_0
...
/path/to/keyset/1.0/books_800M_uint64_ks_0
...
/path/to/keyset/2.0/books_800M_uint64_ks_0
...
```
