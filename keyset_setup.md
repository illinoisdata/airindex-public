# Setup Keyset

After setting up the [datasets](https://github.com/illinoisdata/airindex/blob/main/dataset_setup.md), clone AirIndex repository.
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
