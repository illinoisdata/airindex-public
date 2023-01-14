# Setup Dataset

Clone SOSD repository (https://anonymous.4open.science/r/SOSD-03E7).
```bash
git clone https://github.com/illinoisdata/SOSD.git
cd SOSD
```

Download and create the datasets.
```bash
./scripts/download.sh
python gen_gmm.py  --n 800 --k 10
```

Move these datasets into the target storage to benchmark on. Afterwards, these should be valid absolute paths.
```
/path/to/data/books_800M_uint64
/path/to/data/fb_200M_uint64
/path/to/data/osm_cellids_800M_uint64
/path/to/data/wiki_ts_200M_uint64
/path/to/data/gmm_k100_800M_uint64
```
