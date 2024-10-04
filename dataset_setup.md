# Setup Dataset

Clone SOSD repository.
```bash
git clone https://github.com/illinoisdata/SOSD.git
cd SOSD
```

Download and create the datasets.
```bash
bash scripts/download.sh
python gen_gmm.py  --n 800 --k 100
```

Or, alternatively download from following links directly: [wiki_ts_200M_uint64](https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/SVN8PI), [books_200M_uint32](https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/5YTV8K), [books_800M_uint64](https://www.dropbox.com/s/y2u3nbanbnbmg7n/books_800M_uint64.zst?dl=1), [osm_cellids_800M_uint64](https://www.dropbox.com/s/j1d4ufn4fyb4po2/osm_cellids_800M_uint64.zst?dl=1), [fb_200M_uint64](https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/EATHF7).

Move these datasets into the target storage to benchmark on. Afterwards, these should be valid absolute paths.
```
/path/to/data/books_800M_uint64
/path/to/data/fb_200M_uint64
/path/to/data/osm_cellids_800M_uint64
/path/to/data/wiki_ts_200M_uint64
/path/to/data/gmm_k100_800M_uint64
```
