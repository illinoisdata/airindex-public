THREADS_UPPER=128

BLOB_ROOT=$1
KEYSET_ROOT=$2
DB_ROOT=$3
INDEX_TYPE=$4
OUT_PATH=$5
LOG_LEVEL="info"

# Dataset type shouldn't affect build time.
SOSD_BLOBS=(
  "gmm 200 uint64"
  "gmm 400 uint64"
  "gmm 600 uint64"
  "gmm 800 uint64"
)

explore () {
  read -a sosd_blob <<< $1
  
  PROFILE="--affine-latency-ns 12000000 --affine-bandwidth-mbps 50.0"
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_k100_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_k100_${sosd_blob[1]}M_${sosd_blob[2]}_ks"

  set -x
  RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url "${DB_ROOT}/${blob_name}_${INDEX_TYPE}_$2_$3" --index-builder ${INDEX_TYPE} --index-drafters=step,band_greedy,band_equal --out-path ${OUT_PATH} --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --no-cache --low-load 64 --do-build
  set +x
}

for ((i = 0; i < ${#SOSD_BLOBS[@]}; i++)) do
  for ((num_threads = 1; num_threads <= ${THREADS_UPPER}; num_threads*=2)) do
    export RAYON_NUM_THREADS=${num_threads}
    SECONDS=0
    explore "${SOSD_BLOBS[$i]}"
    DURATION=$SECONDS
    echo "-------TIME---------"
    echo "It takes $(($DURATION * 1000)) milliseconds to build ${SOSD_BLOBS[$i]} with ${num_threads} threads"
    # echo "It takes $(($DURATION * 1000)) milliseconds to build ${SOSD_BLOBS[$i]} with max threads"
    echo "-------TIME---------"
  done
done
