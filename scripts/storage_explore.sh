set -e

DATASET_NAMES=(
    "fb_200M_uint64"
)

LATENCY_UPPER=2**40
BANDWIDTH_UPPER_EXP=15

BLOB_ROOT=$1
KEYSET_ROOT=$2
DB_ROOT=$3
INDEX_TYPE=$4
LOG_LEVEL="info"

SOSD_BLOBS=(
  "fb 200 uint64"
)

explore () {
  read -a sosd_blob <<< $1
  
  PROFILE="--affine-latency-ns $2 --affine-bandwidth-mbps $3"
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"

  set -x
  RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url "${DB_ROOT}/${blob_name}_${INDEX_TYPE}_$2_$3" --index-builder ${INDEX_TYPE} --index-drafters=step,band_greedy,band_equal --out-path sosd_build_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --no-cache --low-load 64 --do-build
  set +x
}

for ((i = 0; i < ${#SOSD_BLOBS[@]}; i++)) do
  for ((j = 2**10; j <= ${LATENCY_UPPER}; j*=2)) do
    for ((k = 10; k <= ${BANDWIDTH_UPPER_EXP}; k+=1)) do
      b=`echo "print(2 ** ${k} * 10 ** -6)" | python`
      explore "${SOSD_BLOBS[$i]}" "${j}" "${b}"
    done
  done
done
