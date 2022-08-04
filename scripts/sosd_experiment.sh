#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 9 ]
then
  echo "Require 9 argument (BLOB_ROOT, KEYSET_ROOT, DB_ROOT, INDEX_BUILDER, INDEX_DRAFTERS, ACTION, REPEAT, RESET_SCRIPT, STORAGE), $# provided"
  echo 'Example: bash scripts/sosd_experiment.sh file://$(pwd)/../SOSD/data file://$(pwd)/../SOSD/keyset file://$(pwd)/tmp/btree btree btree build 1 ~/reload_nfs.sh nfs'
  echo 'Example: bash scripts/sosd_experiment.sh file://$(pwd)/../SOSD/data file://$(pwd)/../SOSD/keyset file://$(pwd)/tmp/enb_stb enb step,band_greedy,band_equal build 1 ~/reload_nfs.sh nfs'
  exit 1
fi

BLOB_ROOT=$1
KEYSET_ROOT=$2
DB_ROOT=$3
INDEX_BUILDER=$4
INDEX_DRAFTERS=$5
ACTION=$6
REPEAT=$7
RESET_SCRIPT=$8
STORAGE=$9
LOG_LEVEL="info"
if [[ $ACTION != "build" && $ACTION != "benchmark" && $ACTION != "breakdown" ]]
then
  echo "Invalid ACTION [build | benchmark | breakdown]"
  exit 1
fi
if [[ $STORAGE == "nfs" ]]
then
  PROFILE="--affine-latency-ns 50000000 --affine-bandwidth-mbps 12.0"
elif [[ $STORAGE == "ssd" ]]
then
  PROFILE="--affine-latency-ns 250000 --affine-bandwidth-mbps 175.0"
else
  echo "Invalid storage type ${STORAGE} [nfs | ssd]"
  exit 1
fi
echo "Using BLOB_ROOT=${BLOB_ROOT}, KEYSET_ROOT=${KEYSET_ROOT}, DB_ROOT=${DB_ROOT}, INDEX_BUILDER=${INDEX_BUILDER}, INDEX_DRAFTERS=${INDEX_DRAFTERS}, ACTION=${ACTION}, REPEAT=${REPEAT} RESET_SCRIPT=${RESET_SCRIPT}, PROFILE=${PROFILE}, LOG_LEVEL=${LOG_LEVEL}"
sleep 5

SOSD_BLOBS=(
  "books 800 uint64"
  "fb 200 uint64"
  "osm_cellids 800 uint64"
  "wiki_ts 200 uint64"
  "gmm_k100 800 uint64"
)

build () {
  read -a sosd_blob <<< $1
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"

  set -x
  RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url "${DB_ROOT}/${blob_name}" --index-builder ${INDEX_BUILDER} --index-drafters=${INDEX_DRAFTERS} --out-path sosd_build_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --no-cache --do-build
  set +x
}

benchmark () {
  read -a sosd_blob <<< $1
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"

  for ((j = 0; j < ${REPEAT}; j++)) do
  bash ${RESET_SCRIPT}
  set -x
  RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url "${DB_ROOT}/${blob_name}" --index-builder ${INDEX_BUILDER} --index-drafters=${INDEX_DRAFTERS} --out-path sosd_benchmark_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks_${j}" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --do-benchmark
  set +x
  done
}

breakdown () {
  read -a sosd_blob <<< $1
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"

  for ((k = 1; k <= 1000000; k*=10)) do
    for ((j = 0; j < ${REPEAT}; j++)) do
    bash ${RESET_SCRIPT}
    set -x
    RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url "${DB_ROOT}/${blob_name}" --index-builder ${INDEX_BUILDER} --index-drafters=${INDEX_DRAFTERS} --out-path sosd_breakdown_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks_${j}" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --do-breakdown --num-samples ${k}
    set +x
    done
  done
}

for ((i = 0; i < ${#SOSD_BLOBS[@]}; i++)) do
  if [[ $ACTION == "build" ]]
  then
    build "${SOSD_BLOBS[$i]}"
  elif [[ $ACTION == "benchmark" ]]
  then
    benchmark "${SOSD_BLOBS[$i]}"
  elif [[ $ACTION == "breakdown" ]]
  then
    breakdown "${SOSD_BLOBS[$i]}"
  fi
done