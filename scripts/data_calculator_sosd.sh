#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 7 ]
then
  echo "Require 7 argument (BLOB_ROOT, KEYSET_ROOT, DB_ROOT, ACTION, REPEAT, RESET_SCRIPT
STORAGE), $# provided"
  exit 1
fi

BLOB_ROOT=$1
KEYSET_ROOT=$2
DB_ROOT=$3
ACTION=$4
REPEAT=$5
RESET_SCRIPT=$6
STORAGE=$7
LOG_LEVEL="info"
if [[ $ACTION != "autocomplete" && $ACTION != "build" && $ACTION != "benchmark" ]]
then
  echo "Invalid ACTION [autocomplete | build | benchmark]"
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
echo "Using BLOB_ROOT= ${BLOB_ROOT}, KEYSET_ROOT= ${KEYSET_ROOT}, DB_ROOT= ${DB_ROOT}, ACTION= ${ACTION}, REPEAT= ${REPEAT}, RESET_SCRIPT= ${RESET_SCRIPT}, PROFILE= ${PROFILE}"sleep 5

SOSD_BLOBS=(
  "books 800 uint64"
  "fb 200 uint64"
  "osm_cellids 800 uint64"
  "wiki_ts 200 uint64"
  "gmm_k100 800 uint64"
)

# Translated from autocomplete step, (layout, node_size)
if [[ $STORAGE == "nfs" ]]
then
  DATA_LAYOUTS=(
    "16384 2"
    "131072  1"
    "16384 2"
    "131072  1"
    "16384 2"
  )
elif [[ $STORAGE == "ssd" ]]
then
  DATA_LAYOUTS=(
    "16384 2"
    "8192  2"
    "16384 2"
    "8192  2"
    "16384 2"
  )
fi
if [[ ($ACTION == "build" || $ACTION == "benchmark") && "${#SOSD_BLOBS[@]}" -ne "${#DATA_LAYOUTS[@]}" ]]
then
  echo "ACTION=build requires same length between SOSD_BLOBS and DATA_LAYOUTS"
  echo "Run ACTION=autocomplete and fill DATA_LAYOUTS accordingly"
  exit 1
fi


autocomplete () {
  read -a sosd_blob <<< $1
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"

  set -x
  RUST_LOG=airindex=${LOG_LEVEL},data_calculator=${LOG_LEVEL} RUST_BACKTRACE=full cargo run --bin data_calculator --release -- --out-path data_calculator_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE}
  set +x
}

build () {
  read -a sosd_blob <<< $1
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"

  read -a data_layout <<< $2
  loads=${data_layout[0]}
  layers=${data_layout[1]}

  set -x
  RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url "${DB_ROOT}/${blob_name}" --index-builder enb_layers --target-layers ${layers} --index-drafters=step --low-load=${loads} --high-load=${loads} --out-path sosd_build_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks_${j}" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --no-cache --do-build
  set +x
}

benchmark () {
  read -a sosd_blob <<< $1
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"

  read -a data_layout <<< $2
  loads=${data_layout[0]}
  layers=${data_layout[1]}

  for ((j = 0; j < ${REPEAT}; j++)) do
  bash ${RESET_SCRIPT}
  set -x
  RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url "${DB_ROOT}/${blob_name}" --index-builder enb_layers --target-layers ${layers} --index-drafters=step --low-load=${loads} --high-load=${loads} --out-path sosd_build_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --do-benchmark
  set +x
  done
}

for ((i = 0; i < ${#SOSD_BLOBS[@]}; i++)) do
  if [[ $ACTION == "autocomplete" ]]
  then
    autocomplete "${SOSD_BLOBS[$i]}"
  elif [[ $ACTION == "build" ]]
  then
    build "${SOSD_BLOBS[$i]}" "${DATA_LAYOUTS[$i]}"
  elif [[ $ACTION == "benchmark" ]]
  then
    benchmark "${SOSD_BLOBS[$i]}" "${DATA_LAYOUTS[$i]}"
  fi
done
