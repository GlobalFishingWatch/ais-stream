#!/bin/bash
set -e

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
source ${THIS_SCRIPT_DIR}/config.sh

REMOTE_OPTS="--runner=DataflowRunner "
LOCAL_OPTS="--runner=DirectRunner"

JOB_NAME=ais-stream-decode-$(date +%Y%m%d%H%M%S)


python stream_decode \
  --job_name=${JOB_NAME} \
  ${REMOTE_OPTS} \
  --streaming \
  --diskSizeGb=50 \
  --usePublicIps=false \
  --log_level=INFO \
  --log_args \
  --project=${GCP_PROJECT} \
  --region=us-central1 \
  --input_topic=${PUBSUB_TOPIC} \
  --output_path=${OUTPUT_PATH} \
  --output_table=${OUTPUT_TABLE} \
  --schema=@${THIS_SCRIPT_DIR}/bigquery-schema.json \
  --window_size=1 \
  --temp_location=gs://${TEMP_GCS_BUCKET}/temp \
  --requirements_file=${THIS_SCRIPT_DIR}/requirements.txt \
  --setup_file=${THIS_SCRIPT_DIR}/setup.py \


