#!/bin/bash

# Copy this template file to config.sh and edit the template values

GCP_PROJECT=[PROJECT_ID]
PUBSUB_TOPIC=projects/${GCP_PROJECT}/topics/[TOPIC]
TEMP_GCS_BUCKET=[BUCKET_NAME]
OUTPUT_PATH=gs://[PATH]/
OUTPUT_TABLE=[DATASET].[TABLE]
SCHEMA=./bigquery-schema.json