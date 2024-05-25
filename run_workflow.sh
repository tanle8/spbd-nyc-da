#!/bin/bash

# Absolute path to the config file:
CONFIG_PATH="/Users/tanle/Source/epita/spbd-nyc-da/config/config.json"

# Load configurations
CONFIG=$(<$CONFIG_PATH)
BUCKET_NAME=$(echo $CONFIG | jq -r '.bucket_name')
CLUSTER_NAME=$(echo $CONFIG | jq -r '.cluster_name')
REGION=$(echo $CONFIG | jq -r '.region')
MACHINE_TYPE=$(echo $CONFIG | jq -r '.machine_type')
NUM_WORKERS=$(echo $CONFIG | jq -r '.num_workers')
IMAGE_VERSION=$(echo $CONFIG | jq -r '.image_version')
PROJECT_ID=$(echo $CONFIG | jq -r '.project_id')
SCRIPTS=$(echo $CONFIG | jq -r '.scripts[]')

# Create a Dataproc Workflow Template
gcloud dataproc workflow-templates create ${CLUSTER_NAME}-template \
    --region=${REGION}

# Set managed cluster configuration
gcloud dataproc workflow-templates set-managed-cluster ${CLUSTER_NAME}-template \
    --region=${REGION} \
    --cluster-name=${CLUSTER_NAME} \
    --master-machine-type=${MACHINE_TYPE} \
    --worker-machine-type=${MACHINE_TYPE} \
    --num-workers=${NUM_WORKERS} \
    --image-version=${IMAGE_VERSION}

# Add jobs to the workflow template
for SCRIPT in ${SCRIPTS}; do
    gcloud dataproc workflow-templates add-job pyspark gs://${BUCKET_NAME}/scripts/${SCRIPT} \
        --step-id=${SCRIPT%.*} \
        --workflow-template=${CLUSTER_NAME}-template \
        --region=${REGION}
done

# Instantiate the workflow
gcloud dataproc workflow-templates instantiate ${CLUSTER_NAME}-template --region=${REGION}
