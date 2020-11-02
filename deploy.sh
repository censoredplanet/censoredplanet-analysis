#!/bin/bash

# to run
#
# ./deploy local
# to deploy to a local docker instance
#
# ./deploy prod
# to deploy to production
#
# ./deploy delete
# to delete the prod instance (usually because you have changed
# some instance boot value and want to recreate it from scratch)

# exit on error
set -e

action=$1

# CHANGE THESE VALUES TO MATCH YOUR PROJECT
# String id of a google cloud project
project="firehook-censoredplanet"
# Int id of a cloud service account with the correct access permissions
service_account_id="654632410498"

if [[ "${action}" == "local" ]]; then
  docker build --tag ${project} .

  # Get service credentials if they are missing
  if [[ ! -f ~/.config/gcloud/${service_account_id}_compute_credentials.json ]]; then
    gcloud iam service-accounts keys create \
    ~/.config/gcloud/${service_account_id}_compute_credentials.json \
    --iam-account ${service_account_id}-compute@developer.gserviceaccount.com
  fi

  docker run -it \
  -v $HOME/.config/gcloud:$HOME/.config/gcloud \
  -e GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/${service_account_id}_compute_credentials.json \
  ${project}

elif [[ "${action}" == "prod" ]]; then
  gcloud builds submit . --tag gcr.io/${project}/pipeline

  # if instance already exists
  if gcloud compute instances list | grep -q ${project}; then
    # update
    gcloud compute instances update-container ${project} \
    --container-image gcr.io/${project}/pipeline:latest
  else
    # otherwise create new instance
    gcloud compute instances create-with-container ${project} \
    --container-image gcr.io/${project}/pipeline:latest \
    --machine-type e2-highmem-16 --zone us-east1-b --boot-disk-size 500GB \
    --service-account ${service_account_id}-compute@developer.gserviceaccount.com \
    --scopes=bigquery,cloud-platform,default
  fi

elif [[ "${action}" == "delete" ]]; then
  gcloud compute instances delete ${project}

else
  echo "Unknown action ${action}"
fi














