#!/bin/bash

# Copyright 2020 Jigsaw Operations LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# to run
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
# GCP zone to deploy to
zone="us-east1-b"

if [[ "${action}" == "backfill" ]]; then
  docker build --tag ${project} .

  # Get service credentials if they are missing
  if [[ ! -f ~/.config/gcloud/${service_account_id}_compute_credentials.json ]]; then
    gcloud iam service-accounts keys create \
    ~/.config/gcloud/${service_account_id}_compute_credentials.json \
    --iam-account ${service_account_id}-compute@developer.gserviceaccount.com
  fi

  # --entrypoint 'python3 -m pipeline.run_beam_tables --env=dev --full'
  docker run -it \
  -v $HOME/.config/gcloud:$HOME/.config/gcloud \
  -e GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/${service_account_id}_compute_credentials.json \
  ${project}

elif [[ "${action}" == "dev" ]]; then
  # For builders outside the VPC security perimeter the build will succeed
  # but throw a logging error, so we ignore errors here
  gcloud builds submit . --tag gcr.io/${project}/pipeline --project ${project} || true

  # Instead check that the latest build succeeded
  if gcloud builds list --limit=1 --project ${project} | grep SUCCESS; then
    echo "Latest build was successful"
  else
    echo "Latest build did not succeed"
    gcloud builds list --limit=1 --project ${project}
    exit 1
  fi

  # If the instance already exists
  if gcloud compute instances list --project ${project} | grep -q ${project}; then
    # update
    gcloud compute instances update-container ${project} --zone ${zone} \
    --container-image gcr.io/${project}/pipeline:latest --project ${project}
  else
    # otherwise create a new instance
    gcloud compute instances create-with-container ${project} \
    --container-image gcr.io/${project}/pipeline:latest \
    --machine-type e2-highmem-4 --zone ${zone} --boot-disk-size 50GB \
    --service-account ${service_account_id}-compute@developer.gserviceaccount.com \
    --scopes=bigquery,cloud-platform,default --project ${project}
  fi

elif [[ "${action}" == "delete" ]]; then
  gcloud compute instances delete ${project} --project ${project} --zone ${zone}

else
  echo "Unknown action ${action}"
fi
