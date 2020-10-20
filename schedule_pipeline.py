# Copyright 2020 Google LLC
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

# pyformat: disable
r"""Orchestrate the pieces of the Censored Planet Data Pipeline.

To deploy to GCE

Build

gcloud builds submit . --tag gcr.io/firehook-censoredplanet/pipeline

Run

gcloud compute instances create-with-container firehook-censoredplanet \
--container-image gcr.io/firehook-censoredplanet/pipeline:latest \
--machine-type e2-highmem-4 --zone us-east1-b --boot-disk-size 100GB \
--service-account 654632410498-compute@developer.gserviceaccount.com \
--scopes=bigquery,cloud-platform,default

or (if container already exists)

gcloud compute instances update-container firehook-censoredplanet \
--container-image gcr.io/firehook-censoredplanet/pipeline:latest


To run locally

Build

docker build --tag firehook-censoredplanet .

Run

gcloud iam service-accounts keys create \
~/.config/gcloud/654632410498-compute_credentials.json \
--iam-account 654632410498-compute@developer.gserviceaccount.com

docker run -it \
-v $HOME/.config/gcloud:$HOME/.config/gcloud \
-e GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/654632410498-compute_credentials.json \
firehook-censoredplanet
"""
# pyformat: enable
import subprocess
import time

import schedule

from mirror.decompress_files.decompress import get_firehook_scanfile_decompressor
from mirror.routeviews.update import get_firehook_routeview_updater
from table.run_queries import rebuild_all_tables


def run_pipeline():
  """Steps of the pipeline to run nightly."""
  get_firehook_scanfile_decompressor().decompress_all_missing_files()
  get_firehook_routeview_updater().transfer_routeviews()

  # This is a very weird hack.
  # We execute the beam pipeline as a seperate process
  # because beam really doesn't like it when the main file for a pipeline
  # execution is not the same file the pipeline run call is made in.
  # It would require all the deps to be packaged and installed on the workers
  # which in our case requires packaging up many google cloud packages
  # which is slow (hangs basic worker machines) and wasteful.
  subprocess.run(['python3', 'pipeline/beam_tables.py', '--env=prod'],
                 check=True,
                 stdout=subprocess.PIPE)

  rebuild_all_tables()


def run():
  run_pipeline()  # run once when starting to catch new errors when deploying

  schedule.every().day.at('04:00').do(run_pipeline)

  while True:
    schedule.run_pending()
    wait = schedule.idle_seconds()
    print('Waiting {} seconds until the next run'.format(wait))
    time.sleep(wait)


if __name__ == '__main__':
  run()
