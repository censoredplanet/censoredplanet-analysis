# pyformat: disable
r"""Orchestrate the pieces of the Censored Planet Data Pipeline.

To deploy to GCE

Build

gcloud builds submit . --tag gcr.io/firehook-censoredplanet/pipeline

Run

gcloud compute instances create-with-container firehook-censoredplanet \
--container-image gcr.io/firehook-censoredplanet/pipeline:latest \
--machine-type n1-highmem-2 \
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

from pipeline.main import incrementally_process_prod
from transfer.decompress_files.main import decompress_all_missing_files
from transfer.routeviews.main import transfer_routeviews


def run():
  decompress_all_missing_files()
  transfer_routeviews()
  incrementally_process_prod()


if __name__ == "__main__":
  run()
