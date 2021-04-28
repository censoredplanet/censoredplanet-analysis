# Production

## Running an Automated Pipeline

There are two main top-level pieces

`python -m mirror.data_transfer`

This sets up a daily data transfer job to copy scan files from the Censored
Planet cloud bucket to an internal bucket.

`python -m schedule_pipeline`

This does some additional daily data processing and schedules an daily
incremental Apache Beam pipeline over the data. It expects to be run via a
Docker container on a GCE machine.

`./deploy.sh prod`

Will deploy the main pipeline loop to a GCE machine
