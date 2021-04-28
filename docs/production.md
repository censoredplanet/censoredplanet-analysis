# Production

## Running the Automated Pipeline

There are two main top-level pieces of the production pipeline

 `python -m mirror.data_transfer`

This sets up a daily data transfer job to copy scan files from the Censored
Planet cloud bucket to an internal bucket.

 `python -m schedule_pipeline`

This does some additional daily data processing and schedules a daily
incremental Apache Beam pipeline over the data. It expects to be run via a
Docker container on a GCE machine.

 `./deploy.sh prod`

Will deploy the main pipeline loop to a GCE machine. If the machine does not
exist it will be created, if it does exist it will be updated.

## Turning off the Automated Pipeline

So stop running the automated pipeline run

 `./deploy.sh delete`

Which will delete the GCE machine

## Running Manually

Individual pieces of the pipeline can be run manually.

### Mirroring Data

These scripts pull in a large amount of data from external datasources and
mirror it in the correct locations in google cloud buckets.

 `python -m mirror.untar_files.sync_files`

Decompresses any Censored Planet scan files which have been transfered into the
project but are still compressed. This can also be used as a backfill tool to
decompress missing files.

 `python -m mirror.routeviews.sync_routeviews`

Transfers in the latest missing CAIDA routeview files. (Can only mirror in data
from the last 30 days of data.)

 `python -m mirror.routeviews.bulk_download`

Transfers in all CAIDA routeview files from a certain date. This is used for
backfilling data from older than 30 days.

In all cases to fix missing or incorrect data simple delete the incorrect data
from the google cloud bucket and re-run the appropriate script to re-mirror it.

 `python -m mirror.internal.sync`

Downloads most recent version of Censored Planet resources locally. These
updated resources can then be committed and deployed.

### Processing Data

 `python -m pipeline.run_beam_tables --env=prod --full`

Runs the full Apache Beam pipeline. This will re-process all data and rebuild
existing base tables.

 `python -m pipeline.run_beam_tables --env=prod`

Runs an appending Apache Beam pipeline. This will check for new unprocessed
data, and process and append it to the base tables if they exist.

 `python -m table.run_queries`

Runs queries to recreate any tables derived from the base tables.

## Backfilling data

When changing the output schema of the pipeline it is nessesary to backfill the
old data to match the new format. This is because the nightly pipeline only
appends newly detected data. If the schema changes then the nightly append will
fail because of a schema mismatch.

Here are the steps to run a backfill:

*    `./deploy.sh delete` turn off the nightly pipeline so it doesn't conflict
     with the backfill
*    `python -m pipeline.run_beam_tables --env=prod --scan_type=all` to run
     manual backfill jobs, this can take several hours.
*    Check if the backfill worked the next day
*    if so run `./deploy.sh prod` at head to turn the pipeline back on with
     the new code