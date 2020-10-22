# censoredplanet-analysis

Pipeline for analysing CensoredPlanet data.

Take raw data from the
[Censored Planet Observatory](https://censoredplanet.org/data/raw) and run it
through a pipeline to create bigquery tables for easier data analysis.

Because of the size of the data involved (many TB) this project requires a
devoted Google Cloud project to run in. It is not reccomended to run yourself,
(please contact us if this is your use case) but the code is made available for
anyone who wants to understand how the data pipeline works.

## Running as an automated pipeline

There are two main top-level pieces

`python mirror/scans.py`

This sets up a daily data transfer job to copy scan files from the Censored
Planet cloud bucket to an internal bucket.

`python schedule_pipeline.py`

This does some additional daily data processing and schedules an daily
incremental Apache Beam pipeline over the data. It is run via a Docker container
on a GCE machine.

## Running manually

Individual pieces of the pipeline can be run manually.

### Mirroring Data

These scripts pull in a large amount of data from external datasources and
mirror it in the correct locations in google cloud buckets.

`python mirror/scans.py`

Sets up a daily data transfer job to mirror in scan files from Censored Planet.

`python mirror/decompress_files/decompress.py`

Decompresses any Censored Planet scan files which have been transfered into the
project but are still compressed. This can also be used as a backfill tool to
decompress missing files.

`python mirror/routeviews/update.py`

Transfers in the latest missing CAIDA routeview files.

`python mirror/routeviews/bulk_download.py`

Transfers in all CAIDA routeview files from a certain date. This is used for
backfilling data.

### Processing Data

`python pipeline/beam_tables.py --env=prod --full`

Runs the full Apache Beam pipeline. This will re-process all data and rebuild
existing base tables.

`python table/run_queries.py`

Runs queries to recreate any tables derived from the base tables.

## Testing

To run all tests run

`python -m unittest`

To typecheck all files install mypy and run

`mypy ^env**/*.py`

This produces some spurious errors because of missing types in dependencies
