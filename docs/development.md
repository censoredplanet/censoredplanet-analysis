# Development

## Testing Changes

All tests and lint checks will be run on pull requests using
[github actions](https://github.com/Jigsaw-Code/censoredplanet-analysis/actions)

To run all tests run

`python -m unittest`

There are a few end-to-end tests which aren't run by the unittest framework
because they require cloud resource access. To run these tests manually use the
command

`python -m unittest pipeline.manual_e2e_test.PipelineManualE2eTest`

To typecheck all files install `mypy` and run

`mypy **/*.py --namespace-packages --explicit-package-bases`

To format all files install `yapf` and run

`yapf --in-place --recursive .`

To get all lint errors install `pylint` and run

`python -m pylint **/*.py --rcfile=setup.cfg`

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

Downloads most recent version of Censored Planet resources locally.

### Processing Data

`python -m pipeline.run_beam_tables --env=prod --full`

Runs the full Apache Beam pipeline. This will re-process all data and rebuild
existing base tables.

`python -m table.run_queries`

Runs queries to recreate any tables derived from the base tables.

