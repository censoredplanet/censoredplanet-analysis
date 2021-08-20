# Development

Install the dependencies with 

    pip install -r requirements.txt

## Testing Changes

All tests and lint checks will be run on pull requests using
[github actions](https://github.com/Jigsaw-Code/censoredplanet-analysis/actions)

To typecheck all files install `mypy` and run

    mypy **/*.py --namespace-packages --explicit-package-bases

To format all files install `yapf` and run

    yapf --in-place --recursive .

To get all lint errors install `pylint` and run

    python -m pylint **/*.py --rcfile=setup.cfg

To run unit tests run

    python -m unittest

There are a few end-to-end tests which aren't run by the unittest framework
because they require cloud resource access. To run these tests manually use the
command

    python -m unittest pipeline.manual_e2e_test.PipelineManualE2eTest

## Running development pipelines

To test changes before merging them into production it helps to run development
pipelines. These pipelines run in the cloud over a few days of data, and write
to side tables named after the username of the developer.

To test a full data reload:

    python -m pipeline.run_beam_tables --env=user --scan_type=http --full

To test an appending data reload. (This requires a table to already exist):

    python -m pipeline.run_beam_tables --env=user --scan_type=http

Options for `scan_type` are `echo`, `discard`, `http`, `https` and `satellite`

To test specific dates run a pipeline like

   python -m pipeline.run_beam_tables --env=user --scan_type=http --start_date=2021-01-01 --end_date=2021-01-30

If only `start_date` is specified the pipeline will run from that date until
the latest data.

If only `end_date` is specified the pipeline will run from the earliest data
to that date.

If neither is specified the pipeline will automatically pick some reasonable
dates to help avoid unintentially running test pipelines over large amounts of
data. For appending pipelines this will be the latest week of data, and for
full pipelines it will be the previous week of data.

## Access

If you're authenticating to `firehook-censoredplanet` as the user represented
by `~/.config/gcloud/application_default_credentials.json` then authentication
should work by default. If you're using another key file then run

   export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/legacy_credentials/${USER_EMAIL}/adc.json

to make your credential accessible.
