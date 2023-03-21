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
"""Orchestrate the pieces of the Censored Planet Data Pipeline.

This script is means to be run on a GCE machine.
To deploy to GCE use the deploy.sh script.
"""
import argparse
import subprocess
import sys
import time

from google.cloud import error_reporting  # type: ignore
import schedule

from mirror.routeviews.sync_routeviews import get_firehook_routeview_mirror
from mirror.internal.sync import get_censoredplanet_mirror
from table.run_queries import rebuild_all_tables
import firehook_resources


def run_pipeline(env: str) -> None:
  """Steps of the pipeline to run nightly.

  Args:
    env: one of 'dev' or 'prod', which gcloud project env to use.
  """
  try:
    get_firehook_routeview_mirror(env).sync()
    get_censoredplanet_mirror().sync()

    # This is a very weird hack.
    # We execute the beam pipeline as a seperate process
    # because beam really doesn't like it when the main file for a pipeline
    # execution is not the same file the pipeline run call is made in.
    # It would require all the deps to be packaged and installed on the workers
    # which in our case requires packaging up many google cloud packages
    # which is slow (hangs basic worker machines) and wasteful.
    subprocess.run([
        sys.executable, '-m', 'pipeline.run_beam_tables', f'--env={env}',
        '--scan_type=all', '--export_gc', '--export_bq'
    ],
                   check=True,
                   stdout=subprocess.PIPE)

    if env == 'dev':
      rebuild_all_tables(firehook_resources.DEV_PROJECT_NAME)
    if env == 'prod':
      rebuild_all_tables(firehook_resources.PROD_PROJECT_NAME)
  except Exception:
    # If something goes wrong also log to GCP error console.
    error_reporting.Client().report_exception()
    raise


def run(env: str) -> None:
  """Steps of the pipeline to run nightly.

  Args:
    env: one of 'dev' or 'prod', which gcloud project env to use.
  """
  run_pipeline(env)  # run once when starting to catch new errors when deploying

  schedule.every().day.at('04:00').do(run_pipeline, env)

  while True:
    schedule.run_pending()
    wait = schedule.idle_seconds()
    print(f'Waiting {wait} seconds until the next run')
    time.sleep(wait)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      description='Schedule a nightly pipeline run.')
  parser.add_argument(
      '--env',
      type=str,
      default='dev',
      choices=['dev', 'prod'],
      help='Whether to write to prod or dev gcloud project')
  args = parser.parse_args()

  run(args.env)
