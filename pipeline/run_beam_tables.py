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
"""Code for handling the running of beam pipelines.

To re-run the full beam pipeline manually (and blow away any old tables) run

python -m pipeline.run_beam_tables --env=prod --incremental=False
"""

import argparse
import concurrent.futures
import datetime
import os
import pwd
from typing import Optional, List

from pipeline import beam_tables
from pipeline.metadata import caida_ip_metadata
from pipeline.metadata import maxmind


def run_parallel_pipelines(runner: beam_tables.ScanDataBeamPipelineRunner,
                           dataset: str,
                           scan_types: List[str],
                           incremental_load: bool,
                           start_date: Optional[datetime.date] = None,
                           end_date: Optional[datetime.date] = None) -> bool:
  """Runs beam pipelines for different scan types in parallel.

  Args:
    runner: ScanDataBeamPipelineRunner to run pipelines
    dataset: dataset name to write to like 'prod' or 'laplante
    scan_types: list of scan types to run ['echo' 'http']
    incremental_load: boolean. If true, only load the latest new data, if false
      reload all data.
    start_date: date object, only files after or at this date will be read.
      Mostly only used during development.
    end_date: date object, only files at or before this date will be read.
      Mostly only used during development.

  Returns:
    True on success

  Raises:
    Exception: if any of the pipelines fail or don't finish.
  """
  with concurrent.futures.ThreadPoolExecutor() as pool:
    futures = []
    for scan_type in scan_types:
      table_name = beam_tables.get_table_name(dataset, scan_type,
                                              beam_tables.BASE_TABLE_NAME)
      job_name = beam_tables.get_job_name(table_name, incremental_load)

      future = pool.submit(runner.run_beam_pipeline, scan_type,
                           incremental_load, job_name, table_name, start_date,
                           end_date)
      futures.append(future)

    finished, pending = concurrent.futures.wait(
        futures, return_when=concurrent.futures.FIRST_EXCEPTION)

    # Raise any exceptions
    for future in finished:
      future.result()

    if pending:
      raise Exception('Some pipelines failed to finish: ', pending,
                      'finished: ', finished)
    return True


def run_user_pipelines(runner: beam_tables.ScanDataBeamPipelineRunner,
                       dataset: str, scan_types: List[str],
                       incremental_load: bool) -> None:
  """Run user beam pipelines for testing.

  Users only needs to load a week of data for testing.
  - For incremental loads use the last week of data.
  - for full loads use data from two weeks ago.
  (This is to make it easier to test the full and then incremental loads
  together when developing.)

  Args:
    runner: ScanDataBeamPipelineRunner
    dataset: string of a dataset to write to. When running user pipelines this
      should be the user's name, like 'laplante'
    scan_types: List of 'echo', 'discard', 'http', and/or 'https'
    incremental_load: boolean. If true, only load the latest new data.
  """
  if incremental_load:
    end_day = datetime.date.today()
  else:
    end_day = datetime.date.today() - datetime.timedelta(days=7)
  start_day = end_day - datetime.timedelta(days=7)

  run_parallel_pipelines(
      runner,
      dataset,
      scan_types,
      incremental_load,
      start_date=start_day,
      end_date=end_day)


def get_firehook_beam_pipeline_runner(
) -> beam_tables.ScanDataBeamPipelineRunner:
  """Factory function to get a beam pipeline class with firehook values."""
  # importing here to avoid beam pickling issues
  import firehook_resources  # pylint: disable=import-outside-toplevel

  return beam_tables.ScanDataBeamPipelineRunner(
      firehook_resources.PROJECT_NAME, beam_tables.SCAN_BIGQUERY_SCHEMA,
      firehook_resources.INPUT_BUCKET, firehook_resources.BEAM_STAGING_LOCATION,
      firehook_resources.BEAM_TEMP_LOCATION, caida_ip_metadata.CaidaIpMetadata,
      firehook_resources.CAIDA_FILE_LOCATION,
      firehook_resources.SIGNATURE_FILE_LOCATION, maxmind.MaxmindIpMetadata,
      firehook_resources.MAXMIND_FILE_LOCATION)


def main(parsed_args: argparse.Namespace) -> None:
  """Parse namespace arguments and run a beam pipeline based on them.

  Args:
    parsed_args: an argparse namespace with 'full', 'env' and 'scan_type' args.
  """
  incremental = not parsed_args.full

  if parsed_args.scan_type == 'all':
    selected_scan_types = list(beam_tables.ALL_SCAN_TYPES)
    # TODO turn back on DNS once it works in the cloud.
    selected_scan_types.remove('dns')
  else:
    selected_scan_types = [parsed_args.scan_type]

  firehook_runner = get_firehook_beam_pipeline_runner()

  if parsed_args.env == 'user':
    username = pwd.getpwuid(os.getuid()).pw_name
    run_user_pipelines(firehook_runner, username, selected_scan_types,
                       incremental)
  elif parsed_args.env == 'prod':
    run_parallel_pipelines(firehook_runner, beam_tables.PROD_DATASET_NAME,
                           selected_scan_types, incremental)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Run a beam pipeline over scans')
  parser.add_argument(
      '--full',
      action='store_true',
      default=False,
      help='Run over all files and not just the latest (rebuilds tables)')
  parser.add_argument(
      '--env',
      type=str,
      default='user',
      choices=['user', 'prod'],
      help='Whether to write to prod or test tables')
  parser.add_argument(
      '--scan_type',
      type=str,
      default='echo',
      choices=['all'] + list(beam_tables.ALL_SCAN_TYPES),
      help='Which type of scan to run over')
  args = parser.parse_args()

  main(args)
