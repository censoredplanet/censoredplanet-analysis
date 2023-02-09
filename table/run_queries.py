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
"""Rebuild any tables derived from the base scan tables.

Run as

python3 -m table.run_queries
"""

import argparse
from pprint import pprint
from typing import List, Optional

from google.cloud import bigquery as cloud_bigquery  # type: ignore

import firehook_resources

PROJECT_NAME_PLACEHOLDER = 'PROJECT_NAME'
BASE_PLACEHOLDER = 'BASE_DATASET'
DERIVED_PLACEHOLDER = 'DERIVED_DATASET'
EARLIEST_DATE_PLACEHOLDER = 'EARLIEST_DATE'

DEFAULT_BASE_DATASET = 'base'
DEFAULT_DERIVED_DATASET = 'derived'


def _run_query(
    client: cloud_bigquery.Client, filepath: str, project_name: str,
    base_dataset: str, derived_dataset: str,
    earliest_date: Optional[str]) -> cloud_bigquery.table.RowIterator:
  with open(filepath, encoding='utf-8') as sql:
    query = sql.read()

    query = query.replace(PROJECT_NAME_PLACEHOLDER, project_name)
    query = query.replace(BASE_PLACEHOLDER, base_dataset)
    query = query.replace(DERIVED_PLACEHOLDER, derived_dataset)
    if earliest_date:
      query = query.replace(EARLIEST_DATE_PLACEHOLDER, earliest_date)

    query_job = client.query(query)
  return query_job.result()


def _run_scripts(script_filepaths: List[str], project_name: str,
                 base_dataset: str, derived_dataset: str,
                 earliest_date: Optional[str]) -> None:
  """Rebuild all the derived bigquery tables from the base table data.

  Args:
    project_name: name of gcloud project
    base_dataset: bq dataset name to read from (ex: 'base')
    derived_dataset: bq dataset name to write to (ex: 'derived')
  """
  client = cloud_bigquery.Client(project=project_name)

  for filepath in script_filepaths:
    try:
      _run_query(client, filepath, project_name, base_dataset, derived_dataset,
                 earliest_date)
    except Exception as ex:
      pprint(('Failed SQL query', filepath))
      raise ex


def _get_earliest_missing_date_hyperquack(project_name: str, base_dataset: str,
                                          derived_dataset: str) -> str:
  """Find the earliest date which exists in the base tables but not the derived tables. 

  Args:
    project_name: name of gcloud project
    base_dataset: bq dataset name to read from (ex: 'base')
    derived_dataset: bq dataset name to write to (ex: 'derived')

  Raises:
    Exception if the base and derived tables have the same dates
  """
  base_dataset_prefix = f'{project_name}.{base_dataset}'

  echo_table_name = f'{base_dataset_prefix}.echo_scan'
  discard_table_name = f'{base_dataset_prefix}.discard_scan'
  http_table_name = f'{base_dataset_prefix}.http_scan'
  https_table_name = f'{base_dataset_prefix}.https_scan'

  echo_table_dates = _get_all_dates(project_name, echo_table_name)
  discard_table_dates = _get_all_dates(project_name, discard_table_name)
  http_table_dates = _get_all_dates(project_name, http_table_name)
  https_table_dates = _get_all_dates(project_name, https_table_name)

  # TODO: could this cause problems if one base table is updated days ahead of another
  # and then blocks the dates from the second table from getting appended?
  all_base_table_dates = echo_table_dates + discard_table_dates + http_table_dates + https_table_dates

  derived_table_name = f'{project_name}.{derived_dataset}.merged_reduced_scans_v2'
  derived_table_dates = _get_all_dates(project_name, derived_table_name)

  missing_dates = List(set(derived_table_dates) - set(all_base_table_dates))

  if not missing_dates:
    raise Exception(
        f"All the dates in the base tables at {base_dataset_prefix} already exist in the derived table {derived_table_name}"
    )

  pprint("Missing dates:")
  pprint(missing_dates)

  earliest_date = sorted(missing_dates)[0]

  pprint("Earliest missing date:")
  pprint(earliest_date)

  return earliest_date


def _get_all_dates(project_name: str, table_name: str) -> List[str]:
  client = cloud_bigquery.Client(project=project_name)

  query = f'SELECT DISTINCT(date) AS date FROM `{table_name}`'
  rows = client.query(query)
  dates = [row.date for row in rows]
  return dates


def rebuild_hyperquack_table(project_name: str, base_dataset: str,
                             derived_dataset: str) -> None:
  _run_scripts(['table/queries/merged_reduced_scans.sql'], project_name,
               base_dataset, derived_dataset, None)


def rebuild_satellite_table(project_name: str, base_dataset: str,
                            derived_dataset: str) -> None:
  _run_scripts(['table/queries/derived_satellite_scans.sql'], project_name,
               base_dataset, derived_dataset, None)


def append_hyperquack_table(project_name: str, base_dataset: str,
                            derived_dataset: str) -> None:
  earliest_missing_date = _get_earliest_missing_date_hyperquack(
      project_name, base_dataset, derived_dataset)

  _run_scripts(['table/queries/merged_reduced_scans_append.sql'], project_name,
               base_dataset, derived_dataset, earliest_missing_date)


def append_satellite_table(project_name: str, base_dataset: str,
                           derived_dataset: str) -> None:
  earliest_missing_date = _get_earliest_missing_date_satellite(
      project_name, base_dataset, derived_dataset)

  _run_scripts(['table/queries/derived_satellite_scans_append.sql'],
               project_name, base_dataset, derived_dataset,
               earliest_missing_date)


def rebuild_all_tables(project_name: str, base_dataset: str,
                       derived_dataset: str) -> None:
  """Rebuild all the derived bigquery tables from the base table data.

  Args:
    project_name: name of gcloud project
    base_dataset: bq dataset name to read from (ex: 'base')
    derived_dataset: bq dataset name to write to (ex: 'derived')
  """
  rebuild_hyperquack_table(project_name, base_dataset, derived_dataset)
  rebuild_satellite_table(project_name, base_dataset, derived_dataset)


def append_all_tables(project_name: str, base_dataset: str,
                      derived_dataset: str) -> None:
  """Rebuild all the derived bigquery tables from the base table data.

  Args:
    project_name: name of gcloud project
    base_dataset: bq dataset name to read from (ex: 'base')
    derived_dataset: bq dataset name to write to (ex: 'derived')
  """
  append_hyperquack_table(project_name, base_dataset, derived_dataset)
  append_satellite_table(project_name, base_dataset, derived_dataset)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Run a beam pipeline over scans')

  parser.add_argument(
      '--input',
      type=str,
      default=DEFAULT_BASE_DATASET,
      help='Input dataset to query from')
  parser.add_argument(
      '--output',
      type=str,
      required=True,
      help=
      'Output dataset to write to. To write to the dashboard table use --output=derived'
  )
  parser.add_argument(
      '--env',
      type=str,
      default='dev',
      choices=['dev', 'prod'],
      help='Whether to use the prod or dev project')
  parser.add_argument(
      '--full',
      action='store_true',
      default=False,
      help='Fully rebuild the tables instead of just appending new data')
  args = parser.parse_args()

  if args.env == 'dev':
    bq_project_name = firehook_resources.DEV_PROJECT_NAME
  if args.env == 'prod':
    bq_project_name = firehook_resources.PROD_PROJECT_NAME

  if args.full:
    rebuild_all_tables(bq_project_name, args.input, args.output)
  else:
    append_all_tables(bq_project_name, args.input, args.output)
