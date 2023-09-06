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
import glob
from pprint import pprint

from google.cloud import bigquery as cloud_bigquery  # type: ignore

import firehook_resources

PROJECT_NAME_PLACEHOLDER = 'PROJECT_NAME'
BASE_PLACEHOLDER = 'BASE_DATASET'
DERIVED_PLACEHOLDER = 'DERIVED_DATASET'

DEFAULT_BASE_DATASET = 'base'
DEFAULT_DERIVED_DATASET = 'derived'


def _run_query(client: cloud_bigquery.Client, filepath: str, project_name: str,
               base_dataset: str,
               derived_dataset: str) -> cloud_bigquery.table.RowIterator:
  with open(filepath, encoding='utf-8') as sql_file:
    query_text = sql_file.read()
    return _run_query_text(client, query_text, project_name, base_dataset,
                           derived_dataset)


# This is used to patch the query for E2E testing
def _run_query_text(client: cloud_bigquery.Client, query: str,
                    project_name: str, base_dataset: str,
                    derived_dataset: str) -> cloud_bigquery.table.RowIterator:
  query = query.replace(PROJECT_NAME_PLACEHOLDER, project_name)
  query = query.replace(BASE_PLACEHOLDER, base_dataset)
  query = query.replace(DERIVED_PLACEHOLDER, derived_dataset)

  query_job = client.query(query)
  return query_job.result()


def rebuild_all_tables(project_name: str,
                       base_dataset: str = DEFAULT_BASE_DATASET,
                       derived_dataset: str = DEFAULT_DERIVED_DATASET) -> None:
  """Rebuild all the derived bigquery tables from the base table data.

  Args:
    project_name: name of gcloud project
    base_dataset: bq dataset name to read from (ex: 'base')
    derived_dataset: bq dataset name to write to (ex: 'derived')
  """
  client = cloud_bigquery.Client(project=project_name)

  for filepath in glob.glob('table/queries/*.sql'):
    try:
      _run_query(client, filepath, project_name, base_dataset, derived_dataset)
    except Exception as ex:
      pprint(('Failed SQL query', filepath))
      raise ex


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
  args = parser.parse_args()

  if args.env == 'dev':
    bq_project_name = firehook_resources.DEV_PROJECT_NAME
  if args.env == 'prod':
    bq_project_name = firehook_resources.PROD_PROJECT_NAME

  rebuild_all_tables(
      bq_project_name, base_dataset=args.input, derived_dataset=args.output)
