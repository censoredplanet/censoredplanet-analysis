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

client = cloud_bigquery.Client(project=firehook_resources.PROJECT_NAME)

BASE_PLACEHOLDER = 'BASE_DATASET'
DERIVED_PLACEHOLDER = 'DERIVED_DATASET'

DEFAULT_BASE_DATASET = 'base'
DEFAULT_DERIVED_DATASET = 'derived'


def _run_query(filepath: str, base_dataset: str,
               derived_dataset: str) -> cloud_bigquery.table.RowIterator:
  with open(filepath, encoding='utf-8') as sql:
    query = sql.read()

    query = query.replace(BASE_PLACEHOLDER, base_dataset)
    query = query.replace(DERIVED_PLACEHOLDER, derived_dataset)

    query_job = client.query(query)
  return query_job.result()


def rebuild_all_tables(base_dataset: str = DEFAULT_BASE_DATASET,
                       derived_dataset: str = DEFAULT_DERIVED_DATASET) -> None:
  for filepath in glob.glob('table/queries/derived_satellite_scans.sql'):
    try:
      _run_query(filepath, base_dataset, derived_dataset)
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
      help='Output dataset to write to. To write to prod use --output=derived')
  args = parser.parse_args()

  rebuild_all_tables(base_dataset=args.input, derived_dataset=args.output)
