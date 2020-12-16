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

python3 tables/run_queries.py
"""

import glob
from pprint import pprint

from google.cloud import bigquery as cloud_bigquery

client = cloud_bigquery.Client()


def run_query(filepath: str) -> cloud_bigquery.table.RowIterator:
  with open(filepath) as sql:
    query_job = client.query(sql.read())
  return query_job.result()


def rebuild_all_tables() -> None:
  for filepath in glob.glob('table/queries/*.sql'):
    try:
      run_query(filepath)
    except Exception as ex:  # pylint: disable=broad-except
      pprint(('Failed SQL query', filepath))
      pprint(ex)


if __name__ == '__main__':
  rebuild_all_tables()
