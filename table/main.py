# Copyright 2020 Google LLC
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

python3 tables/main.py
"""

from google.cloud import bigquery as cloud_bigquery

client = cloud_bigquery.Client()


def run_query(sql: str, destination_table: str):
  job_config = cloud_bigquery.QueryJobConfig(
      destination=destination_table,
      allow_large_results=True,
      create_disposition=cloud_bigquery.job.CreateDisposition.CREATE_IF_NEEDED,
      write_disposition=cloud_bigquery.job.WriteDisposition.WRITE_TRUNCATE)
  query_job = client.query(sql, job_config=job_config)
  query_job.result()


def rebuild_all_tables():
  # TODO refactor this process to be more structured once we have more scripts.
  sql = open('table/queries/merged_scans.sql').read()
  run_query(sql, 'firehook-censoredplanet.merged_results.cleaned_error_scans')


if __name__ == '__main__':
  rebuild_all_tables()
