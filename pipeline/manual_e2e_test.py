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
"""E2E test which runs a full pipeline over a few lines of test data.

The pipeline is different from the usual Dataflow pipeline, since it runs
locally, and only reads a few lines of test data.
However it does do a full write to bigquery.

The local pipeline runs twice, once for a full load, and once incrementally.
"""

from typing import List
import unittest
import warnings

from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery as cloud_bigquery
from google.cloud.exceptions import NotFound

import firehook_resources
from pipeline import run_beam_tables

BEAM_TEST_TABLE = 'test.test'
BQ_TEST_TABLE = firehook_resources.PROJECT_NAME + '.test.test'

JOB_NAME = 'manual_test_job'


# These methods are used to monkey patch the data_to_load method in beam_tables
# in order to return our test data.
#
# These files represent different types scan types (echo/discard/http/https)
# The actual pipeline doesn't write different scan types to the same table
# But since the table schemas are all the same we do it here to test all the
# different types of fields.
#
# These files contain real test data, usually 4 measurements each, the first 2
# are measurements that succeeded, the last two are measurements that failed.
def local_data_to_load_1(*_) -> List[str]:
  return [
      'pipeline/e2e_test_data_http.json', 'pipeline/e2e_test_data_https.json'
  ]


def local_data_to_load_2(*_) -> List[str]:
  return [
      'pipeline/e2e_test_data_echo.json', 'pipeline/e2e_test_data_discard.json'
  ]


def get_local_pipeline_options(*_) -> PipelineOptions:
  # This method is used to monkey patch the get_pipeline_options method in
  # beam_tables in order to run a local pipeline.
  return PipelineOptions(
      runner='DirectRunner',
      job_name=JOB_NAME,
      project=firehook_resources.PROJECT_NAME,
      temp_location=firehook_resources.BEAM_TEMP_LOCATION)


def run_local_pipeline(incremental=False):
  test_runner = run_beam_tables.get_firehook_beam_pipeline_runner()

  # Monkey patch the get_pipeline_options method to run a local pipeline
  test_runner.get_pipeline_options = get_local_pipeline_options
  # Monkey patch the data_to_load method to load only local data
  if incremental:
    test_runner.data_to_load = local_data_to_load_1
  else:
    test_runner.data_to_load = local_data_to_load_2

  test_runner.run_beam_pipeline('test', incremental, JOB_NAME, BEAM_TEST_TABLE,
                                None, None)


def clean_up_bq_table(client: cloud_bigquery.Client, table_name: str):
  try:
    client.get_table(table_name)
    client.delete_table(table_name)
  except NotFound:
    pass


class PipelineManualE2eTest(unittest.TestCase):

  def test_pipeline_e2e(self):
    # Suppress some unittest socket warnings in beam code we don't control
    warnings.simplefilter('ignore', ResourceWarning)

    client = cloud_bigquery.Client()

    # Clean up table if previous run failed to
    clean_up_bq_table(client, BQ_TEST_TABLE)

    run_local_pipeline(incremental=False)

    written_rows = list(client.query('SELECT * FROM ' + BQ_TEST_TABLE).result())
    self.assertEqual(len(written_rows), 28)

    run_local_pipeline(incremental=True)

    written_rows = list(client.query('SELECT * FROM ' + BQ_TEST_TABLE).result())
    self.assertEqual(len(written_rows), 53)

    # Domain appear different numbers of times in the test table depending on
    # how their measurement succeeded/failed.
    expected_single_domains = [
        'boingboing.net', 'box.com', 'google.com.ua', 'mos.ru', 'scribd.com',
        'uploaded.to', 'www.blubster.com', 'www.orthodoxconvert.info'
    ]
    expected_triple_domains = ['www.arabhra.org']
    expected_sextuple_domains = [
        'discover.com', 'peacefire.org', 'secondlife.com', 'www.89.com',
        'www.casinotropez.com', 'www.epa.gov', 'www.sex.com'
    ]
    all_expected_domains = (
        expected_single_domains + expected_triple_domains * 3 +
        expected_sextuple_domains * 6)

    written_domains = [row[0] for row in written_rows]
    self.assertListEqual(sorted(written_domains), sorted(all_expected_domains))

    clean_up_bq_table(client, BQ_TEST_TABLE)


# This test is not run by default in unittest because it takes about a minute
# to run, plus it reads from and writes to bigquery.
#
# To run it manually use the command
# python3 -m unittest pipeline.manual_e2e_test.PipelineManualE2eTest
if __name__ == '__main__':
  unittest.main()
