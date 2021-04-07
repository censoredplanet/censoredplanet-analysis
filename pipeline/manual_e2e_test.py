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
"""E2E test which runs a full pipeline over a few lines of test data.

The pipeline is different from the usual Dataflow pipeline, since it runs
locally, and only reads a few lines of test data.
However it does do a full write to bigquery.

The local pipeline runs twice, once for a full load, and once incrementally.
"""

import datetime
import os
import pwd
from typing import List, Any
import unittest
import warnings

from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery as cloud_bigquery
from google.cloud.exceptions import NotFound

import firehook_resources
from pipeline import run_beam_tables
from pipeline.metadata import ip_metadata

# The test table is written into the <project>:<username> dataset
username = pwd.getpwuid(os.getuid()).pw_name
BEAM_TEST_TABLE = f'{username}.manual_test'
BQ_TEST_TABLE = f'{firehook_resources.PROJECT_NAME}.{BEAM_TEST_TABLE}'

JOB_NAME = 'manual_test_job'


# These methods are used to monkey patch the data_to_load method in beam_tables
# in order to return our test data.
#
# These files represent different types scan types (echo/discard/http/https)
# The actual pipeline doesn't write different scan types to the same table
# But since the table schemas are all the same we do it here to test all the
# different types of fields.
#
# These files contain real sample data, usually 4 measurements each, the first 2
# are measurements that succeeded, the last two are measurements that failed.
def local_data_to_load_http_and_https(*_: List[Any]) -> List[str]:
  return [
      'pipeline/e2e_test_data/http_results.json',
      'pipeline/e2e_test_data/https_results.json'
  ]


def local_data_to_load_discard_and_echo(*_: List[Any]) -> List[str]:
  return [
      'pipeline/e2e_test_data/discard_results.json',
      'pipeline/e2e_test_data/echo_results.json'
  ]


def local_data_to_load_3(*_: List[Any]) -> List[str]:
  return [
      'pipeline/e2e_test_data/Satellitev1_2018-01-01/resolvers.json',
      'pipeline/e2e_test_data/Satellitev1_2018-01-01/tagged_resolvers.json',
      'pipeline/e2e_test_data/Satellitev1_2018-01-01/tagged_answers.json',
      'pipeline/e2e_test_data/Satellitev1_2018-01-01/answers_control.json',
      'pipeline/e2e_test_data/Satellitev1_2018-01-01/interference.json'
  ]


def get_local_pipeline_options(*_: List[Any]) -> PipelineOptions:
  # This method is used to monkey patch the get_pipeline_options method in
  # beam_tables in order to run a local pipeline.
  return PipelineOptions(
      runner='DirectRunner',
      job_name=JOB_NAME,
      project=firehook_resources.PROJECT_NAME,
      temp_location=firehook_resources.BEAM_TEMP_LOCATION)


def run_local_pipeline(incremental: bool = False) -> None:
  """Run a local pipeline.

  Reads local files but writes to bigquery.

  Args:
    incremental: bool, whether to run a full or incremental local pipeline.
  """
  # pylint: disable=protected-access
  test_runner = run_beam_tables.get_firehook_beam_pipeline_runner()

  # Monkey patch the get_pipeline_options method to run a local pipeline
  test_runner._get_pipeline_options = get_local_pipeline_options  # type: ignore
  # Monkey patch the data_to_load method to load only local data
  if incremental:
    test_runner._data_to_load = local_data_to_load_http_and_https  # type: ignore
  else:
    test_runner._data_to_load = local_data_to_load_discard_and_echo  # type: ignore

  test_runner.run_beam_pipeline('test', incremental, JOB_NAME, BEAM_TEST_TABLE,
                                None, None)
  # pylint: enable=protected-access


def run_local_pipeline_satellite() -> None:
  # run_local_pipeline for satellite - scan_type must be 'dns'
  # pylint: disable=protected-access
  test_runner = run_beam_tables.get_firehook_beam_pipeline_runner()
  test_runner._get_pipeline_options = get_local_pipeline_options  # type: ignore
  test_runner._data_to_load = local_data_to_load_3  # type: ignore

  test_runner.run_beam_pipeline('dns', True, JOB_NAME, BEAM_TEST_TABLE, None,
                                None)
  # pylint: enable=protected-access


def clean_up_bq_table(client: cloud_bigquery.Client, table_name: str) -> None:
  try:
    client.get_table(table_name)
    client.delete_table(table_name)
  except NotFound:
    pass


def get_bq_rows(client: cloud_bigquery.Client, table_name: str) -> List:
  return list(client.query(f'SELECT * FROM {table_name}').result())


class PipelineManualE2eTest(unittest.TestCase):
  """Manual tests that require access to cloud project resources."""

  def test_pipeline_e2e(self) -> None:
    """Test the full pipeline by running it twice locally on a few files."""
    # Suppress some unittest socket warnings in beam code we don't control
    warnings.simplefilter('ignore', ResourceWarning)
    client = cloud_bigquery.Client()

    try:
      run_local_pipeline(incremental=False)

      written_rows = get_bq_rows(client, BQ_TEST_TABLE)
      self.assertEqual(len(written_rows), 28)

      run_local_pipeline(incremental=True)

      written_rows = get_bq_rows(client, BQ_TEST_TABLE)
      self.assertEqual(len(written_rows), 53)

      run_local_pipeline_satellite()

      written_rows = get_bq_rows(client, BQ_TEST_TABLE)
      self.assertEqual(len(written_rows), 57)

      # Domain appear different numbers of times in the test table depending on
      # how their measurement succeeded/failed.
      expected_single_domains = [
          'boingboing.net', 'box.com', 'google.com.ua', 'mos.ru', 'scribd.com',
          'uploaded.to', 'www.blubster.com', 'www.orthodoxconvert.info',
          'biblegateway.com', 'ar.m.wikipedia.org', 'www.ecequality.org',
          'www.usacasino.com'
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
      self.assertListEqual(
          sorted(written_domains), sorted(all_expected_domains))

    finally:
      clean_up_bq_table(client, BQ_TEST_TABLE)

  def test_ipmetadata_init(self) -> None:
    # This E2E test requires the user to have access to the
    # gs://censoredplanet_geolocation bucket.
    ip_metadata_db = ip_metadata.get_firehook_ip_metadata_db(
        datetime.date(2018, 7, 27))
    metadata = ip_metadata_db.lookup('1.1.1.1')

    self.assertEqual(metadata, ('1.1.1.0/24', 13335, 'CLOUDFLARENET',
                                'Cloudflare, Inc.', 'Content', 'US'))


# This test is not run by default in unittest because it takes about a minute
# to run, plus it reads from and writes to bigquery.
#
# To run it manually use the command
# python3 -m unittest pipeline.manual_e2e_test.PipelineManualE2eTest
if __name__ == '__main__':
  unittest.main()
