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
"""Unit tests for the beam pipeline."""

from __future__ import absolute_import

import datetime
import unittest

from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline import beam_tables
from pipeline.metadata.ip_metadata_chooser import FakeIpMetadataChooserFactory


class PipelineMainTest(unittest.TestCase):
  """Unit tests for beam pipeline steps."""

  def setUp(self) -> None:
    self.maxDiff = None  # pylint: disable=invalid-name

  def test_get_table_name(self) -> None:
    """Test creating a table name given params."""
    base_table_name = 'scan'

    prod_dataset = 'base'
    user_dataset = 'laplante'

    self.assertEqual(
        beam_tables.get_table_name(prod_dataset, 'echo', base_table_name),
        'base.echo_scan')
    self.assertEqual(
        beam_tables.get_table_name(user_dataset, 'discard', base_table_name),
        'laplante.discard_scan')
    self.assertEqual(
        beam_tables.get_table_name(prod_dataset, 'http', base_table_name),
        'base.http_scan')
    self.assertEqual(
        beam_tables.get_table_name(user_dataset, 'https', base_table_name),
        'laplante.https_scan')

  def test_get_job_name(self) -> None:
    """Test getting the name for the beam job"""
    self.assertEqual(
        beam_tables.get_bq_job_name('base.scan_echo', False),
        'write-base-scan-echo')
    self.assertEqual(
        beam_tables.get_bq_job_name('base.scan_discard', True),
        'append-base-scan-discard')
    self.assertEqual(
        beam_tables.get_bq_job_name('laplante.scan_http', False),
        'write-laplante-scan-http')
    self.assertEqual(
        beam_tables.get_bq_job_name('laplante.scan_https', True),
        'append-laplante-scan-https')
    self.assertEqual(
        beam_tables.get_gcs_job_name('gs://firehook-test/scans/echo', False),
        'write-gs-firehook-test-scans-echo')
    self.assertEqual(
        beam_tables.get_gcs_job_name('gs://firehook-test/scans/discard', True),
        'append-gs-firehook-test-scans-discard')
    self.assertEqual(
        beam_tables.get_gcs_job_name('gs://firehook-test/avirkud/http', False),
        'write-gs-firehook-test-avirkud-http')
    self.assertEqual(
        beam_tables.get_gcs_job_name('gs://firehook-test/avirkud/https', True),
        'append-gs-firehook-test-avirkud-https')

  # pylint: disable=protected-access

  def test_get_full_table_name(self) -> None:
    project = 'firehook-censoredplanet'
    runner = beam_tables.ScanDataBeamPipelineRunner(
        project, '', '', '', FakeIpMetadataChooserFactory())

    full_name = runner._get_full_table_name('prod.echo_scan')
    self.assertEqual(full_name, 'firehook-censoredplanet:prod.echo_scan')

  def test_read_scan_text(self) -> None:  # pylint: disable=no-self-use
    """Test reading lines from compressed and uncompressed files"""
    with TestPipeline() as p:
      lines = beam_tables._read_scan_text(
          p,
          ['pipeline/test_results_1.json', 'pipeline/test_results_2.json.gz'])

      # Due to some detail of how beam's test util works
      # The test line strings here need to be double-quoted.
      # The actual strings in the pipeline are not double-quoted.
      expected = [
          ('pipeline/test_results_1.json', '"test line 1.1"'),
          ('pipeline/test_results_1.json', '"test line 1.2"'),
          ('pipeline/test_results_2.json.gz', '"test line 2.1"'),
          ('pipeline/test_results_2.json.gz', '"test line 2.2"'),
      ]

      beam_test_util.assert_that(lines, beam_test_util.equal_to(expected))

  def test_between_dates(self) -> None:
    """Test logic to include filenames based on their creation dates."""
    filename = 'gs://firehook-scans/http/CP_Quack-http-2020-05-11-01-02-08/results.json'

    self.assertTrue(
        beam_tables._between_dates(filename, datetime.date(2020, 5, 10),
                                   datetime.date(2020, 5, 12)))
    self.assertTrue(
        beam_tables._between_dates(filename, datetime.date(2020, 5, 11),
                                   datetime.date(2020, 5, 11)))
    self.assertTrue(
        beam_tables._between_dates(filename, None, datetime.date(2020, 5, 12)))
    self.assertTrue(
        beam_tables._between_dates(filename, datetime.date(2020, 5, 10), None))
    self.assertTrue(beam_tables._between_dates(filename, None, None))

  def test_not_between_dates(self) -> None:
    """Test logic to filter filenames based on their creation dates."""
    filename = 'gs://firehook-scans/http/CP_Quack-http-2020-05-11-01-02-08/results.json'

    self.assertFalse(
        beam_tables._between_dates(filename, datetime.date(2020, 5, 12),
                                   datetime.date(2020, 5, 10)))
    self.assertFalse(
        beam_tables._between_dates(filename, None, datetime.date(2020, 5, 10)))
    self.assertFalse(
        beam_tables._between_dates(filename, datetime.date(2020, 5, 12), None))

  # pylint: enable=protected-access


if __name__ == '__main__':
  unittest.main()
