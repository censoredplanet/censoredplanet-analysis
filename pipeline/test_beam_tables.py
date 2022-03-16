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

import datetime
import unittest

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline.metadata.flatten_base import Row, IpMetadata
from pipeline import beam_tables
from pipeline.metadata.ip_metadata_chooser import FakeIpMetadataChooserFactory
from pipeline.metadata import satellite


class PipelineMainTest(unittest.TestCase):
  """Unit tests for beam pipeline steps."""

  def setUp(self) -> None:
    self.maxDiff = None  # pylint: disable=invalid-name

  # pylint: disable=protected-access

  def test_get_bigquery_schema_hyperquack(self) -> None:
    """Test getting the right bigquery schema for data types."""
    echo_schema = beam_tables._get_bigquery_schema('echo')
    all_hyperquack_top_level_columns = list(
        beam_tables.HYPERQUACK_BIGQUERY_SCHEMA.keys())
    self.assertListEqual(
        list(echo_schema.keys()), all_hyperquack_top_level_columns)

  def test_get_bigquery_schema_satellite(self) -> None:
    satellite_schema = beam_tables._get_bigquery_schema('satellite')
    all_satellite_top_level_columns = list(
        beam_tables.SATELLITE_BIGQUERY_SCHEMA.keys())
    self.assertListEqual(
        list(satellite_schema.keys()), all_satellite_top_level_columns)

  def test_get_bigquery_schema_blockpage(self) -> None:
    blockpage_schema = beam_tables._get_bigquery_schema('blockpage')
    self.assertEqual(blockpage_schema, satellite.BLOCKPAGE_BIGQUERY_SCHEMA)

  def test_get_beam_bigquery_schema(self) -> None:
    """Test making a bigquery schema for beam's table writing."""
    test_field = {
        'string_field': ('string', 'nullable'),
        'int_field': ('integer', 'repeated'),
    }

    table_schema = beam_tables._get_beam_bigquery_schema(test_field)

    expected_field_schema_1 = beam_bigquery.TableFieldSchema()
    expected_field_schema_1.name = 'string_field'
    expected_field_schema_1.type = 'string'
    expected_field_schema_1.mode = 'nullable'

    expected_field_schema_2 = beam_bigquery.TableFieldSchema()
    expected_field_schema_2.name = 'int_field'
    expected_field_schema_2.type = 'integer'
    expected_field_schema_2.mode = 'repeated'

    expected_table_schema = beam_bigquery.TableSchema()
    expected_table_schema.fields.append(expected_field_schema_1)
    expected_table_schema.fields.append(expected_field_schema_2)

    self.assertEqual(table_schema, expected_table_schema)

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
        beam_tables.get_job_name('base.scan_echo', False),
        'write-base-scan-echo')
    self.assertEqual(
        beam_tables.get_job_name('base.scan_discard', True),
        'append-base-scan-discard')
    self.assertEqual(
        beam_tables.get_job_name('laplante.scan_http', False),
        'write-laplante-scan-http')
    self.assertEqual(
        beam_tables.get_job_name('laplante.scan_https', True),
        'append-laplante-scan-https')

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

  def test_add_metadata(self) -> None:  # pylint: disable=no-self-use
    """Test adding IP metadata to mesurements."""
    rows = [
        Row(
            domain='www.example.com',
            ip='8.8.8.8',
            date='2020-01-01',
            success=True,
        ),
        Row(
            domain='www.example.com',
            ip='1.1.1.1',
            date='2020-01-01',
            success=False,
        ),
        Row(
            domain='www.example.com',
            ip='8.8.8.8',
            date='2020-01-02',
            success=False,
        ),
        Row(
            domain='www.example.com',
            ip='1.1.1.1',
            date='2020-01-02',
            success=True,
        )
    ]

    p = TestPipeline()
    rows = (p | beam.Create(rows))

    runner = beam_tables.ScanDataBeamPipelineRunner(
        '', '', '', '', FakeIpMetadataChooserFactory())

    rows_with_metadata = runner._add_metadata(rows)

    expected = [
        Row(
            domain='www.example.com',
            ip='8.8.8.8',
            date='2020-01-01',
            success=True,
            netblock='8.8.8.0/24',
            asn=15169,
            as_name='GOOGLE',
            as_full_name='Google LLC',
            as_class='Content',
            country='US',
        ),
        Row(
            domain='www.example.com',
            ip='1.1.1.1',
            date='2020-01-01',
            success=False,
            netblock='1.0.0.1/24',
            asn=13335,
            as_name='CLOUDFLARENET',
            as_full_name='Cloudflare Inc.',
            as_class='Content',
            country='US',
        ),
        Row(
            domain='www.example.com',
            ip='8.8.8.8',
            date='2020-01-02',
            success=False,
            netblock='8.8.8.0/24',
            asn=15169,
            as_name='GOOGLE',
            as_full_name='Google LLC',
            as_class='Content',
            country='US',
        ),
        Row(
            domain='www.example.com',
            ip='1.1.1.1',
            date='2020-01-02',
            success=True,
            netblock='1.0.0.1/24',
            asn=13335,
            as_name='CLOUDFLARENET',
            as_full_name='Cloudflare Inc.',
            as_class='Content',
            country='US',
        )
    ]

    beam_test_util.assert_that(rows_with_metadata,
                               beam_test_util.equal_to(expected))

  def test_add_ip_metadata_caida(self) -> None:
    """Test merging given IP metadata with given measurements."""
    runner = beam_tables.ScanDataBeamPipelineRunner(
        '', '', '', '', FakeIpMetadataChooserFactory())

    metadatas = list(
        runner._add_ip_metadata('2020-01-01', ['1.1.1.1', '8.8.8.8']))

    expected_key_1: satellite.DateIpKey = ('2020-01-01', '1.1.1.1')
    expected_value_1 = IpMetadata(
        ip='1.1.1.1',
        date='2020-01-01',
        netblock='1.0.0.1/24',
        asn=13335,
        as_name='CLOUDFLARENET',
        as_full_name='Cloudflare Inc.',
        as_class='Content',
        country='US',
        organization='Fake Cloudflare Sub-Org',
    )

    expected_key_2: satellite.DateIpKey = ('2020-01-01', '8.8.8.8')
    expected_value_2 = IpMetadata(
        ip='8.8.8.8',
        date='2020-01-01',
        netblock='8.8.8.0/24',
        asn=15169,
        as_name='GOOGLE',
        as_full_name='Google LLC',
        as_class='Content',
        country='US',
        # No organization data is added since the ASN doesn't match dbip
    )

    self.assertListEqual(metadatas, [(expected_key_1, expected_value_1),
                                     (expected_key_2, expected_value_2)])

  def disabled_test_add_ip_metadata_maxmind(self) -> None:
    """Test merging given IP metadata with given measurements."""
    # TODO turn back on once maxmind is reenabled.

    runner = beam_tables.ScanDataBeamPipelineRunner(
        '', '', '', '', FakeIpMetadataChooserFactory())

    metadatas = list(runner._add_ip_metadata('2020-01-01', ['1.1.1.3']))

    # Test Maxmind lookup when country data is missing
    # Cloudflare IPs return Australia
    expected_key_1 = ('2020-01-01', '1.1.1.3')
    expected_value_1 = IpMetadata(
        ip='1.1.1.3',
        date='2020-01-01',
        netblock='1.0.0.1/24',
        asn=13335,
        as_name='CLOUDFLARENET',
        as_full_name='Cloudflare Inc.',
        as_class='Content',
        organization='Fake Cloudflare Sub-Org',
        country='AU')

    self.assertListEqual(metadatas, [(expected_key_1, expected_value_1)])

  # pylint: enable=protected-access


if __name__ == '__main__':
  unittest.main()
