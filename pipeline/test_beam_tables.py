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
from typing import Dict, List
import unittest
import json

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline import beam_tables
from pipeline.metadata.fake_caida_ip_metadata import FakeCaidaIpMetadata
from pipeline.metadata.maxmind import FakeMaxmindIpMetadata
from pipeline.metadata import flatten


class PipelineMainTest(unittest.TestCase):
  """Unit tests for beam pipeline steps."""

  # pylint: disable=protected-access

  def test_get_bigquery_schema(self) -> None:
    """Test getting the right bigquery schema for data types."""
    echo_schema = beam_tables._get_bigquery_schema('echo')
    self.assertEqual(echo_schema, beam_tables.SCAN_BIGQUERY_SCHEMA)

    satellite_schema = beam_tables._get_bigquery_schema('satellite')
    all_satellite_top_level_columns = (
        list(beam_tables.SCAN_BIGQUERY_SCHEMA.keys()) +
        list(beam_tables.SATELLITE_BIGQUERY_SCHEMA.keys()))
    self.assertListEqual(
        list(satellite_schema.keys()), all_satellite_top_level_columns)

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
    runner = beam_tables.ScanDataBeamPipelineRunner(project, '', '', '',
                                                    FakeCaidaIpMetadata, '',
                                                    FakeMaxmindIpMetadata, '')

    full_name = runner._get_full_table_name('prod.echo_scan')
    self.assertEqual(full_name, 'firehook-censoredplanet:prod.echo_scan')

  def test_read_scan_text(self) -> None:  # pylint: disable=no-self-use
    """Test reading lines from compressed and uncompressed files"""
    p = TestPipeline()
    pipeline = beam_tables._read_scan_text(
        p, ['pipeline/test_results_1.json', 'pipeline/test_results_2.json.gz'])

    beam_test_util.assert_that(
        pipeline,
        beam_test_util.equal_to([
            'test line 1.1', 'test line 1.2', 'test line 2.1', 'test line 2.2'
        ]))

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
    rows: List[beam_tables.Row] = [{
        'domain': 'www.example.com',
        'ip': '8.8.8.8',
        'date': '2020-01-01',
        'success': True,
    }, {
        'domain': 'www.example.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
        'success': False,
    }, {
        'domain': 'www.example.com',
        'ip': '8.8.8.8',
        'date': '2020-01-02',
        'success': False,
    }, {
        'domain': 'www.example.com',
        'ip': '1.1.1.1',
        'date': '2020-01-02',
        'success': True,
    }]

    p = TestPipeline()
    rows = (p | beam.Create(rows))

    runner = beam_tables.ScanDataBeamPipelineRunner('', '', '', '',
                                                    FakeCaidaIpMetadata, '',
                                                    FakeMaxmindIpMetadata, '')

    rows_with_metadata = runner._add_metadata(rows)
    beam_test_util.assert_that(
        rows_with_metadata,
        beam_test_util.equal_to([{
            'domain': 'www.example.com',
            'ip': '8.8.8.8',
            'date': '2020-01-01',
            'success': True,
            'netblock': '8.8.8.0/24',
            'asn': 15169,
            'as_name': 'GOOGLE',
            'as_full_name': 'Google LLC',
            'as_class': 'Content',
            'country': 'US',
        }, {
            'domain': 'www.example.com',
            'ip': '1.1.1.1',
            'date': '2020-01-01',
            'success': False,
            'netblock': '1.0.0.1/24',
            'asn': 13335,
            'as_name': 'CLOUDFLARENET',
            'as_full_name': 'Cloudflare Inc.',
            'as_class': 'Content',
            'country': 'US',
        }, {
            'domain': 'www.example.com',
            'ip': '8.8.8.8',
            'date': '2020-01-02',
            'success': False,
            'netblock': '8.8.8.0/24',
            'asn': 15169,
            'as_name': 'GOOGLE',
            'as_full_name': 'Google LLC',
            'as_class': 'Content',
            'country': 'US',
        }, {
            'domain': 'www.example.com',
            'ip': '1.1.1.1',
            'date': '2020-01-02',
            'success': True,
            'netblock': '1.0.0.1/24',
            'asn': 13335,
            'as_name': 'CLOUDFLARENET',
            'as_full_name': 'Cloudflare Inc.',
            'as_class': 'Content',
            'country': 'US',
        }]))

  def test_make_date_ip_key(self) -> None:
    row = {'date': '2020-01-01', 'ip': '1.2.3.4', 'other_field': None}
    self.assertEqual(
        beam_tables._make_date_ip_key(row), ('2020-01-01', '1.2.3.4'))

  def test_add_ip_metadata(self) -> None:
    """Test merging given IP metadata with given measurements."""
    runner = beam_tables.ScanDataBeamPipelineRunner('', '', '', '',
                                                    FakeCaidaIpMetadata, '',
                                                    FakeMaxmindIpMetadata, '')

    metadatas = list(
        runner._add_ip_metadata('2020-01-01',
                                ['1.1.1.1', '8.8.8.8', '1.1.1.3']))

    expected_key_1: beam_tables.DateIpKey = ('2020-01-01', '1.1.1.1')
    expected_value_1: beam_tables.Row = {
        'netblock': '1.0.0.1/24',
        'asn': 13335,
        'as_name': 'CLOUDFLARENET',
        'as_full_name': 'Cloudflare Inc.',
        'as_class': 'Content',
        'country': 'US',
    }

    expected_key_2: beam_tables.DateIpKey = ('2020-01-01', '8.8.8.8')
    expected_value_2: beam_tables.Row = {
        'netblock': '8.8.8.0/24',
        'asn': 15169,
        'as_name': 'GOOGLE',
        'as_full_name': 'Google LLC',
        'as_class': 'Content',
        'country': 'US',
    }

    # Test Maxmind lookup when country data is missing
    # Cloudflare IPs return Australia
    expected_key_3 = ('2020-01-01', '1.1.1.3')
    expected_value_3 = {
        'netblock': '1.0.0.1/24',
        'asn': 13335,
        'as_name': 'CLOUDFLARENET',
        'as_full_name': 'Cloudflare Inc.',
        'as_class': 'Content',
        'country': None,
    }
    expected_value_3['country'] = 'AU'

    self.assertListEqual(metadatas, [(expected_key_1, expected_value_1),
                                     (expected_key_2, expected_value_2),
                                     (expected_key_3, expected_value_3)])

  def test_merge_metadata_with_rows(self) -> None:
    """Test merging IP metadata pcollection with rows pcollection."""
    key: beam_tables.DateIpKey = ('2020-01-01', '1.1.1.1')
    ip_metadata: beam_tables.Row = {
        'netblock': '1.0.0.1/24',
        'asn': 13335,
        'as_name': 'CLOUDFLARENET',
        'as_full_name': 'Cloudflare Inc.',
        'as_class': 'Content',
        'country': 'US',
    }
    rows: List[beam_tables.Row] = [{
        'domain': 'www.example.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
    }, {
        'domain': 'www.example2.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
    }]
    value: Dict[str, List[beam_tables.Row]] = {
        beam_tables.IP_METADATA_PCOLLECTION_NAME: [ip_metadata],
        beam_tables.ROWS_PCOLLECION_NAME: rows
    }

    expected_rows = [{
        'domain': 'www.example.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
        'netblock': '1.0.0.1/24',
        'asn': 13335,
        'as_name': 'CLOUDFLARENET',
        'as_full_name': 'Cloudflare Inc.',
        'as_class': 'Content',
        'country': 'US',
    }, {
        'domain': 'www.example2.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
        'netblock': '1.0.0.1/24',
        'asn': 13335,
        'as_name': 'CLOUDFLARENET',
        'as_full_name': 'Cloudflare Inc.',
        'as_class': 'Content',
        'country': 'US',
    }]

    rows_with_metadata = list(beam_tables._merge_metadata_with_rows(key, value))
    self.assertListEqual(rows_with_metadata, expected_rows)

  def test_read_satellite_tags(self) -> None:
    """Test reading rows from Satellite tag files."""
    tagged_resolver1 = {'resolver': '1.1.1.1', 'country': 'United States'}
    tagged_resolver2 = {'resolver': '1.1.1.3', 'country': 'Australia'}
    # yapf: disable
    tagged_answer1 = {
      'ip': '60.210.17.137',
      'asname': 'CHINA169-BACKBONE CHINA UNICOM China169 Backbone',
      'asnum': 4837,
      'cert': 'a2fed117238c94a04ba787cfe69e93de36cc8571bab44d5481df9becb9beec75',
      'http': 'e3c1d34ca489928190b45f0535624b872717d1edd881c8ab4b2c62f898fcd4a5'
    }

    row1 = {'ip': '1.1.1.1', 'date': '2020-12-17', 'country': 'US'}
    row2 = {'ip': '1.1.1.3', 'date': '2020-12-17', 'country': 'AU'}
    row3 = {
      'ip': '60.210.17.137',
      'date': '2020-12-17',
      'asname': 'CHINA169-BACKBONE CHINA UNICOM China169 Backbone',
      'asnum': 4837,
      'cert': 'a2fed117238c94a04ba787cfe69e93de36cc8571bab44d5481df9becb9beec75',
      'http': 'e3c1d34ca489928190b45f0535624b872717d1edd881c8ab4b2c62f898fcd4a5'
    }
    # yapf: enable

    data = [
        json.dumps(tagged_resolver1),
        json.dumps(tagged_resolver2),
        json.dumps(tagged_answer1)
    ]
    expected = [row1, row2, row3]
    result = [
        next(beam_tables._read_satellite_tags('2020-12-17', d)) for d in data
    ]
    self.assertListEqual(result, expected)

  def test_process_satellite_v1(self) -> None:  # pylint: disable=no-self-use
    """Test processing of Satellite v1 interference and tag files."""
    # yapf: disable
    _data = [
      ("CP_Satellite-2020-09-02-12-00-01/interference.json", {'resolver': '1.1.1.3','query': 'signal.org', 'answers': {'13.249.134.38': ['ip', 'http', 'asnum', 'asname'], '13.249.134.44': ['ip', 'http', 'asnum', 'asname'],'13.249.134.74': ['ip', 'http', 'asnum', 'asname'], '13.249.134.89': ['ip', 'http', 'asnum', 'asname']}, 'passed': True}),
      ("CP_Satellite-2020-09-02-12-00-01/interference.json", {'resolver': '1.1.1.3','query': 'adl.org', 'answers': {'192.124.249.107': ['ip', 'no_tags']}, 'passed': True}),
    ]

    data = [(filename, json.dumps(d)) for filename, d in _data]

    _tags = [
        ("CP_Satellite-2020-09-02-12-00-01/resolvers.json", {'name': 'special','resolver': '1.1.1.3'}),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_resolvers.json", {'resolver': '1.1.1.3', 'country': 'United States'}),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", {'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af','ip': '13.249.134.38'}),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", {'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71','ip': '13.249.134.44'}),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", {'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe','ip': '13.249.134.74'}),
        ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", {'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac','ip': '13.249.134.89'})
    ]

    tags = [(filename, json.dumps(t)) for filename, t in _tags]

    expected = [
        {
          'ip': '1.1.1.3',
          'country': 'US',
          'name': 'special',
          'domain': 'signal.org',
          'error': None,
          'anomaly': False,
          'success': True,
          'received': [
              {'ip': '13.249.134.38', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af', 'matches_control': 'ip http asnum asname'},
              {'ip': '13.249.134.44', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71', 'matches_control': 'ip http asnum asname'},
              {'ip': '13.249.134.74', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe', 'matches_control': 'ip http asnum asname'},
              {'ip': '13.249.134.89', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac', 'matches_control': 'ip http asnum asname'}
          ],
          'date': '2020-09-02'
        },
        {
          'ip': '1.1.1.3',
          'country': 'US',
          'name': 'special',
          'domain': 'adl.org',
          'error': None,
          'anomaly': False,
          'success': True,
          'received': [
              {'ip': '192.124.249.107', 'matches_control': 'ip'}
          ],
          'date': '2020-09-02'
        }
    ]
    # yapf: enable

    with TestPipeline() as p:
      lines = p | 'create data' >> beam.Create(data)
      lines2 = p | 'create tags' >> beam.Create(tags)

      final = beam_tables._process_satellite_with_tags(lines, lines2)
      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_process_satellite_v2(self) -> None:  # pylint: disable=no-self-use
    """Test processing of Satellite v2 interference and tag files."""
    # yapf: disable
    data = [
      ("CP_Satellite-2021-03-01-12-00-01/results.json", """{"vp":"185.228.169.37","location":{"country_code":"IE","country_name":"Ireland"},"test_url":"ar.m.wikipedia.org","response":{"198.35.26.96":["cert","asnum","asname"],"rcode":["0","0","0"]},"passed_control":true,"connect_error":false,"in_control_group":true,"anomaly":false,"confidence":{"average":60,"matches":[60],"untagged_controls":false,"untagged_response":false},"start_time":"2021-03-01 12:43:25.3438285 -0500 EST m=+0.421998701","end_time":"2021-03-01 12:43:25.3696119 -0500 EST m=+0.447782001"}"""),
      ("CP_Satellite-2021-03-01-12-00-01/results.json", """{"vp":"156.154.71.37","location":{"country_code":"US","country_name":"United States"},"test_url":"www.usacasino.com","response":{"15.126.193.233":["no_tags"],"rcode":["0","0","0"]},"passed_control":true,"connect_error":false,"in_control_group":true,"anomaly":true,"confidence":{"average":0,"matches":[0],"untagged_controls":false,"untagged_response":true},"start_time":"2021-03-01 12:43:25.3438285 -0500 EST m=+0.421998701","end_time":"2021-03-01 12:43:25.3696119 -0500 EST m=+0.447782001"}"""),
    ]

    tags = [
      ("CP_Satellite-2021-03-01-12-00-01/tagged_resolvers.json", """{"location":{"country_code":"IE","country_name":"Ireland"},"vp":"185.228.169.37"}"""),
      ("CP_Satellite-2021-03-01-12-00-01/tagged_resolvers.json", """{"location":{"country_code":"US","country_name":"United States"},"vp":"156.154.71.37"}"""),
      ("CP_Satellite-2021-03-01-12-00-01/resolvers.json", """{"name":"rdns37b.ultradns.net.","vp":"156.154.71.37"}"""),
      ("CP_Satellite-2021-03-01-12-00-01/resolvers.json", """{"name":"customfilter37-dns2.cleanbrowsing.org.","vp":"185.228.169.37"}"""),
      ("CP_Satellite-2021-03-01-12-00-01/tagged_responses.json", """{"asname":"WIKIMEDIA","asnum":14907,"cert":"9eb21a74a3cf1ecaaf6b19253025b4ca38f182e9f1f3e7355ba3c3004d4b7a10","http":"7b4b4d1bfb0a645c990f55557202f88be48e1eee0c10bdcc621c7b682bf7d2ca","ip":"198.35.26.96"}""")
    ]

    expected = [
      {
        'ip': '185.228.169.37',
        'country': 'IE',
        'name': 'customfilter37-dns2.cleanbrowsing.org.',
        'domain': 'ar.m.wikipedia.org',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': [
            {'ip': '198.35.26.96', 'asname': 'WIKIMEDIA','asnum': 14907,'cert': '9eb21a74a3cf1ecaaf6b19253025b4ca38f182e9f1f3e7355ba3c3004d4b7a10','http': '7b4b4d1bfb0a645c990f55557202f88be48e1eee0c10bdcc621c7b682bf7d2ca', 'matches_control': 'cert asnum asname'},
        ],
        'rcode': ['0', '0', '0'],
        'date': '2021-03-01',
        'start_time': '2021-03-01 12:43:25.3438285 -0500 EST m=+0.421998701',
        'end_time': '2021-03-01 12:43:25.3696119 -0500 EST m=+0.447782001'
      },
      {
        'ip': '156.154.71.37',
        'country': 'US',
        'name': 'rdns37b.ultradns.net.',
        'domain': 'www.usacasino.com',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': [
            {'ip': '15.126.193.233', 'matches_control': ''},
        ],
        'rcode': ['0', '0', '0'],
        'date': '2021-03-01',
        'start_time': '2021-03-01 12:43:25.3438285 -0500 EST m=+0.421998701',
        'end_time': '2021-03-01 12:43:25.3696119 -0500 EST m=+0.447782001'
      }
    ]
    # yapf: enable

    with TestPipeline() as p:
      lines = p | 'create data' >> beam.Create(data)
      lines2 = p | 'create tags' >> beam.Create(tags)

      final = beam_tables._process_satellite_with_tags(lines, lines2)
      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))

  def test_partition_satellite_input(self) -> None:  # pylint: disable=no-self-use
    """Test partitioning of Satellite tag and answer input files."""
    data = [("CP_Satellite-2020-09-02-12-00-01/resolvers.json", "tag"),
            ("CP_Satellite-2020-09-02-12-00-01/resolvers.json", "tag"),
            ("CP_Satellite-2020-09-02-12-00-01/tagged_resolvers.json", "tag"),
            ("CP_Satellite-2020-09-02-12-00-01/tagged_resolvers.json", "tag"),
            ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", "tag"),
            ("CP_Satellite-2020-09-02-12-00-01/tagged_answers.json", "tag"),
            ("CP_Satellite-2020-09-02-12-00-01/interference.json", "row"),
            ("CP_Satellite-2020-09-02-12-00-01/interference.json", "row")]

    expected_tags = data[0:6]
    expected_rows = data[6:]

    with TestPipeline() as p:
      lines = p | 'create data' >> beam.Create(data)

      tags, rows = lines | beam.Partition(
          beam_tables._partition_satellite_input, 2)

      beam_test_util.assert_that(
          tags,
          beam_test_util.equal_to(expected_tags),
          label='assert_that/tags')
      beam_test_util.assert_that(
          rows,
          beam_test_util.equal_to(expected_rows),
          label='assert_that/rows')

  def test_calculate_confidence(self) -> None:
    """Test calculating the confidence metrics for Satellite v1 data."""
    # yapf: disable
    scans: List[beam_tables.Row] = [
      {
        'ip': '114.114.114.110',
        'country': 'CN',
        'name': 'name',
        'domain': 'abs-cbn.com',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': [{'ip': '104.20.161.134', 'matches_control': ''}],
        'date': '2020-09-02'
      },
      {
        'ip': '1.1.1.3',
        'country': 'US',
        'name': 'special',
        'domain': 'signal.org',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': [
            {'ip': '13.249.134.38', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af', 'matches_control': 'ip http asnum asname'},
            {'ip': '13.249.134.44', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71', 'matches_control': 'ip http asnum asname'},
            {'ip': '13.249.134.74', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe', 'matches_control': 'ip http asnum asname'},
            {'ip': '13.249.134.89', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac', 'matches_control': 'ip http asnum asname'}
        ],
        'date': '2020-09-02'
      },
      {
        'ip': '1.1.1.3',
        'country': 'US',
        'name': 'special',
        'domain': 'signal.org',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': [
            {'ip': '13.249.134.38', 'asname': 'AS1','asnum': 11111,'cert': None,'http': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af', 'matches_control': ''},
            {'ip': '13.249.134.44', 'asname': 'AS2','asnum': 22222,'cert': 'cert','http': '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71', 'matches_control': 'asnum asname'},
            {'ip': '13.249.134.74', 'asname': 'AS2','asnum': 22222,'cert': None,'http': '2054d0fd3887e0ded023879770d6cde57633b7881f609f1042d90fedf41685fe', 'matches_control': 'ip http asnum asname'},
            {'ip': '13.249.134.89', 'asname': 'AS2','asnum': 22222,'cert': None,'http': '0509322329cdae79475531a019a3628aa52598caa0135c5534905f0c4b4f1bac', 'matches_control': 'ip http asnum asname'}
        ],
        'date': '2020-09-02'
      }
    ]

    expected = [
      {
        'average': 0,
        'matches': [0],
        'untagged_controls': False,
        'untagged_response': True
      },
      {
        'average': 100,
        'matches': [100, 100, 100, 100],
        'untagged_controls': False,
        'untagged_response': False
      },
      {
        'average': 62.5,
        'matches': [0, 50, 100, 100],
        'untagged_controls': False,
        'untagged_response': False
      }
    ]
    # yapf: enable
    result = [
        beam_tables._calculate_confidence(scan, 1)['confidence']
        for scan in scans
    ]
    self.assertListEqual(result, expected)

  def test_verify(self) -> None:
    """Test verification of Satellite v1 data."""
    # yapf: disable
    scans: List[beam_tables.Row] = [
      {
        'ip': '114.114.114.110',
        'country': 'CN',
        'name': 'name',
        'domain': 'abs-cbn.com',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': [{'ip': '104.20.161.134', 'matches_control': ''}],
        'date': '2020-09-02'
      },
      {
        'ip': '114.114.114.110',
        'country': 'CN',
        'name': 'name',
        'domain': 'ar.m.wikipedia.org',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': [{'ip': '198.35.26.96', 'matches_control': ''}],
        'date': '2020-09-02'
      },
      {
        'ip': '1.1.1.3',
        'country': 'US',
        'name': 'special',
        'domain': 'signal.org',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': [
            {'ip': '13.249.134.38', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': 'c5ba7f2da503045170f1d66c3e9f84576d8f3a606bb246db589a8f62c65921af', 'matches_control': ''},
            {'ip': '13.249.134.44', 'asname': 'AMAZON-02','asnum': 16509,'cert': None,'http': '256e35b8bace0e9fe95f308deb35f82117cd7317f90a08f181516c31abe95b71', 'matches_control': ''},
        ],
        'date': '2020-09-02'
      },
    ]
    # yapf: enable

    # mock data for the global interference IP - DOMAIN mapping
    flatten.INTERFERENCE_IPDOMAIN = {
        '104.20.161.134': {'abs-cbn.com', 'xyz.com', 'blah.com'},
        '198.35.26.96': {'ar.m.wikipedia.org'},
    }
    expected = [
        # answer IP is returned for multiple domains: likely to be interference
        (False, ''),
        # answer IP is returned for one domain: false positive
        (True, 'domain_below_threshold'),
        # answer IPs are CDN: false positive
        (True, 'is_CDN is_CDN'),
    ]
    result = []
    for scan in scans:
      scan = beam_tables._verify(scan)
      result.append(
          (scan['verify']['excluded'], scan['verify']['exclude_reason']))

    self.assertListEqual(result, expected)

  # pylint: enable=protected-access


if __name__ == '__main__':
  unittest.main()
