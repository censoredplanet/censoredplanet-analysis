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

import datetime
from pprint import pprint
import unittest

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline import beam_tables


class PipelineMainTest(unittest.TestCase):

  def test_get_bigquery_schema(self):
    test_field = {
        'string_field': 'string',
    }

    table_schema = beam_tables.get_bigquery_schema(test_field)

    expected_field_schema = beam_bigquery.TableFieldSchema()
    expected_field_schema.name = 'string_field'
    expected_field_schema.type = 'string'
    expected_field_schema.mode = 'nullable'

    expected_table_schema = beam_bigquery.TableSchema()
    expected_table_schema.fields.append(expected_field_schema)

    self.assertEqual(table_schema, expected_table_schema)

  def test_get_table_name(self):
    project = 'firehook-censoredplanet'
    table_name = 'scan'
    dataset_suffix = '_results'

    runner = beam_tables.ScanDataBeamPipelineRunner(project, table_name,
                                                    dataset_suffix, None, None,
                                                    None, None)

    self.assertEqual(
        runner.get_table_name('echo', 'prod'),
        'firehook-censoredplanet:echo_results.scan')
    self.assertEqual(
        runner.get_table_name('discard', 'dev'),
        'firehook-censoredplanet:discard_results.scan_test')
    self.assertEqual(
        runner.get_table_name('http', 'prod'),
        'firehook-censoredplanet:http_results.scan')
    self.assertEqual(
        runner.get_table_name('https', 'dev'),
        'firehook-censoredplanet:https_results.scan_test')

  def test_source_from_filename(self):
    self.assertEqual(
        beam_tables.source_from_filename(
            'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'
        ), 'CP_Quack-echo-2020-08-23-06-01-02')

    self.assertEqual(
        beam_tables.source_from_filename(
            'gs://firehook-scans/http/CP_Quack-http-2020-09-13-01-02-07/results.json'
        ), 'CP_Quack-http-2020-09-13-01-02-07')

  def test_read_scan_text(self):
    p = TestPipeline()
    pipeline = beam_tables.read_scan_text(
        p, ['pipeline/test_results_1.json', 'pipeline/test_results_2.json.gz'])

    beam_test_util.assert_that(
        pipeline,
        beam_test_util.equal_to([
            'test line 1.1', 'test line 1.2', 'test line 2.1', 'test line 2.2'
        ]))

  def test_between_dates(self):
    filename = 'gs://firehook-scans/http/CP_Quack-http-2020-05-11-01-02-08/results.json'

    self.assertTrue(
        beam_tables.between_dates(filename, datetime.date(2020, 5, 10),
                                  datetime.date(2020, 5, 12)))
    self.assertTrue(
        beam_tables.between_dates(filename, datetime.date(2020, 5, 11),
                                  datetime.date(2020, 5, 11)))
    self.assertTrue(
        beam_tables.between_dates(filename, None, datetime.date(2020, 5, 12)))
    self.assertTrue(
        beam_tables.between_dates(filename, datetime.date(2020, 5, 10), None))
    self.assertTrue(beam_tables.between_dates(filename, None, None))

  def test_not_between_dates(self):
    filename = 'gs://firehook-scans/http/CP_Quack-http-2020-05-11-01-02-08/results.json'

    self.assertFalse(
        beam_tables.between_dates(filename, datetime.date(2020, 5, 12),
                                  datetime.date(2020, 5, 10)))
    self.assertFalse(
        beam_tables.between_dates(filename, None, datetime.date(2020, 5, 10)))
    self.assertFalse(
        beam_tables.between_dates(filename, datetime.date(2020, 5, 12), None))

  def test_flatten_measurement(self):
    line = """{
      "Server":"1.2.3.4",
      "Keyword":"www.example.com",
      "Retries":1,
      "Results":[
        {
          "Sent":"GET / HTTP/1.1 Host: www.example.com",
          "Received":"HTTP/1.1 403 Forbidden",
          "Success":false,
          "Error":"Incorrect echo response",
          "StartTime":"2020-09-20T07:45:09.643770291-04:00",
          "EndTime":"2020-09-20T07:45:10.088851843-04:00"
        },
        {
          "Sent":"GET / HTTP/1.1 Host: www.example.com",
          "Received": "HTTP/1.1 503 Service Unavailable",
          "Success":false,
          "Error":"Incorrect echo response",
          "StartTime":"2020-09-20T07:45:16.170427683-04:00",
          "EndTime":"2020-09-20T07:45:16.662093893-04:00"
        }
      ],
      "Blocked":true,
      "FailSanity":false,
      "StatefulBlock":false
    }"""

    expected_rows = [{
        'domain': 'www.example.com',
        'ip': '1.2.3.4',
        'date': '2020-09-20',
        'start_time': '2020-09-20T07:45:09.643770291-04:00',
        'end_time': '2020-09-20T07:45:10.088851843-04:00',
        'retries': 1,
        'sent': 'GET / HTTP/1.1 Host: www.example.com',
        'received': 'HTTP/1.1 403 Forbidden',
        'error': 'Incorrect echo response',
        'blocked': True,
        'success': False,
        'fail_sanity': False,
        'stateful_block': False,
        'measurement_id': '',
        'source': 'CP_Quack-echo-2020-08-23-06-01-02',
    }, {
        'domain': 'www.example.com',
        'ip': '1.2.3.4',
        'date': '2020-09-20',
        'start_time': '2020-09-20T07:45:16.170427683-04:00',
        'end_time': '2020-09-20T07:45:16.662093893-04:00',
        'retries': 1,
        'sent': 'GET / HTTP/1.1 Host: www.example.com',
        'received': 'HTTP/1.1 503 Service Unavailable',
        'error': 'Incorrect echo response',
        'blocked': True,
        'success': False,
        'fail_sanity': False,
        'stateful_block': False,
        'measurement_id': '',
        'source': 'CP_Quack-echo-2020-08-23-06-01-02',
    }]

    filename = 'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'
    rows = list(beam_tables.flatten_measurement(filename, line))
    self.assertEqual(len(rows), 2)

    # Measurement ids should be the same
    self.assertEqual(rows[0]['measurement_id'], rows[1]['measurement_id'])
    # But they're randomly generated,
    # so we can't test them against the full expected rows.
    rows[0]['measurement_id'] = ''
    rows[1]['measurement_id'] = ''

    self.assertListEqual(rows, expected_rows)

  def test_flatten_measurement_received_content(self):
    # HTTP/HTTPS scans have a Received field with json content.
    # Currently we expect this field to be flattened into a string.
    line = """{
      "Server":"184.50.171.225",
      "Keyword":"www.csmonitor.com",
      "Retries":0,
      "Results":[
        {
         "Sent":"www.csmonitor.com",
         "Received":{
            "status_line":"301 Moved Permanently",
            "headers":{
               "Content-Length":["0"],
               "Date":["Sun, 13 Sep 2020 05:10:58 GMT"],
               "Location":["https://www.csmonitor.com/"],
               "Server":["HTTP Proxy/1.0"]
            },
            "body":""
         },
         "Success":false,
         "Error":"Incorrect web response: status lines don't match",
         "StartTime":"2020-09-13T01:10:57.499263112-04:00",
         "EndTime":"2020-09-13T01:10:58.077524926-04:00"
        }
      ],
      "Blocked":true,
      "FailSanity":false,
      "StatefulBlock":false
    }"""

    recieved_substring = ('{"status_line": "301 Moved Permanently", '
                          '"headers": {"Content-Length": ["0"], '
                          '"Date": ["Sun, 13 Sep 2020 05:10:58 GMT"], '
                          '"Location": ["https://www.csmonitor.com/"], '
                          '"Server": ["HTTP Proxy/1.0"]}, "body": ""}')

    expected_row = {
        'domain': 'www.csmonitor.com',
        'ip': '184.50.171.225',
        'date': '2020-09-13',
        'start_time': '2020-09-13T01:10:57.499263112-04:00',
        'end_time': '2020-09-13T01:10:58.077524926-04:00',
        'retries': 0,
        'sent': 'www.csmonitor.com',
        'received': recieved_substring,
        'error': 'Incorrect web response: status lines don\'t match',
        'blocked': True,
        'success': False,
        'fail_sanity': False,
        'stateful_block': False,
        'measurement_id': '',
        'source': 'CP_Quack-http-2020-09-13-01-02-07',
    }
    filename = 'gs://firehook-scans/http/CP_Quack-http-2020-09-13-01-02-07/results.json'

    row = list(beam_tables.flatten_measurement(filename, line))[0]
    # We can't test the measurement id because it's random
    row['measurement_id'] = ''
    self.assertEqual(row, expected_row)

  def test_flatten_measurement_invalid_json(self):
    line = 'invalid json'

    with self.assertLogs(level='WARNING') as cm:
      rows = list(beam_tables.flatten_measurement('test_filename.json', line))
      self.assertEqual(
          cm.output[0], 'WARNING:root:JSONDecodeError: '
          'Expecting value: line 1 column 1 (char 0)\n'
          'Filename: test_filename.json\ninvalid json\n')

    self.assertEqual(len(rows), 0)

  def test_add_metadata(self):
    rows = [{
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

    rows_with_metadata = beam_tables.add_metadata(rows)
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

  def test_make_date_ip_key(self):
    row = {'date': '2020-01-01', 'ip': '1.2.3.4', 'other_field': None}
    self.assertEqual(
        beam_tables.make_date_ip_key(row), ('2020-01-01', '1.2.3.4'))

  def test_add_ip_metadata(self):
    metadatas = list(
        beam_tables.add_ip_metadata('2020-01-01', ['1.1.1.1', '8.8.8.8']))

    expected_key_1 = ('2020-01-01', '1.1.1.1')
    expected_value_1 = {
        'netblock': '1.0.0.1/24',
        'asn': 13335,
        'as_name': 'CLOUDFLARENET',
        'as_full_name': 'Cloudflare Inc.',
        'as_class': 'Content',
        'country': 'US',
    }

    expected_key_2 = ('2020-01-01', '8.8.8.8')
    expected_value_2 = {
        'netblock': '8.8.8.0/24',
        'asn': 15169,
        'as_name': 'GOOGLE',
        'as_full_name': 'Google LLC',
        'as_class': 'Content',
        'country': 'US',
    }

    self.assertListEqual(metadatas, [(expected_key_1, expected_value_1),
                                     (expected_key_2, expected_value_2)])

  def test_merge_metadata_with_rows(self):
    key = ('2020-01-01', '1.1.1.1')
    ip_metadata = {
        'netblock': '1.0.0.1/24',
        'asn': 13335,
        'as_name': 'CLOUDFLARENET',
        'as_full_name': 'Cloudflare Inc.',
        'as_class': 'Content',
        'country': 'US',
    }
    rows = [{
        'domain': 'www.example.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
    }, {
        'domain': 'www.example2.com',
        'ip': '1.1.1.1',
        'date': '2020-01-01',
    }]
    value = {
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

    rows_with_metadata = list(beam_tables.merge_metadata_with_rows(key, value))
    self.assertListEqual(rows_with_metadata, expected_rows)

  def test_get_job_name(self):
    self.assertEqual(
        beam_tables.get_job_name('echo', False, 'dev'),
        'echo-flatten-add-metadata-dev')
    self.assertEqual(
        beam_tables.get_job_name('discard', True, 'dev'),
        'discard-flatten-add-metadata-dev-incremental')
    self.assertEqual(
        beam_tables.get_job_name('http', False, 'prod'),
        'http-flatten-add-metadata-prod')
    self.assertEqual(
        beam_tables.get_job_name('https', True, 'prod'),
        'https-flatten-add-metadata-prod-incremental')


if __name__ == '__main__':
  unittest.main()
