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

from pprint import pprint
import unittest
import warnings

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery as cloud_bigquery
from google.cloud.exceptions import NotFound

import firehook_resources
from pipeline import beam_tables

BEAM_TEST_TABLE = 'test.test'
BQ_TEST_TABLE = firehook_resources.PROJECT_NAME + '.test.test'


def run_write_pipeline(rows, incremental=False):
  if incremental:
    job_name = 'write-test-test'
  else:
    job_name = 'append-test-test'

  pipeline_options = PipelineOptions(
      runner='DirectRunner',
      job_name=job_name,
      project=firehook_resources.PROJECT_NAME,
      temp_location=firehook_resources.BEAM_TEMP_LOCATION)

  with beam.Pipeline(options=pipeline_options) as p:
    test_runner = beam_tables.ScanDataBeamPipelineRunner(
        firehook_resources.PROJECT_NAME, beam_tables.SCAN_BIGQUERY_SCHEMA, None,
        None, None, None, None)
    pcoll = (p | beam.Create(rows))
    test_runner.write_to_bigquery(pcoll, BEAM_TEST_TABLE, incremental)


class PipelineManualBigqueryTest(unittest.TestCase):

  def test_manual_write_to_bigquery(self):
    # Suppress some unittest socket warnings in beam code we don't control
    warnings.simplefilter('ignore', ResourceWarning)

    client = cloud_bigquery.Client()
    try:
      # Clean up table if previous run failed to
      client.get_table(BQ_TEST_TABLE)
      client.delete_table(BQ_TEST_TABLE)
    except NotFound:
      pass

    # These rows represent different types scan types (echo/discard/http/https)
    # The actual pipeline doesn't write different scan types to the same table
    # But since the table schemas are all the same we do it here to test all the
    # different types of fields.
    rows1 = [
        {
            'domain': 'www.arabhra.org',
            'ip': '213.175.166.157',
            'date': '2020-11-06',
            'start_time': '2020-11-06T15:24:21.124508839-05:00',
            'end_time': '2020-11-06T15:24:21.812075476-05:00',
            'retries': 2,
            'sent': 'www.arabhra.org',
            'received_status': '302 Found',
            'received_body': '<fake-html-body>',
            'received_tls_version': 771,
            'received_tls_cipher_suite': 49199,
            'received_tls_cert': 'fake-cert-string',
            'received_headers': [
                'Content-Language: en',
                'Content-Type: text/html; charset=iso-8859-1',
                'Date: Fri, 06 Nov 2020 20:24:21 GMT',
                'Location: https://jobs.bankaudi.com.lb/OA_HTML/IrcVisitor.jsp',
                'X-Frame-Options: SAMEORIGIN',
            ],
            'error': 'Incorrect web response: status lines don\'t match',
            'netblock': '213.175.166.0/24',
            'asn': 9051,
            'as_name': '',
            'as_full_name': 'IncoNet Data Management sal',
            'as_class': 'Transit/Access',
            'country': 'LB',
            'blocked': False,
            'success': False,
            'fail_sanity': False,
            'stateful_block': False,
            'measurement_id': 'b644c883038145428e4df7f08f91f409',
            'source': 'CP_Quack-https-2020-11-06-15-15-31',
        },
        {
            'domain': 'www.csmonitor.com',
            'ip': '184.50.171.225',
            'date': '2020-09-13',
            'start_time': '2020-09-13T01:10:57.499263112-04:00',
            'end_time': '2020-09-13T01:10:58.077524926-04:00',
            'retries': 0,
            'sent': 'www.csmonitor.com',
            'received_status': '301 Moved Permanently',
            'received_body': 'test body',
            'received_headers': [
                'Content-Length: 0',
                'Date: Sun, 13 Sep 2020 05:10:58 GMT',
                'Location: https://www.csmonitor.com/',
                'Server: HTTP Proxy/1.0',
            ],
            'error': 'Incorrect web response: status lines don\'t match',
            'netblock': '184.50.160.0/20',
            'asn': 16625,
            'as_name': 'AKAMAI-AS',
            'as_full_name': 'Akamai Technologies, Inc.',
            'as_class': 'Content',
            'country': 'US',
            'blocked': True,
            'success': False,
            'fail_sanity': False,
            'stateful_block': False,
            'measurement_id': 'ea164e70f2ec4e6288e5c48b36983648',
            'source': 'CP_Quack-http-2020-09-13-01-02-07',
        },
    ]

    rows2 = [
        {
            'domain': 'scribd.com',
            'ip': '170.248.33.11',
            'date': '2020-11-09',
            'start_time': '2020-11-09T01:10:47.826486107-05:00',
            'end_time': '2020-11-09T01:10:47.84869292-05:00',
            'retries': 0,
            'sent': 'scribd.com',
            # test writing 'received_headers' as null (no key in this dict)
            'netblock': '170.248.32.0/23',
            'asn': 3573,
            'as_name': 'ACCENTURE',
            'as_full_name': 'Accenture LLP',
            'as_class': 'Enterprise',
            'country': 'US',
            'blocked': False,
            'success': True,
            'fail_sanity': False,
            'stateful_block': False,
            'measurement_id': '1c1cb51ab63d409bb07b96cf22f4afff',
            'source': 'CP_Quack-http-2020-11-09-01-02-08',
        },
        {
            'domain': 'www.example.com',
            'ip': '1.1.1.1',
            'date': '2020-09-20',
            'start_time': '2020-09-20T07:45:16.170427683-04:00',
            'end_time': '2020-09-20T07:45:16.662093893-04:00',
            'retries': 1,
            'sent': 'GET / HTTP/1.1 Host: www.example.com',
            'received_status': 'HTTP/1.1 503 Service Unavailable',
            # test writing received_headers as an empty list,
            # both ways are valid
            'received_headers': [],
            'error': 'Incorrect echo response',
            'netblock': '1.0.0.1/24',
            'asn': 13335,
            'as_name': 'CLOUDFLARENET',
            'as_full_name': 'Cloudflare Inc.',
            'as_class': 'Content',
            'country': 'US',
            'blocked': True,
            'success': False,
            'fail_sanity': False,
            'stateful_block': False,
            'measurement_id': 'a20840a462e94cb0b156cb6f025558da',
            'source': 'CP_Quack-echo-2020-08-23-06-01-02',
        }
    ]

    run_write_pipeline(rows1, incremental=False)

    written_rows = client.query('SELECT * FROM ' + BQ_TEST_TABLE).result()
    self.assertEqual(len(list(written_rows)), 2)

    run_write_pipeline(rows2, incremental=True)

    written_rows = client.query('SELECT * FROM ' + BQ_TEST_TABLE).result()
    self.assertEqual(len(list(written_rows)), 4)

    client.delete_table(BQ_TEST_TABLE)


# This test is not run by default in unittest because it writes to bigquery
# to run it manually use the command
#
# python3 -m unittest pipeline.manual_test_bigquery_write.PipelineManualBigqueryTest
if __name__ == '__main__':
  unittest.main()
