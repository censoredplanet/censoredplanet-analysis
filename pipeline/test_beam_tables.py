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

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline import beam_tables
from pipeline.metadata.fake_ip_metadata import FakeIpMetadata


class PipelineMainTest(unittest.TestCase):
  """Unit tests for beam pipeline steps."""

  # pylint: disable=protected-access

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
    runner = beam_tables.ScanDataBeamPipelineRunner(project, {}, '', '', '',
                                                    FakeIpMetadata, '')

    full_name = runner._get_full_table_name('prod.echo_scan')
    self.assertEqual(full_name, 'firehook-censoredplanet:prod.echo_scan')

  def test_source_from_filename(self) -> None:
    """Test getting the data source name from the filename."""
    self.assertEqual(
        beam_tables._source_from_filename(
            'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'
        ), 'CP_Quack-echo-2020-08-23-06-01-02')

    self.assertEqual(
        beam_tables._source_from_filename(
            'gs://firehook-scans/http/CP_Quack-http-2020-09-13-01-02-07/results.json'
        ), 'CP_Quack-http-2020-09-13-01-02-07')

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

  def test_parse_received_headers(self) -> None:
    """Test parsing HTTP/S header fields into a flat format."""
    headers = {
        'Content-Language': ['en', 'fr'],
        'Content-Type': ['text/html; charset=iso-8859-1']
    }

    flat_headers = beam_tables._parse_received_headers(headers)

    expected_headers = [
        'Content-Language: en', 'Content-Language: fr',
        'Content-Type: text/html; charset=iso-8859-1'
    ]

    self.assertListEqual(flat_headers, expected_headers)

  def test_parse_received_data_http(self) -> None:
    """Test parsing sample HTTP data."""
    # yapf: disable
    received = {
        'status_line': '403 Forbidden',
        'headers': {},
        'body': '<html><head><meta http-equiv="Content-Type" content="text/html; charset=windows-1256"><title>MNN3-1(1)</title></head><body><iframe src="http://10.10.34.35:80" style="width: 100%; height: 100%" scrolling="no" marginwidth="0" marginheight="0" frameborder="0" vspace="0" hspace="0"></iframe></body></html>\r\n\r\n'
    }

    expected = {
        'received_status': '403 Forbidden',
        'received_body': '<html><head><meta http-equiv="Content-Type" content="text/html; charset=windows-1256"><title>MNN3-1(1)</title></head><body><iframe src="http://10.10.34.35:80" style="width: 100%; height: 100%" scrolling="no" marginwidth="0" marginheight="0" frameborder="0" vspace="0" hspace="0"></iframe></body></html>\r\n\r\n',
        'received_headers': []
    }
    # yapf: enable

    parsed = beam_tables._parse_received_data(received)
    self.assertDictEqual(parsed, expected)

  def test_parse_received_data_no_header_field(self) -> None:
    """Test parsing reciveed HTTP/S data missing a header field."""
    # yapf: disable
    received = {
        'status_line': '403 Forbidden',
        'body': '<test-body>'
        # No 'headers' field
    }

    expected = {
        'received_status': '403 Forbidden',
        'received_body': '<test-body>',
        'received_headers': []
    }
    # yapf: enable

    parsed = beam_tables._parse_received_data(received)
    self.assertDictEqual(parsed, expected)

  def test_parse_received_data_https(self) -> None:
    """Test parsing example HTTPS data."""
    # yapf: disable
    received = {
        'status_line': '403 Forbidden',
        'headers': {
            'Content-Length': ['278'],
            'Content-Type': ['text/html'],
            'Date': ['Fri, 06 Nov 2020 20:24:19 GMT'],
            'Expires': ['Fri, 06 Nov 2020 20:24:19 GMT'],
            'Mime-Version': ['1.0'],
            'Server': ['AkamaiGHost'],
            'Set-Cookie': [
                'bm_sz=6A1BDB4DFCA371F55C598A6D50C7DC3F~YAAQtTXdWKzJ+ZR1AQAA6zY7nwmc3d1xb2D5pqi3WHoMGfNsB8zB22LP5Kz/15sxdI3d3qznv4NzhGdb6CjijzFezAd18NREhybEvZMSZe2JHkjBjli/y1ZRMgC512ln7CCHURjS03UWDIzVrpwPV3Z/h/mq00NF2+LgHsDPelEZoArYVmEwH7OtE4zHAePErKw=; Domain=.discover.com; Path=/; Expires=Sat, 07 Nov 2020 00:24:19 GMT; Max-Age=14400; HttpOnly_abck=7A29878FA7120EC680C6E591A8FF3F5A~-1~YAAQtTXdWK3J+ZR1AQAA6zY7nwR93cThkIxWHn0icKtS6Wgb6NVHSQ80nZ6I2DzczA+1qn/0rXSGZUcFvW/+7tmDF0lHsieeRwnmIydhPELwAsNLjfBMF1lJm9Y7u4ppUpD4WtbRJ1g+Qhd9CLcelH3fQ8AVmJn/jRNN8WrisA8GKuUhpfjv9Gp1aGGqzv12H8u3Ogt/9oOv4Y8nKuS7CWipsFuPftCMeTBVbPn/JsV/NzttmkuFikLj8PwmpNecqlhaH1Ra32XDl/hVsCFWaPm4wdWO3d2WDK8Em0sHzklyTV4iFo6itVlCEHQ=~-1~-1~-1; Domain=.discover.com; Path=/; Expires=Sat, 06 Nov 2021 20:24:19 GMT; Max-Age=31536000; Secure'
            ]
        },
        'body': '<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD><BODY>\n<H1>Access Denied</H1>\n \nYou don\'t have permission to access "discover.com" on this server.<P>\nReference 18b535dd581604694259a71c660\n</BODY>\n</HTML>\n',
        'tls': {
            'version': 771,
            'cipher_suite': 49199,
            'cert': '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'
        }
    }

    expected = {
        'received_status': '403 Forbidden',
        'received_body': '<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD><BODY>\n<H1>Access Denied</H1>\n \nYou don\'t have permission to access "discover.com" on this server.<P>\nReference 18b535dd581604694259a71c660\n</BODY>\n</HTML>\n',
        'received_tls_version': 771,
        'received_tls_cipher_suite': 49199,
        'received_tls_cert': 'MIIG1DCCBbygAwIBAgIQBFzDKr18mq0F13LVYhA6FjANBgkqhkiG9w0BAQsFADB1MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3d3cuZGlnaWNlcnQuY29tMTQwMgYDVQQDEytEaWdpQ2VydCBTSEEyIEV4dGVuZGVkIFZhbGlkYXRpb24gU2VydmVyIENBMB4XDTIwMTAwNTAwMDAwMFoXDTIxMTAwNjEyMDAwMFowgdQxHTAbBgNVBA8MFFByaXZhdGUgT3JnYW5pemF0aW9uMRMwEQYLKwYBBAGCNzwCAQMTAkpQMRcwFQYDVQQFEw4wMTAwLTAxLTAwODgyNDELMAkGA1UEBhMCSlAxDjAMBgNVBAgTBVRva3lvMRMwEQYDVQQHEwpDaGl5b2RhLUt1MTkwNwYDVQQKEzBUb2tpbyBNYXJpbmUgYW5kIE5pY2hpZG8gRmlyZSBJbnN1cmFuY2UgQ28uIEx0ZC4xGDAWBgNVBAMTD3d3dy50YWJpa29yZS5qcDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAN+0RFcFIoCHvFTJs/+hexC5SxrKDAytiHNDzXLYaOFuf2LA+7UN33QE8dnINmV0ma7Udd1r8KmXJWJPeTxIJyskad8VNwx0oF00ENS56GYl/y37Y85DE5MQhaQwPEiyQL0TsrL/K2bNYjvEPklBVEOi1vtiOOTZWnUH86MxSe3PwmmXDaFgd3174Z8lEmi20Jl3++Tr/jNeBMw3Sg3KuLW8IUTl6+33mr3Z1u2u6yFN4d7mXlzyo0BxOwlJ1NwJbTzyFnBAfAZ2gJFVFQtuoWdgh9XIquhdFoxCfj/h9zxFK+64xJ+sXGSL5SiEZeBfmvG8SrW4OBSvHzyUSzJKCrsCAwEAAaOCAv4wggL6MB8GA1UdIwQYMBaAFD3TUKXWoK3u80pgCmXTIdT4+NYPMB0GA1UdDgQWBBQKix8NngHND9LiEWxMPAOBE6MwjDAnBgNVHREEIDAeggt0YWJpa29yZS5qcIIPd3d3LnRhYmlrb3JlLmpwMA4GA1UdDwEB/wQEAwIFoDAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwdQYDVR0fBG4wbDA0oDKgMIYuaHR0cDovL2NybDMuZGlnaWNlcnQuY29tL3NoYTItZXYtc2VydmVyLWczLmNybDA0oDKgMIYuaHR0cDovL2NybDQuZGlnaWNlcnQuY29tL3NoYTItZXYtc2VydmVyLWczLmNybDBLBgNVHSAERDBCMDcGCWCGSAGG/WwCATAqMCgGCCsGAQUFBwIBFhxodHRwczovL3d3dy5kaWdpY2VydC5jb20vQ1BTMAcGBWeBDAEBMIGIBggrBgEFBQcBAQR8MHowJAYIKwYBBQUHMAGGGGh0dHA6Ly9vY3NwLmRpZ2ljZXJ0LmNvbTBSBggrBgEFBQcwAoZGaHR0cDovL2NhY2VydHMuZGlnaWNlcnQuY29tL0RpZ2lDZXJ0U0hBMkV4dGVuZGVkVmFsaWRhdGlvblNlcnZlckNBLmNydDAJBgNVHRMEAjAAMIIBBAYKKwYBBAHWeQIEAgSB9QSB8gDwAHYA9lyUL9F3MCIUVBgIMJRWjuNNExkzv98MLyALzE7xZOMAAAF093gqNAAABAMARzBFAiEAz0WGut1b8na4VKfulIqCPRbV+lv05YdPNT2xfWreNAYCIDU3JiavbsMjE/r0M9P2c7B07U72W4TK/PdlsKCg5t1PAHYAXNxDkv7mq0VEsV6a1FbmEDf71fpH3KFzlLJe5vbHDsoAAAF093gqgwAABAMARzBFAiApVQum+1q4C4drBI7t6aObwa5jtmWd/BHVTLPgcdhMXgIhAKv+7bC9X9wstKB0OGQbVVX/qsJ5fzf4Y8zNUaklAQiKMA0GCSqGSIb3DQEBCwUAA4IBAQAD02pESpGPgJSMTpFVm4VRufgwW95fxA/sch63U94owcOmNtrniSoOr8QwLMAVta6VFU6wddbTBd4vz8zauo4R6uAeFaiUBaFaKb5V2bONGclfjTZ7nsDxsowLracGrRx/rQjjovRo2656g5Iu898WIfADxIvsGc5CICGqLB9GvofVWNNb/DoOXf/vLQJj9m5+ZCi0CrIdh31IB/acHsQ8jWr4VlqPGiz2PIdKjBLuI9ckFbMQ/9DCTWfuJhSfwA3kk2EeUa6WlRrjDhJLasjrEmQiSIf3oywdsPspSYOkT91TFUvzjOmK/yZeApxPJmDvjxpum5GZYnn6QthKxMzL',
        'received_headers': [
            'Content-Length: 278',
            'Content-Type: text/html',
            'Date: Fri, 06 Nov 2020 20:24:19 GMT',
            'Expires: Fri, 06 Nov 2020 20:24:19 GMT',
            'Mime-Version: 1.0',
            'Server: AkamaiGHost',
            'Set-Cookie: bm_sz=6A1BDB4DFCA371F55C598A6D50C7DC3F~YAAQtTXdWKzJ+ZR1AQAA6zY7nwmc3d1xb2D5pqi3WHoMGfNsB8zB22LP5Kz/15sxdI3d3qznv4NzhGdb6CjijzFezAd18NREhybEvZMSZe2JHkjBjli/y1ZRMgC512ln7CCHURjS03UWDIzVrpwPV3Z/h/mq00NF2+LgHsDPelEZoArYVmEwH7OtE4zHAePErKw=; Domain=.discover.com; Path=/; Expires=Sat, 07 Nov 2020 00:24:19 GMT; Max-Age=14400; HttpOnly_abck=7A29878FA7120EC680C6E591A8FF3F5A~-1~YAAQtTXdWK3J+ZR1AQAA6zY7nwR93cThkIxWHn0icKtS6Wgb6NVHSQ80nZ6I2DzczA+1qn/0rXSGZUcFvW/+7tmDF0lHsieeRwnmIydhPELwAsNLjfBMF1lJm9Y7u4ppUpD4WtbRJ1g+Qhd9CLcelH3fQ8AVmJn/jRNN8WrisA8GKuUhpfjv9Gp1aGGqzv12H8u3Ogt/9oOv4Y8nKuS7CWipsFuPftCMeTBVbPn/JsV/NzttmkuFikLj8PwmpNecqlhaH1Ra32XDl/hVsCFWaPm4wdWO3d2WDK8Em0sHzklyTV4iFo6itVlCEHQ=~-1~-1~-1; Domain=.discover.com; Path=/; Expires=Sat, 06 Nov 2021 20:24:19 GMT; Max-Age=31536000; Secure'
        ],
    }
    # yapf: enable

    parsed = beam_tables._parse_received_data(received)
    self.assertDictEqual(parsed, expected)

  def test_flatten_measurement_echo(self) -> None:
    """Test parsing an example Echo measurement."""
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

    expected_rows: List[beam_tables.Row] = [{
        'domain': 'www.example.com',
        'ip': '1.2.3.4',
        'date': '2020-09-20',
        'start_time': '2020-09-20T07:45:09.643770291-04:00',
        'end_time': '2020-09-20T07:45:10.088851843-04:00',
        'retries': 1,
        'sent': 'GET / HTTP/1.1 Host: www.example.com',
        'received_status': 'HTTP/1.1 403 Forbidden',
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
        'received_status': 'HTTP/1.1 503 Service Unavailable',
        'error': 'Incorrect echo response',
        'blocked': True,
        'success': False,
        'fail_sanity': False,
        'stateful_block': False,
        'measurement_id': '',
        'source': 'CP_Quack-echo-2020-08-23-06-01-02',
    }]

    filename = 'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'
    rows = list(beam_tables._flatten_measurement(filename, line))
    self.assertEqual(len(rows), 2)

    # Measurement ids should be the same
    self.assertEqual(rows[0]['measurement_id'], rows[1]['measurement_id'])
    # But they're randomly generated,
    # so we can't test them against the full expected rows.
    rows[0]['measurement_id'] = ''
    rows[1]['measurement_id'] = ''

    self.assertListEqual(rows, expected_rows)

  def test_flatten_measurement_http_success(self) -> None:
    """Test parsing an example successful HTTP measurement

    Not all measurements recieve any data/errors,
    in that case the received_ and error fields should not exist
    and will end up Null in bigquery.
    """

    line = """{
      "Server":"170.248.33.11",
      "Keyword":"scribd.com",
      "Retries":0,
      "Results":[
        {
         "Sent":"scribd.com",
         "Success":true,
         "StartTime":"2020-11-09T01:10:47.826486107-05:00",
         "EndTime":"2020-11-09T01:10:47.84869292-05:00"
        }
      ],
      "Blocked":false,
      "FailSanity":false,
      "StatefulBlock":false
    }"""

    expected_row = {
        'domain': 'scribd.com',
        'ip': '170.248.33.11',
        'date': '2020-11-09',
        'start_time': '2020-11-09T01:10:47.826486107-05:00',
        'end_time': '2020-11-09T01:10:47.84869292-05:00',
        'retries': 0,
        'sent': 'scribd.com',
        'blocked': False,
        'success': True,
        'fail_sanity': False,
        'stateful_block': False,
        'measurement_id': '',
        'source': 'CP_Quack-http-2020-11-09-01-02-08',
    }

    filename = 'gs://firehook-scans/http/CP_Quack-http-2020-11-09-01-02-08/results.json'

    row = list(beam_tables._flatten_measurement(filename, line))[0]
    # We can't test the measurement id because it's random
    row['measurement_id'] = ''
    self.assertEqual(row, expected_row)

  def test_flatten_measurement_http(self) -> None:
    """Test parsing an unsuccessful HTTP measurement."""
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
            "body":"test body"
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

    expected_row = {
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
        'blocked': True,
        'success': False,
        'fail_sanity': False,
        'stateful_block': False,
        'measurement_id': '',
        'source': 'CP_Quack-http-2020-09-13-01-02-07',
    }
    filename = 'gs://firehook-scans/http/CP_Quack-http-2020-09-13-01-02-07/results.json'

    row = list(beam_tables._flatten_measurement(filename, line))[0]
    # We can't test the measurement id because it's random
    row['measurement_id'] = ''
    self.assertEqual(row, expected_row)

  def test_flatten_measurement_https(self) -> None:
    """Test parsing an unsuccessful HTTPS measurement."""
    line = """{
      "Server":"213.175.166.157",
      "Keyword":"www.arabhra.org",
      "Retries":2,
      "Results":[
        {
         "Sent":"www.arabhra.org",
         "Received":{
           "status_line":"302 Found",
           "headers":{
             "Content-Language":["en"],
             "Content-Type":["text/html; charset=iso-8859-1"],
             "Date":["Fri, 06 Nov 2020 20:24:21 GMT"],
             "Location":[
               "https://jobs.bankaudi.com.lb/OA_HTML/IrcVisitor.jsp"
             ],
             "Set-Cookie":[
               "BIGipServer~IFRMS-WEB~IFRMS-OHS-HTTPS=rd7o00000000000000000000ffffc0a8fde4o4443; expires=Fri, 06-Nov-2020 21:24:21 GMT; path=/; Httponly; Secure",
               "TS016c74f4=01671efb9a1a400535e215d6f76498a5887425fed793ca942baa75f16076e60e1350988222922fa06fc16f53ef016d9ecd38535fcabf14861525811a7c3459e91086df326f; Path=/"
             ],
             "X-Frame-Options":["SAMEORIGIN"]
            },
            "body":"\\u003c!DOCTYPE HTML PUBLIC \\\"-//IETF//DTD HTML 2.0//EN\\\"\\u003e\\n\\u003cHTML\\u003e\\u003cHEAD\\u003e\\n\\u003cTITLE\\u003e302 Found\\u003c/TITLE\\u003e\\n\\u003c/HEAD\\u003e\\u003cBODY\\u003e\\n\\u003cH1\\u003eFound\\u003c/H1\\u003e\\nThe document has moved \\u003cA HREF=\\\"https://jobs.bankaudi.com.lb/OA_HTML/IrcVisitor.jsp\\\"\\u003ehere\\u003c/A\\u003e.\\u003cP\\u003e\\n\\u003c/BODY\\u003e\\u003c/HTML\\u003e\\n",
            "tls":{
              "version":771,
              "cipher_suite":49199,
              "cert":"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"
            }
          },
          "Success":false,
          "Error":"Incorrect web response: status lines don't match",
          "StartTime":"2020-11-06T15:24:21.124508839-05:00",
          "EndTime":"2020-11-06T15:24:21.812075476-05:00"
        }
      ],
      "Blocked":false,
      "FailSanity":false,
      "StatefulBlock":false
    }
    """
    filename = 'gs://firehook-scans/http/CP_Quack-https-2020-11-06-15-15-31/results.json'

    # yapf: disable
    expected_row: beam_tables.Row = {
        'domain': 'www.arabhra.org',
        'ip': '213.175.166.157',
        'date': '2020-11-06',
        'start_time': '2020-11-06T15:24:21.124508839-05:00',
        'end_time': '2020-11-06T15:24:21.812075476-05:00',
        'retries': 2,
        'sent': 'www.arabhra.org',
        'received_status': '302 Found',
        # The received_body field in the json has a lot of unicode escapes
        # but the interpreted string in the output should not.
        'received_body': '<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n<HTML><HEAD>\n<TITLE>302 Found</TITLE>\n</HEAD><BODY>\n<H1>Found</H1>\nThe document has moved <A HREF=\"https://jobs.bankaudi.com.lb/OA_HTML/IrcVisitor.jsp\">here</A>.<P>\n</BODY></HTML>\n',
        'received_tls_version': 771,
        'received_tls_cipher_suite': 49199,
        'received_tls_cert': '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',
        'received_headers': [
            'Content-Language: en',
            'Content-Type: text/html; charset=iso-8859-1',
            'Date: Fri, 06 Nov 2020 20:24:21 GMT',
            'Location: https://jobs.bankaudi.com.lb/OA_HTML/IrcVisitor.jsp',
            'Set-Cookie: BIGipServer~IFRMS-WEB~IFRMS-OHS-HTTPS=rd7o00000000000000000000ffffc0a8fde4o4443; expires=Fri, 06-Nov-2020 21:24:21 GMT; path=/; Httponly; Secure',
            'Set-Cookie: TS016c74f4=01671efb9a1a400535e215d6f76498a5887425fed793ca942baa75f16076e60e1350988222922fa06fc16f53ef016d9ecd38535fcabf14861525811a7c3459e91086df326f; Path=/',
            'X-Frame-Options: SAMEORIGIN',
        ],
        'error': 'Incorrect web response: status lines don\'t match',
        'blocked': False,
        'success': False,
        'fail_sanity': False,
        'stateful_block': False,
        'measurement_id': '',
        'source': 'CP_Quack-https-2020-11-06-15-15-31',
    }
    # yapf: enable

    row = list(beam_tables._flatten_measurement(filename, line))[0]
    # We can't test the measurement id because it's random
    row['measurement_id'] = ''

    self.assertEqual(row, expected_row)

  def test_flatten_measurement_invalid_json(self) -> None:
    """Test logging an error when parsing invalid JSON."""
    line = 'invalid json'

    with self.assertLogs(level='WARNING') as cm:
      rows = list(beam_tables._flatten_measurement('test_filename.json', line))
      self.assertEqual(
          cm.output[0], 'WARNING:root:JSONDecodeError: '
          'Expecting value: line 1 column 1 (char 0)\n'
          'Filename: test_filename.json\ninvalid json\n')

    self.assertEqual(len(rows), 0)

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

    runner = beam_tables.ScanDataBeamPipelineRunner('', {}, '', '', '',
                                                    FakeIpMetadata, '')

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
    runner = beam_tables.ScanDataBeamPipelineRunner('', {}, '', '', '',
                                                    FakeIpMetadata, '')

    metadatas = list(
        runner._add_ip_metadata('2020-01-01', ['1.1.1.1', '8.8.8.8']))

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

    self.assertListEqual(metadatas, [(expected_key_1, expected_value_1),
                                     (expected_key_2, expected_value_2)])

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

  # pylint: enable=protected-access


if __name__ == '__main__':
  unittest.main()
