"""Unit tests for flattening data"""

import unittest
from typing import List

from pipeline.metadata import flatten


class FlattenMeasurementTest(unittest.TestCase):
  """Unit tests for pipeline flattening."""

  def setUp(self) -> None:
    self.maxDiff = None  # pylint: disable=invalid-name

  # pylint: disable=protected-access

  def test_source_from_filename(self) -> None:
    """Test getting the data source name from the filename."""
    self.assertEqual(
        flatten.source_from_filename(
            'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'
        ), 'CP_Quack-echo-2020-08-23-06-01-02')

    self.assertEqual(
        flatten.source_from_filename(
            'gs://firehook-scans/http/CP_Quack-http-2020-09-13-01-02-07/results.json'
        ), 'CP_Quack-http-2020-09-13-01-02-07')

  def test_extract_domain_from_sent_field_echo_discard(self) -> None:
    """Test parsing url from sent field in echo and discard tests."""
    sent = "GET / HTTP/1.1\r\nHost: example5718349450314.com\r\n"
    self.assertEqual("example5718349450314.com",
                     flatten._extract_domain_from_sent_field(sent))

  def test_extract_domain_from_sent_field_discard_error(self) -> None:
    """Test parsing url from sent field."""
    # This sent format is an error
    # but it appears in the data, so we parse it.
    sent = "GET www.bbc.co.uk HTTP/1.1\r\nHost: /\r\n"
    self.assertEqual("www.bbc.co.uk",
                     flatten._extract_domain_from_sent_field(sent))

  def test_extract_url_from_sent_field(self) -> None:
    """Test parsing full url from sent field in echo and discard tests."""
    sent = "GET /videos/news/fresh-incidents-of-violence-in-assam/videoshow/15514015.cms HTTP/1.1\r\nHost: timesofindia.indiatimes.com\r\n"
    self.assertEqual(
        "timesofindia.indiatimes.com/videos/news/fresh-incidents-of-violence-in-assam/videoshow/15514015.cms",
        flatten._extract_domain_from_sent_field(sent))

  def test_extract_url_from_sent_field_error(self) -> None:
    """Test parsing full url from sent field in echo and discard tests."""
    # This sent format is an error
    # but it appears in the data, so we parse it.
    sent = "GET www.guaribas.pi.gov.br HTTP/1.1\r\nHost: /portal1/intro.asp?iIdMun=100122092\r\n"
    self.assertEqual(
        "www.guaribas.pi.gov.br/portal1/intro.asp?iIdMun=100122092",
        flatten._extract_domain_from_sent_field(sent))

  def test_extract_domain_from_sent_field_http(self) -> None:
    """Test parsing url from sent field for HTTP/S"""
    sent = "www.apple.com"
    self.assertEqual("www.apple.com",
                     flatten._extract_domain_from_sent_field(sent))

  def test_extract_domain_from_sent_field_invalid(self) -> None:
    """Test parsing url from sent field."""
    sent = "Get sdkfjhsd something incorrect"
    with self.assertRaises(Exception) as context:
      flatten._extract_domain_from_sent_field(sent)
    self.assertIn('unknown sent field format:', str(context.exception))

  def test_parse_received_headers(self) -> None:
    """Test parsing HTTP/S header fields into a flat format."""
    headers = {
        'Content-Language': ['en', 'fr'],
        'Content-Type': ['text/html; charset=iso-8859-1']
    }

    flat_headers = flatten.parse_received_headers(headers)

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
        'received_headers': [],
        'blockpage': True,
        'page_signature': 'b_nat_ir_national_1',
    }
    # yapf: enable
    flattener = flatten.FlattenMeasurement()
    flattener.setup()
    parsed = flattener._parse_received_data(received, True)
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
        'received_headers': [],
        'blockpage': None,
        'page_signature': None,
    }
    # yapf: enable
    flattener = flatten.FlattenMeasurement()
    flattener.setup()
    parsed = flattener._parse_received_data(received, True)
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
        'received_tls_cert': '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',
        'received_headers': [
            'Content-Length: 278',
            'Content-Type: text/html',
            'Date: Fri, 06 Nov 2020 20:24:19 GMT',
            'Expires: Fri, 06 Nov 2020 20:24:19 GMT',
            'Mime-Version: 1.0',
            'Server: AkamaiGHost',
            'Set-Cookie: bm_sz=6A1BDB4DFCA371F55C598A6D50C7DC3F~YAAQtTXdWKzJ+ZR1AQAA6zY7nwmc3d1xb2D5pqi3WHoMGfNsB8zB22LP5Kz/15sxdI3d3qznv4NzhGdb6CjijzFezAd18NREhybEvZMSZe2JHkjBjli/y1ZRMgC512ln7CCHURjS03UWDIzVrpwPV3Z/h/mq00NF2+LgHsDPelEZoArYVmEwH7OtE4zHAePErKw=; Domain=.discover.com; Path=/; Expires=Sat, 07 Nov 2020 00:24:19 GMT; Max-Age=14400; HttpOnly_abck=7A29878FA7120EC680C6E591A8FF3F5A~-1~YAAQtTXdWK3J+ZR1AQAA6zY7nwR93cThkIxWHn0icKtS6Wgb6NVHSQ80nZ6I2DzczA+1qn/0rXSGZUcFvW/+7tmDF0lHsieeRwnmIydhPELwAsNLjfBMF1lJm9Y7u4ppUpD4WtbRJ1g+Qhd9CLcelH3fQ8AVmJn/jRNN8WrisA8GKuUhpfjv9Gp1aGGqzv12H8u3Ogt/9oOv4Y8nKuS7CWipsFuPftCMeTBVbPn/JsV/NzttmkuFikLj8PwmpNecqlhaH1Ra32XDl/hVsCFWaPm4wdWO3d2WDK8Em0sHzklyTV4iFo6itVlCEHQ=~-1~-1~-1; Domain=.discover.com; Path=/; Expires=Sat, 06 Nov 2021 20:24:19 GMT; Max-Age=31536000; Secure'
        ],
        'blockpage': False,
        'page_signature': 'x_on_this_server',
    }
    # yapf: enable
    flattener = flatten.FlattenMeasurement()
    flattener.setup()
    parsed = flattener._parse_received_data(received, True)
    self.assertDictEqual(parsed, expected)

  def test_flattenmeasurement_echo_v1(self) -> None:
    """Test parsing an example Echo v1 measurement."""
    line = """{
      "Server":"1.2.3.4",
      "Keyword":"www.test.com",
      "Retries":1,
      "Results":[
        {
          "Sent":"GET / HTTP/1.1\\r\\nHost: www.test.com\\r\\n\\r\\n",
          "Received": "HTTP/1.1 503 Service Unavailable",
          "Success":false,
          "Error":"Incorrect echo response",
          "StartTime":"2020-09-20T07:45:09.643770291-04:00",
          "EndTime":"2020-09-20T07:45:10.088851843-04:00"

        },
        {
          "Sent":"",
          "Received":"",
          "Success":false,
          "Error":"timeout",
          "StartTime":"2020-09-20T07:45:12.643770291-04:00",
          "EndTime":"2020-09-20T07:45:13.088851843-04:00"
        },
        {
          "Sent":"",
          "Success":false,
          "Error":"timeout",
          "StartTime":"2020-09-20T07:45:16.170427683-04:00",
          "EndTime":"2020-09-20T07:45:16.662093893-04:00"
        },
        {
          "Sent":"GET / HTTP/1.1\\r\\nHost: example5718349450314.com\\r\\n\\r\\n",
          "Received":"HTTP/1.1 403 Forbidden",
          "Success":false,
          "Error":"Incorrect echo response",
          "StartTime":"2020-09-20T07:45:18.170427683-04:00",
          "EndTime":"2020-09-20T07:45:18.662093893-04:00"
        }
      ],
      "Blocked":true,
      "FailSanity":true,
      "StatefulBlock":false
    }"""

    expected_rows: List[flatten.Row] = [
        {
            'domain': 'www.test.com',
            'category': None,
            'ip': '1.2.3.4',
            'date': '2020-09-20',
            'start_time': '2020-09-20T07:45:09.643770291-04:00',
            'end_time': '2020-09-20T07:45:10.088851843-04:00',
            'received_status': 'HTTP/1.1 503 Service Unavailable',
            'error': 'Incorrect echo response',
            'blockpage': False,
            'page_signature': 'x_generic_503_4',
            'anomaly': True,
            'success': False,
            'stateful_block': False,
            'is_control': False,
            'controls_failed': True,
            'measurement_id': '',
            'source': 'CP_Quack-echo-2020-08-23-06-01-02',
        },
        {
            'domain':
                'www.test.com',  # domain is populated even though sent was empty
            'category': None,
            'ip': '1.2.3.4',
            'date': '2020-09-20',
            'start_time': '2020-09-20T07:45:12.643770291-04:00',
            'end_time': '2020-09-20T07:45:13.088851843-04:00',
            'received_status': '',
            'error': 'timeout',
            'blockpage': None,
            'page_signature': None,
            'anomaly': True,
            'success': False,
            'stateful_block': False,
            'is_control': False,  # calculated even though sent was empty
            'controls_failed': True,
            'measurement_id': '',
            'source': 'CP_Quack-echo-2020-08-23-06-01-02',
        },
        {
            'domain':
                '',  # missing control domain is not populated when sent is empty
            'category': None,
            'ip': '1.2.3.4',
            'date': '2020-09-20',
            'start_time': '2020-09-20T07:45:16.170427683-04:00',
            'end_time': '2020-09-20T07:45:16.662093893-04:00',
            'error': 'timeout',
            'anomaly': True,
            'success': False,
            'stateful_block': False,
            'is_control': True,  # calculated even though sent was empty
            'controls_failed': True,
            'measurement_id': '',
            'source': 'CP_Quack-echo-2020-08-23-06-01-02',
        },
        {
            'domain': 'example5718349450314.com',
            'category': None,
            'ip': '1.2.3.4',
            'date': '2020-09-20',
            'start_time': '2020-09-20T07:45:18.170427683-04:00',
            'end_time': '2020-09-20T07:45:18.662093893-04:00',
            'received_status': 'HTTP/1.1 403 Forbidden',
            'blockpage': None,
            'page_signature': None,
            'error': 'Incorrect echo response',
            'anomaly': True,
            'success': False,
            'stateful_block': False,
            'is_control': True,
            'controls_failed': True,
            'measurement_id': '',
            'source': 'CP_Quack-echo-2020-08-23-06-01-02',
        },
    ]

    filename = 'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'

    flattener = flatten.FlattenMeasurement()
    flattener.setup()
    rows = list(flattener.process((filename, line)))
    self.assertEqual(len(rows), 4)

    # Measurement ids should be the same
    self.assertEqual(rows[0]['measurement_id'], rows[1]['measurement_id'])
    self.assertEqual(rows[0]['measurement_id'], rows[2]['measurement_id'])
    self.assertEqual(rows[0]['measurement_id'], rows[3]['measurement_id'])
    # But they're randomly generated,
    # so we can't test them against the full expected rows.
    rows[0]['measurement_id'] = ''
    rows[1]['measurement_id'] = ''
    rows[2]['measurement_id'] = ''
    rows[3]['measurement_id'] = ''

    self.assertListEqual(rows, expected_rows)

  def test_flattenmeasurement_echo_v2(self) -> None:
    """Test parsing an example successful Echo v2 measurement."""
    line = """{
      "vp": "146.112.255.132",
      "location": {
        "country_name": "United States",
        "country_code": "US"
      },
      "service": "echo",
      "test_url": "104.com.tw",
      "response": [
        {
          "matches_template": true,
          "control_url": "control-9f26cf1579e1e31d.com",
          "start_time": "2021-05-30T01:01:03.783547451-04:00",
          "end_time": "2021-05-30T01:01:03.829470254-04:00"
        },
        {
          "matches_template": true,
          "start_time": "2021-05-30T01:01:03.829473355-04:00",
          "end_time": "2021-05-30T01:01:03.855786298-04:00"
        },
        {
          "matches_template": false,
          "error": "dial tcp 0.0.0.0:0->204.116.44.177:7: bind: address already in use",
          "start_time": "2021-05-30T01:01:41.43715135-04:00",
          "end_time": "2021-05-30T01:01:41.484703377-04:00"
        }
      ],
      "anomaly": true,
      "controls_failed": false,
      "stateful_block": false,
      "tag": "2021-05-30T01:01:01"
    }"""

    expected_rows: List[flatten.Row] = [{
        'domain': 'control-9f26cf1579e1e31d.com',
        'category': None,
        'ip': '146.112.255.132',
        'date': '2021-05-30',
        'start_time': '2021-05-30T01:01:03.783547451-04:00',
        'end_time': '2021-05-30T01:01:03.829470254-04:00',
        'anomaly': True,
        'success': True,
        'stateful_block': False,
        'is_control': True,
        'controls_failed': False,
        'measurement_id': '',
        'source': 'CP_Quack-echo-2021-05-30-01-01-01',
    }, {
        'domain': '104.com.tw',
        'category': 'Social Networking',
        'ip': '146.112.255.132',
        'date': '2021-05-30',
        'start_time': '2021-05-30T01:01:03.829473355-04:00',
        'end_time': '2021-05-30T01:01:03.855786298-04:00',
        'anomaly': True,
        'success': True,
        'stateful_block': False,
        'is_control': False,
        'controls_failed': False,
        'measurement_id': '',
        'source': 'CP_Quack-echo-2021-05-30-01-01-01',
    }, {
        'domain':
            '104.com.tw',
        'category':
            'Social Networking',
        'ip':
            '146.112.255.132',
        'date':
            '2021-05-30',
        'start_time':
            '2021-05-30T01:01:41.43715135-04:00',
        'end_time':
            '2021-05-30T01:01:41.484703377-04:00',
        "error":
            "dial tcp 0.0.0.0:0->204.116.44.177:7: bind: address already in use",
        'anomaly':
            True,
        'success':
            False,
        'stateful_block':
            False,
        'is_control':
            False,
        'controls_failed':
            False,
        'measurement_id':
            '',
        'source':
            'CP_Quack-echo-2021-05-30-01-01-01',
    }]

    filename = 'gs://firehook-scans/echo/CP_Quack-echo-2021-05-30-01-01-01/results.json'

    flattener = flatten.FlattenMeasurement()
    flattener.setup()
    rows = list(flattener.process((filename, line)))
    self.assertEqual(len(rows), 3)

    # Measurement ids should be the same
    self.assertEqual(rows[0]['measurement_id'], rows[1]['measurement_id'])
    self.assertEqual(rows[0]['measurement_id'], rows[2]['measurement_id'])
    # But they're randomly generated,
    # so we can't test them against the full expected rows.
    rows[0]['measurement_id'] = ''
    rows[1]['measurement_id'] = ''
    rows[2]['measurement_id'] = ''

    self.assertListEqual(rows, expected_rows)

  def test_flattenmeasurement_discard_v2(self) -> None:
    """Test parsing an example failed Discard v2 measurement."""
    line = """{
      "vp": "117.78.42.54",
      "location": {
        "country_name": "China",
        "country_code": "CN"
      },
      "service": "discard",
      "test_url": "123rf.com",
      "response": [
        {
          "matches_template": true,
          "control_url": "control-2e116cc633eb1fbd.com",
          "start_time": "2021-05-31T12:46:33.600692607-04:00",
          "end_time": "2021-05-31T12:46:35.244736764-04:00"
        },
        {
          "matches_template": false,
          "response": "",
          "error": "read tcp 141.212.123.235:11397->117.78.42.54:9: read: connection reset by peer",
          "start_time": "2021-05-31T12:46:45.544781756-04:00",
          "end_time": "2021-05-31T12:46:45.971628233-04:00"
        },
        {
          "matches_template": true,
          "control_url": "control-be2b77e1cde11c02.com",
          "start_time": "2021-05-31T12:46:48.97188782-04:00",
          "end_time": "2021-05-31T12:46:50.611808471-04:00"
        }
      ],
      "anomaly": true,
      "controls_failed": false,
      "stateful_block": false,
      "tag": "2021-05-31T12:43:21"
    }"""

    expected_rows: List[flatten.Row] = [{
        'domain': 'control-2e116cc633eb1fbd.com',
        'category': None,
        'ip': '117.78.42.54',
        'date': '2021-05-31',
        'start_time': '2021-05-31T12:46:33.600692607-04:00',
        'end_time': '2021-05-31T12:46:35.244736764-04:00',
        'anomaly': True,
        'success': True,
        'stateful_block': False,
        'is_control': True,
        'controls_failed': False,
        'measurement_id': '',
        'source': 'CP_Quack-discard-2021-05-31-12-43-21',
    }, {
        'domain':
            '123rf.com',
        'category':
            'E-commerce',
        'ip':
            '117.78.42.54',
        'date':
            '2021-05-31',
        'start_time':
            '2021-05-31T12:46:45.544781756-04:00',
        'end_time':
            '2021-05-31T12:46:45.971628233-04:00',
        'error':
            'read tcp 141.212.123.235:11397->117.78.42.54:9: read: connection reset by peer',
        'received_status':
            '',
        'blockpage':
            None,
        'page_signature':
            None,
        'anomaly':
            True,
        'success':
            False,
        'stateful_block':
            False,
        'is_control':
            False,
        'controls_failed':
            False,
        'measurement_id':
            '',
        'source':
            'CP_Quack-discard-2021-05-31-12-43-21',
    }, {
        'domain': 'control-be2b77e1cde11c02.com',
        'category': None,
        'ip': '117.78.42.54',
        'date': '2021-05-31',
        'start_time': '2021-05-31T12:46:48.97188782-04:00',
        'end_time': '2021-05-31T12:46:50.611808471-04:00',
        'anomaly': True,
        'success': True,
        'stateful_block': False,
        'is_control': True,
        'controls_failed': False,
        'measurement_id': '',
        'source': 'CP_Quack-discard-2021-05-31-12-43-21',
    }]

    filename = 'gs://firehook-scans/discard/CP_Quack-discard-2021-05-31-12-43-21/results.json'

    flattener = flatten.FlattenMeasurement()
    flattener.setup()
    rows = list(flattener.process((filename, line)))
    self.assertEqual(len(rows), 3)

    # Measurement ids should be the same
    self.assertEqual(rows[0]['measurement_id'], rows[1]['measurement_id'])
    self.assertEqual(rows[0]['measurement_id'], rows[2]['measurement_id'])
    # But they're randomly generated,
    # so we can't test them against the full expected rows.
    rows[0]['measurement_id'] = ''
    rows[1]['measurement_id'] = ''
    rows[2]['measurement_id'] = ''

    self.assertListEqual(rows, expected_rows)

  def test_flattenmeasurement_http_success_v1(self) -> None:
    """Test parsing an example successful HTTP v1 measurement

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
        'category': 'File-sharing',
        'ip': '170.248.33.11',
        'date': '2020-11-09',
        'start_time': '2020-11-09T01:10:47.826486107-05:00',
        'end_time': '2020-11-09T01:10:47.84869292-05:00',
        'anomaly': False,
        'success': True,
        'stateful_block': False,
        'is_control': False,
        'controls_failed': False,
        'measurement_id': '',
        'source': 'CP_Quack-http-2020-11-09-01-02-08',
    }

    filename = 'gs://firehook-scans/http/CP_Quack-http-2020-11-09-01-02-08/results.json'

    flattener = flatten.FlattenMeasurement()
    flattener.setup()
    row = list(flattener.process((filename, line)))[0]
    # We can't test the measurement id because it's random
    row['measurement_id'] = ''
    self.assertEqual(row, expected_row)

  def test_flattenmeasurement_http_v1(self) -> None:
    """Test parsing an unsuccessful HTTP v1 measurement."""
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
        'category': 'Religion',
        'ip': '184.50.171.225',
        'date': '2020-09-13',
        'start_time': '2020-09-13T01:10:57.499263112-04:00',
        'end_time': '2020-09-13T01:10:58.077524926-04:00',
        'received_status': '301 Moved Permanently',
        'received_body': 'test body',
        'received_headers': [
            'Content-Length: 0',
            'Date: Sun, 13 Sep 2020 05:10:58 GMT',
            'Location: https://www.csmonitor.com/',
            'Server: HTTP Proxy/1.0',
        ],
        'blockpage': None,
        'page_signature': None,
        'error': 'Incorrect web response: status lines don\'t match',
        'anomaly': True,
        'success': False,
        'stateful_block': False,
        'is_control': False,
        'controls_failed': False,
        'measurement_id': '',
        'source': 'CP_Quack-http-2020-09-13-01-02-07',
    }
    filename = 'gs://firehook-scans/http/CP_Quack-http-2020-09-13-01-02-07/results.json'

    flattener = flatten.FlattenMeasurement()
    flattener.setup()
    row = list(flattener.process((filename, line)))[0]
    # We can't test the measurement id because it's random
    row['measurement_id'] = ''
    self.assertEqual(row, expected_row)

  def test_flattenmeasurement_http_v2(self) -> None:
    """Test parsing an unsuccessful HTTP v2 measurement."""
    line = """{
      "vp": "167.207.140.67",
      "location": {
        "country_name": "United States",
        "country_code": "US"
      },
      "service": "http",
      "test_url": "1337x.to",
      "response": [
        {
          "matches_template": true,
          "control_url": "control-a459b35b8d53c7eb.com",
          "start_time": "2021-05-30T01:02:13.620124638-04:00",
          "end_time": "2021-05-30T01:02:14.390201235-04:00"
        },
        {
          "matches_template": false,
          "response": {
            "status_line": "503 Service Unavailable",
            "headers": {
              "Cache-Control": [
                "no-store, no-cache"
              ],
              "Content-Length": [
                "1395"
              ],
              "Content-Type": [
                "text/html; charset=UTF-8"
              ],
              "Expires": [
                "Thu, 01 Jan 1970 00:00:00 GMT"
              ],
              "P3p": [
                "CP=\\"CAO PSA OUR\\""
              ],
              "Pragma": [
                "no-cache"
              ]
            },
            "body": "<html>short body for test</html>"
          },
          "start_time": "2021-05-30T01:02:14.390233996-04:00",
          "end_time": "2021-05-30T01:02:15.918981416-04:00"
        }
      ],
      "anomaly": true,
      "controls_failed": false,
      "stateful_block": false,
      "tag": "2021-05-30T01:01:01"
    }"""

    expected_rows: List[flatten.Row] = [{
        'domain': 'control-a459b35b8d53c7eb.com',
        'category': None,
        'ip': '167.207.140.67',
        'date': '2021-05-30',
        'start_time': '2021-05-30T01:02:13.620124638-04:00',
        'end_time': '2021-05-30T01:02:14.390201235-04:00',
        'anomaly': True,
        'success': True,
        'stateful_block': False,
        'is_control': True,
        'controls_failed': False,
        'measurement_id': '',
        'source': 'CP_Quack-http-2021-05-30-01-01-01',
    }, {
        'domain': '1337x.to',
        'category': 'Media sharing',
        'ip': '167.207.140.67',
        'date': '2021-05-30',
        'start_time': '2021-05-30T01:02:14.390233996-04:00',
        'end_time': '2021-05-30T01:02:15.918981416-04:00',
        'received_status': '503 Service Unavailable',
        'received_body': '<html>short body for test</html>',
        'received_headers': [
            'Cache-Control: no-store, no-cache', 'Content-Length: 1395',
            'Content-Type: text/html; charset=UTF-8',
            'Expires: Thu, 01 Jan 1970 00:00:00 GMT', 'P3p: CP=\"CAO PSA OUR\"',
            'Pragma: no-cache'
        ],
        'blockpage': None,
        'page_signature': None,
        'anomaly': True,
        'success': False,
        'stateful_block': False,
        'is_control': False,
        'controls_failed': False,
        'measurement_id': '',
        'source': 'CP_Quack-http-2021-05-30-01-01-01',
    }]

    filename = 'gs://firehook-scans/http/CP_Quack-http-2021-05-30-01-01-01/results.json'

    flattener = flatten.FlattenMeasurement()
    flattener.setup()
    rows = list(flattener.process((filename, line)))
    self.assertEqual(len(rows), 2)

    # Measurement ids should be the same
    self.assertEqual(rows[0]['measurement_id'], rows[1]['measurement_id'])
    # But they're randomly generated,
    # so we can't test them against the full expected rows.
    rows[0]['measurement_id'] = ''
    rows[1]['measurement_id'] = ''

    self.assertListEqual(rows, expected_rows)

  def test_flattenmeasurement_https_v1(self) -> None:
    """Test parsing an unsuccessful HTTPS v1 measurement."""
    # yapf: disable
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
    # yapf: enable

    filename = 'gs://firehook-scans/http/CP_Quack-https-2020-11-06-15-15-31/results.json'

    # yapf: disable
    expected_row: flatten.Row = {
        'domain': 'www.arabhra.org',
        'category': 'Intergovernmental Organizations',
        'ip': '213.175.166.157',
        'date': '2020-11-06',
        'start_time': '2020-11-06T15:24:21.124508839-05:00',
        'end_time': '2020-11-06T15:24:21.812075476-05:00',
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
        'anomaly': False,
        'success': False,
        'stateful_block': False,
        'is_control': False,
        'controls_failed': False,
        'measurement_id': '',
        'source': 'CP_Quack-https-2020-11-06-15-15-31',
    }
    # yapf: enable

    flattener = flatten.FlattenMeasurement()
    flattener.setup()
    row = list(flattener.process((filename, line)))[0]
    # We can't test the measurement id because it's random
    row['measurement_id'] = ''

    self.assertEqual(row, expected_row)

  def test_flattenmeasurement_https_v2(self) -> None:
    """Test parsing an unsuccessful HTTPS v2 measurement."""
    # yapf: disable
    line = """{
      "vp": "41.0.4.132",
      "location": {
        "country_name": "South Africa",
        "country_code": "ZA"
      },
      "service": "https",
      "test_url": "antivigilancia.org",
      "response": [
        {
          "matches_template": false,
          "response": {
            "status_line": "200 OK",
            "headers": {
              "Cache-Control": [
                "no-store, no-cache, must-revalidate, post-check=0, pre-check=0"
              ],
              "Charset": [
                "utf-8"
              ]
            },
            "body": "<!DOCTYPE html> replaced long body",
            "TlsVersion": 771,
            "CipherSuite": 49199,
            "Certificate": "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"
          },
          "control_url": "control-1b13950f35f3208b.com",
          "start_time": "2021-04-26T04:38:36.78214922-04:00",
          "end_time": "2021-04-26T04:38:39.114151569-04:00"
        }
      ],
      "anomaly": false,
      "controls_failed": true,
      "stateful_block": false,
      "tag": "2021-04-26T04:21:46"
    }"""
    # yapf: enable

    filename = 'gs://firehook-scans/https/CP_Quack-https-2021-04-26-04-21-46/results.json'

    # yapf: disable
    expected_row: flatten.Row = {
        'domain': 'control-1b13950f35f3208b.com',
        'category': None,
        'ip': '41.0.4.132',
        'date': '2021-04-26',
        'start_time': '2021-04-26T04:38:36.78214922-04:00',
        'end_time': '2021-04-26T04:38:39.114151569-04:00',
        'received_status': '200 OK',
        'received_body': '<!DOCTYPE html> replaced long body',
        'received_tls_version': 771,
        'received_tls_cipher_suite': 49199,
        'received_tls_cert': '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',
        'received_headers': [
            'Cache-Control: no-store, no-cache, must-revalidate, post-check=0, pre-check=0',
            'Charset: utf-8'
        ],
        'anomaly': False,
        'success': False,
        'stateful_block': False,
        'is_control': True,
        'controls_failed': True,
        'measurement_id': '',
        'source': 'CP_Quack-https-2021-04-26-04-21-46',
    }
    # yapf: enable

    flattener = flatten.FlattenMeasurement()
    flattener.setup()
    row = list(flattener.process((filename, line)))[0]
    # We can't test the measurement id because it's random
    row['measurement_id'] = ''

    self.assertEqual(row, expected_row)

  def test_flattenmeasurement_invalid_json(self) -> None:
    """Test logging an error when parsing invalid JSON."""
    line = 'invalid json'

    with self.assertLogs(level='WARNING') as cm:
      flattener = flatten.FlattenMeasurement()
      flattener.setup()
      rows = list(flattener.process(('test_filename.json', line)))
      self.assertEqual(
          cm.output[0], 'WARNING:root:JSONDecodeError: '
          'Expecting value: line 1 column 1 (char 0)\n'
          'Filename: test_filename.json\ninvalid json\n')

    self.assertEqual(len(rows), 0)

  def test_flattenmeasurement_dns(self) -> None:
    """Test flattening of Satellite measurements."""

    filenames = [
        'gs://firehook-scans/satellite/CP_Satellite-2020-09-02-12-00-01/interference.json',
        'gs://firehook-scans/satellite/CP_Satellite-2020-09-02-12-00-01/interference.json',
        'gs://firehook-scans/satellite/CP_Satellite-2020-09-02-12-00-01/interference.json',
        'gs://firehook-scans/satellite/CP_Satellite-2021-03-01-12-00-01/interference.json'
    ]

    # yapf: disable
    interference = [
        """{
          "resolver":"67.69.184.215",
          "query":"asana.com",
          "answers":{
            "151.101.1.184":["ip", "http", "cert", "asnum", "asname"],
            "151.101.129.184":["ip", "http", "cert", "asnum", "asname"],
            "151.101.193.184":["ip", "http", "cert", "asnum", "asname"],
            "151.101.65.184":["ip", "cert", "asnum", "asname"]
          },
          "passed":true
        }
        """,
        """{
          "resolver":"145.239.6.50",
          "query":"www.ecequality.org",
          "answers":{
            "160.153.136.3":[]
          },
          "passed":false
        }
        """,
        """{
          "resolver":"185.228.168.149",
          "query":"www.sportsinteraction.com",
          "error":"no_answer"
        }
        """,
        """{
          "vp":"114.114.114.110",
          "location":{
            "country_name":"China",
            "country_code":"CN"
          },
          "test_url":"abs-cbn.com",
          "response":{
            "104.20.161.135":[],
            "104.20.161.134":[],
            "rcode":["0","0","0"]
          },
          "passed_control":true,
          "connect_error":false,
          "in_control_group":true,
          "anomaly":true,
          "confidence":{
            "average":0,
            "matches":[0,0],
            "untagged_controls":false,
            "untagged_response":false
          },
          "start_time":"2021-03-05 15:32:05.324807502 -0500 EST m=+6.810936646",
          "end_time":"2021-03-05 15:32:05.366104911 -0500 EST m=+6.852233636"
        }
        """
    ]

    expected = [
      {
        'domain': 'asana.com',
        'category': 'E-commerce',
        'ip': '67.69.184.215',
        'date': '2020-09-02',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': {'ip': '151.101.1.184', 'matches_control': 'ip http cert asnum asname'},
        'measurement_id': ''
      },
      {
        'domain': 'asana.com',
        'category': 'E-commerce',
        'ip': '67.69.184.215',
        'date': '2020-09-02',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': {'ip': '151.101.129.184', 'matches_control': 'ip http cert asnum asname'},
        'measurement_id': ''
      },
      {
        'domain': 'asana.com',
        'category': 'E-commerce',
        'ip': '67.69.184.215',
        'date': '2020-09-02',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': {'ip': '151.101.193.184', 'matches_control': 'ip http cert asnum asname'},
        'measurement_id': ''
      },
      {
        'domain': 'asana.com',
        'category': 'E-commerce',
        'ip': '67.69.184.215',
        'date': '2020-09-02',
        'error': None,
        'anomaly': False,
        'success': True,
        'received': {'ip': '151.101.65.184', 'matches_control': 'ip cert asnum asname'},
        'measurement_id': ''
      },
      {
        'domain': 'www.ecequality.org',
        'category': 'LGBT',
        'ip': '145.239.6.50',
        'date': '2020-09-02',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': {'ip': '160.153.136.3', 'matches_control': ''},
        'measurement_id': ''
      },
      {
        'domain':  'www.sportsinteraction.com',
        'category': 'Gambling',
        'ip': '185.228.168.149',
        'date': '2020-09-02',
        'error': "no_answer",
        'anomaly': None,
        'success': False,
        'received': None,
        'measurement_id': ''
      },
      {
        'domain': 'abs-cbn.com',
        'category': 'Culture',
        'ip': '114.114.114.110',
        'country': 'CN',
        'date': '2021-03-05',
        'start_time': '2021-03-05T15:32:05.324807502-05:00',
        'end_time': '2021-03-05T15:32:05.366104911-05:00',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': {'ip': '104.20.161.135', 'matches_control': ''},
        'rcode': ["0", "0", "0"],
        'measurement_id': ''
      },
      {
        'domain': 'abs-cbn.com',
        'category': 'Culture',
        'ip': '114.114.114.110',
        'country': 'CN',
        'date': '2021-03-05',
        'start_time': '2021-03-05T15:32:05.324807502-05:00',
        'end_time': '2021-03-05T15:32:05.366104911-05:00',
        'error': None,
        'anomaly': True,
        'success': True,
        'received': {'ip': '104.20.161.134', 'matches_control': ''},
        'rcode': ["0", "0", "0"],
        'measurement_id': ''
      }
    ]
    # yapf: enable

    result = []
    for filename, i in zip(filenames, interference):
      flattener = flatten.FlattenMeasurement()
      flattener.setup()
      rows = flattener.process((filename, i))
      # remove random measurement id
      for row in rows:
        row['measurement_id'] = ''
        result.append(row.copy())
    self.assertListEqual(result, expected)
