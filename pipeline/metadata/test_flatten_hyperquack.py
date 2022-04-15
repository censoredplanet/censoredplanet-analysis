"""Unit tests for hyperquack flattening functions"""

import unittest

from pipeline.metadata.schema import HyperquackRow, HttpsResponse
from pipeline.metadata.blockpage import BlockpageMatcher
from pipeline.metadata.domain_categories import DomainCategoryMatcher

from pipeline.metadata import flatten_hyperquack


def get_hyperquack_flattener() -> flatten_hyperquack.HyperquackFlattener:
  blockpage_matcher = BlockpageMatcher()
  category_matcher = DomainCategoryMatcher()
  return flatten_hyperquack.HyperquackFlattener(blockpage_matcher,
                                                category_matcher)


class FlattenHyperquackTest(unittest.TestCase):
  """Unit tests for hyperquack pipeline flattening."""

  def setUp(self) -> None:
    self.maxDiff = None  # pylint: disable=invalid-name

  # pylint: disable=protected-access

  def test_extract_domain_from_sent_field_echo_discard(self) -> None:
    """Test parsing url from sent field in echo and discard tests."""
    sent = "GET / HTTP/1.1\r\nHost: example5718349450314.com\r\n"
    self.assertEqual("example5718349450314.com",
                     flatten_hyperquack._extract_domain_from_sent_field(sent))

  def test_extract_domain_from_sent_field_discard_error(self) -> None:
    """Test parsing url from sent field."""
    # This sent format is an error
    # but it appears in the data, so we parse it.
    sent = "GET www.bbc.co.uk HTTP/1.1\r\nHost: /\r\n"
    self.assertEqual("www.bbc.co.uk",
                     flatten_hyperquack._extract_domain_from_sent_field(sent))

  def test_extract_url_from_sent_field(self) -> None:
    """Test parsing full url from sent field in echo and discard tests."""
    sent = "GET /videos/news/fresh-incidents-of-violence-in-assam/videoshow/15514015.cms HTTP/1.1\r\nHost: timesofindia.indiatimes.com\r\n"
    self.assertEqual(
        "timesofindia.indiatimes.com/videos/news/fresh-incidents-of-violence-in-assam/videoshow/15514015.cms",
        flatten_hyperquack._extract_domain_from_sent_field(sent))

  def test_extract_url_from_sent_field_error(self) -> None:
    """Test parsing full url from sent field in echo and discard tests."""
    # This sent format is an error
    # but it appears in the data, so we parse it.
    sent = "GET www.guaribas.pi.gov.br HTTP/1.1\r\nHost: /portal1/intro.asp?iIdMun=100122092\r\n"
    self.assertEqual(
        "www.guaribas.pi.gov.br/portal1/intro.asp?iIdMun=100122092",
        flatten_hyperquack._extract_domain_from_sent_field(sent))

  def test_extract_domain_from_sent_field_http(self) -> None:
    """Test parsing url from sent field for HTTP/S"""
    sent = "www.apple.com"
    self.assertEqual("www.apple.com",
                     flatten_hyperquack._extract_domain_from_sent_field(sent))

  def test_extract_domain_from_sent_field_invalid(self) -> None:
    """Test parsing url from sent field."""
    sent = "Get sdkfjhsd something incorrect"
    with self.assertRaises(Exception) as context:
      flatten_hyperquack._extract_domain_from_sent_field(sent)
    self.assertIn('unknown sent field format:', str(context.exception))

  # pylint: enable=protected-access

  def test_flattenmeasurement_echo_v1(self) -> None:
    """Test parsing an example Echo v1 measurement."""
    line = {
        "Server": "1.2.3.4",
        "Keyword": "www.test.com",
        "Retries": 1,
        "Results": [{
            "Sent": "GET / HTTP/1.1\r\nHost: www.test.com\r\n\r\n",
            "Received": "HTTP/1.1 503 Service Unavailable",
            "Success": False,
            "Error": "Incorrect echo response",
            "StartTime": "2020-09-20T07:45:09.643770291-04:00",
            "EndTime": "2020-09-20T07:45:10.088851843-04:00"
        }, {
            "Sent": "",
            "Received": "",
            "Success": False,
            "Error": "i/o timeout",
            "StartTime": "2020-09-20T07:45:12.643770291-04:00",
            "EndTime": "2020-09-20T07:45:13.088851843-04:00"
        }, {
            "Sent": "",
            "Success": False,
            "Error": "i/o timeout",
            "StartTime": "2020-09-20T07:45:16.170427683-04:00",
            "EndTime": "2020-09-20T07:45:16.662093893-04:00"
        }, {
            "Sent": "GET / HTTP/1.1\r\nHost: example5718349450314.com\r\n\r\n",
            "Received": "HTTP/1.1 403 Forbidden",
            "Success": False,
            "Error": "Incorrect echo response",
            "StartTime": "2020-09-20T07:45:18.170427683-04:00",
            "EndTime": "2020-09-20T07:45:18.662093893-04:00"
        }],
        "Blocked": True,
        "FailSanity": True,
        "StatefulBlock": False
    }

    expected_rows = [
        HyperquackRow(
            domain='www.test.com',
            category=None,
            ip='1.2.3.4',
            date='2020-09-20',
            start_time='2020-09-20T07:45:09.643770291-04:00',
            end_time='2020-09-20T07:45:10.088851843-04:00',
            error='Incorrect echo response',
            anomaly=True,
            success=False,
            stateful_block=False,
            is_control=False,
            controls_failed=True,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-echo-2020-08-23-06-01-02',
            received=HttpsResponse(
                is_known_blockpage=False,
                status='HTTP/1.1 503 Service Unavailable',
                page_signature='x_generic_503_4',
            ),
            outcome='content/mismatch'),
        HyperquackRow(
            domain=
            'www.test.com',  # domain is populated even though sent was empty
            category=None,
            ip='1.2.3.4',
            date='2020-09-20',
            start_time='2020-09-20T07:45:12.643770291-04:00',
            end_time='2020-09-20T07:45:13.088851843-04:00',
            error='i/o timeout',
            anomaly=True,
            success=False,
            stateful_block=False,
            is_control=False,  # calculated even though sent was empty
            controls_failed=True,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-echo-2020-08-23-06-01-02',
            received=HttpsResponse(
                is_known_blockpage=None,
                status='',
                page_signature=None,
            ),
            outcome='read/timeout'),
        HyperquackRow(
            domain=
            '',  # missing control domain is not populated when sent is empty
            category='Control',
            ip='1.2.3.4',
            date='2020-09-20',
            start_time='2020-09-20T07:45:16.170427683-04:00',
            end_time='2020-09-20T07:45:16.662093893-04:00',
            error='i/o timeout',
            anomaly=True,
            success=False,
            stateful_block=False,
            is_control=True,  # calculated even though sent was empty
            controls_failed=True,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-echo-2020-08-23-06-01-02',
            outcome='read/timeout'),
        HyperquackRow(
            domain='example5718349450314.com',
            category='Control',
            ip='1.2.3.4',
            date='2020-09-20',
            start_time='2020-09-20T07:45:18.170427683-04:00',
            end_time='2020-09-20T07:45:18.662093893-04:00',
            error='Incorrect echo response',
            anomaly=True,
            success=False,
            stateful_block=False,
            is_control=True,
            controls_failed=True,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-echo-2020-08-23-06-01-02',
            received=HttpsResponse(
                is_known_blockpage=None,
                status='HTTP/1.1 403 Forbidden',
                page_signature=None,
            ),
            outcome='content/mismatch'),
    ]

    filename = 'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'

    flattener = get_hyperquack_flattener()
    rows = list(
        flattener.process_hyperquack(filename, line,
                                     '81e2a76dafe04131bc38fc6ec7bbddca'))

    self.assertEqual(len(rows), 4)
    self.assertListEqual(rows, expected_rows)

  def test_flattenmeasurement_echo_v2(self) -> None:
    """Test parsing an example successful Echo v2 measurement."""
    line = {
        "vp": "146.112.255.132",
        "location": {
            "country_name": "United States",
            "country_code": "US"
        },
        "service": "echo",
        "test_url": "104.com.tw",
        "response": [{
            "matches_template": True,
            "control_url": "control-9f26cf1579e1e31d.com",
            "start_time": "2021-05-30T01:01:03.783547451-04:00",
            "end_time": "2021-05-30T01:01:03.829470254-04:00"
        }, {
            "matches_template": True,
            "start_time": "2021-05-30T01:01:03.829473355-04:00",
            "end_time": "2021-05-30T01:01:03.855786298-04:00"
        }, {
            "matches_template":
                False,
            "error":
                "dial tcp 0.0.0.0:0->204.116.44.177:7: bind: address already in use",
            "start_time":
                "2021-05-30T01:01:41.43715135-04:00",
            "end_time":
                "2021-05-30T01:01:41.484703377-04:00"
        }],
        "anomaly": True,
        "controls_failed": False,
        "stateful_block": False,
        "tag": "2021-05-30T01:01:01"
    }

    expected_rows = [
        HyperquackRow(
            domain='control-9f26cf1579e1e31d.com',
            category='Control',
            ip='146.112.255.132',
            date='2021-05-30',
            start_time='2021-05-30T01:01:03.783547451-04:00',
            end_time='2021-05-30T01:01:03.829470254-04:00',
            anomaly=True,
            success=True,
            stateful_block=False,
            is_control=True,
            controls_failed=False,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-echo-2021-05-30-01-01-01',
            outcome='expected/match'),
        HyperquackRow(
            domain='104.com.tw',
            category='Social Networking',
            ip='146.112.255.132',
            date='2021-05-30',
            start_time='2021-05-30T01:01:03.829473355-04:00',
            end_time='2021-05-30T01:01:03.855786298-04:00',
            anomaly=True,
            success=True,
            stateful_block=False,
            is_control=False,
            controls_failed=False,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-echo-2021-05-30-01-01-01',
            outcome='expected/match'),
        HyperquackRow(
            domain='104.com.tw',
            category='Social Networking',
            ip='146.112.255.132',
            date='2021-05-30',
            start_time='2021-05-30T01:01:41.43715135-04:00',
            end_time='2021-05-30T01:01:41.484703377-04:00',
            error=
            "dial tcp 0.0.0.0:0->204.116.44.177:7: bind: address already in use",
            anomaly=True,
            success=False,
            stateful_block=False,
            is_control=False,
            controls_failed=False,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-echo-2021-05-30-01-01-01',
            outcome='setup/system_failure')
    ]

    filename = 'gs://firehook-scans/echo/CP_Quack-echo-2021-05-30-01-01-01/results.json'

    flattener = get_hyperquack_flattener()
    rows = list(
        flattener.process_hyperquack(filename, line,
                                     '81e2a76dafe04131bc38fc6ec7bbddca'))

    self.assertEqual(len(rows), 3)
    self.assertListEqual(rows, expected_rows)

  def test_flattenmeasurement_discard_v2(self) -> None:
    """Test parsing an example failed Discard v2 measurement."""
    line = {
        "vp": "117.78.42.54",
        "location": {
            "country_name": "China",
            "country_code": "CN"
        },
        "service": "discard",
        "test_url": "123rf.com",
        "response": [{
            "matches_template": True,
            "control_url": "control-2e116cc633eb1fbd.com",
            "start_time": "2021-05-31T12:46:33.600692607-04:00",
            "end_time": "2021-05-31T12:46:35.244736764-04:00"
        }, {
            "matches_template":
                False,
            "response":
                "",
            "error":
                "read tcp 141.212.123.235:11397->117.78.42.54:9: read: connection reset by peer",
            "start_time":
                "2021-05-31T12:46:45.544781756-04:00",
            "end_time":
                "2021-05-31T12:46:45.971628233-04:00"
        }, {
            "matches_template": True,
            "control_url": "control-be2b77e1cde11c02.com",
            "start_time": "2021-05-31T12:46:48.97188782-04:00",
            "end_time": "2021-05-31T12:46:50.611808471-04:00"
        }],
        "anomaly": True,
        "controls_failed": False,
        "stateful_block": False,
        "tag": "2021-05-31T12:43:21"
    }

    expected_rows = [
        HyperquackRow(
            domain='control-2e116cc633eb1fbd.com',
            category='Control',
            ip='117.78.42.54',
            date='2021-05-31',
            start_time='2021-05-31T12:46:33.600692607-04:00',
            end_time='2021-05-31T12:46:35.244736764-04:00',
            anomaly=True,
            success=True,
            stateful_block=False,
            is_control=True,
            controls_failed=False,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-discard-2021-05-31-12-43-21',
            outcome='expected/match'),
        HyperquackRow(
            domain='123rf.com',
            category='E-commerce',
            ip='117.78.42.54',
            date='2021-05-31',
            start_time='2021-05-31T12:46:45.544781756-04:00',
            end_time='2021-05-31T12:46:45.971628233-04:00',
            error=
            'read tcp 141.212.123.235:11397->117.78.42.54:9: read: connection reset by peer',
            anomaly=True,
            success=False,
            stateful_block=False,
            is_control=False,
            controls_failed=False,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-discard-2021-05-31-12-43-21',
            received=HttpsResponse(
                is_known_blockpage=None,
                status='',
                page_signature=None,
            ),
            outcome='read/tcp.reset'),
        HyperquackRow(
            domain='control-be2b77e1cde11c02.com',
            category='Control',
            ip='117.78.42.54',
            date='2021-05-31',
            start_time='2021-05-31T12:46:48.97188782-04:00',
            end_time='2021-05-31T12:46:50.611808471-04:00',
            anomaly=True,
            success=True,
            stateful_block=False,
            is_control=True,
            controls_failed=False,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-discard-2021-05-31-12-43-21',
            outcome='expected/match')
    ]

    filename = 'gs://firehook-scans/discard/CP_Quack-discard-2021-05-31-12-43-21/results.json'

    flattener = get_hyperquack_flattener()
    rows = list(
        flattener.process_hyperquack(filename, line,
                                     '81e2a76dafe04131bc38fc6ec7bbddca'))

    self.assertEqual(len(rows), 3)
    self.assertListEqual(rows, expected_rows)

  def test_flattenmeasurement_http_success_v1(self) -> None:
    """Test parsing an example successful HTTP v1 measurement

    Not all measurements recieve any data/errors,
    in that case the received_ and error fields should not exist
    and will end up Null in bigquery.
    """

    line = {
        "Server": "170.248.33.11",
        "Keyword": "scribd.com",
        "Retries": 0,
        "Results": [{
            "Sent": "scribd.com",
            "Success": True,
            "StartTime": "2020-11-09T01:10:47.826486107-05:00",
            "EndTime": "2020-11-09T01:10:47.84869292-05:00"
        }],
        "Blocked": False,
        "FailSanity": False,
        "StatefulBlock": False
    }

    expected_row = HyperquackRow(
        domain='scribd.com',
        category='File-sharing',
        ip='170.248.33.11',
        date='2020-11-09',
        start_time='2020-11-09T01:10:47.826486107-05:00',
        end_time='2020-11-09T01:10:47.84869292-05:00',
        anomaly=False,
        success=True,
        stateful_block=False,
        is_control=False,
        controls_failed=False,
        measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
        source='CP_Quack-http-2020-11-09-01-02-08',
        outcome='expected/match')

    filename = 'gs://firehook-scans/http/CP_Quack-http-2020-11-09-01-02-08/results.json'

    flattener = get_hyperquack_flattener()
    row = list(
        flattener.process_hyperquack(filename, line,
                                     '81e2a76dafe04131bc38fc6ec7bbddca'))[0]
    self.assertEqual(row, expected_row)

  def test_flattenmeasurement_http_v1(self) -> None:
    """Test parsing an unsuccessful HTTP v1 measurement."""
    line = {
        "Server": "184.50.171.225",
        "Keyword": "www.csmonitor.com",
        "Retries": 0,
        "Results": [{
            "Sent": "www.csmonitor.com",
            "Received": {
                "status_line": "301 Moved Permanently",
                "headers": {
                    "Content-Length": ["0"],
                    "Date": ["Sun, 13 Sep 2020 05:10:58 GMT"],
                    "Location": ["https://www.csmonitor.com/"],
                    "Server": ["HTTP Proxy/1.0"]
                },
                "body": "test body"
            },
            "Success": False,
            "Error": "Incorrect web response: status lines don't match",
            "StartTime": "2020-09-13T01:10:57.499263112-04:00",
            "EndTime": "2020-09-13T01:10:58.077524926-04:00"
        }],
        "Blocked": True,
        "FailSanity": False,
        "StatefulBlock": False
    }

    expected_row = HyperquackRow(
        domain='www.csmonitor.com',
        category='Religion',
        ip='184.50.171.225',
        date='2020-09-13',
        start_time='2020-09-13T01:10:57.499263112-04:00',
        end_time='2020-09-13T01:10:58.077524926-04:00',
        error='Incorrect web response: status lines don\'t match',
        anomaly=True,
        success=False,
        stateful_block=False,
        is_control=False,
        controls_failed=False,
        measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
        source='CP_Quack-http-2020-09-13-01-02-07',
        received=HttpsResponse(
            status='301 Moved Permanently',
            body='test body',
            headers=[
                'Content-Length: 0',
                'Date: Sun, 13 Sep 2020 05:10:58 GMT',
                'Location: https://www.csmonitor.com/',
                'Server: HTTP Proxy/1.0',
            ],
            is_known_blockpage=False,
            page_signature='p_fp_33',
        ),
        outcome='content/status_mismatch:301')
    filename = 'gs://firehook-scans/http/CP_Quack-http-2020-09-13-01-02-07/results.json'

    flattener = get_hyperquack_flattener()
    row = list(
        flattener.process_hyperquack(filename, line,
                                     '81e2a76dafe04131bc38fc6ec7bbddca'))[0]
    self.assertEqual(row, expected_row)

  def test_flattenmeasurement_http_v2(self) -> None:
    """Test parsing an unsuccessful HTTP v2 measurement."""
    line = {
        "vp": "167.207.140.67",
        "location": {
            "country_name": "United States",
            "country_code": "US"
        },
        "service": "http",
        "test_url": "1337x.to",
        "response": [{
            "matches_template": True,
            "control_url": "control-a459b35b8d53c7eb.com",
            "start_time": "2021-05-30T01:02:13.620124638-04:00",
            "end_time": "2021-05-30T01:02:14.390201235-04:00"
        }, {
            "matches_template": False,
            "response": {
                "status_line": "503 Service Unavailable",
                "headers": {
                    "Cache-Control": ["no-store, no-cache"],
                    "Content-Length": ["1395"],
                    "Content-Type": ["text/html; charset=UTF-8"],
                    "Expires": ["Thu, 01 Jan 1970 00:00:00 GMT"],
                    "P3p": ["CP=\"CAO PSA OUR\""],
                    "Pragma": ["no-cache"]
                },
                "body": "<html>short body for test</html>"
            },
            "start_time": "2021-05-30T01:02:14.390233996-04:00",
            "end_time": "2021-05-30T01:02:15.918981416-04:00"
        }],
        "anomaly": True,
        "controls_failed": False,
        "stateful_block": False,
        "tag": "2021-05-30T01:01:01"
    }

    expected_rows = [
        HyperquackRow(
            domain='control-a459b35b8d53c7eb.com',
            category='Control',
            ip='167.207.140.67',
            date='2021-05-30',
            start_time='2021-05-30T01:02:13.620124638-04:00',
            end_time='2021-05-30T01:02:14.390201235-04:00',
            anomaly=True,
            success=True,
            stateful_block=False,
            is_control=True,
            controls_failed=False,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-http-2021-05-30-01-01-01',
            outcome='expected/match'),
        HyperquackRow(
            domain='1337x.to',
            category='Media sharing',
            ip='167.207.140.67',
            date='2021-05-30',
            start_time='2021-05-30T01:02:14.390233996-04:00',
            end_time='2021-05-30T01:02:15.918981416-04:00',
            anomaly=True,
            success=False,
            stateful_block=False,
            is_control=False,
            controls_failed=False,
            measurement_id='81e2a76dafe04131bc38fc6ec7bbddca',
            source='CP_Quack-http-2021-05-30-01-01-01',
            received=HttpsResponse(
                status='503 Service Unavailable',
                body='<html>short body for test</html>',
                headers=[
                    'Cache-Control: no-store, no-cache', 'Content-Length: 1395',
                    'Content-Type: text/html; charset=UTF-8',
                    'Expires: Thu, 01 Jan 1970 00:00:00 GMT',
                    'P3p: CP=\"CAO PSA OUR\"', 'Pragma: no-cache'
                ],
                is_known_blockpage=False,
                page_signature='x_generic_503_4',
            ),
            outcome='content/mismatch')
    ]

    filename = 'gs://firehook-scans/http/CP_Quack-http-2021-05-30-01-01-01/results.json'

    flattener = get_hyperquack_flattener()
    rows = list(
        flattener.process_hyperquack(filename, line,
                                     '81e2a76dafe04131bc38fc6ec7bbddca'))

    self.assertEqual(len(rows), 2)
    self.assertListEqual(rows, expected_rows)

  def test_flattenmeasurement_https_v1(self) -> None:
    """Test parsing an unsuccessful HTTPS v1 measurement."""
    # yapf: disable
    line = {
      "Server": "213.175.166.157",
      "Keyword": "www.arabhra.org",
      "Retries": 2,
      "Results": [
        {
         "Sent": "www.arabhra.org",
         "Received": {
           "status_line": "302 Found",
           "headers": {
             "Content-Language": ["en"],
             "Content-Type": ["text/html; charset=iso-8859-1"],
             "Date": ["Fri, 06 Nov 2020 20:24:21 GMT"],
             "Location": [
               "https://jobs.bankaudi.com.lb/OA_HTML/IrcVisitor.jsp"
             ],
             "Set-Cookie": [
               "BIGipServer~IFRMS-WEB~IFRMS-OHS-HTTPS=rd7o00000000000000000000ffffc0a8fde4o4443; expires=Fri, 06-Nov-2020 21:24:21 GMT; path=/; Httponly; Secure",
               "TS016c74f4=01671efb9a1a400535e215d6f76498a5887425fed793ca942baa75f16076e60e1350988222922fa06fc16f53ef016d9ecd38535fcabf14861525811a7c3459e91086df326f; Path=/"
             ],
             "X-Frame-Options": ["SAMEORIGIN"]
            },
            "body": '<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n<HTML><HEAD>\n<TITLE>302 Found</TITLE>\n</HEAD><BODY>\n<H1>Found</H1>\nThe document has moved <A HREF=\"https://jobs.bankaudi.com.lb/OA_HTML/IrcVisitor.jsp\">here</A>.<P>\n</BODY></HTML>\n',
            "tls": {
              "version": 771,
              "cipher_suite": 49199,
              "cert": "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"
            }
          },
          "Success": False,
          "Error": "Incorrect web response: status lines don't match",
          "StartTime": "2020-11-06T15:24:21.124508839-05:00",
          "EndTime": "2020-11-06T15:24:21.812075476-05:00"
        }
      ],
      "Blocked": False,
      "FailSanity": False,
      "StatefulBlock": False
    }
    # yapf: enable

    filename = 'gs://firehook-scans/http/CP_Quack-https-2020-11-06-15-15-31/results.json'

    # yapf: disable
    expected_row = HyperquackRow(
        domain = 'www.arabhra.org',
        category = 'Intergovernmental Organizations',
        ip = '213.175.166.157',
        date = '2020-11-06',
        start_time = '2020-11-06T15:24:21.124508839-05:00',
        end_time = '2020-11-06T15:24:21.812075476-05:00',
        error = 'Incorrect web response: status lines don\'t match',
        anomaly = False,
        success = False,
        stateful_block = False,
        is_control = False,
        controls_failed = False,
        measurement_id = '81e2a76dafe04131bc38fc6ec7bbddca',
        source = 'CP_Quack-https-2020-11-06-15-15-31',
        received=HttpsResponse(
            status = '302 Found',
            body = '<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n<HTML><HEAD>\n<TITLE>302 Found</TITLE>\n</HEAD><BODY>\n<H1>Found</H1>\nThe document has moved <A HREF=\"https://jobs.bankaudi.com.lb/OA_HTML/IrcVisitor.jsp\">here</A>.<P>\n</BODY></HTML>\n',
            tls_version = 771,
            tls_cipher_suite = 49199,
            tls_cert = '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',
            tls_cert_common_name = 'jobs.bankaudi.com.lb',
            tls_cert_issuer = 'Entrust Certification Authority - L1M',
            tls_cert_start_date = '2020-05-12T11:23:03',
            tls_cert_end_date = '2021-05-12T11:53:02',
            tls_cert_alternative_names = ['jobs.bankaudi.com.lb', 'www.jobs.bankaudi.com.lb'],
            headers = [
                'Content-Language: en',
                'Content-Type: text/html; charset=iso-8859-1',
                'Date: Fri, 06 Nov 2020 20:24:21 GMT',
                'Location: https://jobs.bankaudi.com.lb/OA_HTML/IrcVisitor.jsp',
                'Set-Cookie: BIGipServer~IFRMS-WEB~IFRMS-OHS-HTTPS=rd7o00000000000000000000ffffc0a8fde4o4443; expires=Fri, 06-Nov-2020 21:24:21 GMT; path=/; Httponly; Secure',
                'Set-Cookie: TS016c74f4=01671efb9a1a400535e215d6f76498a5887425fed793ca942baa75f16076e60e1350988222922fa06fc16f53ef016d9ecd38535fcabf14861525811a7c3459e91086df326f; Path=/',
                'X-Frame-Options: SAMEORIGIN',
            ],
        ),
        outcome='content/status_mismatch:302'
    )
    # yapf: enable

    flattener = get_hyperquack_flattener()
    row = list(
        flattener.process_hyperquack(filename, line,
                                     '81e2a76dafe04131bc38fc6ec7bbddca'))[0]
    self.assertEqual(row, expected_row)

  def test_flattenmeasurement_https_v2(self) -> None:
    """Test parsing an unsuccessful HTTPS v2 measurement."""
    # yapf: disable
    line = {
      "vp": "41.0.4.132",
      "location": {
        "country_name": "South Africa",
        "country_code": "ZA"
      },
      "service": "https",
      "test_url": "antivigilancia.org",
      "response": [
        {
          "matches_template": False,
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
      "anomaly": False,
      "controls_failed": True,
      "stateful_block": False,
      "tag": "2021-04-26T04:21:46"
    }
    # yapf: enable

    filename = 'gs://firehook-scans/https/CP_Quack-https-2021-04-26-04-21-46/results.json'

    # yapf: disable
    expected_row = HyperquackRow(
        domain = 'control-1b13950f35f3208b.com',
        category = 'Control',
        ip = '41.0.4.132',
        date = '2021-04-26',
        start_time = '2021-04-26T04:38:36.78214922-04:00',
        end_time = '2021-04-26T04:38:39.114151569-04:00',
        anomaly = False,
        success = False,
        stateful_block = False,
        is_control = True,
        controls_failed = True,
        measurement_id = '81e2a76dafe04131bc38fc6ec7bbddca',
        source = 'CP_Quack-https-2021-04-26-04-21-46',
        received=HttpsResponse(
            status = '200 OK',
            body = '<!DOCTYPE html> replaced long body',
            tls_version = 771,
            tls_cipher_suite = 49199,
            tls_cert = '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',
            tls_cert_common_name = 'www.signinghub.co.za',
            tls_cert_issuer = 'Entrust Certification Authority - L1M',
            tls_cert_start_date = '2021-03-01T08:52:17',
            tls_cert_end_date = '2022-03-03T08:52:16',
            tls_cert_alternative_names = ['www.signinghub.co.za', 'signinghub.co.za'],
            headers = [
                'Cache-Control: no-store, no-cache, must-revalidate, post-check=0, pre-check=0',
                'Charset: utf-8'
            ],
        ),
        outcome='content/mismatch'
    )
    # yapf: enable

    flattener = get_hyperquack_flattener()
    row = list(
        flattener.process_hyperquack(filename, line,
                                     '81e2a76dafe04131bc38fc6ec7bbddca'))[0]
    self.assertEqual(row, expected_row)
