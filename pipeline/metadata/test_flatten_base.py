"""Unit tests for common flattening functions"""

import unittest
from typing import Tuple, List, Optional

from pipeline.metadata.blockpage import BlockpageMatcher

from pipeline.metadata import flatten_base
from pipeline.metadata.schema import HttpsResponse


class FlattenBaseTest(unittest.TestCase):
  """Unit tests for pipeline flattening."""

  def test_source_from_filename(self) -> None:
    """Test getting the data source name from the filename."""
    self.assertEqual(
        flatten_base.source_from_filename(
            'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'
        ), 'CP_Quack-echo-2020-08-23-06-01-02')

    self.assertEqual(
        flatten_base.source_from_filename(
            'gs://firehook-scans/http/CP_Quack-http-2020-09-13-01-02-07/results.json'
        ), 'CP_Quack-http-2020-09-13-01-02-07')

  def test_parse_received_headers(self) -> None:
    """Test parsing HTTP/S header fields into a flat format."""
    headers = {
        'Content-Language': ['en', 'fr'],
        'Content-Type': ['text/html; charset=iso-8859-1']
    }

    flat_headers = flatten_base.parse_received_headers(headers)

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

    expected = HttpsResponse(
        is_known_blockpage = True,
        status = '403 Forbidden',
        body = '<html><head><meta http-equiv="Content-Type" content="text/html; charset=windows-1256"><title>MNN3-1(1)</title></head><body><iframe src="http://10.10.34.35:80" style="width: 100%; height: 100%" scrolling="no" marginwidth="0" marginheight="0" frameborder="0" vspace="0" hspace="0"></iframe></body></html>\r\n\r\n',
        headers = [],
        page_signature = 'b_nat_ir_national_1',
    )
    # yapf: enable
    blockpage_matcher = BlockpageMatcher()
    parsed = flatten_base.parse_received_data(blockpage_matcher, received, 'blocked.com', True)
    self.assertEqual(parsed, expected)

  def test_parse_received_data_no_header_field(self) -> None:
    """Test parsing reciveed HTTP/S data missing a header field."""
    received = {
        'status_line': '403 Forbidden',
        'body': '<test-body>'
        # No 'headers' field
    }

    expected = HttpsResponse(
        status='403 Forbidden',
        body='<test-body>',
        headers=[],
        is_known_blockpage=None,
        page_signature=None,
    )
    blockpage_matcher = BlockpageMatcher()
    parsed = flatten_base.parse_received_data(blockpage_matcher, received, 'blocked.com', True)
    self.assertEqual(parsed, expected)

  def test_parse_received_data_http_status_line_false_positive(self) -> None:
    """Test parsing sample HTTP data with a false positive match in the status line."""
    received = {
        'status_line': '521 Origin Down',
        'headers': {},
        'body': '<html><head></title></head><body>test/body></html>'
    }

    expected = HttpsResponse(
        is_known_blockpage=False,
        status='521 Origin Down',
        body='<html><head></title></head><body>test/body></html>',
        headers=[],
        page_signature='x_521',
    )
    blockpage_matcher = BlockpageMatcher()
    parsed = flatten_base.parse_received_data(blockpage_matcher, received, True)
    self.assertEqual(parsed, expected)

  def test_parse_received_data_http_header_false_positive(self) -> None:
    """Test parsing sample HTTP data with a blockpage match in the header."""
    received = {
        'status_line': '403 Forbidden',
        'headers': {
            'Server': ['Barracuda/NGFirewall']
        },
        'body': '<html><head></title></head><body>test/body></html>'
    }

    expected = HttpsResponse(
        is_known_blockpage=True,
        status='403 Forbidden',
        body='<html><head></title></head><body>test/body></html>',
        headers=['Server: Barracuda/NGFirewall'],
        page_signature='a_prod_barracuda_2',
    )
    blockpage_matcher = BlockpageMatcher()
    parsed = flatten_base.parse_received_data(blockpage_matcher, received, 'blocked.com', True)
    self.assertEqual(parsed, expected)

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

    expected = HttpsResponse(
        status = '403 Forbidden',
        body = '<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD><BODY>\n<H1>Access Denied</H1>\n \nYou don\'t have permission to access "discover.com" on this server.<P>\nReference 18b535dd581604694259a71c660\n</BODY>\n</HTML>\n',
        tls_version = 771,
        tls_cipher_suite = 49199,
        tls_cert = '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',
        tls_cert_common_name = 'www.tabikore.jp',
        tls_cert_issuer = 'DigiCert SHA2 Extended Validation Server CA',
        tls_cert_start_date = '2020-10-05T00:00:00',
        tls_cert_end_date = '2021-10-06T12:00:00',
        tls_cert_alternative_names = ['tabikore.jp', 'www.tabikore.jp'],
        headers = [
            'Content-Length: 278',
            'Content-Type: text/html',
            'Date: Fri, 06 Nov 2020 20:24:19 GMT',
            'Expires: Fri, 06 Nov 2020 20:24:19 GMT',
            'Mime-Version: 1.0',
            'Server: AkamaiGHost',
            'Set-Cookie: bm_sz=6A1BDB4DFCA371F55C598A6D50C7DC3F~YAAQtTXdWKzJ+ZR1AQAA6zY7nwmc3d1xb2D5pqi3WHoMGfNsB8zB22LP5Kz/15sxdI3d3qznv4NzhGdb6CjijzFezAd18NREhybEvZMSZe2JHkjBjli/y1ZRMgC512ln7CCHURjS03UWDIzVrpwPV3Z/h/mq00NF2+LgHsDPelEZoArYVmEwH7OtE4zHAePErKw=; Domain=.discover.com; Path=/; Expires=Sat, 07 Nov 2020 00:24:19 GMT; Max-Age=14400; HttpOnly_abck=7A29878FA7120EC680C6E591A8FF3F5A~-1~YAAQtTXdWK3J+ZR1AQAA6zY7nwR93cThkIxWHn0icKtS6Wgb6NVHSQ80nZ6I2DzczA+1qn/0rXSGZUcFvW/+7tmDF0lHsieeRwnmIydhPELwAsNLjfBMF1lJm9Y7u4ppUpD4WtbRJ1g+Qhd9CLcelH3fQ8AVmJn/jRNN8WrisA8GKuUhpfjv9Gp1aGGqzv12H8u3Ogt/9oOv4Y8nKuS7CWipsFuPftCMeTBVbPn/JsV/NzttmkuFikLj8PwmpNecqlhaH1Ra32XDl/hVsCFWaPm4wdWO3d2WDK8Em0sHzklyTV4iFo6itVlCEHQ=~-1~-1~-1; Domain=.discover.com; Path=/; Expires=Sat, 06 Nov 2021 20:24:19 GMT; Max-Age=31536000; Secure'
        ],
        is_known_blockpage = False,
        page_signature = 'x_on_this_server',
    )
    # yapf: enable
    blockpage_matcher = BlockpageMatcher()
    parsed = flatten_base.parse_received_data(blockpage_matcher, received, 'www.tabikore.jp', True)
    self.assertEqual(parsed, expected)

  def test_parse_cert_invalid(self) -> None:
    """Test parsing an invalid certificate."""
    cert_str = "invalid certificate text"
    with self.assertLogs(level='WARNING') as cm:
      parsed = flatten_base.parse_cert(cert_str, 'example.com')
      self.assertEqual(
          cm.output[0], 'WARNING:root:ValueError: '
          'Unable to load PEM file. '
          'See https://cryptography.io/en/latest/faq.html#why-can-t-i-import-my-pem-file '
          'for more details. InvalidData(InvalidByte(7, 32))\n'
          'Cert: invalid certificate text\n')
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str]] = (None, None, None, None, [])
    self.assertEqual(parsed, expected)

  def test_parse_cert_no_subject_alternative_names(self) -> None:
    """Test parsing a certificate with no subject alternative names."""
    # yapf: disable
    cert_str = "MIICxjCCAa6gAwIBAgIUbwuTH716p+5ULWPvIU6RP1QIIiwwDQYJKoZIhvcNAQELBQAwHTEbMBkGA1UEAwwSY2Vuc29yZWRwbGFuZXQub3JnMB4XDTIyMDQwNjAwMjAxNloXDTIyMDUwNzAwMjAxNlowHTEbMBkGA1UEAwwSY2Vuc29yZWRwbGFuZXQub3JnMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4H6+8AY3CuT/fjcxqJmFocYwqPUdaWc1o0X/YJmqde2dNuvkoVfvJfhWhjfbex0B42ha5Qy+qiV1WwApmtGNk0VdGTPvHgeFrrRKS8fv6tt4AqYUkF49UCyTfao+iVlZUKVu1IPdd5eOZ8AKqfBUbDjMVArEOG4G4OMqaNHtP+FoXvTyEGinu5S4wnx4ioQgmfMzU+/HdRvxI8UftDKggyyvLLhEIJaduY+y6Au+2Dtnx1+AShCVWUAx6cbCqC6HZ36tZBFJsDdENXQ9yGOzdmEIcvGbPIKdU1HYBUcd4opO+lqHs1NTk6lMpvtzj/F2rXkz1fjLjEfQy8cMaDuufQIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQAumw/AvuONpiUY9RlakrUkpb05wwRZUyHlVSLAkuYE6WXfSjg5VlRtwb8C7U5KCz9p5oFVK0FgGFYgBTqGYqbMEBDJELPBvFaQ5zg21/Uhwb0KEYqLvDNqlltaW2EoJwof+KntTj8OVaWqFMX1JvgPkswwNWs605opX+8z3W3pDh1YK8HyENlHig/jGVIJrXRRCFo8tbGTAJvgPEnW9s+xDCkql8tuXojiaCf56B3XHrus0E7NZLNzXI2qSoJxIrAedSbtfnD3Mw6bjoDG8sW+Y743TeIx/dFWxlX8uY4G+pg8Cyg1BiGLkNWWFGStK9JpyekN7khsuEdGgzj9YQep"
    # yapf: enable
    parsed = flatten_base.parse_cert(cert_str, 'censoredplanet.org')
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str]] = (
                        'censoredplanet.org',
                        'censoredplanet.org',
                        '2022-04-06T00:20:16',
                        '2022-05-07T00:20:16',
                        [],
                    )
    self.assertEqual(parsed, expected)

  def test_parse_cert_self_signed(self) -> None:
    """Test parsing a self signed certificate."""
    # yapf: disable
    cert_str = "MIIBeTCCAR+gAwIBAgIIFj3y08kD21kwCgYIKoZIzj0EAwIwLDEPMA0GA1UECgwGU2t5RE5TMRkwFwYDVQQDDBBTa3lETlMgU2VydmVyIENBMB4XDTIxMDYyNzAxMTM1MloXDTIxMDYyOTAxMTM1MlowGTEXMBUGA1UEAxMOd3d3LnRpa3Rvay5jb20wWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAATq1oeKtxjrDQ0JccJGzr8oQ3O4o048yFtA4DUFp93Ssk/3TAcwLzaRRHqXsuvUQeXtCQWeIJi06jlUOQtfkVi2oz4wPDAfBgNVHSMEGDAWgBSVpcfn4Rsi7YJxAk4dWHT+QcjokTAZBgNVHREEEjAQgg53d3cudGlrdG9rLmNvbTAKBggqhkjOPQQDAgNIADBFAiEA7cSgmPKszqefVPK5oNiK8SuiyJzggF75M9tQbMePOhMCID+sPNOYgJEgoLd3YVnNuh5VDW0AWvmmzfoNBCKlLOzW"
    # yapf: enable
    parsed = flatten_base.parse_cert(cert_str, 'www.tiktok.com')
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str]] = ('www.tiktok.com', 'SkyDNS Server CA',
                                  '2021-06-27T01:13:52', '2021-06-29T01:13:52',
                                  ['www.tiktok.com'])
    self.assertEqual(parsed, expected)

  def test_parse_cert_missing_fields(self) -> None:
    """Test parsing a certificate with missing fields."""
    # yapf: disable
    cert_str = "MIIC7TCCAdWgAwIBAgIJAKnnHjNBTIeNMA0GCSqGSIb3DQEBBQUAMA0xCzAJBgNVBAYTAlJVMB4XDTIyMDEyMDA3MDIzN1oXDTQ5MDYwNjA3MDIzN1owDTELMAkGA1UEBhMCUlUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDPRtBvMF9d+GnHYzdn2wMgTmyKip8z1cyyoEriPZtM7KCu3TAHXJclz7H9Zt5jpzwSxUXpFcvLGxfEUo/tHxJw5CoHJYQGKxDvlQcn2FV3MrVmrFUd9Pg2BXgwAEdIMSZjDjLkBvvHBWxdz6uUI/r9YGjalawvvYelhrZGK+5h7w1Vx4cew3sOxuuOcnY+9SG8FbLYtEj2/Ase9Nwu+fBIXNS2nSZYtZta2sQVtcJEg24Ppqg2Ak1gHMPtDqJpD27OVuRiJXLlhwl4LD4gamH9nhaQM558W/D2h4ubMOA9mx8RmyEZVEKB7Mb0PGo45vcDQMQu5azXiqofuahOEjjRAgMBAAGjUDBOMB0GA1UdDgQWBBTLPcKME7F77rRYROBi7VJJW1Y7hzAfBgNVHSMEGDAWgBTLPcKME7F77rRYROBi7VJJW1Y7hzAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBBQUAA4IBAQBvRkE4QZ8d4096lRqP6GP50366cSbe4Sw6tB8yDZ+qNNRhHmLA6XpwSf2W2WG703DfCAFsKPR2hAAZKgl7LeEhM0MjElHikBpIqSE3VX7CxyAj3FBpD5i9oR9ztvtYkk/LSdXES4cXYR+gHP2FFKT2yVQc5b7ZDN2OQuuXJP6mCLWaDGH4Zz0IOQnTmtx6ZF6DGSOcNx0u4dS/Ki+DYAy0gXYsAzdi8QIPCBUyxNSn78PY4ayZfyUq86hPGwiSHn1AJvsf+4wn6X781aFNr5ReouModCB+kX5CrCOFJqIlo9KOhpPf8v7ZfsSWpEGSSENkcArkXpZHHtbKyQmPUvhU"
    # yapf: enable
    parsed = flatten_base.parse_cert(cert_str, 'example.com')
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str]] = (None, None, '2022-01-20T07:02:37',
                                  '2049-06-06T07:02:37', [])
    self.assertEqual(parsed, expected)

  def test_parse_cert_valid(self) -> None:
    self.maxDiff = None

    # yapf: disable
    cert_str = "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"
    # yapf: enable
    parsed = flatten_base.parse_cert(cert_str, 'example.com')
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str]] = (None, None, '2022-01-20T07:02:37',
                                  '2049-06-06T07:02:37', [])
    self.assertEqual(parsed, expected)

