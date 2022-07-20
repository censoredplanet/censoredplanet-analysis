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
    parsed = flatten_base.parse_received_data(blockpage_matcher, received,
                                              'blocked.com', True)
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
    parsed = flatten_base.parse_received_data(blockpage_matcher, received,
                                              'blocked.com', True)
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
    parsed = flatten_base.parse_received_data(blockpage_matcher, received,
                                              'not-blocked.com', True)
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
    parsed = flatten_base.parse_received_data(blockpage_matcher, received,
                                              'not-blocked.com', True)
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
        tls_cert_matches_domain=True,
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
    parsed = flatten_base.parse_received_data(blockpage_matcher, received,
                                              'www.tabikore.jp', True)
    self.assertEqual(parsed, expected)

  def test_parse_received_data_multiple_certs(self) -> None:
    """Test parsing example HTTPS data with a cert chain."""
    # yapf: disable
    received = {
      "status_line": "301 Moved Permanently",
      "headers": {
         "Connection": ["close"],
         "Content-Length": ["0"],
         "Location": ["https://www.mayoclinic.org/"],
         "Server": ["BigIP"]
      },
      "body": "",
      "TlsVersion": 771,
      "CipherSuite": 49199,
      "Certificate": [
         "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"
      ]
    }

    expected = HttpsResponse(
        status = '301 Moved Permanently',
        body = '',
        tls_version = 771,
        tls_cipher_suite = 49199,
        tls_cert = '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',
        tls_cert_matches_domain=True,
        tls_cert_common_name='mwredirects5.mayo.edu',
        tls_cert_issuer='Sectigo RSA Organization Validation Secure Server CA',
        tls_cert_start_date='2022-04-14T00:00:00',
        tls_cert_end_date='2023-04-14T23:59:59',
        tls_cert_alternative_names=[
          'mwredirects5.mayo.edu',
          'arabic.mayoclinic.org',
          'mayo.edu',
          'mayoclinic.com',
          'mayoclinic.org',
          'mayoclinichealthcare.co.uk',
          'mayoclinichealthsystem.org',
          'uscovidplasma.org',
          'www.mayoclinic.com'
        ],
        headers=[
          'Connection: close',
          'Content-Length: 0',
          'Location: https://www.mayoclinic.org/',
          'Server: BigIP'
        ]
    )
    # yapf: enable
    blockpage_matcher = BlockpageMatcher()
    parsed = flatten_base.parse_received_data(blockpage_matcher, received,
                                              'mayo.edu', True)

    self.assertEqual(parsed, expected)

  def test_parse_cert_invalid(self) -> None:
    """Test parsing an invalid certificate."""
    cert_str = "invalid certificate text"
    with self.assertLogs(level='WARNING') as cm:
      parsed = flatten_base.parse_cert(cert_str, 'test.com')
      self.assertIn('Unable to load PEM file.', cm.output[0])
      self.assertIn('Cert: invalid certificate text', cm.output[0])
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str], bool] = (None, None, None, None, [], False)
    self.assertEqual(parsed, expected)

  def test_parse_cert_no_subject_alternative_names(self) -> None:
    """Test parsing a certificate with no subject alternative names."""
    # yapf: disable
    cert_str = "MIICxjCCAa6gAwIBAgIUbwuTH716p+5ULWPvIU6RP1QIIiwwDQYJKoZIhvcNAQELBQAwHTEbMBkGA1UEAwwSY2Vuc29yZWRwbGFuZXQub3JnMB4XDTIyMDQwNjAwMjAxNloXDTIyMDUwNzAwMjAxNlowHTEbMBkGA1UEAwwSY2Vuc29yZWRwbGFuZXQub3JnMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4H6+8AY3CuT/fjcxqJmFocYwqPUdaWc1o0X/YJmqde2dNuvkoVfvJfhWhjfbex0B42ha5Qy+qiV1WwApmtGNk0VdGTPvHgeFrrRKS8fv6tt4AqYUkF49UCyTfao+iVlZUKVu1IPdd5eOZ8AKqfBUbDjMVArEOG4G4OMqaNHtP+FoXvTyEGinu5S4wnx4ioQgmfMzU+/HdRvxI8UftDKggyyvLLhEIJaduY+y6Au+2Dtnx1+AShCVWUAx6cbCqC6HZ36tZBFJsDdENXQ9yGOzdmEIcvGbPIKdU1HYBUcd4opO+lqHs1NTk6lMpvtzj/F2rXkz1fjLjEfQy8cMaDuufQIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQAumw/AvuONpiUY9RlakrUkpb05wwRZUyHlVSLAkuYE6WXfSjg5VlRtwb8C7U5KCz9p5oFVK0FgGFYgBTqGYqbMEBDJELPBvFaQ5zg21/Uhwb0KEYqLvDNqlltaW2EoJwof+KntTj8OVaWqFMX1JvgPkswwNWs605opX+8z3W3pDh1YK8HyENlHig/jGVIJrXRRCFo8tbGTAJvgPEnW9s+xDCkql8tuXojiaCf56B3XHrus0E7NZLNzXI2qSoJxIrAedSbtfnD3Mw6bjoDG8sW+Y743TeIx/dFWxlX8uY4G+pg8Cyg1BiGLkNWWFGStK9JpyekN7khsuEdGgzj9YQep"
    # yapf: enable
    parsed = flatten_base.parse_cert(cert_str, 'censoredplanet.org')
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str],
                    bool] = ('censoredplanet.org', 'censoredplanet.org',
                             '2022-04-06T00:20:16', '2022-05-07T00:20:16', [],
                             True)
    self.assertEqual(parsed, expected)

  def test_parse_cert_self_signed(self) -> None:
    """Test parsing a self signed certificate."""
    # yapf: disable
    cert_str = "MIIBeTCCAR+gAwIBAgIIFj3y08kD21kwCgYIKoZIzj0EAwIwLDEPMA0GA1UECgwGU2t5RE5TMRkwFwYDVQQDDBBTa3lETlMgU2VydmVyIENBMB4XDTIxMDYyNzAxMTM1MloXDTIxMDYyOTAxMTM1MlowGTEXMBUGA1UEAxMOd3d3LnRpa3Rvay5jb20wWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAATq1oeKtxjrDQ0JccJGzr8oQ3O4o048yFtA4DUFp93Ssk/3TAcwLzaRRHqXsuvUQeXtCQWeIJi06jlUOQtfkVi2oz4wPDAfBgNVHSMEGDAWgBSVpcfn4Rsi7YJxAk4dWHT+QcjokTAZBgNVHREEEjAQgg53d3cudGlrdG9rLmNvbTAKBggqhkjOPQQDAgNIADBFAiEA7cSgmPKszqefVPK5oNiK8SuiyJzggF75M9tQbMePOhMCID+sPNOYgJEgoLd3YVnNuh5VDW0AWvmmzfoNBCKlLOzW"
    # yapf: enable
    parsed = flatten_base.parse_cert(cert_str, 'www.tiktok.com')
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str],
                    bool] = ('www.tiktok.com', 'SkyDNS Server CA',
                             '2021-06-27T01:13:52', '2021-06-29T01:13:52',
                             ['www.tiktok.com'], True)
    self.assertEqual(parsed, expected)

  def test_parse_cert_missing_fields(self) -> None:
    """Test parsing a certificate with missing fields."""
    # yapf: disable
    cert_str = "MIIC7TCCAdWgAwIBAgIJAKnnHjNBTIeNMA0GCSqGSIb3DQEBBQUAMA0xCzAJBgNVBAYTAlJVMB4XDTIyMDEyMDA3MDIzN1oXDTQ5MDYwNjA3MDIzN1owDTELMAkGA1UEBhMCUlUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDPRtBvMF9d+GnHYzdn2wMgTmyKip8z1cyyoEriPZtM7KCu3TAHXJclz7H9Zt5jpzwSxUXpFcvLGxfEUo/tHxJw5CoHJYQGKxDvlQcn2FV3MrVmrFUd9Pg2BXgwAEdIMSZjDjLkBvvHBWxdz6uUI/r9YGjalawvvYelhrZGK+5h7w1Vx4cew3sOxuuOcnY+9SG8FbLYtEj2/Ase9Nwu+fBIXNS2nSZYtZta2sQVtcJEg24Ppqg2Ak1gHMPtDqJpD27OVuRiJXLlhwl4LD4gamH9nhaQM558W/D2h4ubMOA9mx8RmyEZVEKB7Mb0PGo45vcDQMQu5azXiqofuahOEjjRAgMBAAGjUDBOMB0GA1UdDgQWBBTLPcKME7F77rRYROBi7VJJW1Y7hzAfBgNVHSMEGDAWgBTLPcKME7F77rRYROBi7VJJW1Y7hzAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBBQUAA4IBAQBvRkE4QZ8d4096lRqP6GP50366cSbe4Sw6tB8yDZ+qNNRhHmLA6XpwSf2W2WG703DfCAFsKPR2hAAZKgl7LeEhM0MjElHikBpIqSE3VX7CxyAj3FBpD5i9oR9ztvtYkk/LSdXES4cXYR+gHP2FFKT2yVQc5b7ZDN2OQuuXJP6mCLWaDGH4Zz0IOQnTmtx6ZF6DGSOcNx0u4dS/Ki+DYAy0gXYsAzdi8QIPCBUyxNSn78PY4ayZfyUq86hPGwiSHn1AJvsf+4wn6X781aFNr5ReouModCB+kX5CrCOFJqIlo9KOhpPf8v7ZfsSWpEGSSENkcArkXpZHHtbKyQmPUvhU"
    # yapf: enable
    parsed = flatten_base.parse_cert(cert_str, 'example.com')
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str], bool] = (None, None, '2022-01-20T07:02:37',
                                        '2049-06-06T07:02:37', [], False)
    self.assertEqual(parsed, expected)

  def test_parse_cert_valid_domain_match(self) -> None:
    """Test parsing a certificate with a matching domain."""
    # yapf: disable
    cert_str = "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"
    # yapf: enable
    parsed = flatten_base.parse_cert(cert_str, 'example.com')
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str],
                    bool] = ('www.example.org',
                             'DigiCert TLS RSA SHA256 2020 CA1',
                             '2022-03-14T00:00:00', '2023-03-14T23:59:59', [
                                 'www.example.org', 'example.net',
                                 'example.edu', 'example.com', 'example.org',
                                 'www.example.com', 'www.example.edu',
                                 'www.example.net'
                             ], True)
    self.assertEqual(parsed, expected)

  def test_parse_cert_invalid_domain_match(self) -> None:
    """Test parsing a certificate which doesn't matching the domain."""
    # yapf: disable
    cert_str = "MIIFUzCCBDugAwIBAgIRAMTb+HK81LppCsW0nvGcs44wDQYJKoZIhvcNAQELBQAwRjELMAkGA1UEBhMCVVMxIjAgBgNVBAoTGUdvb2dsZSBUcnVzdCBTZXJ2aWNlcyBMTEMxEzARBgNVBAMTCkdUUyBDQSAxQzMwHhcNMjIwNjA2MDkzOTU0WhcNMjIwODI5MDkzOTUzWjAZMRcwFQYDVQQDEw53d3cuZ29vZ2xlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAM0Qblby+hIyBIh7Zc0NtOIY9xAX0AdqGrkabsw1IxkkQTtzdpKZv5RuuZjvt8u91OuCp2fiXV6ATgphmL3rIN9tSqAok2z0FCtEo2dszkoS9sc84URIx4nDkqLTfT54pbLk3n9Y0+MBXyUw7Yrc1RTaduwks1DrHDPIiNzXOcNYGI3g0oUZqCZmqaCapuffTPiEk5j5j+7Ji8jfSgV8NAKpKQI3DBa8zZh7TxDKiVEeKmqDRSP5lG+cabaSDMu8CztZYMLn43jR5HmDE+pwCHkWgsxEN7l1oP0ykihtsbooTYaU6VjEk2YXP26G2dELJ0CtDeqPXMJKbqu15MUBIWUCAwEAAaOCAmcwggJjMA4GA1UdDwEB/wQEAwIFoDATBgNVHSUEDDAKBggrBgEFBQcDATAMBgNVHRMBAf8EAjAAMB0GA1UdDgQWBBRBXNvFDk+723JHoyKTl20W1NfyGDAfBgNVHSMEGDAWgBSKdH+vhc3ulc09nNDiRhTzcTUdJzBqBggrBgEFBQcBAQReMFwwJwYIKwYBBQUHMAGGG2h0dHA6Ly9vY3NwLnBraS5nb29nL2d0czFjMzAxBggrBgEFBQcwAoYlaHR0cDovL3BraS5nb29nL3JlcG8vY2VydHMvZ3RzMWMzLmRlcjAZBgNVHREEEjAQgg53d3cuZ29vZ2xlLmNvbTAhBgNVHSAEGjAYMAgGBmeBDAECATAMBgorBgEEAdZ5AgUDMDwGA1UdHwQ1MDMwMaAvoC2GK2h0dHA6Ly9jcmxzLnBraS5nb29nL2d0czFjMy96ZEFUdDBFeF9Gay5jcmwwggEEBgorBgEEAdZ5AgQCBIH1BIHyAPAAdgBGpVXrdfqRIDC1oolp9PN9ESxBdL79SbiFq/L8cP5tRwAAAYE4mYoQAAAEAwBHMEUCIQCcNloItrqEPusKeFK/io4SA5UyOF0Wsd121iU7qQOxRgIgBqjzDDLJ3eMJgXaddzeA//mjvZiaN7TSirVWgHxAjs4AdgBRo7D1/QF5nFZtuDd4jwykeswbJ8v3nohCmg3+1IsF5QAAAYE4mYngAAAEAwBHMEUCIQCPgsZK8DLkbQgDwOpkBJsdcrrepYipEp+JX6Um+RlhrwIgMXcHDwA15W7pp2I5zhtir8PCtYOg5Elfp9SYURr1ZlEwDQYJKoZIhvcNAQELBQADggEBAFaGFDi9HLix0rtIqOi1T0eFAi0A/xjYnA2C0l/84WD+XDI/eK/CIOQAHAsO7mxke1/pWq2H/t5OPMylarKhZJCxrxhEz+ZtX2J3Ucy6zwYqOzDGoF59UnwGLaTuEW6nal+2sAxlStdq2kM6Kw4A2oMfq+PyH0SSuTr8fa3K/AXJUQMofE0egYUFsQJeS3mifa4EkoiMQny9jld2BFldLz8Tt4xO4LS+YhOlMP3I0/11S05o5vinxfqxMMuwYrIDy1EzFJabwu8c5Ng8Eqy8bjPW4zIecn8DnKklhkjTMMtQLMyf+4hvUHfDN639QRt6rNOjbq6mf17K/eW4p0dnZrE="
    # yapf: enable
    parsed = flatten_base.parse_cert(cert_str, 'googlevideo.com')
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str],
                    bool] = ('www.google.com', 'GTS CA 1C3',
                             '2022-06-06T09:39:54', '2022-08-29T09:39:53',
                             ['www.google.com'], False)
    self.assertEqual(parsed, expected)

  def test_parse_cert_wildcard_subdomain(self) -> None:
    """Test parsing a matching certificate with a wildcard subdomain."""
    # yapf: disable
    cert_str = "MIIEnzCCA4egAwIBAgIRAJajPS/p/EdOEpSJ/YyHeh0wDQYJKoZIhvcNAQELBQAwRjELMAkGA1UEBhMCVVMxIjAgBgNVBAoTGUdvb2dsZSBUcnVzdCBTZXJ2aWNlcyBMTEMxEzARBgNVBAMTCkdUUyBDQSAxQzMwHhcNMjIwNjA2MDk0MzUxWhcNMjIwODI5MDk0MzUwWjAcMRowGAYDVQQDDBEqLmFwaXMuZ29vZ2xlLmNvbTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABI3y4pDB4xVH+NlFQEXcPWwCi1YoJJPAKqzOOnVmANq1r+A9l9UeAHwY0OiLiJsq7tpq6IvPFk1/CPFsFU9msFyjggJ7MIICdzAOBgNVHQ8BAf8EBAMCB4AwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQUZLZ7ixRMwqpTUZrk9OOtBT29AqUwHwYDVR0jBBgwFoAUinR/r4XN7pXNPZzQ4kYU83E1HScwagYIKwYBBQUHAQEEXjBcMCcGCCsGAQUFBzABhhtodHRwOi8vb2NzcC5wa2kuZ29vZy9ndHMxYzMwMQYIKwYBBQUHMAKGJWh0dHA6Ly9wa2kuZ29vZy9yZXBvL2NlcnRzL2d0czFjMy5kZXIwLQYDVR0RBCYwJIIRKi5hcGlzLmdvb2dsZS5jb22CD2FwaXMuZ29vZ2xlLmNvbTAhBgNVHSAEGjAYMAgGBmeBDAECATAMBgorBgEEAdZ5AgUDMDwGA1UdHwQ1MDMwMaAvoC2GK2h0dHA6Ly9jcmxzLnBraS5nb29nL2d0czFjMy96ZEFUdDBFeF9Gay5jcmwwggEEBgorBgEEAdZ5AgQCBIH1BIHyAPAAdgBGpVXrdfqRIDC1oolp9PN9ESxBdL79SbiFq/L8cP5tRwAAAYE4nSonAAAEAwBHMEUCIQDYwW4kRF7NyT5qAa3e21kU4XbjyW+jqYNtxTVpjd02QQIgDNTEsPYQ5rigeRXSqXPas1LtL3xlDwiM3X98tqNzjWwAdgBByMqx3yJGShDGoToJQodeTjGLGwPr60vHaPCQYpYG9gAAAYE4nSoyAAAEAwBHMEUCIFIxAaC4Z81/AqowHCHsU87/M/T5wzrG6+VEC9PBLPstAiEAxEAgyXh6LKhaKaB9/BWd1C1U/GichB0E3oJMtEH+QvMwDQYJKoZIhvcNAQELBQADggEBAORt9igcFw6RZ9Ll4InzSBVMe6nt030ZITSEbd5JSKT8z55g9oEMxlFpegBH5kmrqpG4goA5wQd7gqc/tnr06cuzCNkYeL1PPNNGisKRvkDwRCIlgQf22DQ9Sd+n5uAi7vlTrSWlPznhXw/E7hD1Dost3idHwcxGz6oYXZCFBZbRfV2I0D6DFxezcYe41d36W3s9ByCM8sh7w0/b/cfphJKW2i0qWjyuNS1v82p25TChIlNDaGnuiPb15vL7v+IXcsjfhGY8iSDgcQ1Cjs447dsKQxJvQU0p7zLDD8+4CF+7Tlh92fMihtaXAoIXXAz4VRpfGRCRm/npQb/fjIlCnYo="
    # yapf: enable
    parsed = flatten_base.parse_cert(cert_str, 'android.apis.google.com')
    expected: Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
                    List[str],
                    bool] = ('*.apis.google.com', 'GTS CA 1C3',
                             '2022-06-06T09:43:51', '2022-08-29T09:43:50',
                             ['*.apis.google.com', 'apis.google.com'], True)
    self.assertEqual(parsed, expected)
