"""Unit tests for hyperquack."""

import json
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam.testing.util as beam_test_util

from pipeline.metadata import hyperquack
from pipeline.metadata.schema import HyperquackRow, IpMetadata, HttpsResponse
from pipeline.metadata.ip_metadata_chooser import FakeIpMetadataChooserFactory
from pipeline.metadata.add_metadata import MetadataAdder


class HyperquackTest(unittest.TestCase):
  """unit testing for overall hyperquack pipeline"""

  def test_process_hyperquack_lines(self) -> None:
    """Test processing a single hyperquack line fully"""

    data_filenames = [
        "CP_Quack-https-2022-03-31-23-40-26/results.json",
    ]

    # yapf: disable
    _data = [{
        "vp": "104.17.67.3",
        "location": {},
        "service": "https",
        "test_url": "1337x.to",
        "response": [{
            "matches_template": False,
            "response": {
                "status_line": "200 OK",
                "headers": {
                    "Alt-Svc": ["h3=\":443\"; ma=86400, h3-29=\":443\"; ma=86400"],
                    "Cf-Cache-Status": ["DYNAMIC"],
                    "Cf-Ray": ["6f4e4da53e5e7e37-DTW"],
                    "Content-Type": ["text/html"],
                    "Date": ["Fri, 01 Apr 2022 03:40:26 GMT"],
                    "Expect-Ct": ["max-age=604800, report-uri=\"https://report-uri.cloudflare.com/cdn-cgi/beacon/expect-ct\""],
                    "Nel": ["{\"success_fraction\":0,\"report_to\":\"cf-nel\",\"max_age\":604800}"],
                    "Server": ["cloudflare"],
                    "Vary": ["Accept-Encoding"],
                    "X-Frame-Options": ["DENY"]
                },
                "body": "body a",
                "TlsVersion": 771,
                "CipherSuite": 9195,
                "Certificate": "MIIFLjCCBNSgAwIBAgIQBhTy7YX2of9FwDxEyfQb1jAKBggqhkjOPQQDAjBKMQswCQYDVQQGEwJVUzEZMBcGA1UEChMQQ2xvdWRmbGFyZSwgSW5jLjEgMB4GA1UEAxMXQ2xvdWRmbGFyZSBJbmMgRUNDIENBLTMwHhcNMjEwNzA5MDAwMDAwWhcNMjIwNzA4MjM1OTU5WjB1MQswCQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZyYW5jaXNjbzEZMBcGA1UEChMQQ2xvdWRmbGFyZSwgSW5jLjEeMBwGA1UEAxMVc25pLmNsb3VkZmxhcmVzc2wuY29tMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEw2Ard/LZZh8zIyYzluZM+813stVbMghpUwR7cVIW1zkUzxpp6KFHXdXy11axUfv40aFFzbRtXwrJYQDZoJ3mR6OCA28wggNrMB8GA1UdIwQYMBaAFKXON+rrsHUOlGeItEX62SQQh5YfMB0GA1UdDgQWBBSBNxPW8hCKlK0p3egif26mcj5IeDA2BgNVHREELzAtgggxMzM3eC50b4IKKi4xMzM3eC50b4IVc25pLmNsb3VkZmxhcmVzc2wuY29tMA4GA1UdDwEB/wQEAwIHgDAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwewYDVR0fBHQwcjA3oDWgM4YxaHR0cDovL2NybDMuZGlnaWNlcnQuY29tL0Nsb3VkZmxhcmVJbmNFQ0NDQS0zLmNybDA3oDWgM4YxaHR0cDovL2NybDQuZGlnaWNlcnQuY29tL0Nsb3VkZmxhcmVJbmNFQ0NDQS0zLmNybDA+BgNVHSAENzA1MDMGBmeBDAECAjApMCcGCCsGAQUFBwIBFhtodHRwOi8vd3d3LmRpZ2ljZXJ0LmNvbS9DUFMwdgYIKwYBBQUHAQEEajBoMCQGCCsGAQUFBzABhhhodHRwOi8vb2NzcC5kaWdpY2VydC5jb20wQAYIKwYBBQUHMAKGNGh0dHA6Ly9jYWNlcnRzLmRpZ2ljZXJ0LmNvbS9DbG91ZGZsYXJlSW5jRUNDQ0EtMy5jcnQwDAYDVR0TAQH/BAIwADCCAX0GCisGAQQB1nkCBAIEggFtBIIBaQFnAHYAKXm+8J45OSHwVnOfY6V35b5XfZxgCvj5TV0mXCVdx4QAAAF6i3ixQwAABAMARzBFAiBVd/jDNSKeCa66mS/GeSTohSOfLkturC5wsXdDzbt3swIhAKDAClmoVXgjLDoNioYTpHPwUZIQgB/Plf1ec6T3PM1HAHYAQcjKsd8iRkoQxqE6CUKHXk4xixsD6+tLx2jwkGKWBvYAAAF6i3ixMwAABAMARzBFAiEApaMxT/bS7Ze1bOo2XiEP/UZwx+C/g+fFYFzIrxR5B+MCIHLweH3GxT3dedYmJfZ62Wh2sjtVAW63GMLwjx38X5ZbAHUA36Veq2iCTx9sre64X04+WurNohKkal6OOxLAIERcKnMAAAF6i3ixXQAABAMARjBEAiB3RpGRk+GsJds1lCcfu0DPoUqA6DFv2fkvDqmeXx2XZQIgCU61EV+lUuiQMoEyiyYdgP/GevUZHP6NwptXJwjkJx0wCgYIKoZIzj0EAwIDSAAwRQIgaWbSos6z7xucrvst6O9y18Th+beVBmQ1l5WwahHf4V8CIQDCMixPJrHUGenFeOHDGtxzI51YO50zl4ufbyP6n8zyWQ=="
            },
            "error": "Cipher suites do not match;Certificates do not match;Status lines do not match;Content-Length header field missing;Bodies do not match;",
            "start_time": "2022-03-31T23:40:25.75122271-04:00",
            "end_time": "2022-03-31T23:40:26.287401066-04:00"
        },
        {
            "matches_template": True,
            "control_url": "control-759c4df3ce6e8418.com",
            "start_time": "2022-03-31T23:40:44.354253273-04:00",
            "end_time": "2022-03-31T23:40:44.383259487-04:00"
        }],
        "anomaly": True,
        "controls_failed": False,
        "stateful_block": False,
        "tag": "2022-03-31T23:39:30"
    }]
    # yapf: enable

    data = zip(data_filenames, [json.dumps(d) for d in _data])

    expected = [
        HyperquackRow(
            domain='1337x.to',
            category='Media sharing',
            ip='104.17.67.3',
            date='2022-03-31',
            start_time='2022-03-31T23:40:25.75122271-04:00',
            end_time='2022-03-31T23:40:26.287401066-04:00',
            error=
            'Cipher suites do not match;Certificates do not match;Status lines do not match;Content-Length header field missing;Bodies do not match;',
            anomaly=True,
            success=False,
            is_control=False,
            controls_failed=False,
            measurement_id='c978424387b45ba1b78f28438063f835',
            source='CP_Quack-https-2022-03-31-23-40-26',
            ip_metadata=IpMetadata(),
            received=HttpsResponse(
                is_known_blockpage=None,
                page_signature=None,
                status='200 OK',
                body='body a',
                tls_version=771,
                tls_cipher_suite=9195,
                tls_cert=
                'MIIFLjCCBNSgAwIBAgIQBhTy7YX2of9FwDxEyfQb1jAKBggqhkjOPQQDAjBKMQswCQYDVQQGEwJVUzEZMBcGA1UEChMQQ2xvdWRmbGFyZSwgSW5jLjEgMB4GA1UEAxMXQ2xvdWRmbGFyZSBJbmMgRUNDIENBLTMwHhcNMjEwNzA5MDAwMDAwWhcNMjIwNzA4MjM1OTU5WjB1MQswCQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZyYW5jaXNjbzEZMBcGA1UEChMQQ2xvdWRmbGFyZSwgSW5jLjEeMBwGA1UEAxMVc25pLmNsb3VkZmxhcmVzc2wuY29tMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEw2Ard/LZZh8zIyYzluZM+813stVbMghpUwR7cVIW1zkUzxpp6KFHXdXy11axUfv40aFFzbRtXwrJYQDZoJ3mR6OCA28wggNrMB8GA1UdIwQYMBaAFKXON+rrsHUOlGeItEX62SQQh5YfMB0GA1UdDgQWBBSBNxPW8hCKlK0p3egif26mcj5IeDA2BgNVHREELzAtgggxMzM3eC50b4IKKi4xMzM3eC50b4IVc25pLmNsb3VkZmxhcmVzc2wuY29tMA4GA1UdDwEB/wQEAwIHgDAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwewYDVR0fBHQwcjA3oDWgM4YxaHR0cDovL2NybDMuZGlnaWNlcnQuY29tL0Nsb3VkZmxhcmVJbmNFQ0NDQS0zLmNybDA3oDWgM4YxaHR0cDovL2NybDQuZGlnaWNlcnQuY29tL0Nsb3VkZmxhcmVJbmNFQ0NDQS0zLmNybDA+BgNVHSAENzA1MDMGBmeBDAECAjApMCcGCCsGAQUFBwIBFhtodHRwOi8vd3d3LmRpZ2ljZXJ0LmNvbS9DUFMwdgYIKwYBBQUHAQEEajBoMCQGCCsGAQUFBzABhhhodHRwOi8vb2NzcC5kaWdpY2VydC5jb20wQAYIKwYBBQUHMAKGNGh0dHA6Ly9jYWNlcnRzLmRpZ2ljZXJ0LmNvbS9DbG91ZGZsYXJlSW5jRUNDQ0EtMy5jcnQwDAYDVR0TAQH/BAIwADCCAX0GCisGAQQB1nkCBAIEggFtBIIBaQFnAHYAKXm+8J45OSHwVnOfY6V35b5XfZxgCvj5TV0mXCVdx4QAAAF6i3ixQwAABAMARzBFAiBVd/jDNSKeCa66mS/GeSTohSOfLkturC5wsXdDzbt3swIhAKDAClmoVXgjLDoNioYTpHPwUZIQgB/Plf1ec6T3PM1HAHYAQcjKsd8iRkoQxqE6CUKHXk4xixsD6+tLx2jwkGKWBvYAAAF6i3ixMwAABAMARzBFAiEApaMxT/bS7Ze1bOo2XiEP/UZwx+C/g+fFYFzIrxR5B+MCIHLweH3GxT3dedYmJfZ62Wh2sjtVAW63GMLwjx38X5ZbAHUA36Veq2iCTx9sre64X04+WurNohKkal6OOxLAIERcKnMAAAF6i3ixXQAABAMARjBEAiB3RpGRk+GsJds1lCcfu0DPoUqA6DFv2fkvDqmeXx2XZQIgCU61EV+lUuiQMoEyiyYdgP/GevUZHP6NwptXJwjkJx0wCgYIKoZIzj0EAwIDSAAwRQIgaWbSos6z7xucrvst6O9y18Th+beVBmQ1l5WwahHf4V8CIQDCMixPJrHUGenFeOHDGtxzI51YO50zl4ufbyP6n8zyWQ==',
                tls_cert_matches_domain=True,
                tls_cert_common_name='sni.cloudflaressl.com',
                tls_cert_issuer='Cloudflare Inc ECC CA-3',
                tls_cert_start_date='2021-07-09T00:00:00',
                tls_cert_end_date='2022-07-08T23:59:59',
                tls_cert_alternative_names=[
                    '1337x.to', '*.1337x.to', 'sni.cloudflaressl.com'
                ],
                headers=[
                    'Alt-Svc: h3=":443"; ma=86400, h3-29=":443"; ma=86400',
                    'Cf-Cache-Status: DYNAMIC', 'Cf-Ray: 6f4e4da53e5e7e37-DTW',
                    'Content-Type: text/html',
                    'Date: Fri, 01 Apr 2022 03:40:26 GMT',
                    'Expect-Ct: max-age=604800, report-uri="https://report-uri.cloudflare.com/cdn-cgi/beacon/expect-ct"',
                    'Nel: {"success_fraction":0,"report_to":"cf-nel","max_age":604800}',
                    'Server: cloudflare', 'Vary: Accept-Encoding',
                    'X-Frame-Options: DENY'
                ]),
            stateful_block=False,
            outcome='content/tls_mismatch'),
        HyperquackRow(
            domain='control-759c4df3ce6e8418.com',
            category='Control',
            ip='104.17.67.3',
            date='2022-03-31',
            start_time='2022-03-31T23:40:44.354253273-04:00',
            end_time='2022-03-31T23:40:44.383259487-04:00',
            error=None,
            anomaly=True,
            success=True,
            is_control=True,
            controls_failed=False,
            measurement_id='c978424387b45ba1b78f28438063f835',
            source='CP_Quack-https-2022-03-31-23-40-26',
            ip_metadata=IpMetadata(),
            received=HttpsResponse(),
            stateful_block=False,
            outcome='expected/match')
    ]

    fake_metadata_chooser = FakeIpMetadataChooserFactory()
    metadata_adder = MetadataAdder(fake_metadata_chooser)

    with TestPipeline() as p:
      row_lines = p | 'create data' >> beam.Create(data)
      final = hyperquack.process_hyperquack_lines(row_lines, metadata_adder)
      beam_test_util.assert_that(final, beam_test_util.equal_to(expected))
