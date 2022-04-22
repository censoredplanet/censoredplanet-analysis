"""Unit tests for hyperquack outcomes."""

import unittest

from pipeline.metadata import hyperquack_outcome


class HyperquackOutcomeTest(unittest.TestCase):
  """Unit tests of hyperquack outcomes"""

  def test_blockpage(self) -> None:
    outcome = hyperquack_outcome.classify_hyperquack_outcome(
        "Incorrect echo response", "echo", None, False, True,
        "a_prod_watchguard_1", [])
    self.assertEqual("content/blockpage:a_prod_watchguard_1", outcome)

  def test_fp_blockpage_is_mismatch(self) -> None:
    outcome = hyperquack_outcome.classify_hyperquack_outcome(
        "Received response", "discard", None, False, False, "a_prod_cisco", [])
    self.assertEqual("content/mismatch", outcome)

  def test_expected_akamai_mismatch(self) -> None:
    outcome = hyperquack_outcome.classify_hyperquack_outcome(
        "i/o timeout", "https", None, False, None, None,
        ["Server: AkamaiGHost"])
    self.assertEqual("expected/trusted_host:akamai", outcome)

  def test_missing_data_mismatch(self) -> None:
    outcome = hyperquack_outcome.classify_hyperquack_outcome(
        None, "http", None, False, None, None, [])
    self.assertEqual("content/mismatch", outcome)

  def test_expected(self) -> None:
    outcome = hyperquack_outcome.classify_hyperquack_outcome(
        None, "http", None, True, None, None, [])
    self.assertEqual("expected/match", outcome)

  def test_unknown(self) -> None:
    outcome = hyperquack_outcome.classify_hyperquack_outcome(
        "gibberish", "http", None, True, None, None, [])
    self.assertEqual("unknown/unknown", outcome)

  def test_status_mismatch(self) -> None:
    outcome = hyperquack_outcome.classify_hyperquack_outcome(
        "Status lines does not match", "http", "301 Redirect", True, None, None,
        [])
    self.assertEqual("content/status_mismatch:301", outcome)

  def test_multiple_errors(self) -> None:
    outcome = hyperquack_outcome.classify_hyperquack_outcome(
        "Certificates do not match;Bodies do not match", "https", None, True,
        None, None, [])
    self.assertEqual("content/tls_mismatch", outcome)

  # yapf: disable
  def test_discard_errors(self) -> None:
    """Error outcomes for discard"""
    outcomes_and_errors = [
        ('setup/system_failure', 'address already in use'),
        ('read/system', 'close tcp 1.2.3.4:1000->5.6.7.8:2000: shutdown: transport endpoint is not connected'),
        ('dial/tcp.refused', 'connection refused'),
        ('dial/tcp.reset', 'connection reset by peer'),
        ('dial/timeout', 'context deadline exceeded'),
        ('setup/system_failure', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: bind: address already in use'),
        ('dial/tcp.refused', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: connection refused'),
        ('dial/tcp.reset', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: connection reset by peer'),
        ('dial/timeout', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: connection timed out'),
        ('dial/ip.network_unreachable', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: network is unreachable'),
        ('dial/ip.host_no_route', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: no route to host'),
        ('setup/system_failure', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: protocol error'),
        ('setup/system_failure', 'file file+net port.[PORT]: fcntl: too many open files'),
        ('read/timeout', 'i/o timeout'),
        ('dial/ip.network_unreachable', 'network is unreachable'),
        ('dial/ip.host_no_route', 'no route to host'),
        ('setup/system_failure', 'protocol not available'),
        ('read/timeout', 'read tcp 1.2.3.4:1000->5.6.7.8:2000: i/o timeout'),
        ('read/tcp.reset', 'read tcp 1.2.3.4:1000->5.6.7.8:2000: read: connection reset by peer'),
        ('setup/system_failure', 'too many open files'),
        ('write/system', 'write tcp 1.2.3.4:1000->5.6.7.8:2000: write: broken pipe'),
        ('write/tcp.reset', 'write tcp 1.2.3.4:1000->5.6.7.8:2000: write: connection reset by peer'),
        ('write/tcp.reset', 'write tcp 1.2.3.4:1000: write: connection reset by peer'),
        # Hyperquack V1
        ('content/mismatch', 'Received response'),
        # Hyperquack V2
        ('content/mismatch', 'discard response is not empty'),
    ]
    for (expected_outcome, error) in outcomes_and_errors:
      outcome = hyperquack_outcome.classify_hyperquack_outcome(
          error, "discard", None, False, None, None, [])
    self.assertEqual(outcome, expected_outcome, error)

  def test_echo_errors(self) -> None:
    """Error outcomes for echo"""
    outcomes_and_errors = [
        ('setup/system_failure', 'address already in use'),
        ('dial/tcp.refused', 'connection refused'),
        ('dial/tcp.reset', 'connection reset by peer'),
        ('dial/timeout', 'context deadline exceeded'),
        ('setup/system_failure', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: bind: address already in use'),
        ('dial/tcp.refused', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: connection refused'),
        ('dial/tcp.reset', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: connection reset by peer'),
        ('dial/timeout', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: connection timed out'),
        ('dial/ip.network_unreachable', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: network is unreachable'),
        ('dial/ip.host_no_route', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: no route to host'),
        ('setup/system_failure', 'dial tcp 1.2.3.4:1000->5.6.7.8:2000: connect: protocol error'),
        ('setup/system_failure', 'file file+net port.[PORT]: fcntl: too many open files'),
        ('read/timeout', 'i/o timeout'),
        ('dial/ip.network_unreachable', 'network is unreachable'),
        ('dial/ip.host_no_route', 'no route to host'),
        ('setup/system_failure', 'protocol not available'),
        ('read/timeout', 'read tcp 1.2.3.4:1000->5.6.7.8:2000: i/o timeout'),
        ('read/tcp.reset', 'read tcp 1.2.3.4:1000->5.6.7.8:2000: read: connection reset by peer'),
        ('setup/system_failure', 'too many open files'),
        ('write/system', 'write tcp 1.2.3.4:1000->5.6.7.8:2000: write: broken pipe'),
        ('write/tcp.reset', 'write tcp 1.2.3.4:1000->5.6.7.8:2000: write: connection reset by peer'),
        ('write/tcp.reset', 'write tcp 1.2.3.4:1000: write: connection reset by peer'),
        ('content/mismatch', 'EOF'),
        # Hyperquack V1
        ('content/mismatch', 'Incorrect echo response'),
        # Hyperquack V2
        ('content/mismatch', 'echo response does not match echo request'),
    ]
    for (expected_outcome, error) in outcomes_and_errors:
      outcome = hyperquack_outcome.classify_hyperquack_outcome(
        error, "echo", None, False, None, None, [])
      self.assertEqual(outcome, expected_outcome, error)

  def test_http_errors(self) -> None:
    """Error outcomes for HTTP"""
    outcomes_and_errors = [
        ('http/http.invalid', 'Get http://1.2.3.4: 301 response missing Location header'),
        ('http/http.invalid', 'Get http://1.2.3.4: bad Content-Length 123'),
        ('dial/tcp.refused', 'Get http://1.2.3.4: dial tcp 1.2.3.4:1000: connect: connection refused'),
        ('dial/ip.network_unreachable', 'Get http://1.2.3.4: dial tcp 1.2.3.4:1000: connect: network is unreachable'),
        ('dial/ip.host_no_route', 'Get http://1.2.3.4: dial tcp 1.2.3.4:1000: connect: no route to host'),
        ('dial/tcp.refused', 'Get http://1.2.3.4: dial tcp 1.2.3.4:1000: getsockopt: connection refused'),
        ('dial/tcp.reset', 'Get http://1.2.3.4: dial tcp 1.2.3.4:1000: getsockopt: connection reset by peer'),
        ('dial/ip.network_unreachable', 'Get http://1.2.3.4: dial tcp 1.2.3.4:1000: getsockopt: network is unreachable'),
        ('dial/ip.host_no_route', 'Get http://1.2.3.4: dial tcp 1.2.3.4:1000: getsockopt: no route to host'),
        ('setup/system_failure', 'Get http://1.2.3.4: dial tcp 1.2.3.4:1000: socket: too many open files'),
        ('read/http.empty', 'Get http://1.2.3.4: EOF'),
        ('http/http.invalid', 'Get http://1.2.3.4: failed to parse Location header "\x00ttps://web-hosting.com/": parse ttps://web-hosting.com/: first path segment in URL cannot contain colon'),
        ('http/http.invalid', 'Get http://1.2.3.4: malformed HTTP response "SSH-2.0-OpenSSH_5.3"'),
        ('http/http.invalid', 'Get http://1.2.3.4: malformed HTTP status code "403$FOrbidden"'),
        ('http/http.invalid', 'Get http://1.2.3.4: malformed HTTP version "\x01\x00\x00\x00\x00\x00\x00\x00HTTP/1.0"'),
        ('http/http.invalid', 'Get http://1.2.3.4: malformed MIME header line: <html>'),
        ('read/timeout', 'Get http://1.2.3.4: net/http: request canceled (Client.Timeout exceeded while awaiting headers)'),
        ('read/timeout', 'Get http://1.2.3.4: net/http: request canceled while waiting for connection (Client.Timeout exceeded while awaiting headers)'),
        ('read/tcp.reset', 'Get http://1.2.3.4: read tcp 1.2.3.4:1000->1.2.3.4:1000: read: connection reset by peer'),
        ('read/http.truncated_response', 'Get http://1.2.3.4: unexpected EOF'),
        # Hyperquack V1
        ('content/body_mismatch', 'Incorrect web response: bodies don\'t match'),
        ('content/status_mismatch', 'Incorrect web response: status lines don\'t match'),
        # Hyperquack V2
        ('content/body_mismatch', 'Bodies do not match'),
        ('content/status_mismatch', 'Status lines does not match'),
        ('content/header_mismatch', 'header field missing'),
        ('content/header_mismatch', 'header field mismatch'),
    ]
    for (expected_outcome, error) in outcomes_and_errors:
      outcome = hyperquack_outcome.classify_hyperquack_outcome(
          error, "http", None, False, None, None, [])
      self.assertEqual(outcome, expected_outcome, error)

  def test_https_errors(self) -> None:
    """Error outcomes for HTTPS"""
    outcomes_and_errors = [
        ('http/http.invalid', 'Get https://1.2.3.4: 302 response missing Location header'),
        ('http/http.invalid', 'Get https://1.2.3.4: bad Content-Length 123'),
        ('dial/tcp.refused', 'Get https://1.2.3.4: dial tcp 1.2.3.4:1000: connect: connection refused'),
        ('dial/ip.host_no_route', 'Get https://1.2.3.4: dial tcp 1.2.3.4:1000: connect: no route to host'),
        ('dial/tcp.refused', 'Get https://1.2.3.4: dial tcp 1.2.3.4:1000: getsockopt: connection refused'),
        ('dial/tcp.reset', 'Get https://1.2.3.4: dial tcp 1.2.3.4:1000: getsockopt: connection reset by peer'),
        ('dial/ip.network_unreachable', 'Get https://1.2.3.4: dial tcp 1.2.3.4:1000: getsockopt: network is unreachable'),
        ('dial/ip.host_no_route', 'Get https://1.2.3.4: dial tcp 1.2.3.4:1000: getsockopt: no route to host'),
        ('setup/system_failure', 'Get https://1.2.3.4: dial tcp 1.2.3.4:1000: socket: too many open files'),
        ('read/http.empty', 'Get https://1.2.3.4: EOF'),
        ('tls/tls.failed', 'Get https://1.2.3.4: local error: unexpected message'),
        ('http/http.invalid', 'Get https://1.2.3.4: malformed HTTP status code "HTML>"'),
        ('read/timeout', 'Get https://1.2.3.4: net/http: request canceled (Client.Timeout exceeded while awaiting headers)'),
        ('read/timeout', 'Get https://1.2.3.4: net/http: request canceled while waiting for connection (Client.Timeout exceeded while awaiting headers)'),
        ('read/tcp.reset', 'Get https://1.2.3.4: read tcp 1.2.3.4:1000->5.6.7.8:2000: read: connection reset by peer'),
        ('tls/tls.failed', 'Get https://1.2.3.4: remote error: alert(112)'),
        ('tls/tls.failed', 'Get https://1.2.3.4: remote error: bad record MAC'),
        ('tls/tls.failed', 'Get https://1.2.3.4: remote error: error decrypting message'),
        ('tls/tls.failed', 'Get https://1.2.3.4: remote error: handshake failure'),
        ('tls/tls.failed', 'Get https://1.2.3.4: remote error: internal error'),
        ('tls/tls.failed', 'Get https://1.2.3.4: remote error: protocol version not supported'),
        ('tls/tls.failed', 'Get https://1.2.3.4: remote error: unexpected message'),
        ('tls/tls.failed', 'Get https://1.2.3.4: tls: first record does not look like a TLS handshake'),
        ('tls/tls.failed', 'Get https://1.2.3.4: tls: oversized record received with length 123'),
        ('tls/tls.failed', 'Get https://1.2.3.4: tls: received record with version 123 when expecting version 456'),
        ('tls/tls.failed', 'Get https://1.2.3.4: tls: received unexpected handshake message of type *tls.serverKeyExchangeMsg when waiting for *tls.certificateStatusMsg'),
        ('http/http.invalid', 'Get https://1.2.3.4: trailer header without chunked transfer encoding'),
        ('read/http.truncated_response', 'Get https://1.2.3.4: unexpected EOF'),
        # Hyperquack V1
        ('content/body_mismatch', 'Incorrect web response: bodies don\'t match'),
        ('content/tls_mismatch', 'Incorrect web response: certificates don\'t match'),
        ('content/tls_mismatch', 'Incorrect web response: cipher suites don\'t match'),
        ('content/status_mismatch', 'Incorrect web response: status lines don\'t match'),
        ('content/tls_mismatch', 'Incorrect web response: TLS versions don\'t match'),
        # Hyperquack V2
        ('content/body_mismatch', 'Bodies do not match'),
        ('content/tls_mismatch', 'Certificates do not match'),
        ('content/tls_mismatch', 'Cipher suites do not match'),
        ('content/status_mismatch', 'Status lines do not match'),
        ('content/tls_mismatch', 'TLS versions do not match'),
        ('content/header_mismatch', 'header field missing'),
        ('content/header_mismatch', 'header field mismatch'),
    ]
    for (expected_outcome, error) in outcomes_and_errors:
      outcome = hyperquack_outcome.classify_hyperquack_outcome(
          error, "https", None, False, None, None, [])
      self.assertEqual(outcome, expected_outcome, error)
    # yapf: enable
