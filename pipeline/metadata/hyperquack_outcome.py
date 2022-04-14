"""Classify outcomes for roundtrip measurements."""

from __future__ import absolute_import

from typing import List, Optional


def _status_mismatch(received_status: Optional[str]) -> str:
  """

  Returns "content/status_mismatch" or ex: "content/status_mismatch:404"
  """
  if received_status is None or received_status == "":
    return "content/status_mismatch"
  return "content/status_mismatch:" + received_status[:3]


def _get_first_error(errors: Optional[str]) -> Optional[str]:
  """
  errors: string like "error a; error b; error c" or just "error a"

  returns: "error a"
  """
  if errors is None:
    return None

  if ';' in errors:
    return errors[:errors.index(';')]
  return errors  # single error


def _has_akamai_server_header(received_headers: List[str]) -> bool:
  for received_header in received_headers:
    if received_header == "Server: AkamaiGHost" or received_header == "Server: GHost":
      return True
  return False


def _classify_hyperquack_error(error: Optional[str], scan_type: str,
                               received_status: Optional[str]) -> str:
  """
  error string
  scan type: like 'echo, 'discard', etc

  outcome: string like "<stage>/<result>"
  """
  # yapf: disable
  # pylint: ignore=multiple-statements,too-many-return-statements,too-many-branches,too-many-statements

  # Success
  if error is None or error == "": return "expected/match"

  # System failures
  if error.endswith("address already in use"): return "setup/system_failure"
  if error.endswith("protocol error"): return "setup/system_failure"  # ipv4 vs 6 error
  if error.endswith("protocol not available"): return "setup/system_failure"
  if error.endswith("too many open files"): return "setup/system_failure"

  # Dial failures
  if error.endswith("network is unreachable"): return "dial/ip.network_unreachable"
  if error.endswith("no route to host"): return "dial/ip.host_no_route"
  if error.endswith("connection refused"): return "dial/tcp.refused"
  if error.endswith("context deadline exceeded"): return "dial/timeout"
  if error.endswith("connect: connection timed ou"): return "dial/timeout"
  if error.startswith("connection reset by peer"): return "dial/tcp.reset"  # no read: or write: prefix in error
  if error.endswith("connect: connection reset by peer"): return "dial/tcp.reset"
  if error.endswith("getsockopt: connection reset by peer"): return "dial/tcp.reset"

  # TLS failures
  if "tls:" in error: return "tls/tls.failed"
  if "remote error:" in error: return "tls/tls.failed"
  if "local error:" in error: return "tls/tls.failed"
  if error.endswith("readLoopPeekFailLocked: <nil>"): return "tls/tls.failed"
  if error.endswith("no mutual cipher suite"): return "tls/tls.failed"
  if error.endswith("missing ServerKeyExchange message"): return "tls/tls.failed"
  if "TLS handshake timeout" in error: return "tls/timeout"

  # Write failures
  if error.endswith("write: connection reset by peer"): return "write/tcp.reset"
  if error.endswith("write: broken pipe"): return "write/system"

  # Read failures
  if "request canceled" in error: return "read/timeout"
  if error.endswith("i/o timeout"): return "read/timeout"
  if error.endswith("shutdown: transport endpoint is not connected"): return "read/system"
  # TODO: for HTTPS this error could potentially also be SNI blocking in the tls stage
  # find a way to diffentiate this case.
  if "read: connection reset by peer" in error: return "read/tcp.reset"

  # HTTP content verification failures
  if scan_type != "echo" and "unexpected EOF" in error: return "read/http.truncated_response"
  if scan_type != "echo" and "EOF" in error: return "read/http.empty"
  if "http: server closed idle connection" in error: return "read/http.truncated_response"
  if error.endswith("trailer header without chunked transfer encoding"): return "http/http.invalid"
  if error.endswith("response missing Location header"): return "http/http.invalid"
  if "bad Content-Length" in error: return "http/http.invalid"
  if "failed to parse Location header" in error: return "http/http.invalid"
  if "malformed HTTP" in error: return "http/http.invalid"
  if "malformed MIME" in error: return "http/http.invalid"

  # Content verification failures
  if scan_type != "echo" and error.endswith("EOF"): return "content/mismatch" # Echo
  # Hyperquack v1 errors
  if error == "Incorrect echo response": return "content/mismatch" # Echo
  if error == "Received response": return "content/mismatch" # Discard
  if error == "Incorrect web response: status lines don't match": return _status_mismatch(received_status) # HTTP/S
  if error == "Incorrect web response: bodies don't match": return "content/body_mismatch" # HTTP/S
  if error == "Incorrect web response: certificates don't match": return "content/tls_mismatch" # HTTPS
  if error == "Incorrect web response: cipher suites don't match": return "content/tls_mismatch" # HTTPS
  if error == "Incorrect web response: TLS versions don't match": return "content/tls_mismatch" # HTTPS
  # Hyperquack v2 errors
  if error == "echo response does not match echo request": return "content/mismatch" # Echo
  if error == "discard response is not empty": return "content/mismatch" # Discard
  if error == "Status lines does not match": return _status_mismatch(received_status) # HTTP/S
  if error == "Bodies do not match": return "content/body_mismatch" # HTTPS
  if error == "Certificates do not match": return "content/tls_mismatch" # HTTPS
  if error == "Cipher suites do not match": return "content/tls_mismatch" # HTTPS
  if error == "TLS versions do not match": return "content/tls_mismatch" # HTTPS
  # Hyperquack v2 multiple errors
  if error == "Status lines do not match": return _status_mismatch(received_status) # HTTP/S
  if error == "Bodies do not match": return "content/body_mismatch" # HTTP/S
  if "header field missing" in error: return "content/header_mismatch" # HTTP/S
  if "header field mismatch" in error: return "content/header_mismatch" # HTTP/S

  return "unknown/unknown"
  # yapf: enable


def classify_hyperquack_outcome(error: Optional[str], scan_type: str,
                                received_status: Optional[str],
                                matches_template: Optional[bool],
                                is_known_blockpage: Optional[bool],
                                blockpage_fingerprint: Optional[str],
                                received_headers: List[str]) -> str:
  """
  """
  if is_known_blockpage and blockpage_fingerprint:
    return "content/blockpage:" + blockpage_fingerprint

  if _has_akamai_server_header(received_headers):
    return "expected/trusted_host:akamai"

  # Content mismatch for hyperquack v2 which didn't write
  # content verification failures in the error field from 2021-04-26 to 2021-07-21
  if not matches_template and (error is None or error == ""):
    return "content/mismatch"

  return _classify_hyperquack_error(
      _get_first_error(error), scan_type, received_status)
