"""Classify outcomes for roundtrip measurements."""

from __future__ import absolute_import

from typing import List, Optional


def _status_mismatch(received_status: Optional[str]) -> str:
  """
  Args:
    received_status: status string like "200 OK" or None

  Returns:
    outcome like "status_mismatch" or "status_mismatch:404"
  """
  if received_status is None or received_status == "":
    return "status_mismatch"
  return "status_mismatch:" + received_status[:3]


def _get_first_error(errors: Optional[str]) -> Optional[str]:
  """
  Args:
    errors: string like "error a; error b; error c" or just "error a"

  Feturns
    "error a"
  """
  if errors is None:
    return None

  if ';' in errors:
    return errors[:errors.index(';')]
  return errors  # single error


def _has_akamai_server_header(received_headers: List[str]) -> bool:
  for received_header in received_headers:
    if received_header in ('Server: AkamaiGHost', 'Server: GHost'):
      return True
  return False


# yapf: disable
# pylint: disable=multiple-statements
# pylint: disable=too-many-return-statements
# pylint: disable=too-many-branches

def _is_system_failure(error: str) -> bool:
  if error.endswith("address already in use"): return True
  if error.endswith("protocol error"): return True  # ipv4 vs 6 error
  if error.endswith("protocol not available"): return True
  if error.endswith("too many open files"): return True
  return False

def _get_dial_error(error: str) -> Optional[str]:
  if error.endswith("network is unreachable"): return "ip.network_unreachable"
  if error.endswith("no route to host"): return "ip.host_no_route"
  if error.endswith("connection refused"): return "tcp.refused"
  if error.endswith("context deadline exceeded"): return "timeout"
  if error.endswith("connect: connection timed ou"): return "timeout"
  if error.startswith("connection reset by peer"): return "tcp.reset"  # no read: or write: prefix in error
  if error.endswith("connect: connection reset by peer"): return "tcp.reset"
  if error.endswith("getsockopt: connection reset by peer"): return "tcp.reset"
  return None


def _get_tls_failure(error: str) -> Optional[str]:
  if "tls:" in error: return "tls.failed"
  if "remote error:" in error: return "tls.failed"
  if "local error:" in error: return "tls.failed"
  if error.endswith("readLoopPeekFailLocked: <nil>"): return "tls.failed"
  if error.endswith("no mutual cipher suite"): return "tls.failed"
  if error.endswith("missing ServerKeyExchange message"): return "tls.failed"
  if "TLS handshake timeout" in error: return "timeout"
  return None

def _get_write_failure(error: str) -> Optional[str]:
  if error.endswith("write: connection reset by peer"): return "tcp.reset"
  if error.endswith("write: broken pipe"): return "system"
  return None

def _get_read_failure(error: str, scan_type: str) -> Optional[str]:
  if "request canceled" in error: return "timeout"
  if error.endswith("i/o timeout"): return "timeout"
  if error.endswith("shutdown: transport endpoint is not connected"): return "system"
  # TODO: for HTTPS this error could potentially also be SNI blocking in the tls stage
  # find a way to diffentiate this case.
  if "read: connection reset by peer" in error: return "tcp.reset"
  if scan_type != "echo" and "unexpected EOF" in error: return "http.truncated_response"
  if scan_type != "echo" and "EOF" in error: return "http.empty"
  if "http: server closed idle connection" in error: return "http.truncated_response"
  return None

def _get_http_content_verification_failure(error: str) -> Optional[str]:
  if error.endswith("trailer header without chunked transfer encoding"): return "http.invalid"
  if error.endswith("response missing Location header"): return "http.invalid"
  if "bad Content-Length" in error: return "http.invalid"
  if "failed to parse Location header" in error: return "http.invalid"
  if "malformed HTTP" in error: return "http.invalid"
  if "malformed MIME" in error: return "http.invalid"
  return None

def _get_content_mismatch(error: str, scan_type: str, received_status: Optional[str]) -> Optional[str]:
  if scan_type == "echo" and error.endswith("EOF"): return "mismatch" # Echo
  # Hyperquack v1 errors
  if error == "Incorrect echo response": return "mismatch" # Echo
  if error == "Received response": return "mismatch" # Discard
  if error == "Incorrect web response: status lines don't match": return _status_mismatch(received_status) # HTTP/S
  if error == "Incorrect web response: bodies don't match": return "body_mismatch" # HTTP/S
  if error == "Incorrect web response: certificates don't match": return "tls_mismatch" # HTTPS
  if error == "Incorrect web response: cipher suites don't match": return "tls_mismatch" # HTTPS
  if error == "Incorrect web response: TLS versions don't match": return "tls_mismatch" # HTTPS
  # Hyperquack v2 errors
  if error == "echo response does not match echo request": return "mismatch" # Echo
  if error == "discard response is not empty": return "mismatch" # Discard
  if error == "Status lines does not match": return _status_mismatch(received_status) # HTTP/S
  if error == "Bodies do not match": return "body_mismatch" # HTTPS
  if error == "Certificates do not match": return "tls_mismatch" # HTTPS
  if error == "Cipher suites do not match": return "tls_mismatch" # HTTPS
  if error == "TLS versions do not match": return "tls_mismatch" # HTTPS
  # Hyperquack v2 multiple errors
  if error == "Status lines do not match": return _status_mismatch(received_status) # HTTP/S
  if error == "Bodies do not match": return "body_mismatch" # HTTP/S
  if "header field missing" in error: return "header_mismatch" # HTTP/S
  if "header field mismatch" in error: return "header_mismatch" # HTTP/S
  return None


def _classify_hyperquack_error(error: Optional[str], scan_type: str,
                               received_status: Optional[str]) -> str:
  """
  Args:
    error string like 'read tcp 141.212.123.235:11397->117.78.42.54:9: read: connection reset by peer'
    scan type: like 'echo, 'discard', etc

  Returns:
    outcome: string like "<stage>/<result>"
  """
  # Success
  if error is None or error == "": return "expected/match"

  if _is_system_failure(error):
    return "setup/system_failure"

  dial_error = _get_dial_error(error)
  if dial_error: return f"dial/{dial_error}"

  tls_failure = _get_tls_failure(error)
  if tls_failure: return f"tls/{tls_failure}"

  write_failure = _get_write_failure(error)
  if write_failure: return f"write/{write_failure}"

  read_failure = _get_read_failure(error, scan_type)
  if read_failure: return f"read/{read_failure}"

  http_failure = _get_http_content_verification_failure(error)
  if http_failure: return f"http/{http_failure}"

  mismatch_error = _get_content_mismatch(error, scan_type, received_status)
  if mismatch_error: return f"content/{mismatch_error}"

  return "unknown/unknown"

# yapf: enable


def classify_hyperquack_outcome(error: Optional[str], scan_type: str,
                                received_status: Optional[str],
                                matches_template: Optional[bool],
                                is_known_blockpage: Optional[bool],
                                blockpage_fingerprint: Optional[str],
                                received_headers: List[str]) -> str:
  """
  Args:
    error: string like "Incorrect web response: status lines don't match"
    received_status: HTTP status like "200 OK"
    matches_template: did the response match the control template
    is_known_blockpage: did the page content match a blockpage
    blockpage_fingerprint: which known blockpage did we match
    received_headers: HTTP headers


  Returns:
    outcome string like "read/tcp.reset"
  """
  # TODO reorder this to more closely match when we see each signal

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
