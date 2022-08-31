"""Shared functionality for flattening rows."""

from __future__ import absolute_import
from __future__ import annotations  # required to use class as a type inside the class

import os
import logging
import ssl
from typing import Optional, List, Dict, Any, Union, Tuple

from cryptography import x509
from cryptography.hazmat.backends import default_backend

from pipeline.metadata.blockpage import BlockpageMatcher
from pipeline.metadata.schema import HttpsResponse

# pylint: enable=too-many-instance-attributes

# For Hyperquack v1
CONTROL_URLS = [
    'example5718349450314.com',  # echo/discard
    'rtyutgyhefdafioasfjhjhi.com',  # HTTP/S
    'a.root-servers.net',  # Satellite
    'www.example.com'  # Satellite
]


def get_common_name(cert_name: x509.Name) -> Optional[str]:
  """Get the Common Name of a certificate subject or issuer.

  Args:
    cert_name: x509.Name representing a certificate subject or issuer

  Returns:
    Common Name as a string
  """
  try:
    attributes = cert_name.get_attributes_for_oid(x509.oid.NameOID.COMMON_NAME)
    if attributes:
      return str(attributes[0].value)
  except x509.AttributeNotFound:
    logging.warning('x509.AttributeNotFound: Common Name\n')
  return None


def get_alternative_names(cert: x509.Certificate) -> List[str]:
  """Get the Subject Alternative Names of a certificate.

  Args:
    cert: x509.Certificate containing parsed certificate fields

  Returns:
    list of alternative names
  """
  try:
    ext = cert.extensions.get_extension_for_oid(
        x509.ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
    # Cast to x509.SubjectAlternativeName to avoid mypy error.
    san_ext: x509.SubjectAlternativeName = ext.value  # type: ignore
    return san_ext.get_values_for_type(x509.DNSName)
  except x509.extensions.ExtensionNotFound:
    return []


def load_cert_from_str(cert_str: str) -> x509.Certificate:
  """Load certificate from the certificate text base64 string.

  Args:
    cert_str: base64 encoded certificate string

  Returns:
    x509.Certificate containing parsed certificate fields (e.g., Common Name)
  """
  begin = "-----BEGIN CERTIFICATE-----\n"
  end = "\n-----END CERTIFICATE----- "
  # cryptography.x509 requires the PEM headers and footers to parse the certificate.
  cert_pem = bytes(begin + cert_str + end, encoding='utf-8')
  return x509.load_pem_x509_certificate(cert_pem, default_backend())


def cert_matches_domain(cert: x509.Certificate, domain: str) -> bool:
  """Check whether a cert matches a given domain.

  Args:
    cert: parsed x509 certificate
    domain: like 'www.example.com' (no wildcard domains)

  Returns:
    True if the domain matches, False otherwise
  """
  # cert domain fields in the form compatible with the return from
  # https://docs.python.org/3/library/ssl.html#ssl.SSLSocket.getpeercert
  alts = tuple(('DNS', alt_name) for alt_name in get_alternative_names(cert))
  reformatted_cert = {
      'subject': [[('commonName', get_common_name(cert.subject))]],
      'subjectAltName': alts
  }

  try:
    # https://docs.python.org/3/library/ssl.html#ssl.match_hostname
    # is deprecated in favor of openssl
    # but we're using it anyway to avoid having to write our own handler
    # pylint: disable=deprecated-method
    ssl.match_hostname(reformatted_cert, domain)  # type: ignore
    # pylint: enable=deprecated-method
    return True
  except ssl.SSLCertVerificationError:
    return False


def parse_cert(
    cert_str: str, domain: str
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str],
           List[str], bool]:
  """Parse certificate fields from base64 certificate string.

  Args:
    cert_str: base64 encoded certificate string

  Returns:
    tuple containing parsed certificate fields
    (cert_common_name, cert_issuer, cert_start_date, cert_end_date, cert_alternative_names, cert_matches_domain)
  """
  cert_match_domain = False
  cert_common_name = None
  cert_issuer = None
  cert_start_date = None
  cert_end_date = None
  cert_alternative_names = []
  try:
    cert = load_cert_from_str(cert_str)
    cert_common_name = get_common_name(cert.subject)
    cert_issuer = get_common_name(cert.issuer)
    cert_start_date = cert.not_valid_before.isoformat()
    cert_end_date = cert.not_valid_after.isoformat()
    cert_alternative_names = get_alternative_names(cert)
    cert_match_domain = cert_matches_domain(cert, domain)
  except ValueError as e:
    logging.warning('ValueError: %s\nCert: %s\n', e, cert_str)
  return (cert_common_name, cert_issuer, cert_start_date, cert_end_date,
          cert_alternative_names, cert_match_domain)


def parse_received_headers(headers: Dict[str, List[str]]) -> List[str]:
  """Flatten headers from a dictionary of headers to value lists.

  Args:
    headers: Dict from a header key to a list of headers.
      {"Content-Language": ["en", "fr"],
       "Content-Type": ["text/html; charset=iso-8859-1"]}

  Returns:
    A list of key-value headers pairs as flat strings.
    ["Content-Language: en",
     "Content-Language: fr",
     "Content-Type: text/html; charset=iso-8859-1"]
  """
  # TODO decide whether the right approach here is turning each value into its
  # own string, or turning each key into its own string with the values as a
  # comma seperated list.
  # The right answer depends on whether people will be querying mostly for
  # individual values, or for specific combinations of values.
  flat_headers = []
  for key, values in headers.items():
    for value in values:
      flat_headers.append(key + ': ' + value)
  return flat_headers


def source_from_filename(filepath: str) -> str:
  """Get the source string from a scan filename.

  Source represents the .tar.gz container which held this file.

  Args:
    filepath:
    'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'

  Returns:
    Just the 'CP_Quack-echo-2020-08-23-06-01-02' source
  """
  path = os.path.split(filepath)[0]
  path_end = os.path.split(path)[1]
  return path_end


def is_control_url(url: Optional[str]) -> bool:
  return url in CONTROL_URLS


def _reconstruct_http_response(row: HttpsResponse) -> str:
  """Rebuild the HTTP response as a string from its pieces

    Args:
      row: a row with the received_status/body/headers fields

    Returns: a string imitating the original http response
    """
  full_response = (row.status or '') + '\r\n'
  for header in row.headers:
    full_response += header + '\r\n'
  full_response += '\r\n' + (row.body or '')
  return full_response


def _add_blockpage_match(blockpage_matcher: BlockpageMatcher, content: str,
                         anomaly: bool, row: HttpsResponse) -> None:
  """If there's an anomaly check the content for a blockpage match and add to row

  Args:
    content: the string to check for blockpage matches.
      For HTTP/S this is the HTTP body
      For echo/discard this is the entire recieved content
    anomaly: whether there was an anomaly in the measurement
    row: existing row to add blockpage info to.
  """
  if anomaly:
    is_known_blockpage, signature = blockpage_matcher.match_page(content)
    row.is_known_blockpage = is_known_blockpage
    row.page_signature = signature


def parse_received_data(blockpage_matcher: BlockpageMatcher,
                        received: Union[str, Dict[str, Any]], domain: str,
                        anomaly: bool) -> HttpsResponse:
  """Parse a received field into a section of a row to write to bigquery.

  Args:
    blockpage_matcher: Matcher object
    received: a dict parsed from json data, or a str
    anomaly: whether data may indicate blocking

  Returns:
    a dict containing the 'received_' keys/values in SCAN_BIGQUERY_SCHEMA
  """
  row = HttpsResponse()

  if isinstance(received, str):
    row.status = received
    _add_blockpage_match(blockpage_matcher, received, anomaly, row)
    return row

  row.status = received['status_line']
  row.body = received['body']
  row.headers = parse_received_headers(received.get('headers', {}))

  full_http_response = _reconstruct_http_response(row)
  _add_blockpage_match(blockpage_matcher, full_http_response, anomaly, row)

  # hyperquack v1 TLS format
  tls = received.get('tls', None)
  if tls:
    row.tls_version = tls['version']
    row.tls_cipher_suite = tls['cipher_suite']
    row.tls_cert = tls['cert']

  # hyperquack v2 TLS format
  if 'TlsVersion' in received:
    row.tls_version = received['TlsVersion']
    row.tls_cipher_suite = received['CipherSuite']
    if isinstance(received['Certificate'], str):
      row.tls_cert = received['Certificate']
    elif isinstance(received['Certificate'],
                    List) and len(received['Certificate']) > 0:
      row.tls_cert = received['Certificate'][0]

  # Parse certificate fields
  if row.tls_cert:
    (cert_common_name, cert_issuer, cert_start_date, cert_end_date,
     cert_alternative_names, valid) = parse_cert(row.tls_cert, domain)
    row.tls_cert_matches_domain = valid
    row.tls_cert_common_name = cert_common_name
    row.tls_cert_issuer = cert_issuer
    row.tls_cert_start_date = cert_start_date
    row.tls_cert_end_date = cert_end_date
    row.tls_cert_alternative_names = cert_alternative_names

  return row
