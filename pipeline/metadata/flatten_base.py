"""Shared functionality for flattening rows."""

from __future__ import absolute_import

import os
from typing import Optional, Dict, List, Any, Union

from pipeline.metadata.blockpage import BlockpageMatcher

# Custom Type
# All or part of a scan row to be written to bigquery
# ex (only scan data): {'domain': 'test.com', 'ip': '1.2.3.4', 'success' true}
# ex (only ip metadata): {'asn': 13335, 'as_name': 'CLOUDFLAREINC'}
# ex (both): {'domain': 'test.com', 'ip': '1.2.3.4', 'asn': 13335}
Row = Dict[str, Any]

# For Hyperquack v1
CONTROL_URLS = [
    'example5718349450314.com',  # echo/discard
    'rtyutgyhefdafioasfjhjhi.com',  # HTTP/S
    'a.root-servers.net',  # Satellite
    'www.example.com'  # Satellite
]


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


def _reconstruct_http_response(row: Row) -> str:
  """Rebuild the HTTP response as a string from its pieces

    Args:
      row: a row with the received_status/body/headers fields

    Returns: a string imitating the original http response
    """
  full_response = row['received_status'] + '\r\n'
  for header in row['received_headers']:
    full_response += header + '\r\n'
  full_response += '\r\n' + row['received_body']
  return full_response


def _add_blockpage_match(blockpage_matcher: BlockpageMatcher, content: str,
                         anomaly: bool, row: Row) -> None:
  """If there's an anomaly check the content for a blockpage match and add to row

  args:
    content: the string to check for blockpage matches.
      For HTTP/S this is the HTTP body
      For echo/discard this is the entire recieved content
    anomaly: whether there was an anomaly in the measurement
    row: existing row to add blpckpage info to.
  """
  if anomaly:
    blockpage, signature = blockpage_matcher.match_page(content)
    row['blockpage'] = blockpage
    row['page_signature'] = signature


def parse_received_data(blockpage_matcher: BlockpageMatcher,
                        received: Union[str, Dict[str,
                                                  Any]], anomaly: bool) -> Row:
  """Parse a received field into a section of a row to write to bigquery.

  Args:
    blockpage_matcher: Matcher object
    received: a dict parsed from json data, or a str
    anomaly: whether data may indicate blocking

  Returns:
    a dict containing the 'received_' keys/values in SCAN_BIGQUERY_SCHEMA
  """
  if isinstance(received, str):
    row: Row = {'received_status': received}
    _add_blockpage_match(blockpage_matcher, received, anomaly, row)
    return row

  row = {
      'received_status': received['status_line'],
      'received_body': received['body'],
      'received_headers': parse_received_headers(received.get('headers', {})),
  }

  full_http_response = _reconstruct_http_response(row)
  _add_blockpage_match(blockpage_matcher, full_http_response, anomaly, row)

  # hyperquack v1 TLS format
  tls = received.get('tls', None)
  if tls:
    tls_row = {
        'received_tls_version': tls['version'],
        'received_tls_cipher_suite': tls['cipher_suite'],
        'received_tls_cert': tls['cert']
    }
    row.update(tls_row)

  # hyperquack v2 TLS format
  if 'TlsVersion' in received:
    tls_row = {
        'received_tls_version': received['TlsVersion'],
        'received_tls_cipher_suite': received['CipherSuite'],
        'received_tls_cert': received['Certificate']
    }
    row.update(tls_row)

  return row
