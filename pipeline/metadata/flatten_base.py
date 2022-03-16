"""Shared functionality for flattening rows."""

from __future__ import absolute_import
from __future__ import annotations  # required to use class as a type inside the class

import dataclasses
from dataclasses import dataclass
import os
from typing import Optional, List, Dict, Any, Union

from pipeline.metadata.blockpage import BlockpageMatcher

# pylint: disable=too-many-instance-attributes


@dataclass
class SatelliteAnswer():
  """Class for keeping track of Satellite answer content"""
  # Keys
  ip: str
  # Metadata
  asnum: Optional[int] = None
  asname: Optional[str] = None
  http: Optional[str] = None
  cert: Optional[str] = None
  matches_control: Optional[str] = None


@dataclass
class SatelliteAnswerMetadata(SatelliteAnswer):
  """Satellite Answer Metadata.

  When this metadata is being passed around
  it needs an additional date field to keep track of when it's valid.
  In the final Row object written to bigwuery we don't include that field.
  """
  date: str = ''


@dataclass
class IpMetadata():
  """Class for keeping track of ip metadata"""
  # Keys
  ip: str
  date: str
  # Metadata
  netblock: Optional[str] = None
  asn: Optional[int] = None
  as_name: Optional[str] = None
  as_full_name: Optional[str] = None
  as_class: Optional[str] = None
  country: Optional[str] = None
  organization: Optional[str] = None
  # Satellite Metadata
  name: Optional[str] = None


# Custom Type
# All or part of a scan row to be written to bigquery
# ex (only scan data): {'domain': 'test.com', 'ip': '1.2.3.4', 'success' true}
# ex (only ip metadata): {'asn': 13335, 'as_name': 'CLOUDFLAREINC'}
# ex (both): {'domain': 'test.com', 'ip': '1.2.3.4', 'asn': 13335}
@dataclass
class Row:  # Corrosponds to BASE_BIGQUERY_SCHEMA
  """Class for keeping track of row content"""
  domain: Optional[str] = None
  category: Optional[str] = None
  ip: Optional[str] = None
  date: Optional[str] = None
  start_time: Optional[str] = None
  end_time: Optional[str] = None
  error: Optional[str] = None
  anomaly: Optional[bool] = None
  success: Optional[bool] = None
  is_control: Optional[bool] = None
  controls_failed: Optional[bool] = None
  measurement_id: Optional[str] = None
  source: Optional[str] = None

  # Metadata
  netblock: Optional[str] = None
  asn: Optional[int] = None
  as_name: Optional[str] = None
  as_full_name: Optional[str] = None
  as_class: Optional[str] = None
  country: Optional[str] = None
  organization: Optional[str] = None

  # imitation update method that matches the semantics of python's dict update
  def update(self, new: Row) -> None:
    for field in dataclasses.fields(new):
      value = getattr(new, field.name)
      if value is not None and value != []:
        setattr(self, field.name, value)


@dataclass
class HyperquackRow(Row):
  """Class for hyperquack specific fields"""
  blockpage: Optional[bool] = None
  page_signature: Optional[str] = None
  stateful_block: Optional[bool] = None
  received_status: Optional[str] = None
  received_body: Optional[str] = None
  received_tls_version: Optional[int] = None
  received_tls_cipher_suite: Optional[int] = None
  received_tls_cert: Optional[str] = None
  received_headers: List[str] = dataclasses.field(default_factory=list)


@dataclass
class SatelliteRow(Row):
  """Class for satellite specific fields"""
  rcode: Optional[int] = None
  name: Optional[str] = None
  is_control_ip: Optional[bool] = None
  average_confidence: Optional[float] = None
  untagged_controls: Optional[bool] = None
  untagged_response: Optional[bool] = None
  excluded: Optional[bool] = None
  exclude_reason: Optional[str] = None
  has_type_a: Optional[bool] = None
  received: List[SatelliteAnswer] = dataclasses.field(default_factory=list)
  matches_confidence: List[float] = dataclasses.field(default_factory=list)


@dataclass
class BlockpageRow():
  """Class for blockpage specific fields"""
  domain: Optional[str] = None
  ip: Optional[str] = None
  date: Optional[str] = None
  start_time: Optional[str] = None
  end_time: Optional[str] = None
  success: Optional[bool] = None
  https: Optional[bool] = None
  source: Optional[str] = None

  blockpage: Optional[bool] = None
  page_signature: Optional[str] = None

  received_status: Optional[str] = None
  received_body: Optional[str] = None
  received_tls_version: Optional[int] = None
  received_tls_cipher_suite: Optional[int] = None
  received_tls_cert: Optional[str] = None
  received_headers: List[str] = dataclasses.field(default_factory=list)

  # imitation update method that matches the semantics of python's dict update
  def update(self, new: Union[BlockpageRow, HyperquackRow]) -> None:
    for field in dataclasses.fields(new):
      value = getattr(new, field.name)
      if value is not None and value != []:
        setattr(self, field.name, value)


# pylint: enable=too-many-instance-attributes

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


def _reconstruct_http_response(row: HyperquackRow) -> str:
  """Rebuild the HTTP response as a string from its pieces

    Args:
      row: a row with the received_status/body/headers fields

    Returns: a string imitating the original http response
    """
  full_response = (row.received_status or '') + '\r\n'
  for header in row.received_headers:
    full_response += header + '\r\n'
  full_response += '\r\n' + (row.received_body or '')
  return full_response


def _add_blockpage_match(blockpage_matcher: BlockpageMatcher, content: str,
                         anomaly: bool, row: HyperquackRow) -> None:
  """If there's an anomaly check the content for a blockpage match and add to row

  args:
    content: the string to check for blockpage matches.
      For HTTP/S this is the HTTP body
      For echo/discard this is the entire recieved content
    anomaly: whether there was an anomaly in the measurement
    row: existing row to add blockpage info to.
  """
  if anomaly:
    blockpage, signature = blockpage_matcher.match_page(content)
    row.blockpage = blockpage
    row.page_signature = signature


def parse_received_data(blockpage_matcher: BlockpageMatcher,
                        received: Union[str, Dict[str, Any]],
                        anomaly: bool) -> HyperquackRow:
  """Parse a received field into a section of a row to write to bigquery.

  Args:
    blockpage_matcher: Matcher object
    received: a dict parsed from json data, or a str
    anomaly: whether data may indicate blocking

  Returns:
    a dict containing the 'received_' keys/values in SCAN_BIGQUERY_SCHEMA
  """
  row = HyperquackRow()

  if isinstance(received, str):
    row.received_status = received
    _add_blockpage_match(blockpage_matcher, received, anomaly, row)
    return row

  row.received_status = received['status_line']
  row.received_body = received['body']
  row.received_headers = parse_received_headers(received.get('headers', {}))

  full_http_response = _reconstruct_http_response(row)
  _add_blockpage_match(blockpage_matcher, full_http_response, anomaly, row)

  # hyperquack v1 TLS format
  tls = received.get('tls', None)
  if tls:
    row.received_tls_version = tls['version']
    row.received_tls_cipher_suite = tls['cipher_suite']
    row.received_tls_cert = tls['cert']

  # hyperquack v2 TLS format
  if 'TlsVersion' in received:
    row.received_tls_version = received['TlsVersion']
    row.received_tls_cipher_suite = received['CipherSuite']
    row.received_tls_cert = received['Certificate']

  return row
