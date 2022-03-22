"""Schema for various data types passed around in the pipeline and bigquery"""

from __future__ import absolute_import
from __future__ import annotations  # required to use class as a type inside the class

import dataclasses
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

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

  def add_tag_to_answer(self, tag: SatelliteAnswerMetadata) -> None:
    """Add tag information to a Satellite answer"""
    if tag.asnum is not None:
      self.asnum = tag.asnum
    if tag.asname is not None:
      self.asname = tag.asname
    if tag.http is not None:
      self.http = tag.http
    if tag.cert is not None:
      self.cert = tag.cert
    if tag.matches_control is not None:
      self.matches_control = tag.matches_control


@dataclass
class SatelliteAnswerMetadata(SatelliteAnswer):
  """Satellite Answer Metadata.

  When this metadata is being passed around
  it needs an additional date field to keep track of when it's valid.
  In the final SatelliteAnswer object written to bigquery
  we don't include that field since it's redundant.
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


@dataclass
class ReceivedHttps:
  """Class for the parsed content of a received HTTP/S request

  These are both passed around independantly, and as part of
  HyperquackRow/BlockpageRow objects
  """
  blockpage: Optional[bool] = None
  page_signature: Optional[str] = None

  received_status: Optional[str] = None
  received_body: Optional[str] = None
  received_tls_version: Optional[int] = None
  received_tls_cipher_suite: Optional[int] = None
  received_tls_cert: Optional[str] = None
  received_headers: List[str] = dataclasses.field(default_factory=list)

  def update_received(self, new: ReceivedHttps) -> None:
    """Imitation update method that matches the semantics of python's dict update

    Both HyperquackRow and BlockpageRow use this method
    to add in ReceivedHttps fields to themselves.
    """
    if new.blockpage is not None:
      self.blockpage = new.blockpage
    if new.page_signature is not None:
      self.page_signature = new.page_signature
    if new.received_status is not None:
      self.received_status = new.received_status
    if new.received_body is not None:
      self.received_body = new.received_body
    if new.received_tls_version is not None:
      self.received_tls_version = new.received_tls_version
    if new.received_tls_cipher_suite is not None:
      self.received_tls_cipher_suite = new.received_tls_cipher_suite
    if new.received_tls_cert is not None:
      self.received_tls_cert = new.received_tls_cert
    if new.received_headers != []:
      self.received_headers = new.received_headers


# All or part of a scan row to be written to bigquery
@dataclass
class Row:  # Corresponds to BASE_BIGQUERY_SCHEMA
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

  def add_metadata_to_row(self, metadata: IpMetadata) -> None:
    """Add metadata info to a Row."""
    if metadata.netblock is not None:
      self.netblock = metadata.netblock
    if metadata.asn is not None:
      self.asn = metadata.asn
    if metadata.as_name is not None:
      self.as_name = metadata.as_name
    if metadata.as_full_name is not None:
      self.as_full_name = metadata.as_full_name
    if metadata.as_class is not None:
      self.as_class = metadata.as_class
    if metadata.country is not None:
      self.country = metadata.country
    if metadata.organization is not None:
      self.organization = metadata.organization


@dataclass
class HyperquackRow(Row, ReceivedHttps):
  """Class for hyperquack specific fields"""
  stateful_block: Optional[bool] = None


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

  def add_metadata_to_row(self, metadata: IpMetadata) -> None:
    Row.add_metadata_to_row(self, metadata)
    if metadata.name:
      self.name = metadata.name


@dataclass
class BlockpageRow(ReceivedHttps):
  """Class for blockpage specific fields"""
  domain: Optional[str] = None
  ip: Optional[str] = None
  date: Optional[str] = None
  start_time: Optional[str] = None
  end_time: Optional[str] = None
  success: Optional[bool] = None
  https: Optional[bool] = None
  source: Optional[str] = None


# key: (type, mode)
BASE_BIGQUERY_SCHEMA = {
    # Columns from Censored Planet data
    'domain': ('string', 'nullable'),
    'category': ('string', 'nullable'),
    'ip': ('string', 'nullable'),
    'date': ('date', 'nullable'),
    'start_time': ('timestamp', 'nullable'),
    'end_time': ('timestamp', 'nullable'),
    'error': ('string', 'nullable'),
    'anomaly': ('boolean', 'nullable'),
    'success': ('boolean', 'nullable'),
    'is_control': ('boolean', 'nullable'),
    'controls_failed': ('boolean', 'nullable'),
    'measurement_id': ('string', 'nullable'),
    'source': ('string', 'nullable'),

    # Columns added from CAIDA data
    'netblock': ('string', 'nullable'),
    'asn': ('integer', 'nullable'),
    'as_name': ('string', 'nullable'),
    'as_full_name': ('string', 'nullable'),
    'as_class': ('string', 'nullable'),
    'country': ('string', 'nullable'),
    # Columns from DBIP
    'organization': ('string', 'nullable'),
}
# Future fields
"""
    'as_traffic': ('integer', 'nullable'),
"""


def _add_schemas(schema_a: Dict[str, Any],
                 schema_b: Dict[str, Any]) -> Dict[str, Any]:
  """Add two bigquery schemas together."""
  full_schema: Dict[str, Any] = {}
  full_schema.update(schema_a)
  full_schema.update(schema_b)
  return full_schema


HYPERQUACK_BIGQUERY_SCHEMA = _add_schemas(
    BASE_BIGQUERY_SCHEMA,
    {
        'blockpage': ('boolean', 'nullable'),
        'page_signature': ('string', 'nullable'),
        'stateful_block': ('boolean', 'nullable'),

        # Column filled in all tables
        'received_status': ('string', 'nullable'),
        # Columns filled only in HTTP/HTTPS tables
        'received_body': ('string', 'nullable'),
        'received_headers': ('string', 'repeated'),
        # Columns filled only in HTTPS tables
        'received_tls_version': ('integer', 'nullable'),
        'received_tls_cipher_suite': ('integer', 'nullable'),
        'received_tls_cert': ('string', 'nullable'),
    })

SATELLITE_BIGQUERY_SCHEMA = _add_schemas(
    BASE_BIGQUERY_SCHEMA, {
        'name': ('string', 'nullable'),
        'is_control_ip': ('boolean', 'nullable'),
        'received': ('record', 'repeated', {
            'ip': ('string', 'nullable'),
            'asnum': ('integer', 'nullable'),
            'asname': ('string', 'nullable'),
            'http': ('string', 'nullable'),
            'cert': ('string', 'nullable'),
            'matches_control': ('string', 'nullable')
        }),
        'rcode': ('integer', 'nullable'),
        'average_confidence': ('float', 'nullable'),
        'matches_confidence': ('float', 'repeated'),
        'untagged_controls': ('boolean', 'nullable'),
        'untagged_response': ('boolean', 'nullable'),
        'excluded': ('boolean', 'nullable'),
        'exclude_reason': ('string', 'nullable'),
        'has_type_a': ('boolean', 'nullable')
    })

BLOCKPAGE_BIGQUERY_SCHEMA = {
    # Columns from Censored Planet data
    'domain': ('string', 'nullable'),
    'ip': ('string', 'nullable'),
    'date': ('date', 'nullable'),
    'start_time': ('timestamp', 'nullable'),
    'end_time': ('timestamp', 'nullable'),
    'success': ('boolean', 'nullable'),
    'https': ('boolean', 'nullable'),
    'source': ('string', 'nullable'),
    'blockpage': ('boolean', 'nullable'),
    'page_signature': ('string', 'nullable'),

    # Column filled in all tables
    'received_status': ('string', 'nullable'),
    # Columns filled only in HTTP/HTTPS tables
    'received_body': ('string', 'nullable'),
    'received_headers': ('string', 'repeated'),
    # Columns filled only in HTTPS tables
    'received_tls_version': ('integer', 'nullable'),
    'received_tls_cipher_suite': ('integer', 'nullable'),
    'received_tls_cert': ('string', 'nullable'),
}
