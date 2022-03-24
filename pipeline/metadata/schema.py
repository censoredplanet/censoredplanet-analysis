"""Schema for various data types passed around in the pipeline and bigquery"""

from __future__ import absolute_import
from __future__ import annotations  # required to use class as a type inside the class

import dataclasses
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union

# pylint: disable=too-many-instance-attributes


@dataclass
class IpMetadata():
  """Class for keeping track of ip metadata"""
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
class IpMetadataWithKeys(IpMetadata):
  """Extension of IpMetadata with ip and date keys."""
  # Keys
  ip: str = ''
  date: str = ''


def merge_ip_metadata(base: IpMetadata, new: IpMetadata) -> None:
  """Merge metadata info into an existing metadata."""
  if new.netblock is not None:
    base.netblock = new.netblock
  if new.asn is not None:
    base.asn = new.asn
  if new.as_name is not None:
    base.as_name = new.as_name
  if new.as_full_name is not None:
    base.as_full_name = new.as_full_name
  if new.as_class is not None:
    base.as_class = new.as_class
  if new.country is not None:
    base.country = new.country
  if new.organization is not None:
    base.organization = new.organization
  if new.name is not None:
    base.name = new.name


@dataclass
class SatelliteTags():
  """Satellite tags that are not metadata specifically about the answer ip"""
  http: Optional[str] = None
  cert: Optional[str] = None
  matches_control: Optional[str] = None


@dataclass
class SatelliteAnswer():
  """Class for keeping track of Satellite answer content"""
  # Keys
  ip: str
  tags: SatelliteTags = dataclasses.field(default_factory=SatelliteTags)
  ip_metadata: IpMetadata = dataclasses.field(default_factory=IpMetadata)


@dataclass
class SatelliteAnswerWithKeys(SatelliteAnswer):
  """Satellite Answer Metadata.

  When this metadata is being passed around
  it needs an additional date field to keep track of when it's valid.
  """
  date: str = ''


def merge_satellite_tags(base: SatelliteTags, new: SatelliteTags) -> None:
  if new.http is not None:
    base.http = new.http
  if new.cert is not None:
    base.cert = new.cert
  if new.matches_control is not None:
    base.matches_control = new.matches_control


def merge_satellite_answers(base: SatelliteAnswer,
                            new: SatelliteAnswer) -> None:
  """Add tag information to a Satellite answer"""
  merge_satellite_tags(base.tags, new.tags)
  merge_ip_metadata(base.ip_metadata, new.ip_metadata)


@dataclass
class ReceivedHttps:
  """Class for the parsed content of a received HTTP/S request

  These are both passed around independantly, and as part of
  HyperquackRow/BlockpageRow objects
  """
  blockpage: Optional[bool] = None
  page_signature: Optional[str] = None

  status: Optional[str] = None
  body: Optional[str] = None
  tls_version: Optional[int] = None
  tls_cipher_suite: Optional[int] = None
  tls_cert: Optional[str] = None
  headers: List[str] = dataclasses.field(default_factory=list)


# All or part of a scan row to be written to bigquery
@dataclass
class BigqueryRow:  # Corresponds to BASE_BIGQUERY_SCHEMA
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

  ip_metadata: IpMetadata = dataclasses.field(default_factory=IpMetadata)


@dataclass
class HyperquackRow(BigqueryRow):
  """Class for hyperquack specific fields"""
  received: ReceivedHttps = dataclasses.field(default_factory=ReceivedHttps)
  stateful_block: Optional[bool] = None


@dataclass
class SatelliteRow(BigqueryRow):
  """Class for satellite specific fields"""
  rcode: Optional[int] = None
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
  received: ReceivedHttps = dataclasses.field(default_factory=ReceivedHttps)

  domain: Optional[str] = None
  ip: Optional[str] = None
  date: Optional[str] = None
  start_time: Optional[str] = None
  end_time: Optional[str] = None
  success: Optional[bool] = None
  https: Optional[bool] = None
  source: Optional[str] = None


def flatten_for_bigquery(
    row: Union[BigqueryRow, BlockpageRow]) -> Dict[str, Any]:
  if isinstance(row, HyperquackRow):
    return flatten_for_bigquery_hyperquack(row)
  if isinstance(row, SatelliteRow):
    return flatten_for_bigquery_satellite(row)
  if isinstance(row, BlockpageRow):
    return flatten_for_bigquery_blockpage(row)
  raise Exception(f'Unknown row type: {type(row)}')


def flatten_for_bigquery_hyperquack(row: HyperquackRow) -> Dict[str, Any]:
  """Convert a structured hyperquack dataclass into a flat dict."""
  flat: Dict[str, Any] = {
      'domain': row.domain,
      'category': row.category,
      'ip': row.ip,
      'date': row.date,
      'start_time': row.start_time,
      'end_time': row.end_time,
      'error': row.error,
      'anomaly': row.anomaly,
      'success': row.success,
      'is_control': row.is_control,
      'controls_failed': row.controls_failed,
      'measurement_id': row.measurement_id,
      'source': row.source,
      'stateful_block': row.stateful_block,
      'netblock': row.ip_metadata.netblock,
      'asn': row.ip_metadata.asn,
      'as_name': row.ip_metadata.as_name,
      'as_full_name': row.ip_metadata.as_full_name,
      'as_class': row.ip_metadata.as_class,
      'country': row.ip_metadata.country,
      'organization': row.ip_metadata.organization,
      'blockpage': row.received.blockpage,
      'page_signature': row.received.page_signature,
      'received_status': row.received.status,
      'received_body': row.received.body,
      'received_headers': row.received.headers,
      'received_tls_version': row.received.tls_version,
      'received_tls_cipher_suite': row.received.tls_cipher_suite,
      'received_tls_cert': row.received.tls_cert,
  }
  return flat


def flatten_for_bigquery_satellite(row: SatelliteRow) -> Dict[str, Any]:
  """Convert a structured satellite dataclass into a flat dict."""
  flat: Dict[str, Any] = {
      'domain': row.domain,
      'category': row.category,
      'ip': row.ip,
      'date': row.date,
      'start_time': row.start_time,
      'end_time': row.end_time,
      'error': row.error,
      'anomaly': row.anomaly,
      'success': row.success,
      'is_control': row.is_control,
      'controls_failed': row.controls_failed,
      'measurement_id': row.measurement_id,
      'source': row.source,
      'is_control_ip': row.is_control_ip,
      'rcode': row.rcode,
      'average_confidence': row.average_confidence,
      'matches_confidence': row.matches_confidence,
      'untagged_controls': row.untagged_controls,
      'untagged_response': row.untagged_response,
      'excluded': row.excluded,
      'exclude_reason': row.exclude_reason,
      'has_type_a': row.has_type_a,
      'received': [],
      'name': row.ip_metadata.name,
      'netblock': row.ip_metadata.netblock,
      'asn': row.ip_metadata.asn,
      'as_name': row.ip_metadata.as_name,
      'as_full_name': row.ip_metadata.as_full_name,
      'as_class': row.ip_metadata.as_class,
      'country': row.ip_metadata.country,
      'organization': row.ip_metadata.organization,
  }

  for received_answer in row.received:
    answer = {
        'ip': received_answer.ip,
        'asnum': received_answer.ip_metadata.asn,
        'asname': received_answer.ip_metadata.as_name,
        'http': received_answer.tags.http,
        'cert': received_answer.tags.cert,
        'matches_control': received_answer.tags.matches_control,
    }
    flat['received'].append(answer)
  return flat


def flatten_for_bigquery_blockpage(row: BlockpageRow) -> Dict[str, Any]:
  """Convert a structured blockpage dataclass into a flat dict."""
  flat: Dict[str, Any] = {
      'domain': row.domain,
      'ip': row.ip,
      'date': row.date,
      'start_time': row.start_time,
      'end_time': row.end_time,
      'success': row.success,
      'source': row.source,
      'https': row.https,
      'blockpage': row.received.blockpage,
      'page_signature': row.received.page_signature,
      'received_status': row.received.status,
      'received_body': row.received.body,
      'received_headers': row.received.headers,
      'received_tls_version': row.received.tls_version,
      'received_tls_cipher_suite': row.received.tls_cipher_suite,
      'received_tls_cert': row.received.tls_cert,
  }
  return flat


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
