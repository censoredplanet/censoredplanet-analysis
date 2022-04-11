"""Schema for various data types passed around in the pipeline and bigquery"""

from __future__ import absolute_import
from __future__ import annotations  # required to use class as a type inside the class

import dataclasses
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union

from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery

# pylint: disable=too-many-instance-attributes

SCAN_TYPE_SATELLITE = 'satellite'
SCAN_TYPE_PAGE_FETCH = 'page_fetch'


@dataclass
class HttpsResponse:
  """Class for the parsed content of a received HTTP/S request

  These are both passed around independantly, and as part of
  HyperquackRow/PageFetchRow objects
  """
  is_known_blockpage: Optional[bool] = None
  page_signature: Optional[str] = None

  status: Optional[str] = None
  body: Optional[str] = None
  tls_version: Optional[int] = None
  tls_cipher_suite: Optional[int] = None
  tls_cert: Optional[str] = None
  tls_cert_common_name: Optional[str] = None
  tls_cert_issuer: Optional[str] = None
  tls_cert_start_date: Optional[str] = None
  tls_cert_end_date: Optional[str] = None
  tls_cert_alternative_names: List[str] = dataclasses.field(
      default_factory=list)
  headers: List[str] = dataclasses.field(default_factory=list)


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
class MatchesControl():
  """Class to keep track of which answer fields matched the control."""
  ip: Optional[bool] = None
  http: Optional[bool] = None
  cert: Optional[bool] = None
  asnum: Optional[bool] = None
  asname: Optional[bool] = None


@dataclass
class SatelliteAnswer():
  """Class for keeping track of Satellite answer content"""
  # Keys
  ip: str
  http: Optional[str] = None
  cert: Optional[str] = None
  matches_control: Optional[MatchesControl] = None
  match_confidence: Optional[float] = None

  ip_metadata: IpMetadata = dataclasses.field(default_factory=IpMetadata)
  http_response: Optional[HttpsResponse] = None
  http_error: Optional[str] = None
  https_response: Optional[HttpsResponse] = None
  https_error: Optional[str] = None


@dataclass
class SatelliteAnswerWithKeys(SatelliteAnswer):
  """Satellite Answer Metadata.

  When this metadata is being passed around
  it needs an additional date field to keep track of when it's valid.
  """
  date: str = ''


def merge_satellite_answers(base: SatelliteAnswer,
                            new: SatelliteAnswer) -> None:
  """Add tag information to a Satellite answer"""
  if new.http is not None:
    base.http = new.http
  if new.cert is not None:
    base.cert = new.cert
  if new.matches_control is not None:
    base.matches_control = new.matches_control
  if new.match_confidence is not None:
    base.match_confidence = new.match_confidence

  if new.http_response is not None:
    base.http_response = new.http_response
  if new.http_error is not None:
    base.http_error = new.http_error
  if new.https_response is not None:
    base.https_response = new.https_response
  if new.https_error is not None:
    base.https_error = new.https_error

  merge_ip_metadata(base.ip_metadata, new.ip_metadata)


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
  received: HttpsResponse = dataclasses.field(default_factory=HttpsResponse)
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


@dataclass
class PageFetchRow():
  """Class for fetched page specific fields"""
  received: HttpsResponse = dataclasses.field(default_factory=HttpsResponse)

  domain: Optional[str] = None
  ip: Optional[str] = None
  date: Optional[str] = None
  start_time: Optional[str] = None
  end_time: Optional[str] = None
  success: Optional[bool] = None
  https: Optional[bool] = None
  source: Optional[str] = None
  error: Optional[str] = None


def flatten_for_bigquery(
    row: Union[BigqueryRow, PageFetchRow]) -> Dict[str, Any]:
  if isinstance(row, HyperquackRow):
    return flatten_for_bigquery_hyperquack(row)
  if isinstance(row, SatelliteRow):
    return flatten_for_bigquery_satellite(row)
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
      'blockpage': row.received.is_known_blockpage,
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
    http_response = received_answer.http_response or HttpsResponse()
    https_response = received_answer.https_response or HttpsResponse()
    matches_control = received_answer.matches_control or MatchesControl()

    # yapf: disable
    answer: Dict[str, Any] = {
        'ip': received_answer.ip,
        'http': received_answer.http,
        'cert': received_answer.cert,
        'matches_control': {
          'ip': matches_control.ip,
          'http': matches_control.http,
          'cert': matches_control.cert,
          'asnum': matches_control.asnum,
          'asname': matches_control.asname,
        },
        # Ip Metadata
        'asnum': received_answer.ip_metadata.asn,
        'asname': received_answer.ip_metadata.as_name,
        # HTTP
        'http_error': received_answer.http_error,
        'http_response_status': http_response.status,
        'http_response_headers': http_response.headers,
        'http_response_body': http_response.body,
        'http_analysis_page_signature': http_response.page_signature,
        'http_analysis_is_known_blockpage': http_response.is_known_blockpage,
        # HTTPS
        'https_error': received_answer.https_error,
        'https_response_tls_version': https_response.tls_version,
        'https_response_tls_cipher_suite': https_response.tls_cipher_suite,
        'https_response_tls_cert': https_response.tls_cert,
        'https_response_tls_cert_common_name': https_response.tls_cert_common_name,
        'https_response_tls_cert_issuer': https_response.tls_cert_issuer,
        'https_response_tls_cert_start_date': https_response.tls_cert_start_date,
        'https_response_tls_cert_end_date': https_response.tls_cert_end_date,
        'https_response_tls_cert_alternative_names': https_response.tls_cert_alternative_names,
        'https_response_status': https_response.status,
        'https_response_headers': https_response.headers,
        'https_response_body': https_response.body,
        'https_analysis_page_signature': https_response.page_signature,
        'https_analysis_is_known_blockpage': https_response.is_known_blockpage,
    }
    # yapf: enable
    flat['received'].append(answer)
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
    BASE_BIGQUERY_SCHEMA,
    {
        'name': ('string', 'nullable'),
        'is_control_ip': ('boolean', 'nullable'),
        'received': (
            'record',
            'repeated',
            {
                'ip': ('string', 'nullable'),
                'asnum': ('integer', 'nullable'),
                'asname': ('string', 'nullable'),
                'http': ('string', 'nullable'),
                'cert': ('string', 'nullable'),
                'matches_control': ('record', 'nullable', {
                    'ip': ('boolean', 'nullable'),
                    'http': ('boolean', 'nullable'),
                    'cert': ('boolean', 'nullable'),
                    'asnum': ('boolean', 'nullable'),
                    'asname': ('boolean', 'nullable'),
                }),
                'match_confidence': ('float', 'nullable'),
                # HTTP
                'http_error': ('string', 'nullable'),
                'http_analysis_is_known_blockpage': ('boolean', 'nullable'),
                'http_analysis_page_signature': ('string', 'nullable'),
                'http_response_status': ('string', 'nullable'),
                'http_response_body': ('string', 'nullable'),
                'http_response_headers': ('string', 'repeated'),
                # HTTPS
                'https_error': ('string', 'nullable'),
                'https_analysis_is_known_blockpage': ('boolean', 'nullable'),
                'https_analysis_page_signature': ('string', 'nullable'),
                'https_response_status': ('string', 'nullable'),
                'https_response_body': ('string', 'nullable'),
                'https_response_headers': ('string', 'repeated'),
                'https_response_tls_version': ('integer', 'nullable'),
                'https_response_tls_cipher_suite': ('integer', 'nullable'),
                'https_response_tls_cert': ('string', 'nullable'),
                'https_response_tls_cert_common_name': ('string', 'nullable'),
                'https_response_tls_cert_issuer': ('string', 'nullable'),
                'https_response_tls_cert_start_date': ('timestamp', 'nullable'),
                'https_response_tls_cert_end_date': ('timestamp', 'nullable'),
                'https_response_tls_cert_alternative_names':
                    ('string', 'repeated'),
            }),
        'rcode': ('integer', 'nullable'),
        'average_confidence': ('float', 'nullable'),
        'untagged_controls': ('boolean', 'nullable'),
        'untagged_response': ('boolean', 'nullable'),
        'excluded': ('boolean', 'nullable'),
        'exclude_reason': ('string', 'nullable'),
        'has_type_a': ('boolean', 'nullable')
    })


def get_bigquery_schema(scan_type: str) -> Dict[str, Any]:
  """Get the appropriate schema for the given scan type.

  Args:
    scan_type: str, one of 'echo', 'discard', 'http', 'https',
      or 'satellite'

  Returns:
    A nested Dict with bigquery fields like BASE_BIGQUERY_SCHEMA.
  """
  if scan_type == SCAN_TYPE_SATELLITE:
    return SATELLITE_BIGQUERY_SCHEMA
  # Otherwise Hyperquack
  return HYPERQUACK_BIGQUERY_SCHEMA


def get_beam_bigquery_schema(
    fields: Dict[str, Any]) -> beam_bigquery.TableSchema:
  """Return a beam bigquery schema for the output table.

  Args:
    fields: dict of {'field_name': ['column_type', 'column_mode']}

  Returns:
    A bigquery table schema
  """
  table_schema = beam_bigquery.TableSchema()
  table_fields = _get_beam_bigquery_schema_list(fields)
  table_schema.fields = table_fields
  return table_schema


def _get_beam_bigquery_schema_list(
    fields: Dict[str, Any]) -> List[beam_bigquery.TableFieldSchema]:
  """A helper method for get_beam_bigquery_schema which returns a list of fields."""
  field_list: List[beam_bigquery.TableFieldSchema] = []

  for (name, attributes) in fields.items():
    field_type = attributes[0]
    mode = attributes[1]

    field_schema = beam_bigquery.TableFieldSchema()
    field_schema.name = name
    field_schema.type = field_type
    field_schema.mode = mode

    if field_type == 'record':
      substruct: Dict[str, Any] = attributes[2]
      subfields: List[
          beam_bigquery.TableFieldSchema] = _get_beam_bigquery_schema_list(
              substruct)
      field_schema.fields = subfields

    field_list.append(field_schema)
  return field_list
