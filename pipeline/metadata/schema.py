"""Schema for various data types passed around in the pipeline and bigquery"""

from __future__ import absolute_import
from __future__ import annotations  # required to use class as a type inside the class

import dataclasses
import json
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union, NamedTuple

from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam import coders

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
  tls_cert_has_trusted_ca: Optional[bool] = None
  tls_cert_matches_domain: Optional[bool] = None
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
  # Satellite metadata related to resolver aggregate statistics
  non_zero_rcode_rate: Optional[float] = None
  private_ip_rate: Optional[float] = None
  zero_ip_rate: Optional[float] = None
  connect_error_rate: Optional[float] = None
  invalid_cert_rate: Optional[float] = None


@dataclass
class IpMetadataWithSourceKey(IpMetadata):
  """Extension of IpMetadata with ip and source keys."""
  # Keys
  ip: str = ''
  source: str = ''


@dataclass
class IpMetadataWithDateKey(IpMetadata):
  """Extension of IpMetadata with ip and date keys."""
  # Keys
  ip: str = ''
  date: str = ''


def merge_ip_metadata(base: IpMetadata, new: IpMetadata) -> None:
  """Merge metadata info into an existing metadata."""
  # pylint: disable=too-many-branches
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
  # Satellite resolver metadata
  if new.name is not None:
    base.name = new.name
  if new.non_zero_rcode_rate is not None:
    base.non_zero_rcode_rate = new.non_zero_rcode_rate
  if new.private_ip_rate is not None:
    base.private_ip_rate = new.private_ip_rate
  if new.zero_ip_rate is not None:
    base.zero_ip_rate = new.zero_ip_rate
  if new.connect_error_rate is not None:
    base.connect_error_rate = new.connect_error_rate
  if new.invalid_cert_rate is not None:
    base.invalid_cert_rate = new.invalid_cert_rate
  # pylint: enable=too-many-branches


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
class SatelliteAnswerWithSourceKey(SatelliteAnswer):
  """Satellite Answer Metadata.

  When this metadata is being passed around
  it needs an additional source field to keep track of when it's valid.
  """
  source: str = ''


@dataclass
class SatelliteAnswerWithDateKey(SatelliteAnswer):
  """Satellite Answer Metadata.

  When this metadata is being passed around
  it needs an additional date field to keep track of when it's valid.
  """
  date: str = ''


# Some operations can take either kind of key
SatelliteAnswerWithAnyKey = Union[SatelliteAnswerWithSourceKey,
                                  SatelliteAnswerWithDateKey]


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
  retry: Optional[int] = None
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
  outcome: Optional[str] = None


BigqueryInputRow = NamedTuple(
  'BigqueryInputRow',
  [('domain', str),
   ('category', str),
   ('ip', str),
   ('ddate', str),
   ('state_time', str),
   ('end_time', str),
   ('retry', int),
   ('error', str),
   ('anomaly', bool),
   ('success', bool),
   ('is_control', bool),
   ('controls_failed', bool),
   ('stateful_block', bool),
   ('measurement_id', str),
   ('source', str),
   ('outcome', str)
   ]
  )
coders.registry.register_coder(BigqueryInputRow, coders.RowCoder)


def convert_byperquack_row_to_bq_row_format(row: HyperquackRow) -> BigqueryInputRow:
  return BigqueryInputRow(
    row.domain, 
    row.category,
    row.ip,
    row.date,
    row.start_time,
    row.end_time,
    row.retry,
    row.error,
    row.anomaly,
    row.success,
    row.is_control,
    row.controls_failed,
    row.stateful_block,
    row.measurement_id,
    row.source,
    row.outcome
  )



BigqueryOutputRow = NamedTuple(
  'BigqueryOutputRow',
  [('ddate', str),
   ('source', str),
   ('country_name', str),
   ('network', str),
   ('domain', str),
   ('outcome', str),
   ('subnetwork', str),
   ('category', str),
   ('count', int),
   ('unexpected_count', int)
   ]
  )
coders.registry.register_coder(BigqueryOutputRow, coders.RowCoder)



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


@dataclass
class DashboardRow():
  """A row in the dashboard table"""
  date: Optional[str] = None
  source: Optional[str] = None
  country_name: Optional[str] = None
  network: Optional[str] = None
  subnetwork: Optional[str] = None
  domain: Optional[str] = None
  domain_category: Optional[str] = None
  outcome: Optional[str] = None
  unexpected_count: Optional[int] = None
  count: Optional[int] = None


def flatten_to_dict(row: Union[BigqueryRow, PageFetchRow]) -> Dict[str, Any]:
  if isinstance(row, HyperquackRow):
    return flatten_to_dict_hyperquack(row)
  if isinstance(row, SatelliteRow):
    return flatten_to_dict_satellite(row)
  raise Exception(f'Unknown row type: {type(row)}')


# yapf: disable
def flatten_to_dict_hyperquack(row: HyperquackRow) -> Dict[str, Any]:
  """Convert a structured hyperquack dataclass into a flat dict."""
  flat: Dict[str, Any] = {
      'domain': row.domain,
      'domain_category': row.category,
      'domain_is_control': row.is_control,
      'date': row.date,
      'start_time': row.start_time,
      'end_time': row.end_time,
      'retry': row.retry,
      'server_ip': row.ip,
      'server_netblock': row.ip_metadata.netblock,
      'server_asn': row.ip_metadata.asn,
      'server_as_name': row.ip_metadata.as_name,
      'server_as_full_name': row.ip_metadata.as_full_name,
      'server_as_class': row.ip_metadata.as_class,
      'server_country': row.ip_metadata.country,
      'server_organization': row.ip_metadata.organization,
      'received_error': row.error,
      'received_tls_version': row.received.tls_version,
      'received_tls_cipher_suite': row.received.tls_cipher_suite,
      'received_tls_cert': row.received.tls_cert,
      'received_tls_cert_matches_domain': row.received.tls_cert_matches_domain,
      'received_tls_cert_common_name': row.received.tls_cert_common_name,
      'received_tls_cert_issuer': row.received.tls_cert_issuer,
      'received_tls_cert_alternative_names': row.received.tls_cert_alternative_names,
      'received_status': row.received.status,
      'received_headers': row.received.headers,
      'received_body': row.received.body,
      'is_known_blockpage': row.received.is_known_blockpage,
      'page_signature': row.received.page_signature,
      'outcome': row.outcome,
      'matches_template': row.success,
      'no_response_in_measurement_matches_template': row.anomaly,
      'controls_failed': row.controls_failed,
      'stateful_block': row.stateful_block,
      'measurement_id': row.measurement_id,
      'source': row.source,
  }
  return flat
# yapf: enable


def flatten_to_dict_satellite(row: SatelliteRow) -> Dict[str, Any]:
  """Convert a structured satellite dataclass into a flat dict."""
  flat: Dict[str, Any] = {
      'domain': row.domain,
      'domain_category': row.category,
      'domain_is_control': row.is_control,
      'date': row.date,
      'start_time': row.start_time,
      'end_time': row.end_time,
      'retry': row.retry,
      'resolver_ip': row.ip,
      'resolver_name': row.ip_metadata.name,
      'resolver_is_trusted': row.is_control_ip,
      'resolver_netblock': row.ip_metadata.netblock,
      'resolver_asn': row.ip_metadata.asn,
      'resolver_as_name': row.ip_metadata.as_name,
      'resolver_as_full_name': row.ip_metadata.as_full_name,
      'resolver_as_class': row.ip_metadata.as_class,
      'resolver_country': row.ip_metadata.country,
      'resolver_organization': row.ip_metadata.organization,
      'resolver_non_zero_rcode_rate': row.ip_metadata.non_zero_rcode_rate,
      'resolver_private_ip_rate': row.ip_metadata.private_ip_rate,
      'resolver_zero_ip_rate': row.ip_metadata.zero_ip_rate,
      'resolver_connect_error_rate': row.ip_metadata.connect_error_rate,
      'resolver_invalid_cert_rate': row.ip_metadata.invalid_cert_rate,
      'received_error': row.error,
      'received_rcode': row.rcode,
      'answers': [],
      'success': row.success,
      'anomaly': row.anomaly,
      'domain_controls_failed': row.controls_failed,
      'average_confidence': row.average_confidence,
      'untagged_controls': row.untagged_controls,
      'untagged_response': row.untagged_response,
      'excluded': row.excluded,
      'exclude_reason': row.exclude_reason,
      'has_type_a': row.has_type_a,
      'measurement_id': row.measurement_id,
      'source': row.source,
  }

  for received_answer in row.received:
    http_response = received_answer.http_response or HttpsResponse()
    https_response = received_answer.https_response or HttpsResponse()
    matches_control = received_answer.matches_control or MatchesControl()

    # yapf: disable
    answer: Dict[str, Any] = {
        'ip': received_answer.ip,
        # Ip Metadata
        'asn': received_answer.ip_metadata.asn,
        'as_name': received_answer.ip_metadata.as_name,
        'ip_organization': received_answer.ip_metadata.organization,
        'censys_http_body_hash': received_answer.http,
        'censys_ip_cert': received_answer.cert,
        'matches_control': {
          'ip': matches_control.ip,
          'censys_http_body_hash': matches_control.http,
          'censys_ip_cert': matches_control.cert,
          'asn': matches_control.asnum,
          'as_name': matches_control.asname,
        },
        # HTTP
        'http_error': received_answer.http_error,
        'http_response_status': http_response.status,
        'http_response_headers': http_response.headers,
        'http_response_body': http_response.body,
        'http_analysis_is_known_blockpage': http_response.is_known_blockpage,
        'http_analysis_page_signature': http_response.page_signature,
        # HTTPS
        'https_error': received_answer.https_error,
        'https_tls_version': https_response.tls_version,
        'https_tls_cipher_suite': https_response.tls_cipher_suite,
        'https_tls_cert': https_response.tls_cert,
        'https_tls_cert_common_name': https_response.tls_cert_common_name,
        'https_tls_cert_issuer': https_response.tls_cert_issuer,
        'https_tls_cert_start_date': https_response.tls_cert_start_date,
        'https_tls_cert_end_date': https_response.tls_cert_end_date,
        'https_tls_cert_alternative_names': https_response.tls_cert_alternative_names,
        'https_tls_cert_has_trusted_ca': https_response.tls_cert_has_trusted_ca,
        'https_tls_cert_matches_domain': https_response.tls_cert_matches_domain,
        'https_response_status': https_response.status,
        'https_response_headers': https_response.headers,
        'https_response_body': https_response.body,
        'https_analysis_is_known_blockpage': https_response.is_known_blockpage,
        'https_analysis_page_signature': https_response.page_signature,
    }
    # yapf: enable
    flat['answers'].append(answer)
  return flat


def dict_to_gcs_json_string(measurement_dict: Dict[str, Any]) -> str:
  """Convert dict of measurement data to json string with selected GCS fields."""
  if 'Quack' in measurement_dict['source']:
    return json.dumps(dict_to_gcs_dict_hyperquack(measurement_dict))
  if 'Satellite' in measurement_dict['source']:
    return json.dumps(dict_to_gcs_dict_satellite(measurement_dict))
  raise Exception(f'Unknown dict source: {measurement_dict["source"]}')


def dict_to_gcs_dict_hyperquack(
    measurement_dict: Dict[str, Any]) -> Dict[str, Any]:
  """Update dict of Hyperquack data to contain only selected GCS fields."""
  measurement_dict.pop('domain_category')
  measurement_dict.pop('is_known_blockpage')
  measurement_dict.pop('page_signature')
  measurement_dict.pop('outcome')
  measurement_dict.pop('received_tls_cert_matches_domain')
  return measurement_dict


def dict_to_gcs_dict_satellite(
    measurement_dict: Dict[str, Any]) -> Dict[str, Any]:
  """Update dict of Satellite data to contain only selected GCS fields."""
  measurement_dict.pop('domain_category')
  measurement_dict.pop('success')
  measurement_dict.pop('anomaly')
  measurement_dict.pop('domain_controls_failed')
  measurement_dict.pop('average_confidence')
  measurement_dict.pop('untagged_controls')
  measurement_dict.pop('untagged_response')
  measurement_dict.pop('excluded')
  measurement_dict.pop('exclude_reason')
  measurement_dict.pop('has_type_a')
  measurement_dict.pop('resolver_non_zero_rcode_rate')
  measurement_dict.pop('resolver_private_ip_rate')
  measurement_dict.pop('resolver_zero_ip_rate')
  measurement_dict.pop('resolver_connect_error_rate')
  measurement_dict.pop('resolver_invalid_cert_rate')
  for i in range(0, len(measurement_dict['answers'])):
    measurement_dict['answers'][i].pop('matches_control')
    measurement_dict['answers'][i].pop('match_confidence', None)
    measurement_dict['answers'][i].pop('http_analysis_is_known_blockpage')
    measurement_dict['answers'][i].pop('http_analysis_page_signature')
    measurement_dict['answers'][i].pop('https_tls_cert_has_trusted_ca')
    measurement_dict['answers'][i].pop('https_tls_cert_matches_domain')
    measurement_dict['answers'][i].pop('https_analysis_is_known_blockpage')
    measurement_dict['answers'][i].pop('https_analysis_page_signature')
  return measurement_dict


HYPERQUACK_BIGQUERY_SCHEMA = {
    #Domain
    'domain': ('string', 'nullable'),
    'domain_category': ('string', 'nullable'),
    'domain_is_control': ('boolean', 'nullable'),

    # Time
    'date': ('date', 'nullable'),
    'start_time': ('timestamp', 'nullable'),
    'end_time': ('timestamp', 'nullable'),
    'retry': ('integer', 'nullable'),

    # Resolver fields
    'server_ip': ('string', 'nullable'),
    # Columns added from CAIDA data
    'server_netblock': ('string', 'nullable'),
    'server_asn': ('integer', 'nullable'),
    'server_as_name': ('string', 'nullable'),
    'server_as_full_name': ('string', 'nullable'),
    'server_as_class': ('string', 'nullable'),
    'server_country': ('string', 'nullable'),
    # Columns from DBIP
    'server_organization': ('string', 'nullable'),

    # Received data
    'received_error': ('string', 'nullable'),
    # Columns filled only in HTTPS tables
    'received_tls_version': ('integer', 'nullable'),
    'received_tls_cipher_suite': ('integer', 'nullable'),
    'received_tls_cert': ('bytes', 'nullable'),
    'received_tls_cert_matches_domain': ('boolean', 'nullable'),
    'received_tls_cert_common_name': ('string', 'nullable'),
    'received_tls_cert_issuer': ('string', 'nullable'),
    'received_tls_cert_alternative_names': ('string', 'repeated'),

    # Column filled in all tables
    'received_status': ('string', 'nullable'),
    'received_headers': ('string', 'repeated'),
    'received_body': ('string', 'nullable'),

    # Blockpages
    'is_known_blockpage': ('boolean', 'nullable'),
    'page_signature': ('string', 'nullable'),

    # Analysis
    'outcome': ('string', 'nullable'),
    'matches_template': ('boolean', 'nullable'),
    'no_response_in_measurement_matches_template': ('boolean', 'nullable'),
    'controls_failed': ('boolean', 'nullable'),
    'stateful_block': ('boolean', 'nullable'),

    # Internal
    'measurement_id': ('string', 'nullable'),
    'source': ('string', 'nullable'),
}

SATELLITE_BIGQUERY_SCHEMA = {
    # Domain fields
    'domain': ('string', 'nullable'),
    'domain_category': ('string', 'nullable'),
    'domain_is_control': ('boolean', 'nullable'),

    # Time
    'date': ('date', 'nullable'),
    'start_time': ('timestamp', 'nullable'),
    'end_time': ('timestamp', 'nullable'),
    'retry': ('integer', 'nullable'),

    # Resolver fields
    'resolver_ip': ('string', 'nullable'),
    'resolver_name': ('string', 'nullable'),
    'resolver_is_trusted': ('boolean', 'nullable'),
    # Columns added from CAIDA data
    'resolver_netblock': ('string', 'nullable'),
    'resolver_asn': ('integer', 'nullable'),
    'resolver_as_name': ('string', 'nullable'),
    'resolver_as_full_name': ('string', 'nullable'),
    'resolver_as_class': ('string', 'nullable'),
    'resolver_country': ('string', 'nullable'),
    # Columns from DBIP
    'resolver_organization': ('string', 'nullable'),
    # Columns from resolver profile
    'resolver_non_zero_rcode_rate': ('float', 'nullable'),
    'resolver_private_ip_rate': ('float', 'nullable'),
    'resolver_zero_ip_rate': ('float', 'nullable'),
    'resolver_connect_error_rate': ('float', 'nullable'),
    'resolver_invalid_cert_rate': ('float', 'nullable'),

    # Observation
    'received_error': ('string', 'nullable'),
    'received_rcode': ('integer', 'nullable'),
    'answers': (
        'record',
        'repeated',
        {
            'ip': ('string', 'nullable'),
            'asn': ('integer', 'nullable'),
            'as_name': ('string', 'nullable'),
            'ip_organization': ('string', 'nullable'),
            'censys_http_body_hash': ('string', 'nullable'),
            'censys_ip_cert': ('string', 'nullable'),
            'matches_control': ('record', 'nullable', {
                'ip': ('boolean', 'nullable'),
                'censys_http_body_hash': ('boolean', 'nullable'),
                'censys_ip_cert': ('boolean', 'nullable'),
                'asn': ('boolean', 'nullable'),
                'as_name': ('boolean', 'nullable'),
            }),
            'match_confidence': ('float', 'nullable'),
            # HTTP
            'http_error': ('string', 'nullable'),
            'http_response_status': ('string', 'nullable'),
            'http_response_headers': ('string', 'repeated'),
            'http_response_body': ('string', 'nullable'),
            'http_analysis_is_known_blockpage': ('boolean', 'nullable'),
            'http_analysis_page_signature': ('string', 'nullable'),
            # HTTPS
            'https_error': ('string', 'nullable'),
            'https_tls_version': ('integer', 'nullable'),
            'https_tls_cipher_suite': ('integer', 'nullable'),
            'https_tls_cert': ('bytes', 'nullable'),
            'https_tls_cert_common_name': ('string', 'nullable'),
            'https_tls_cert_issuer': ('string', 'nullable'),
            'https_tls_cert_start_date': ('timestamp', 'nullable'),
            'https_tls_cert_end_date': ('timestamp', 'nullable'),
            'https_tls_cert_alternative_names': ('string', 'repeated'),
            'https_tls_cert_has_trusted_ca': ('boolean', 'nullable'),
            'https_tls_cert_matches_domain': ('boolean', 'nullable'),
            'https_response_status': ('string', 'nullable'),
            'https_response_headers': ('string', 'repeated'),
            'https_response_body': ('string', 'nullable'),
            'https_analysis_is_known_blockpage': ('boolean', 'nullable'),
            'https_analysis_page_signature': ('string', 'nullable'),
        }),

    # Analysis
    'success': ('boolean', 'nullable'),
    'anomaly': ('boolean', 'nullable'),
    'domain_controls_failed': ('boolean', 'nullable'),
    'average_confidence': ('float', 'nullable'),
    'untagged_controls': ('boolean', 'nullable'),
    'untagged_response': ('boolean', 'nullable'),
    'excluded': ('boolean', 'nullable'),
    'exclude_reason': ('string', 'nullable'),
    'has_type_a': ('boolean', 'nullable'),

    # Internal
    'measurement_id': ('string', 'nullable'),
    'source': ('string', 'nullable')
}


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
