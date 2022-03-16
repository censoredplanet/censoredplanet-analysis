"""Helpers for merging metadata with rows in beam pipelines"""

from __future__ import absolute_import

from copy import deepcopy
from typing import Tuple, Dict, List, Iterator, Union, Iterable

from pipeline.metadata.flatten_base import Row, SatelliteRow, IpMetadata, SatelliteAnswer, SatelliteAnswerMetadata

# A key containing a date and IP
# ex: ("2020-01-01", '1.2.3.4')
DateIpKey = Tuple[str, str]

# PCollection key names used internally by the beam pipeline
IP_METADATA_PCOLLECTION_NAME = 'metadata'
ROWS_PCOLLECION_NAME = 'rows'
RECEIVED_IPS_PCOLLECTION_NAME = 'received_ips'


def make_date_ip_key(
    tag: Union[IpMetadata, Row, SatelliteAnswerMetadata]) -> DateIpKey:
  """Makes a tuple key of the date and ip from a given row dict."""
  return (tag.date or '', tag.ip or '')


def _add_tag_to_answer(answer: SatelliteAnswer,
                       tag: SatelliteAnswerMetadata) -> None:
  if tag.asnum:
    answer.asnum = tag.asnum
  if tag.asname:
    answer.asname = tag.asname
  if tag.http:
    answer.http = tag.http
  if tag.cert:
    answer.cert = tag.cert
  if tag.matches_control:
    answer.matches_control = tag.matches_control


def _add_metadata_to_row(row: Row, metadata: IpMetadata) -> None:
  if metadata.netblock:
    row.netblock = metadata.netblock
  if metadata.asn:
    row.asn = metadata.asn
  if metadata.as_name:
    row.as_name = metadata.as_name
  if metadata.as_full_name:
    row.as_full_name = metadata.as_full_name
  if metadata.as_class:
    row.as_class = metadata.as_class
  if metadata.country:
    row.country = metadata.country
  if metadata.organization:
    row.organization = metadata.organization

  if isinstance(row, SatelliteRow) and metadata.name:
    row.name = metadata.name


def merge_metadata_with_rows(  # pylint: disable=unused-argument
    key: DateIpKey, value: Dict[str, Union[List[Row],
                                           List[IpMetadata]]]) -> Iterator[Row]:
  # pyformat: disable
  """Merge a list of rows with their corresponding metadata information.

  Args:
    key: The DateIpKey tuple that we joined on. This is thrown away.
    value: A two-element dict
      {IP_METADATA_PCOLLECTION_NAME: list (often one element) containing IpMetadata
               ROWS_PCOLLECION_NAME: Many element list containing Rows}
      where ipmetadata is a dict of the format {column_name, value}
       ('netblock': '1.0.0.1/24', 'asn': 13335, 'as_name': 'CLOUDFLARENET', ...)
      and row is a dict of the format {column_name, value}
       ('domain': 'test.com', 'ip': '1.1.1.1', 'success': true ...)
    rows_pcollection_name: default ROWS_PCOLLECION_NAME
      set if joining a pcollection with a different name

  Yields:
    row dict {column_name, value} containing both row and metadata cols/values
  """
  # pyformat: enable
  if value[IP_METADATA_PCOLLECTION_NAME]:
    ip_metadatas: List[IpMetadata] = value[
        IP_METADATA_PCOLLECTION_NAME]  # type: ignore
  else:
    ip_metadatas = []
  rows: List[Row] = value[ROWS_PCOLLECION_NAME]  # type: ignore

  for row in rows:
    new_row = deepcopy(row)
    for ip_metadata in ip_metadatas:
      _add_metadata_to_row(new_row, ip_metadata)

    yield new_row


def merge_satellite_tags_with_answers(  # pylint: disable=unused-argument
    key: DateIpKey,
    value: Dict[str, Union[List[SatelliteAnswer],
                           List[Tuple[str, SatelliteAnswerMetadata]]]]
) -> Iterator[Tuple[str, SatelliteAnswer]]:
  """
  Args:
    key: DateIp key, unused
    value:
      {RECEIVED_IPS_PCOLLECTION_NAME:
          list of Tuple[roundtrip_id, SatelliteAnswer]s without metadata
       IP_METADATA_PCOLLECTION_NAME:
           list of SatelliteAnswerMetadata with metadata
      }

  Yields:
    The Tuple[roundtrip_id, SatelliteAnswer]s with metadata added.
  """
  received_ips: List[Tuple[str, SatelliteAnswer]] = value[
      RECEIVED_IPS_PCOLLECTION_NAME]  # type: ignore
  tags: List[SatelliteAnswerMetadata] = value[
      IP_METADATA_PCOLLECTION_NAME]  # type: ignore

  for (roundtrip_id, answer) in received_ips:
    for tag in tags:
      _add_tag_to_answer(answer, tag)
    yield (roundtrip_id, answer)


def merge_tagged_answers_with_rows(
    key: str,  # pylint: disable=unused-argument
    value: Dict[str, Union[List[SatelliteRow],
                           List[List[Tuple[str, SatelliteAnswerMetadata]]]]]
) -> SatelliteRow:
  """
  Args:
    key: roundtrip_id, unused
    value:
      {ROWS_PCOLLECION_NAME: One element list containing a row
       RECEIVED_IPS_PCOLLECTION_NAME: One element list of many element
                             list containing tagged answer rows
      }
      ex:
        {ROWS_PCOLLECION_NAME: [{
            'ip': '1.2.3.4'
            'domain': 'ex.com'
            'received' : [{
                'ip': '4.5.6.7'
              }, {
                'ip': '5.6.7.8'
              },
            ]
          }],
         RECEIVED_IPS_PCOLLECTION_NAME: [[{
            'ip': '4.5.6.7'
            'asname': 'AMAZON-AES',
            'asnum': 14618,
          }, {
            'ip': '5.6.7.8'
            'asname': 'CLOUDFLARE',
            'asnum': 13335,
          }]]
        }

  Returns: The row with the tagged answers inserted
    {
      'ip': '1.2.3.4'
      'domain': 'ex.com'
      'received' : [{
          'ip': '4.5.6.7'
          'asname': 'AMAZON-AES',
          'asnum': 14618,
        }, {
          'ip': '5.6.7.8'
          'asname': 'CLOUDFLARE',
          'asnum': 13335,
        },
      ]
    }
  """
  row: SatelliteRow = value[ROWS_PCOLLECION_NAME][0]  # type: ignore

  if len(value[RECEIVED_IPS_PCOLLECTION_NAME]) == 0:  # No tags
    return row
  tagged_answers_with_ids: List[Tuple[str, SatelliteAnswerMetadata]] = value[
      RECEIVED_IPS_PCOLLECTION_NAME][0]  # type: ignore
  tagged_answers: Iterable[SatelliteAnswerMetadata] = list(
      map(lambda x: x[1], tagged_answers_with_ids))

  for untagged_answer in row.received:
    for tags in tagged_answers:
      if tags.ip == untagged_answer.ip:
        _add_tag_to_answer(untagged_answer, tags)
  return row
