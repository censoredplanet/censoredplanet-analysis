"""Helpers for merging metadata with rows in beam pipelines"""

from __future__ import absolute_import

from copy import deepcopy
from typing import Tuple, Dict, List, Iterator, Union, Iterable

from pipeline.metadata.schema import BigqueryRow, SatelliteRow, IpMetadata, SatelliteAnswer, SatelliteAnswerMetadata, add_metadata_to_row, add_tags_to_answer

# A key containing a date and IP
# ex: ("2020-01-01", '1.2.3.4')
DateIpKey = Tuple[str, str]

# PCollection key names used internally by the beam pipeline
IP_METADATA_PCOLLECTION_NAME = 'metadata'
ROWS_PCOLLECION_NAME = 'rows'
RECEIVED_IPS_PCOLLECTION_NAME = 'received_ips'


def make_date_ip_key(
    tag: Union[IpMetadata, BigqueryRow, SatelliteAnswerMetadata]) -> DateIpKey:
  """Makes a tuple key of the date and ip from a given row dict."""
  return (tag.date or '', tag.ip or '')


def merge_metadata_with_rows(  # pylint: disable=unused-argument
    key: DateIpKey,
    value: Dict[str, Union[List[BigqueryRow],
                           List[IpMetadata]]]) -> Iterator[BigqueryRow]:
  # pyformat: disable
  """Merge a list of rows with their corresponding metadata information.

  Args:
    key: The DateIpKey tuple that we joined on. This is thrown away.
    value: A two-element dict
      {IP_METADATA_PCOLLECTION_NAME: list (often one element) containing IpMetadata
               ROWS_PCOLLECION_NAME: Many element list containing Rows}
    rows_pcollection_name: default ROWS_PCOLLECION_NAME
      set if joining a pcollection with a different name

  Yields:
    Rows containing both row and metadata cols/values
  """
  # pyformat: enable
  if value[IP_METADATA_PCOLLECTION_NAME]:
    ip_metadatas: List[IpMetadata] = value[
        IP_METADATA_PCOLLECTION_NAME]  # type: ignore
  else:
    ip_metadatas = []
  rows: List[BigqueryRow] = value[ROWS_PCOLLECION_NAME]  # type: ignore

  for row in rows:
    new_row = deepcopy(row)
    for ip_metadata in ip_metadatas:
      add_metadata_to_row(new_row, ip_metadata)

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
      add_tags_to_answer(answer, tag)
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
        {ROWS_PCOLLECION_NAME: [SatelliteRow(
            ip='1.2.3.4'
            domain='ex.com'
            date
            received=[SatelliteAnswer(
                'ip='4.5.6.7'
              }, SatelliteAnswer(
                'ip='5.6.7.8'
              },
            ]
          }],
         RECEIVED_IPS_PCOLLECTION_NAME: [[
           SatelliteAnswerMetadata(
            ip='4.5.6.7'
            asname='AMAZON-AES',
            asnum=14618,
          ), SatelliteAnswerMetadata(
            ip='5.6.7.8'
            asname='CLOUDFLARE',
            asnum=13335,
          )]]
        }

  Returns: The row with the tagged answers inserted
    SatelliteRow(
      ip='1.2.3.4'
      domain='ex.com'
      received=[SatelliteAnswer(
          ip='4.5.6.7'
          asname='AMAZON-AES',
          asnum=14618,
        ), SatelliteAnswer(
          ip='5.6.7.8'
          asname='CLOUDFLARE',
          asnum=13335,
        ),
      ]
    )
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
        add_tags_to_answer(untagged_answer, tags)
  return row
