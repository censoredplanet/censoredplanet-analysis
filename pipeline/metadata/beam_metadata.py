"""Helpers for merging metadata with rows in beam pipelines"""

from __future__ import absolute_import

from typing import Tuple, Dict, List, Iterator, Union, Iterable

from apache_beam.metrics.metric import Metrics

from pipeline.metadata.schema import BigqueryRow, SatelliteRow, PageFetchRow, IpMetadataWithDateKey, IpMetadataWithSourceKey, SatelliteAnswer, SatelliteAnswerWithSourceKey, SatelliteAnswerWithAnyKey, merge_ip_metadata, merge_satellite_answers
from pipeline.metadata.metrics import METRIC_NAMESPACE, NUM_ROWS_WITH_METADATA

# A key containing a date and IP
# ex: ('2020-01-01', '1.2.3.4')
DateIpKey = Tuple[str, str]

# A key containing a source and IP
# ex: ('CP_Satellite-2022-01-02-12-00-01', '1.2.3.4')
SourceIpKey = Tuple[str, str]

# A key containing a date and domain
# ex: ('2020-01-01', 'example.com')
DateDomainKey = Tuple[str, str]

# A key containing a source and domain
# ex: ('CP_Satellite-2022-01-02-12-00-01', 'example.com')
SourceDomainKey = Tuple[str, str]

# A key containing a domain, source and ip
# ex: ('example.com', 'CP_Satellite-2022-01-02-12-00-01', '1.2.3.4')
DomainSourceIpKey = Tuple[str, str, str]

# PCollection key names used internally by the beam pipeline
IP_METADATA_PCOLLECTION_NAME = 'metadata'
ROWS_PCOLLECION_NAME = 'rows'
RECEIVED_IPS_PCOLLECTION_NAME = 'received_ips'
BLOCKPAGE_PCOLLECTION_NAME = 'blockpage'


def make_date_ip_key(
    tag: Union[IpMetadataWithDateKey, BigqueryRow]) -> DateIpKey:
  """Makes a tuple key of the date and ip from a given row dict."""
  return (tag.date or '', tag.ip or '')


def make_source_ip_key(
    tag: Union[IpMetadataWithSourceKey, BigqueryRow,
               SatelliteAnswerWithSourceKey]
) -> SourceIpKey:
  """Makes a tuple key of the source and ip from a given row dict."""
  return (tag.source or '', tag.ip or '')


def make_domain_source_ip_key(row: PageFetchRow) -> DomainSourceIpKey:
  return (row.domain or '', row.source or '', row.ip or '')


def make_source_domain_key(row: SatelliteRow) -> SourceDomainKey:
  return (row.source or '', row.domain or '')


def merge_metadata_with_rows(  # pylint: disable=unused-argument
    key: DateIpKey,
    value: Dict[str,
                Union[List[BigqueryRow],
                      List[IpMetadataWithDateKey]]]) -> Iterator[BigqueryRow]:
  # pyformat: disable
  """Merge a list of rows with their corresponding metadata information.

  Args:
    key: The DateIpKey tuple that we joined on. This is thrown away.
    value: A two-element dict
      {IP_METADATA_PCOLLECTION_NAME: list (often one element) containing IpMetadataWithDateKey
               ROWS_PCOLLECION_NAME: Many element list containing Rows}
    rows_pcollection_name: default ROWS_PCOLLECION_NAME
      set if joining a pcollection with a different name

  Yields:
    Rows containing both row and metadata cols/values
  """
  # pyformat: enable
  if value[IP_METADATA_PCOLLECTION_NAME]:
    ip_metadatas: List[IpMetadataWithDateKey] = value[
        IP_METADATA_PCOLLECTION_NAME]  # type: ignore
  else:
    ip_metadatas = []
  rows: List[BigqueryRow] = value[ROWS_PCOLLECION_NAME]  # type: ignore

  for row in rows:
    for ip_metadata in ip_metadatas:
      merge_ip_metadata(row.ip_metadata, ip_metadata)
    Metrics.counter(METRIC_NAMESPACE, NUM_ROWS_WITH_METADATA).inc()
    yield row


def merge_satellite_tags_with_answers(  # pylint: disable=unused-argument
    key: SourceIpKey,
    value: Dict[str, Union[List[SatelliteAnswer],
                           List[Tuple[str, SatelliteAnswerWithSourceKey]]]]
) -> Iterator[Tuple[str, SatelliteAnswer]]:
  """
  Args:
    key: SourceIpKey key, unused
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
  tags: List[SatelliteAnswerWithSourceKey] = value[
      IP_METADATA_PCOLLECTION_NAME]  # type: ignore

  for (roundtrip_id, answer) in received_ips:
    for tag in tags:
      merge_satellite_answers(answer, tag)
    yield (roundtrip_id, answer)


def merge_satellite_metadata_with_answers(  # pylint: disable=unused-argument
    key: DateIpKey, value: Dict[str, Union[List[SatelliteAnswer],
                                           List[Tuple[str,
                                                      IpMetadataWithDateKey]]]]
) -> Iterator[Tuple[str, SatelliteAnswer]]:
  """
  Args:
    key: DateIpKey key, unused
    value:
      {RECEIVED_IPS_PCOLLECTION_NAME:
          list of Tuple[roundtrip_id, SatelliteAnswer]s without metadata
       IP_METADATA_PCOLLECTION_NAME: single IpMetadataWithDateKey
      }

  Yields:
    The Tuple[roundtrip_id, SatelliteAnswer]s with metadata added.
  """
  received_ips: List[Tuple[str, SatelliteAnswer]] = value[
      RECEIVED_IPS_PCOLLECTION_NAME]  # type: ignore
  ip_metadata: IpMetadataWithDateKey = value[IP_METADATA_PCOLLECTION_NAME][
      0]  # type: ignore

  for (roundtrip_id, answer) in received_ips:
    merge_ip_metadata(answer.ip_metadata, ip_metadata)
    yield (roundtrip_id, answer)


def merge_tagged_answers_with_rows(
    key: str,  # pylint: disable=unused-argument
    value: Dict[str, Union[List[SatelliteRow],
                           List[List[Tuple[str, SatelliteAnswerWithAnyKey]]]]]
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
  tagged_answers_with_ids: List[Tuple[str, SatelliteAnswerWithAnyKey]] = value[
      RECEIVED_IPS_PCOLLECTION_NAME][0]  # type: ignore
  tagged_answers: Iterable[SatelliteAnswerWithAnyKey] = list(
      map(lambda x: x[1], tagged_answers_with_ids))

  for untagged_answer in row.received:
    for tags in tagged_answers:
      if tags.ip == untagged_answer.ip:
        merge_satellite_answers(untagged_answer, tags)
  return row


def merge_page_fetches_with_answers(
    key: DomainSourceIpKey,  # pylint: disable=unused-argument
    value: Dict[str, Union[List[PageFetchRow],
                           List[Tuple[str, SatelliteAnswerWithSourceKey]]]]
) -> Iterator[Tuple[str, SatelliteAnswer]]:
  """Add fetched page info to answers."""
  answers: List[Tuple[str, SatelliteAnswer]] = value[
      RECEIVED_IPS_PCOLLECTION_NAME]  # type: ignore
  page_fetches: List[PageFetchRow] = value[
      BLOCKPAGE_PCOLLECTION_NAME]  # type: ignore

  if not page_fetches:
    for (roundtrip_id, answer) in answers:
      yield (roundtrip_id, answer)

  https_page_fetches = [
      page_fetch for page_fetch in page_fetches if page_fetch.https is True
  ]
  http_page_fetches = [
      page_fetch for page_fetch in page_fetches if page_fetch.https is False
  ]

  if len(https_page_fetches) > 1 or len(http_page_fetches) > 1:
    raise Exception(
        f"Unexpected blockpages. Expected <= 1 HTTPS and HTTP: {page_fetches}")

  for (roundtrip_id, answer) in answers:
    if https_page_fetches:
      answer.https_error = https_page_fetches[0].error
      answer.https_response = https_page_fetches[0].received
    if http_page_fetches:
      answer.http_error = http_page_fetches[0].error
      answer.http_response = http_page_fetches[0].received
    yield (roundtrip_id, answer)
