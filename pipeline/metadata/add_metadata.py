"""Class for adding ip metadata to rows"""

from __future__ import absolute_import

import datetime
import logging
from typing import List, Tuple, Iterator, Iterable
import uuid

import apache_beam as beam

from pipeline.metadata.beam_metadata import DateIpKey, IP_METADATA_PCOLLECTION_NAME, ROWS_PCOLLECION_NAME, RECEIVED_IPS_PCOLLECTION_NAME, make_date_ip_key, merge_metadata_with_rows, merge_tagged_answers_with_rows, merge_satellite_metadata_with_answers
from pipeline.metadata.schema import BigqueryRow, IpMetadataWithDateKey, SatelliteRow, SatelliteAnswer, SatelliteAnswerWithDateKey
from pipeline.metadata.ip_metadata_chooser import IpMetadataChooserFactory


def set_random_roundtrip_id(row: SatelliteRow) -> Tuple[str, SatelliteRow]:
  """Add a roundtrip_id field to a row."""
  roundtrip_id = uuid.uuid4().hex
  return (roundtrip_id, row)


def _get_received_ips_with_roundtrip_id_and_date(
    row_with_id: Tuple[str, SatelliteRow]
) -> Iterable[Tuple[DateIpKey, Tuple[str, SatelliteAnswer]]]:
  """Get all the individual received ips answers from a row.

  Args:
    row_with_id Tuple[roundtrip_id, SatelliteRow]

  Yields individual recieved answers that also include the roundtrip and source
    ex:
      Tuple(
        Tuple('2022-01-02', '1.2.3.4')
        Tuple('abc' # roundtrip id
              SatelliteAnswer(ip: '1.2.3.4')))
      Tuple(
        Tuple('2022-01-02', '4.5.6.7')
        Tuple('abc' # roundtrip id
              SatelliteAnswer(ip: '4.5.6.7')))
  """
  (roundtrip_id, row) = row_with_id
  roundtrip_date = row.date or ''

  for answer in row.received:
    key: DateIpKey = (roundtrip_date, answer.ip)
    yield (key, (roundtrip_id, answer))


class MetadataAdder():
  """This class exists to provide access to the instantiated IpMetadata inside a beam operation."""

  def __init__(self,
               metadata_chooser_factory: IpMetadataChooserFactory) -> None:
    self.metadata_chooser_factory = metadata_chooser_factory

  def annotate_row_ip(
      self, rows: beam.pvalue.PCollection[BigqueryRow]
  ) -> beam.pvalue.PCollection[BigqueryRow]:
    """Add ip metadata to a collection of roundtrip rows.

    Args:
      rows: beam.PCollection[BigqueryRow]

    Returns:
      PCollection[BigqueryRow]
      The same rows as above with with additional metadata columns added.
    """

    # PCollection[Tuple[DateIpKey,BigqueryRow]]
    rows_keyed_by_ip_and_date = (
        rows | 'key by ips and dates' >>
        beam.Map(lambda row: (make_date_ip_key(row), row)).with_output_types(
            Tuple[DateIpKey, BigqueryRow]))

    # PCollection[DateIpKey]
    # pylint: disable=no-value-for-parameter
    ips_and_dates = (
        rows_keyed_by_ip_and_date | 'get ip and date keys per row' >>
        beam.Keys().with_output_types(DateIpKey))

    # PCollection[DateIpKey]
    deduped_ips_and_dates = (
        # pylint: disable=no-value-for-parameter
        ips_and_dates | 'dedup' >> beam.Distinct().with_output_types(DateIpKey))

    # PCollection[Tuple[date,List[ip]]]
    grouped_ips_by_dates = (
        deduped_ips_and_dates | 'group by date' >>
        beam.GroupByKey().with_output_types(Tuple[str, Iterable[str]]))

    # PCollection[Tuple[DateIpKey,IpMetadataWithDateKey]]
    ips_with_metadata = (
        grouped_ips_by_dates | 'get ip metadata' >> beam.FlatMapTuple(
            self._annotate_ips).with_output_types(Tuple[DateIpKey,
                                                        IpMetadataWithDateKey]))

    # PCollection[Tuple[Tuple[date,ip],Dict[input_name_key,List[BigqueryRow|IpMetadataWithDateKey]]]]
    grouped_metadata_and_rows = (({
        IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
        ROWS_PCOLLECION_NAME: rows_keyed_by_ip_and_date
    }) | 'group by keys' >> beam.CoGroupByKey())

    # PCollection[BigqueryRow]
    rows_with_metadata = (
        grouped_metadata_and_rows | 'merge metadata with rows' >>
        beam.FlatMapTuple(merge_metadata_with_rows))
    # We had to remove the beam type hint here
    # So subsequent operations can take either HyperquackRow or SatelliteRow

    return rows_with_metadata

  def annotate_answer_ips(
      self, rows: beam.pvalue.PCollection[SatelliteRow]
  ) -> beam.pvalue.PCollection[SatelliteRow]:
    """Add ip metadata info to received ip lists in rows

    Args:
      rows: PCollection of SatelliteRows

    Returns:
      SatelliteRows with routeview metadata attatched to answer ips.
    """
    # PCollection[Tuple[roundtrip_id, SatelliteRow]]
    rows_with_roundtrip_id = (
        rows | 'add roundtrip_ids: answer metadata' >>
        beam.Map(set_random_roundtrip_id).with_output_types(
            Tuple[str, SatelliteRow]))

    # PCollection[Tuple[DateIpKey, Tuple[roundtrip_id, SatelliteAnswer]]]
    received_ips_keyed_by_ip_and_date = (
        rows_with_roundtrip_id |
        'get received ips: answer metadata' >> beam.FlatMap(
            _get_received_ips_with_roundtrip_id_and_date).with_output_types(
                Tuple[DateIpKey, Tuple[str, SatelliteAnswer]]))

    #PCollection[DateIpKey]
    # pylint: disable=no-value-for-parameter
    ips_and_dates = (
        received_ips_keyed_by_ip_and_date | 'get ip and date keys per answer' >>
        beam.Keys().with_output_types(DateIpKey))

    # PCollection[DateIpKey]
    deduped_ips_and_dates = (
        # pylint: disable=no-value-for-parameter
        ips_and_dates |
        'dedup answer ips' >> beam.Distinct().with_output_types(DateIpKey))

    # PCollection[Tuple[date,List[ip]]]
    grouped_ips_by_dates = (
        deduped_ips_and_dates | 'group answer ips by date' >>
        beam.GroupByKey().with_output_types(Tuple[str, Iterable[str]]))

    # PCollection[Tuple[DateIpKey,IpMetadataWithDateKey]]
    ips_with_metadata = (
        grouped_ips_by_dates | 'get ip metadata for answers' >>
        beam.FlatMapTuple(self._annotate_ips).with_output_types(
            Tuple[DateIpKey, IpMetadataWithDateKey]))

    grouped_metadata_and_received_ips = (({
        IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
        RECEIVED_IPS_PCOLLECTION_NAME: received_ips_keyed_by_ip_and_date
    }) | 'add received ip metadata: group by keys' >> beam.CoGroupByKey())

    # PCollection[Tuple[roundtrip_id, SatelliteAnswerWithDateKey]]
    received_ips_with_metadata = (
        grouped_metadata_and_received_ips |
        'add received ip metadata: merge tags with answers' >> beam.
        FlatMapTuple(merge_satellite_metadata_with_answers).with_output_types(
            Tuple[str, SatelliteAnswerWithDateKey]))

    # PCollection[Tuple[roundtrip_id, List[Tuple[roundtrip_id, SatelliteAnswerWithDateKey]]]]
    received_ips_grouped_by_roundtrip_ip = (
        received_ips_with_metadata |
        'group received answers by roundtrip: answer metadata' >>
        beam.GroupBy(lambda kv: kv[0]).with_output_types(
            Tuple[str, Iterable[Tuple[str, SatelliteAnswerWithDateKey]]]))

    grouped_rows_and_received_ips = (({
        ROWS_PCOLLECION_NAME: rows_with_roundtrip_id,
        RECEIVED_IPS_PCOLLECTION_NAME: received_ips_grouped_by_roundtrip_ip
    }) | 'add received ip metadata: group by roundtrip' >> beam.CoGroupByKey())

    # PCollection[SatelliteRow] received ip row with roundtrip id
    rows_with_metadata = (
        grouped_rows_and_received_ips |
        'add received ip metadata: add tagged answers back' >> beam.MapTuple(
            merge_tagged_answers_with_rows).with_output_types(SatelliteRow))

    return rows_with_metadata

  def _annotate_ips(
      self, date: str,
      ips: List[str]) -> Iterator[Tuple[DateIpKey, IpMetadataWithDateKey]]:
    """Add Autonymous System metadata for ips in the given rows.

    Args:
      date: a 'YYYY-MM-DD' date key
      ips: a list of ips

    Yields:
      Tuples (DateIpKey, IpMetadataWithDateKey)
    """
    logging.info(
        f'Instantiating metadata tables for {date} to classify {len(ips)} IPs')

    ip_metadata_chooser = self.metadata_chooser_factory.make_chooser(
        datetime.date.fromisoformat(date))

    for ip in ips:
      metadata_key = (date, ip)
      metadata_values = ip_metadata_chooser.get_metadata(ip)

      yield (metadata_key, metadata_values)
