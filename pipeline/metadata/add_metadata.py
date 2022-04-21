"""Class for adding ip metadata to rows"""

from __future__ import absolute_import

import datetime
from typing import List, Tuple, Iterator, Iterable

import apache_beam as beam

from pipeline.metadata.beam_metadata import DateIpKey, IP_METADATA_PCOLLECTION_NAME, ROWS_PCOLLECION_NAME, make_date_ip_key, merge_metadata_with_rows
from pipeline.metadata.schema import BigqueryRow, IpMetadataWithKeys
from pipeline.metadata.ip_metadata_chooser import IpMetadataChooserFactory


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

    # PCollection[Tuple[DateIpKey,IpMetadataWithKeys]]
    ips_with_metadata = (
        grouped_ips_by_dates | 'get ip metadata' >> beam.FlatMapTuple(
            self._annotate_ips).with_output_types(Tuple[DateIpKey,
                                                        IpMetadataWithKeys]))

    # PCollection[Tuple[Tuple[date,ip],Dict[input_name_key,List[BigqueryRow|IpMetadataWithKeys]]]]
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

  def _annotate_ips(
      self, date: str,
      ips: List[str]) -> Iterator[Tuple[DateIpKey, IpMetadataWithKeys]]:
    """Add Autonymous System metadata for ips in the given rows.

    Args:
      date: a 'YYYY-MM-DD' date key
      ips: a list of ips

    Yields:
      Tuples (DateIpKey, IpMetadataWithKeys)
    """
    ip_metadata_chooser = self.metadata_chooser_factory.make_chooser(
        datetime.date.fromisoformat(date))

    for ip in ips:
      metadata_key = (date, ip)
      metadata_values = ip_metadata_chooser.get_metadata(ip)

      yield (metadata_key, metadata_values)
