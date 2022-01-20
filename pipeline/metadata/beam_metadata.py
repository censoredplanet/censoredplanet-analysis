"""Helpers for merging metadata with rows in beam pipelines"""

from __future__ import absolute_import

import datetime
from typing import Tuple, Dict, List, Iterator, Iterable, Optional

import apache_beam as beam

from pipeline.metadata.flatten import Row
from pipeline.metadata.ip_metadata_chooser import IpMetadataChooserFactory, IpMetadataChooser

# A key containing a date and IP
# ex: ("2020-01-01", '1.2.3.4')
DateIpKey = Tuple[str, str]

# PCollection key names used internally by the beam pipeline
IP_METADATA_PCOLLECTION_NAME = 'metadata'
ROWS_PCOLLECION_NAME = 'rows'

EARLIEST_DATA_DATE = datetime.date(2018, 7, 3)


def make_date_ip_key(row: Row) -> DateIpKey:
  """Makes a tuple key of the date and ip from a given row dict."""
  return (row['date'], row['ip'])


def get_all_dates() -> List[datetime.date]:
  """Get all date objects between the start date of the data and now."""
  delta = (datetime.date.today() - EARLIEST_DATA_DATE)

  days = []
  for i in range(delta.days + 1):
    day = EARLIEST_DATA_DATE + datetime.timedelta(days=i)
    days.append(day)

  return days


def merge_metadata_with_rows(  # pylint: disable=unused-argument
    key: DateIpKey,
    value: Dict[str, List[Row]],
    field: str = None) -> Iterator[Row]:
  # pyformat: disable
  """Merge a list of rows with their corresponding metadata information.

  Args:
    key: The DateIpKey tuple that we joined on. This is thrown away.
    value: A two-element dict
      {IP_METADATA_PCOLLECTION_NAME: One element list containing an ipmetadata
               ROWS_PCOLLECION_NAME: Many element list containing row dicts}
      where ipmetadata is a dict of the format {column_name, value}
       {'netblock': '1.0.0.1/24', 'asn': 13335, 'as_name': 'CLOUDFLARENET', ...}
      and row is a dict of the format {column_name, value}
       {'domain': 'test.com', 'ip': '1.1.1.1', 'success': true ...}
    field: indicates a row field to update with metadata instead of the row (default).

  Yields:
    row dict {column_name, value} containing both row and metadata cols/values
  """
  # pyformat: enable
  if value[IP_METADATA_PCOLLECTION_NAME]:
    ip_metadata = value[IP_METADATA_PCOLLECTION_NAME][0]
  else:
    ip_metadata = {}
  rows = value[ROWS_PCOLLECION_NAME]

  for row in rows:
    new_row: Row = {}
    new_row.update(row)
    if field == 'received':
      if new_row['received']:
        new_row['received'].update(ip_metadata)
        new_row['received'].pop('date', None)
        new_row['received'].pop('name', None)
        new_row['received'].pop('country', None)
    else:
      new_row.update(ip_metadata)
    yield new_row


class MetadataAdder(beam.DoFn):
  """Class to add Metadata to rows in a parallelizable way.

  Each row in the PCollection processed by a single MetadataAdder
  must have the same date value, since the IpMetadataChooser will
  be shared between process calls.
  """

  def __init__(self,
               ip_metadata_chooser_factory: IpMetadataChooserFactory) -> None:
    super().__init__()
    self.ip_metadata_chooser_factory = ip_metadata_chooser_factory
    # This can't be created on init because we don't know the right date
    self.ip_metadata_chooser: Optional[IpMetadataChooser] = None

  def _init_metadata_chooser(self, date: datetime.date) -> None:
    #pylint: disable=attribute-defined-outside-init
    self.ip_metadata_chooser = self.ip_metadata_chooser_factory.make_chooser(
        date)
    #pylint: enable=attribute-defined-outside-init

  def process(self, element: Row) -> Iterable[Row]:
    """Add ip metadata to a row."""

    if not self.ip_metadata_chooser:
      self._init_metadata_chooser(datetime.date.fromisoformat(element['date']))

    if not self.ip_metadata_chooser:
      raise Exception("Failed to initialize IPMetadataChooser correctly")

    metadata = self.ip_metadata_chooser.get_metadata(element['ip'])
    element.update(metadata)
    yield element


def _get_date_partition(row: Row, _: int) -> int:
  all_partition_dates = get_all_dates()
  row_date = datetime.date.fromisoformat(row['date'])
  return all_partition_dates.index(row_date)


def add_ip_metadata(
    rows: beam.pvalue.PCollection[Row],
    ip_metadata_chooser_factory: IpMetadataChooserFactory
) -> beam.pvalue.PCollection[Row]:
  """Add IpMetadata to a PCollection of Rows.

  Args:
    Rows
    ip_metadata_chooser_factory: factory with prod values for this pipeline
  """
  all_partition_dates = get_all_dates()

  # Iterable[PCollection[Row]]
  rows_by_date = (
      rows | 'partition by date' >> beam.Partition(_get_date_partition,
                                                   len(all_partition_dates)))

  # List[PCollection[Row]]
  all_rows_with_metadata = []

  # PCollection[Row]
  for (partition_date, rows_with_single_date) in zip(all_partition_dates,
                                                     rows_by_date):
    # PCollection[Row]
    rows_with_metadata_and_single_date = (
        rows_with_single_date |
        f'add metadata for {partition_date.isoformat()}' >> beam.ParDo(
            MetadataAdder(ip_metadata_chooser_factory)).with_output_types(Row))
    all_rows_with_metadata.append(rows_with_metadata_and_single_date)

  # PCollection[Row]
  rows_with_metadata = (
      all_rows_with_metadata |
      'merge date collections' >> beam.Flatten().with_output_types(Row))

  return rows_with_metadata
