"""Helpers for merging metadata with rows in beam pipelines"""

from __future__ import absolute_import

from typing import Tuple, Dict, List, Iterator

from pprint import pprint

from pipeline.metadata.flatten_base import Row, SatelliteAnswer, Tag

# A key containing a date and IP
# ex: ("2020-01-01", '1.2.3.4')
DateIpKey = Tuple[str, str]

# PCollection key names used internally by the beam pipeline
IP_METADATA_PCOLLECTION_NAME = 'metadata'
ROWS_PCOLLECION_NAME = 'rows'


def make_date_ip_key(tag: Tag) -> DateIpKey:
  """Makes a tuple key of the date and ip from a given row dict."""
  return (tag.date or '', tag.ip or '')


def merge_metadata_with_rows(  # pylint: disable=unused-argument
    key: DateIpKey,
    value: Dict[str, List[Tag]],
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
    ip_metadata: Tag = value[IP_METADATA_PCOLLECTION_NAME][0]
  else:
    ip_metadata = Row()
  rows: List[Row] = value[ROWS_PCOLLECION_NAME]


  if len(rows) == 0:
    pprint(("no rows", key))
  else:
    pprint("attempting to add metadata")
    pprint(("key", key))
    pprint(("field", field))
    pprint(("rows", len(rows), rows))
    pprint(("metadata", ip_metadata))

  for row in rows:
    new_row = Row()
    new_row.update(row)
    if field == 'received':
      if new_row.received:
        # Double-flattened rows are stored with a single received ip in each list
        # to be reconstructed later
        answer: SatelliteAnswer = new_row.received[0]
        answer.asnum = ip_metadata.asn
        answer.asname = ip_metadata.as_name

        #new_row.received[0].update(ip_metadata)
        #new_row.received[0].pop('date', None)
        #new_row.received[0].pop('name', None)
        #new_row.received[0].pop('country', None)
    else:
      new_row.update(ip_metadata)
    pprint(("new row", new_row))
    yield new_row
