"""Helpers for merging metadata with rows in beam pipelines"""

from __future__ import absolute_import

from typing import Tuple, Dict, List, Iterator

from pipeline.metadata.flatten import Row

# A key containing a date and IP
# ex: ("2020-01-01", '1.2.3.4')
DateIpKey = Tuple[str, str]

# PCollection key names used internally by the beam pipeline
BEAM_COGROUP_A_SIDE = 'a'
BEAM_COGROUP_B_SIDE = 'b'


def make_date_ip_key(row: Row) -> DateIpKey:
  """Makes a tuple key of the date and ip from a given row dict."""
  return (row['date'], row['ip'])


def merge_metadata_with_rows(  # pylint: disable=unused-argument
    key: DateIpKey, value: Dict[str, List[Row]]) -> Iterator[Row]:
  # pyformat: disable
  """Merge a list of rows with their corresponding metadata information.

  Args:
    key: The DateIpKey tuple that we joined on. This is thrown away.
    value: A two-element dict
      {BEAM_COGROUP_A_SIDE: One element list containing an ipmetadata
       BEAM_COGROUP_B_SIDE: Many element list containing row dicts}
      where ipmetadata is a dict of the format {column_name, value}
       {'netblock': '1.0.0.1/24', 'asn': 13335, 'as_name': 'CLOUDFLARENET', ...}
      and row is a dict of the format {column_name, value}
       {'domain': 'test.com', 'ip': '1.1.1.1', 'success': true ...}

  Yields:
    row dict {column_name, value} containing both row and metadata cols/values
  """
  # pyformat: enable
  if value[BEAM_COGROUP_A_SIDE]:
    ip_metadata = value[BEAM_COGROUP_A_SIDE][0]
  else:
    ip_metadata = {}
  rows = value[BEAM_COGROUP_B_SIDE]

  for row in rows:
    new_row: Row = {}
    new_row.update(row)
    new_row.update(ip_metadata)
    yield new_row
