"""Beam pipeline helper for converting satellite data."""

from __future__ import absolute_import

import datetime
import json
import logging
import pathlib
import re
from typing import Tuple, Dict, Any, Iterator, Iterable
import uuid

import apache_beam as beam

from pipeline.metadata.beam_metadata import DateIpKey, IP_METADATA_PCOLLECTION_NAME, ROWS_PCOLLECION_NAME, make_date_ip_key, merge_metadata_with_rows
from pipeline.metadata.flatten import Row
from pipeline.metadata.lookup_country_code import country_name_to_code
from pipeline.metadata import flatten_satellite
from pipeline.metadata import flatten

BLOCKPAGE_BIGQUERY_SCHEMA = {
    # Columns from Censored Planet data
    'domain': ('string', 'nullable'),
    'ip': ('string', 'nullable'),
    'date': ('date', 'nullable'),
    'start_time': ('timestamp', 'nullable'),
    'end_time': ('timestamp', 'nullable'),
    'success': ('boolean', 'nullable'),
    'https': ('boolean', 'nullable'),
    'source': ('string', 'nullable'),
    'blockpage': ('boolean', 'nullable'),
    'page_signature': ('string', 'nullable'),

    # Column filled in all tables
    'received_status': ('string', 'nullable'),
    # Columns filled only in HTTP/HTTPS tables
    'received_body': ('string', 'nullable'),
    'received_headers': ('string', 'repeated'),
    # Columns filled only in HTTPS tables
    'received_tls_version': ('integer', 'nullable'),
    'received_tls_cipher_suite': ('integer', 'nullable'),
    'received_tls_cert': ('string', 'nullable'),
}

SCAN_TYPE_SATELLITE = 'satellite'
SCAN_TYPE_BLOCKPAGE = 'blockpage'

CDN_REGEX = re.compile("AMAZON|Akamai|OPENDNS|CLOUDFLARENET|GOOGLE")
NUM_DOMAIN_PARTITIONS = 250
NUM_SATELLITE_INPUT_PARTITIONS = 3


def _merge_dicts(dicts: Iterable[Dict[Any, Any]]) -> Dict[Any, Any]:
  """Helper method for merging dictionaries."""
  merged = {}
  for dict_ in dicts:
    merged.update(dict_)
  return merged


def _get_domain_partition(keyed_row: Tuple[DateIpKey, Row], _: int) -> int:
  key = keyed_row[1]
  return hash(key.get('domain')) % NUM_DOMAIN_PARTITIONS


def _get_satellite_date_partition(row: Row, _: int) -> int:
  """Partition Satellite data by date, corresponding to v2.2 format change."""
  if datetime.date.fromisoformat(
      row['date']) < flatten_satellite.SATELLITE_V2_2_START_DATE:
    return 0
  return 1


def _make_date_received_ip_key(row: Row) -> DateIpKey:
  """Makes a tuple key of the date and received ip from a given row dict."""
  from pprint import pprint
  #pprint(("making key", row['date'], row))

  if row['received'] is not None and len(row['received']) > 0:
    return (row['date'], row['received'][0]['ip'])
  return (row['date'], '')


def get_blockpage_table_name(table_name: str, scan_type: str) -> str:
  """Returns an additional table name for writing the blockpage table.

  Args:
    table_name: dataset.table name like 'base.satellite_scan'
    scan_type: 'satellite'

  Returns:
    name like 'base.satellite_blockpage_scan'
  """
  return table_name.replace(f'.{scan_type}',
                            f'.{scan_type}_{SCAN_TYPE_BLOCKPAGE}')


def _read_satellite_tags(filename: str, line: str) -> Iterator[Row]:
  """Read data for IP tagging from Satellite.

    Args:
      filename: source Satellite file
      line: json str (dictionary containing geo tag data)

    Yields:
      A row dict of the format
        {'ip': '1.1.1.1',
         'date': '2020-01-01'

         And then one of:
         'name': 'special',
         or
         'country': 'US',
         or
         'http': ''e3c1d3...' # optional
         'cert': 'a2fed1...' # optional
         'asname': 'CLOUDFLARENET' # optional
         'asnum': 13335 # optional
        }
      Or an empty dictionary
  """
  try:
    scan = json.loads(line)
  except json.decoder.JSONDecodeError as e:
    logging.warning('JSONDecodeError: %s\nFilename: %s\n%s\n', e, filename,
                    line)
    return
  if 'location' in scan:
    # from v2 tagged_resolvers.json, not needed
    return
  if 'name' in scan:
    # from resolvers.json
    tags = {'ip': scan.get('resolver', scan.get('vp')), 'name': scan['name']}
  elif 'country' in scan:
    # from v1 tagged_resolvers.json
    # contains resolver's full country name
    # convert to country code
    tags = {
        'ip': scan['resolver'],
        'country': country_name_to_code(scan['country'])
    }
  elif 'http' in scan:
    # from tagged_answers.json
    tags = {
        'ip': scan['ip'],
        'http': scan['http'],
        'cert': scan['cert'],
        'asname': scan['asname'],
        'asnum': scan['asnum'],
    }
  else:
    raise Exception(f"Unknown satellite tag format: {scan}")
  tags['date'] = re.findall(r'\d\d\d\d-\d\d-\d\d', filename)[0]
  yield tags


def _add_vantage_point_tags(
    rows: beam.pvalue.PCollection[Row],
    ips_with_metadata: beam.pvalue.PCollection[Tuple[DateIpKey, Row]]
) -> beam.pvalue.PCollection[Row]:
  """Add tags for vantage point IPs - resolver name (hostname/control/special) and country

  Args:
      rows: PCollection of measurement rows
      ips_with_metadata: PCollection of dated ips with geo metadata

    Returns:
      PCollection of measurement rows with tag information added to the ip row
  """
  # PCollection[Tuple[DateIpKey,Row]]
  rows_keyed_by_ip_and_date = (
      rows | 'add vp tags: key by ips and dates' >>
      beam.Map(lambda row: (make_date_ip_key(row), row)).with_output_types(
          Tuple[DateIpKey, Row]))

  # PCollection[Tuple[Tuple[date,ip],Dict[input_name_key,List[Row]]]]
  grouped_metadata_and_rows = (({
      IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
      ROWS_PCOLLECION_NAME: rows_keyed_by_ip_and_date
  }) | 'add vp tags: group by keys' >> beam.CoGroupByKey())

  # PCollection[Row]
  rows_with_metadata = (
      grouped_metadata_and_rows | 'add vp tags: merge metadata with rows' >>
      beam.FlatMapTuple(merge_metadata_with_rows).with_output_types(Row))

  return rows_with_metadata


def add_received_ip_tags(
    rows: beam.pvalue.PCollection[Row],
    ips_with_metadata: beam.pvalue.PCollection[Tuple[DateIpKey, Row]]
) -> beam.pvalue.PCollection[Row]:
  """Add tags for answer ips (field received.ip) - asnum, asname, http, cert

  Args:
      rows: PCollection of measurement rows
      ips_with_metadata: PCollection of dated ips with geo metadata

    Returns:
      PCollection of measurement rows with tag information added to the recieved.ip row
  """
  # PCollection[Tuple[DateIpKey,Row]]
  received_keyed_by_ip_and_date = (
      rows | 'key by received ips and dates' >> beam.Map(
          lambda row: (_make_date_received_ip_key(row), row)).with_output_types(
              Tuple[DateIpKey, Row]))

  # Iterable[PCollection[Tuple[DateIpKey,Row]]]
  partition_by_domain = (
      received_keyed_by_ip_and_date | 'partition by domain' >> beam.Partition(
          _get_domain_partition, NUM_DOMAIN_PARTITIONS))

  collections = []
  for i in range(0, NUM_DOMAIN_PARTITIONS):
    elements = partition_by_domain[i]
    # PCollection[Tuple[Tuple[date,ip],Dict[input_name_key,List[Row]]]]
    grouped_received_metadata_and_rows = (({
        IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
        ROWS_PCOLLECION_NAME: elements
    }) | f'group by received ip keys {i}' >> beam.CoGroupByKey())

    # PCollection[Row]
    domain_rows_with_tags = (
        grouped_received_metadata_and_rows | f'tag received ips {i}' >>
        beam.FlatMapTuple(lambda k, v: merge_metadata_with_rows(
            k, v, field='received')).with_output_types(Row))

    collections.append(domain_rows_with_tags)

  # PCollection[Row]
  rows_with_tags = (
      collections |
      'merge domain collections' >> beam.Flatten().with_output_types(Row))

  return rows_with_tags


def unflatten_received_ip_rows(
    rows: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection[Row]:
  """Unflatten so that each row contains a array of answer IPs

  Args:
    rows: roundtrip rows with a single recieved ip

  Returns:
    roundtrip rows aggregated so they have an array of recieved responses
  """
  # PCollection[Tuple[str,Row]]
  keyed_by_roundtrip_id = (
      rows | 'key by roundtrip id' >>
      beam.Map(lambda row:
               (row['roundtrip_id'], row)).with_output_types(Tuple[str, Row]))

  # PCollection[Tuple[str,Iterable[Row]]]
  grouped_by_roundtrip_id = (
      keyed_by_roundtrip_id | 'group by roundtrip id' >> beam.GroupByKey())

  # PCollection[Row]
  unflattened_rows = (
      grouped_by_roundtrip_id | 'unflatten rows' >> beam.FlatMapTuple(
          lambda k, v: _unflatten_satellite(v)).with_output_types(Row))

  return unflattened_rows


def _add_satellite_tags(
    rows: beam.pvalue.PCollection[Row],
    tags: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection[Row]:
  """Add tags for resolvers and answer IPs and unflatten the Satellite measurement rows.

    Args:
      rows: PCollection of measurement rows
      tags: PCollection of geo tag rows

    Returns:
      PCollection of measurement rows containing tag information
  """
  # PCollection[Tuple[DateIpKey,Row]]
  ips_with_metadata = (
      tags | 'tags key by ips and dates' >>
      beam.Map(lambda row: (make_date_ip_key(row), row)) |
      'combine duplicate tags' >>
      beam.CombinePerKey(_merge_dicts).with_output_types(Tuple[DateIpKey, Row]))

  # PCollection[Row]
  rows_with_metadata = _add_vantage_point_tags(rows, ips_with_metadata)

  # Starting with the Satellite v2.2 format, the rows include received ip tags.
  # Received tagging steps are only required prior to v2.2

  # PCollection[Row], PCollection[Row]
  rows_pre_v2_2, rows_v2_2 = (
      rows_with_metadata |
      'partition by date' >> beam.Partition(_get_satellite_date_partition, 2))

  # PCollection[Row]
  rows_pre_v2_2_with_tags = add_received_ip_tags(rows_pre_v2_2,
                                                 ips_with_metadata)

  # PCollection[Row]
  rows_with_tags = ((rows_pre_v2_2_with_tags, rows_v2_2) |
                    'combine date partitions' >> beam.Flatten())

  # PCollection[Row]
  return unflatten_received_ip_rows(rows_with_tags)


def post_processing_satellite(
    rows: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection[Row]:
  """Run post processing on Satellite v1 data (calculate confidence, verify interference).

    Args:
      rows: PCollection of measurement rows

    Returns:
      PCollection of measurement rows with confidence and verify fields
  """

  def _total_tags(key: Tuple[str, str],
                  row: Row) -> Tuple[Tuple[str, str], int]:
    total_tags = 0
    for tag_type in flatten_satellite.SATELLITE_TAGS:
      if tag_type != 'ip':
        type_tags = {
            ans[tag_type] for ans in row['received'] if ans.get(tag_type)
        }
        total_tags += len(type_tags)
    return (key, total_tags)

  def _flat_rows_controls(key: Any, value: Row) -> Iterator[Tuple[Row, int]]:  # pylint: disable=unused-argument
    num_control_tags = 0
    if len(value['control']) > 0:
      num_control_tags = value['control'][0]
    for row in value['test']:
      yield (row, num_control_tags)

  # Partition rows into test measurements and control measurements
  # 'anomaly' is None for control measurements

  # PCollection[Tuple[Tuple[str, str], Row]], PCollection[Tuple[Tuple[str, str], Row]]
  rows, controls = (
      rows | 'key by dates and domains' >> beam.Map(lambda row: (
          (row['date'], row['domain']), row)) | 'partition test and control' >>
      beam.Partition(lambda row, p: int(row[1]['anomaly'] is None), 2))

  # PCollection[Tuple[Tuple[str, str], int]]
  num_ctags = controls | 'calculate # control tags' >> beam.MapTuple(
      _total_tags)

  # PCollection[Row]
  post = ({
      'test': rows,
      'control': num_ctags
  } | 'group rows and # control tags by keys' >> beam.CoGroupByKey() |
          'flatmap to (row, # control tags)' >>
          beam.FlatMapTuple(_flat_rows_controls) |
          'calculate confidence' >> beam.MapTuple(_calculate_confidence) |
          'verify interference' >> beam.Map(_verify).with_output_types(Row))

  # PCollection[Row]
  # pylint: disable=no-value-for-parameter
  controls = (
      controls | 'unkey control' >> beam.Values().with_output_types(Row))

  # PCollection[Row]
  post = ((post, controls) | 'flatten test and control' >> beam.Flatten())

  return post


def _unflatten_satellite(flattened_measurement: Iterable[Row]) -> Iterator[Row]:
  """Unflatten a Satellite measurement.

  Args:
    flattened_measurment: list of dicts representing a flattened measurement,
    where each contains an unique answer IP and tags in the 'received' field
    (other fields are the same for each dict).
    [{'ip':'1.1.1.1','domain':'x.com','measurement_id':'HASH','received':[{'ip':'0.0.0.1','tag':'value1'},...}],
     {'ip':'1.1.1.1','domain':'x.com','measurement_id':'HASH','received':[{'ip':'0.0.0.2','tag':'value2'},...}],


  Yields:
    Row with common fields remaining the same,
    and 'received' mapped to an array of answer IP dictionaries.
    {'ip':'1.1.1.1','domain':'x.com','received':[{'ip':'0.0.0.1','tag':'value1'},{'ip':'0.0.0.2','tag':'value2'}],...}
  """

  if flattened_measurement:
    # Get common fields and update 'received' with array of all answer IPs
    combined: Row = {'received': []}
    for answer in flattened_measurement:
      received = answer.pop('received', None)
      combined.update(answer)
      # For Satellite v2.2, the received field will already contain an array
      if isinstance(received, list):
        combined['received'] += received
      elif received:
        combined['received'].append(received)
    # Remove extra tag fields from the measurement. These may be added
    # during the tagging step if a vantage point also appears as a response IP.
    for tag in ['asname', 'asnum', 'http', 'cert']:
      combined.pop(tag, None)

    combined.pop('roundtrip_id')  # Remove roundtrip id
    yield combined


def flatten_received_ips(row: Row) -> Iterator[Row]:
  """Flatten a row with multiple received ips into rows with a single ip.

  Args:
    row element like
      {
        "field": "value"
        "received" [<dict1>, <dict2>]
      }

  Returns:
    Rows like
      {
        "field": "value"
        "roundtrip_id" = "a"
        "received" [<dict1>]
      }
      {
        "field": "value"
        "roundtrip_id" = "a"
        "received" [<dict2>]
      }
  """
  # This id is used to reconstruct the row structure when unflattening
  row['roundtrip_id'] = uuid.uuid4().hex

  all_received = row['received']

  if len(all_received) == 0:
    yield row
    return

  for received in all_received:
    row['received'] = [received]
    yield row.copy()


def process_satellite_with_tags(
    row_lines: beam.pvalue.PCollection[Tuple[str, str]],
    tag_lines: beam.pvalue.PCollection[Tuple[str, str]]
) -> beam.pvalue.PCollection[Row]:
  """Process Satellite measurements and tags.

  Args:
    row_lines: Row objects
    tag_lines: various

  Returns:
    PCollection[Row] of rows with tag metadata added
  """
  # PCollection[Row]
  rows = (
      row_lines | 'flatten json' >> beam.ParDo(
          flatten.FlattenMeasurement()).with_output_types(Row))

  # PCollection[Row] each with only a single element in the received arrays
  received_ip_flattened_rows = (
      rows | 'flatten received ips' >>
      beam.FlatMap(flatten_received_ips).with_output_types(Row))

  # PCollection[Row]
  tag_rows = (
      tag_lines | 'tag rows' >>
      beam.FlatMapTuple(_read_satellite_tags).with_output_types(Row))

  # PCollection[Row]
  rows_with_metadata = _add_satellite_tags(received_ip_flattened_rows, tag_rows)

  return rows_with_metadata


def process_satellite_blockpages(
    blockpages: beam.pvalue.PCollection[Tuple[str, str]]
) -> beam.pvalue.PCollection[Row]:
  """Process Satellite measurements and tags.

  Args:
    blockpages: Row objects

  Returns:
    PCollection[Row] of blockpage rows in bigquery format
  """
  rows = (
      blockpages | 'flatten blockpages' >> beam.ParDo(
          flatten.FlattenMeasurement()).with_output_types(Row))

  return rows


def partition_satellite_input(
    line: Tuple[str, str],
    num_partitions: int = NUM_SATELLITE_INPUT_PARTITIONS) -> int:
  """Partitions Satellite input into tags (0) blockpages (1) and rows (2).

  Args:
    line: an input line Tuple[filename, line_content]
      filename is "<path>/name.json"
    num_partitions: number of partitions to use, always 3

  Returns:
    int,
      0 - line is a tag file
      1 - line is a blockpage
      2 - line is a dns measurement or control

  Raises:
    Exception if num_partitions != NUM_SATELLITE_INPUT_PARTITIONS
    Exception if an unknown filename is used
  """
  if num_partitions != NUM_SATELLITE_INPUT_PARTITIONS:
    raise Exception(
        "Bad input number of partitions; always use NUM_SATELLITE_INPUT_PARTITIONS."
    )

  filename = pathlib.PurePosixPath(line[0]).name
  if '.gz' in pathlib.PurePosixPath(filename).suffixes:
    filename = pathlib.PurePosixPath(filename).stem

  if filename in [
      flatten_satellite.SATELLITE_RESOLVERS_FILE,
      flatten_satellite.SATELLITE_TAGGED_RESOLVERS_FILE,
      flatten_satellite.SATELLITE_TAGGED_ANSWERS_FILE,
      flatten_satellite.SATELLITE_TAGGED_RESPONSES
  ]:
    return 0
  if filename == flatten_satellite.SATELLITE_BLOCKPAGES_FILE:
    return 1
  if filename in [
      flatten_satellite.SATELLITE_INTERFERENCE_FILE,
      flatten_satellite.SATELLITE_INTERFERENCE_ERR_FILE,
      flatten_satellite.SATELLITE_ANSWERS_CONTROL_FILE,
      flatten_satellite.SATELLITE_RESPONSES_CONTROL_FILE,
      flatten_satellite.SATELLITE_RESULTS_FILE,
      flatten_satellite.SATELLITE_ANSWERS_ERR_FILE
  ]:
    return 2
  raise Exception(f"Unknown filename in Satellite data: {filename}")


def _calculate_confidence(scan: Dict[str, Any],
                          num_control_tags: int) -> Dict[str, Any]:
  """Calculate confidence for a Satellite measurement.

    Args:
      scan: dict containing measurement data
      control_tags: dict containing control tags for the test domain

    Returns:
      scan dict with new records:
        'average_confidence':
          average percentage of tags that match control queries
        'matches_confidence': array of percentage match per answer IP
        'untagged_controls': True if all control IPs have no tags
        'untagged_response': True if all answer IPs have no tags
  """
  scan['average_confidence'] = 0
  scan['matches_confidence'] = []
  scan['untagged_controls'] = num_control_tags == 0
  scan['untagged_response'] = True

  for answer in scan['received'] or []:
    # check tags for each answer IP
    matches_control = answer['matches_control'].split()
    total_tags = 0
    matching_tags = 0

    # calculate number of tags IP has and how many match controls
    for tag in flatten_satellite.SATELLITE_TAGS:
      if tag != 'ip' and answer.get(tag):
        total_tags += 1
        if tag in matches_control:
          matching_tags += 1

    if scan['untagged_response'] and total_tags > 0:
      # at least one answer IP has tags
      scan['untagged_response'] = False

    # calculate percentage of matching tags
    if 'ip' in matches_control:
      # ip is in control response
      ip_match = 100.0
    else:
      if total_tags == 0:
        ip_match = 0.0
      else:
        ip_match = matching_tags * 100 / total_tags
    scan['matches_confidence'].append(ip_match)

  if len(scan['matches_confidence']) > 0:
    scan['average_confidence'] = sum(scan['matches_confidence']) / len(
        scan['matches_confidence'])
  # Sanity check for untagged responses: do not claim interference
  if scan['untagged_response'] or scan['untagged_controls']:
    scan['anomaly'] = False
  return scan


def _verify(scan: Dict[str, Any]) -> Dict[str, Any]:
  """Verify that a Satellite measurement with interference is not a false positive.

    Args:
      scan: dict containing measurement data

    Returns:
      scan dict with new records:
        'excluded': bool, equals true if interference is a false positive
        'exclude_reason': string, reason(s) for false positive
  """
  scan['excluded'] = None
  scan['exclude_reason'] = None

  if scan['anomaly']:
    scan['excluded'] = False
    reasons = []
    # Check received IPs for false positive reasons
    for received in scan['received']:
      asname = received.get('asname')
      if asname and CDN_REGEX.match(asname):
        # CDN IPs
        scan['excluded'] = True
        reasons.append('is_CDN')
    scan['exclude_reason'] = ' '.join(reasons)
  return scan


def process_satellite_lines(
    lines: beam.pvalue.PCollection[Tuple[str, str]]
) -> Tuple[beam.pvalue.PCollection[Row], beam.pvalue.PCollection[Row]]:
  """Process both satellite and blockpage data files.

  Args:
    lines: input lines from all satellite files. Tuple[filename, line]

  Returns:
    post_processed_satellite: rows of satellite scan data
    blockpage_rows: rows of blockpage data
  """
  # PCollection[Tuple[filename,line]] x3
  tags, blockpages, lines = lines | beam.Partition(
      partition_satellite_input, NUM_SATELLITE_INPUT_PARTITIONS)

  # PCollection[Row]
  tagged_satellite = process_satellite_with_tags(lines, tags)
  # PCollection[Row]
  post_processed_satellite = post_processing_satellite(tagged_satellite)

  # PCollection[Row]
  blockpage_rows = process_satellite_blockpages(blockpages)

  return post_processed_satellite, blockpage_rows
