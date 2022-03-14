"""Beam pipeline helper for converting satellite data."""

from __future__ import absolute_import

import datetime
import json
import logging
import pathlib
import re
from typing import Union, List, Tuple, Dict, Iterator, Iterable
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

# A key containing a date and Domain
# ex: ("2020-01-01", 'example.com')
DateDomainKey = Tuple[str, str]

SCAN_TYPE_SATELLITE = 'satellite'
SCAN_TYPE_BLOCKPAGE = 'blockpage'

CDN_REGEX = re.compile("AMAZON|Akamai|OPENDNS|CLOUDFLARENET|GOOGLE")
NUM_SATELLITE_INPUT_PARTITIONS = 4

# PCollection key name used internally by the beam pipeline
RECEIVED_IPS_PCOLLECTION_NAME = 'received_ips'
CONTROLS_PCOLLECTION_NAME = 'controls'


def _get_filename(filepath: str) -> str:
  """Get just filename from filepath

  Args:
    filepath like: "CP_Satellite-2020-12-17-12-00-01/resolvers.json.gz"

  Returns:
    base filename like "resolvers.json"
  """
  filename = pathlib.PurePosixPath(filepath).name
  if '.gz' in pathlib.PurePosixPath(filename).suffixes:
    filename = pathlib.PurePosixPath(filename).stem
  return filename


def _get_satellite_date_partition(row: Row, _: int) -> int:
  """Partition Satellite data by date, corresponding to v2.2 format change."""
  if datetime.date.fromisoformat(
      row['date']) < flatten_satellite.SATELLITE_V2_2_START_DATE:
    return 0
  return 1


def _set_random_roundtrip_id(row: Row) -> Row:
  """Add a roundtrip_id field to a row."""
  row['roundtrip_id'] = uuid.uuid4().hex
  return row


def _delete_roundtrip_ip(row: Row) -> Row:
  """Remove the roundtrip_id field from a row."""
  row.pop('roundtrip_id')
  return row


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


def _get_received_ips_with_roundtrip_id_and_date(row: Row) -> Iterable[Row]:
  """Get all the individual received ips answers from a row.

  Args:
    row: dicts without metadata in the received ips lke:
      {
        'ip': '1.2.3.4'
        'date': '2021-01-01'
        'domain': 'ex.com'
        'roundtrip_id: 'abc'
        'received' : [{
            'ip': '4.5.6.7'
          }, {
            'ip': '5.6.7.8'
          },
        ]
      }

  Yields individual recieved answers that also include the roundtrip and date
    ex:
      {
        'ip': '4.5.6.7',
        'date': '2021-01-01',
        'roundtrip_id: 'abc'
      }, {
        'ip': '5.6.7.8',
        'date': '2021-01-01',
        'roundtrip_id: 'abc'
      }
  """
  roundtrip_id = row['roundtrip_id']
  date = row['date']

  for answer in row['received']:
    received_with_id = answer.copy()
    received_with_id['roundtrip_id'] = roundtrip_id
    received_with_id['date'] = date
    yield received_with_id


def _read_satellite_resolver_tags(filepath: str, line: str) -> Iterator[Row]:
  """Read data for IP tagging from Satellite.

    Args:
      filepath: source Satellite file
      line: json str (dictionary containing geo tag data)

  Yields:
      A row dict of the format
        {
         'ip': '1.1.1.1',
         'date': '2020-01-01'
         'country': 'US'  # optional
         'name': 'one.one.one.one'  # optional
        }
  """
  try:
    scan = json.loads(line)
  except json.decoder.JSONDecodeError as e:
    logging.warning('JSONDecodeError: %s\nFilename: %s\n%s\n', e, filepath,
                    line)
    return

  ip: str = scan.get('resolver') or scan.get('vp')
  date = re.findall(r'\d\d\d\d-\d\d-\d\d', filepath)[0]
  tags = {'ip': ip, 'date': date}

  if 'name' in scan:
    tags['name'] = scan['name']

  if 'location' in scan:
    tags['country'] = scan['location']['country_code']
  if 'country' in scan:
    tags['country'] = country_name_to_code(scan['country'])

  yield tags


def _read_satellite_answer_tags(filepath: str, line: str) -> Iterator[Row]:
  """Read data for IP tagging from Satellite.

    Args:
      filepath: source Satellite file
      line: json str (dictionary containing geo tag data)

  Yields:
      A row dict of the format
        {
         'ip': '1.1.1.1',
         'date': '2020-01-01'
         'http': ''e3c1d3...' # optional
         'cert': 'a2fed1...' # optional
         'asname': 'CLOUDFLARENET' # optional
         'asnum': 13335 # optional
        }
  """
  try:
    scan = json.loads(line)
  except json.decoder.JSONDecodeError as e:
    logging.warning('JSONDecodeError: %s\nFilename: %s\n%s\n', e, filepath,
                    line)
    return

  tags = {
      'ip': scan['ip'],
      'http': scan['http'],
      'cert': scan['cert'],
      'asname': scan['asname'],
      'asnum': scan['asnum'],
      'date': re.findall(r'\d\d\d\d-\d\d-\d\d', filepath)[0]
  }

  yield tags


def _add_vantage_point_tags(
    rows: beam.pvalue.PCollection[Row],
    tags: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection[Row]:
  """Add tags for vantage point IPs - resolver name (hostname/control/special) and country

  Args:
      rows: PCollection of measurement rows
      ips_with_metadata: PCollection of dated ips with geo metadata

    Returns:
      PCollection of measurement rows with tag information added to the ip row
  """
  # PCollection[Tuple[DateIpKey,Row]]
  ips_with_metadata = (
      tags | 'tags key by ips and dates' >>
      beam.Map(lambda row: (make_date_ip_key(row), row)).with_output_types(
          Tuple[DateIpKey, Row]))

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


def _add_satellite_tags(
    rows: beam.pvalue.PCollection[Row],
    resolver_tags: beam.pvalue.PCollection[Row],
    answer_tags: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection[Row]:
  """Add tags for resolvers and answer IPs and unflatten the Satellite measurement rows.

    Args:
      rows: PCollection of measurement rows
      resolver_tags: PCollection of tag info for resolvers
      answer_tags: PCollection of geo/asn tag info for received ips

    Returns:
      PCollection of measurement rows containing tag information
  """
  # PCollection[Row]
  rows_with_metadata = _add_vantage_point_tags(rows, resolver_tags)

  # Starting with the Satellite v2.2 format, the rows include received ip tags.
  # Received tagging steps are only required prior to v2.2

  # PCollection[Row], PCollection[Row]
  rows_pre_v2_2, rows_v2_2 = (
      rows_with_metadata |
      'partition by date' >> beam.Partition(_get_satellite_date_partition, 2))

  # PCollection[Row]
  rows_pre_v2_2_with_tags = add_received_ip_tags(rows_pre_v2_2, answer_tags)

  # PCollection[Row]
  rows_with_tags = ((rows_pre_v2_2_with_tags, rows_v2_2) |
                    'combine date partitions' >> beam.Flatten())

  # PCollection[Row]
  return rows_with_tags


def _total_tags(key: DateDomainKey, row: Row) -> Tuple[DateDomainKey, int]:
  total_tags = 0
  for tag_type in flatten_satellite.SATELLITE_TAGS:
    if tag_type != 'ip':
      type_tags = {
          ans[tag_type] for ans in row['received'] if ans.get(tag_type)
      }
      total_tags += len(type_tags)
  return (key, total_tags)


def _flat_rows_controls(  # pylint: disable=unused-argument
    key: DateDomainKey,
    value: Dict[str, Union[List[Row], List[int]]]) -> Iterator[Tuple[Row, int]]:
  """Flatten out controls vs rows.

  Args:
    key, a date/domain key the data is joined on, unused
    value: a dict
      {
        ROWS_PCOLLECION_NAME: List[Rows]
        CONTROLS_PCOLLECTION_NAME: List[int]
          single element list with the # of control tags for these rows
      }

  Yields:
    Tuple[Row, num_control_tags]
  """
  rows: List[Row] = value[ROWS_PCOLLECION_NAME]  # type: ignore
  num_control_tags_list: List[int] = value[
      CONTROLS_PCOLLECTION_NAME]  # type: ignore

  num_control_tags = 0
  if len(num_control_tags_list) > 0:
    num_control_tags = num_control_tags_list[0]
  for row in rows:
    yield (row, num_control_tags)


def _key_by_date_domain(row: Row) -> Tuple[DateDomainKey, Row]:
  return ((row['date'], row['domain']), row)


def _partition_test_and_controls(row_tuple: Tuple[DateDomainKey, Row],
                                 _: int) -> int:
  """Partition tests from domain and ip controls

  Returns
    0 = domain control
    1 = ip control
    2 = test measurement
  """
  row: Row = row_tuple[1]
  if row['is_control']:
    return 0
  # 'anomaly' is None for control measurements
  if row['is_control_ip'] or row['anomaly'] is None:
    return 1
  return 2


def post_processing_satellite(
    rows: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection[Row]:
  """Run post processing on Satellite v1 data (calculate confidence, verify interference).

    Args:
      rows: PCollection of measurement rows

    Returns:
      PCollection of measurement rows with confidence and verify fields
  """
  # PCollection[Tuple[DateDomainKey, Row]]
  rows_keyed_by_date_domains = (
      rows | 'key by dates and domains' >>
      beam.Map(_key_by_date_domain).with_output_types(Tuple[DateDomainKey, Row])
  )

  # PCollection[Tuple[DateDomainKey, Row]] x3
  domain_controls, ip_controls, tests = (
      rows_keyed_by_date_domains | 'partition test and control' >>
      beam.Partition(_partition_test_and_controls, 3).with_output_types(
          Tuple[DateDomainKey, Row]))

  # PCollection[Tuple[DateDomainKey, int]]
  num_ctags = (
      ip_controls | 'calculate # control tags' >>
      beam.MapTuple(_total_tags).with_output_types(Tuple[DateDomainKey, int]))

  grouped_rows_num_controls = ({
      ROWS_PCOLLECION_NAME: tests,
      CONTROLS_PCOLLECTION_NAME: num_ctags
  } | 'group rows and # control tags by keys' >> beam.CoGroupByKey())

  # PCollection[Tuple[Row, num_controls]]
  rows_with_num_controls = (
      grouped_rows_num_controls | 'flatmap to (row, # control tags)' >>
      beam.FlatMapTuple(_flat_rows_controls).with_output_types(Tuple[Row, int]))

  # PCollection[Row]
  confidence = (
      rows_with_num_controls | 'calculate confidence' >>
      beam.MapTuple(_calculate_confidence).with_output_types(Row))

  # PCollection[Row]
  verified = (
      confidence |
      'verify interference' >> beam.Map(_verify).with_output_types(Row))

  # PCollection[Row]
  # pylint: disable=no-value-for-parameter
  ip_controls = (
      ip_controls | 'unkey ip controls' >> beam.Values().with_output_types(Row))

  # PCollection[Row]
  # pylint: disable=no-value-for-parameter
  domain_controls = (
      domain_controls |
      'unkey domain controls' >> beam.Values().with_output_types(Row))

  # PCollection[Row]
  post = ((verified, ip_controls, domain_controls) |
          'flatten test and controls' >> beam.Flatten())

  return post


def _merge_tagged_answers_with_rows(
    key: str,  # pylint: disable=unused-argument
    value: Dict[str, Union[List[Row], List[List[Row]]]]
) -> Row:
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
  row: Row = value[ROWS_PCOLLECION_NAME][0]  # type: ignore

  if len(value[RECEIVED_IPS_PCOLLECTION_NAME]) == 0:  # No tags
    return row
  tagged_answers: List[Row] = value[RECEIVED_IPS_PCOLLECTION_NAME][
      0]  # type: ignore

  for untagged_answer in row['received']:
    untagged_ip = untagged_answer['ip']
    tagged_answer = [
        answer for answer in tagged_answers if answer['ip'] == untagged_ip
    ][0]

    # remove fields that were used for joining
    tagged_answer.pop('date')
    tagged_answer.pop('roundtrip_id')

    untagged_answer.update(tagged_answer)
  return row


def add_received_ip_tags(
    rows: beam.pvalue.PCollection[Row],
    tags: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection[Row]:
  """Add ip metadata info to received ip lists in rows

  Args:
    rows: PCollection of dicts without metadata in the received ips like:
      {
        'ip': '1.2.3.4'
        'domain': 'ex.com'
        'received' : [{
            'ip': '4.5.6.7'
          }, {
            'ip': '5.6.7.8'
          },
        ]
      }
    tags: PCollection of received ip metadata like:
      {
        'ip': '4.5.6.7'
        'asname': 'AMAZON-AES',
        'asnum': 14618,
      }
      {
        'ip': '5.6.7.8'
        'asname': 'CLOUDFLARE',
        'asnum': 13335,
      }

  Returns a PCollection of rows with metadata tags added to received ips like
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
  # PCollection[Tuple[DateIpKey, row]]
  tags_keyed_by_ip_and_date = (
      tags | 'tags: key by ips and dates' >>
      beam.Map(lambda tag: (make_date_ip_key(tag), tag)).with_output_types(
          Tuple[DateIpKey, Row]))

  # PCollection[Row]
  rows_with_roundtrip_id = (
      rows | 'add roundtrip_ids' >>
      beam.Map(_set_random_roundtrip_id).with_output_types(Row))

  # PCollection[row] received ip row
  received_ips_with_roundtrip_ip_and_date = (
      rows_with_roundtrip_id | 'get received ips' >> beam.FlatMap(
          _get_received_ips_with_roundtrip_id_and_date).with_output_types(Row))

  # PCollection[Tuple[DateIpKey, row]] received ip row
  received_ips_keyed_by_ip_and_date = (
      received_ips_with_roundtrip_ip_and_date |
      'received ip: key by ips and dates' >>
      beam.Map(lambda row: (make_date_ip_key(row), row)).with_output_types(
          Tuple[DateIpKey, Row]))

  grouped_metadata_and_received_ips = (({
      IP_METADATA_PCOLLECTION_NAME: tags_keyed_by_ip_and_date,
      ROWS_PCOLLECION_NAME: received_ips_keyed_by_ip_and_date
  }) | 'add received ip tags: group by keys' >> beam.CoGroupByKey())

  # PCollection[Row] received ip row with
  received_ips_with_metadata = (
      grouped_metadata_and_received_ips |
      'add received ip tags: merge metadata with rows' >>
      beam.FlatMapTuple(merge_metadata_with_rows).with_output_types(Row))

  # PCollection[Tuple[roundtrip_id, Row]]
  rows_keyed_by_roundtrip_id = (
      rows_with_roundtrip_id | 'key row by roundtrip_id' >>
      beam.Map(lambda row:
               (row['roundtrip_id'], row)).with_output_types(Tuple[str, Row]))

  # PCollection[Tuple[roundtrip_id, List[Row]]] # received ip
  received_ips_grouped_by_roundtrip_ip = (
      received_ips_with_metadata | 'group received_by roundtrip' >>
      beam.GroupBy(lambda row: row['roundtrip_id']).with_output_types(
          Tuple[str, Iterable[Row]]))

  grouped_rows_and_received_ips = (({
      ROWS_PCOLLECION_NAME: rows_keyed_by_roundtrip_id,
      RECEIVED_IPS_PCOLLECTION_NAME: received_ips_grouped_by_roundtrip_ip
  }) | 'add received ip tags: group by roundtrip' >> beam.CoGroupByKey())

  # PCollection[Row] received ip row with roundtrip id
  rows_with_metadata_and_roundtrip = (
      grouped_rows_and_received_ips |
      'add received ip tags: add tagged answers back' >>
      beam.MapTuple(_merge_tagged_answers_with_rows).with_output_types(Row))

  rows_with_metadata = (
      rows_with_metadata_and_roundtrip | 'delete roundtrip ids' >>
      beam.Map(_delete_roundtrip_ip).with_output_types(Row))

  return rows_with_metadata


def process_satellite_with_tags(
    row_lines: beam.pvalue.PCollection[Tuple[str, str]],
    answer_lines: beam.pvalue.PCollection[Tuple[str, str]],
    resolver_lines: beam.pvalue.PCollection[Tuple[str, str]]
) -> beam.pvalue.PCollection[Row]:
  """Process Satellite measurements and tags.

  Args:
    row_lines: Tuple[filepath, json_line]
    answer_lines: Tuple[filepath, json_line]
    resolver_lines: Tuple[filepath, json_line]

  Returns:
    PCollection[Row] of rows with tag metadata added
  """
  # PCollection[Row]
  rows = (
      row_lines | 'flatten json' >> beam.ParDo(
          flatten.FlattenMeasurement()).with_output_types(Row))

  # PCollection[Row]
  resolver_tags = (
      resolver_lines | 'resolver tag rows' >>
      beam.FlatMapTuple(_read_satellite_resolver_tags).with_output_types(Row))

  # PCollection[Row]
  answer_tags = (
      answer_lines | 'answer tag rows' >>
      beam.FlatMapTuple(_read_satellite_answer_tags).with_output_types(Row))

  rows_with_metadata = _add_satellite_tags(rows, resolver_tags, answer_tags)

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
  """Partitions Satellite input into answer/resolver_tags, blockpages and rows.

  Args:
    line: an input line Tuple[filename, line_content]
      filename is "<path>/name.json"
    num_partitions: number of partitions to use, always 4

  Returns:
    int,
      0 - line is an answer tag file
      1 - line is a resolver tag file
      2 - line is a blockpage
      3 - line is a dns measurement or control

  Raises:
    Exception if num_partitions != NUM_SATELLITE_INPUT_PARTITIONS
    Exception if an unknown filename is used
  """
  if num_partitions != NUM_SATELLITE_INPUT_PARTITIONS:
    raise Exception(
        "Bad input number of partitions; always use NUM_SATELLITE_INPUT_PARTITIONS."
    )

  filename = _get_filename(line[0])
  if filename in flatten_satellite.SATELLITE_ANSWER_TAG_FILES:
    return 0
  if filename in flatten_satellite.SATELLITE_RESOLVER_TAG_FILES:
    return 1
  if filename == flatten_satellite.SATELLITE_BLOCKPAGES_FILE:
    return 2
  if filename in flatten_satellite.SATELLITE_OBSERVATION_FILES:
    return 3
  raise Exception(f"Unknown filename in Satellite data: {filename}")


def _calculate_confidence(scan: Row, num_control_tags: int) -> Row:
  """Calculate confidence for a Satellite measurement.

    Args:
      scan: row containing measurement data
      control_tags: dict containing control tags for the test domain

    Returns:
      row dict with new records:
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
    matches_control = answer.get('matches_control', '').split()
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
  if scan['untagged_response']:
    scan['anomaly'] = False
  return scan


def _verify(scan: Row) -> Row:
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
  # PCollection[Tuple[filename,line]] x4
  answer_lines, resolver_lines, blockpage_lines, row_lines = lines | beam.Partition(
      partition_satellite_input, NUM_SATELLITE_INPUT_PARTITIONS)

  # PCollection[Row]
  tagged_satellite = process_satellite_with_tags(row_lines, answer_lines,
                                                 resolver_lines)

  # PCollection[Row]
  post_processed_satellite = post_processing_satellite(tagged_satellite)

  # PCollection[Row]
  blockpage_rows = process_satellite_blockpages(blockpage_lines)

  return post_processed_satellite, blockpage_rows
