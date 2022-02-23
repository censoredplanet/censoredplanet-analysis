"""Beam pipeline helper for converting satellite data."""

from __future__ import absolute_import

import datetime
import json
import logging
import pathlib
import re
from typing import Union, List, Tuple, Dict, Any, Iterator, Iterable
import uuid

import apache_beam as beam

from pipeline.metadata.beam_metadata import DateIpKey, BEAM_COGROUP_A_SIDE, BEAM_COGROUP_B_SIDE, make_date_ip_key, merge_metadata_with_rows
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


def _is_vantage_point_tag(row: Row) -> bool:
  """Determine if a given tag should be applied to vantage point or received ips.

  TODO: remove this function and instead do the partitioning
        as we read the data in _read_satellite_tags.
  """
  received_ip_exclusive_tags = flatten_satellite.SATELLITE_TAGS.copy()
  received_ip_exclusive_tags.remove('ip')

  common_tags = received_ip_exclusive_tags & set(row.keys())
  return len(common_tags) == 0


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
  # PCollection[Row]
  vp_tags = (
      tags | 'filter out received ip tags' >>
      beam.Filter(_is_vantage_point_tag).with_output_types(Row))

  # PCollection[Tuple[DateIpKey,Row]]
  ips_with_metadata = (
      vp_tags | 'tags key by ips and dates' >>
      beam.Map(lambda row: (make_date_ip_key(row), row)) |
      'combine duplicate tags' >>
      beam.CombinePerKey(_merge_dicts).with_output_types(Tuple[DateIpKey, Row]))

  # PCollection[Tuple[DateIpKey,Row]]
  rows_keyed_by_ip_and_date = (
      rows | 'add vp tags: key by ips and dates' >>
      beam.Map(lambda row: (make_date_ip_key(row), row)).with_output_types(
          Tuple[DateIpKey, Row]))

  # PCollection[Tuple[Tuple[date,ip],Dict[input_name_key,List[Row]]]]
  grouped_metadata_and_rows = (({
      BEAM_COGROUP_A_SIDE: ips_with_metadata,
      BEAM_COGROUP_B_SIDE: rows_keyed_by_ip_and_date
  }) | 'add vp tags: group by keys' >> beam.CoGroupByKey())

  # PCollection[Row]
  rows_with_metadata = (
      grouped_metadata_and_rows | 'add vp tags: merge metadata with rows' >>
      beam.FlatMapTuple(merge_metadata_with_rows).with_output_types(Row))

  return rows_with_metadata


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
  # PCollection[Row]
  rows_with_metadata = _add_vantage_point_tags(rows, tags)

  # Starting with the Satellite v2.2 format, the rows include received ip tags.
  # Received tagging steps are only required prior to v2.2

  # PCollection[Row], PCollection[Row]
  rows_pre_v2_2, rows_v2_2 = (
      rows_with_metadata |
      'partition by date' >> beam.Partition(_get_satellite_date_partition, 2))

  # PCollection[Row]
  rows_pre_v2_2_with_tags = add_received_ip_tags(rows_pre_v2_2, tags)

  # PCollection[Row]
  rows_with_tags = ((rows_pre_v2_2_with_tags, rows_v2_2) |
                    'combine date partitions' >> beam.Flatten())

  # PCollection[Row]
  return rows_with_tags


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
          (row['date'], row['domain']), row)) |
      'partition test and control' >> beam.Partition(
          lambda row, p: int(row[1]['is_control_ip'] or row[1]['anomaly'] is
                             None), 2))

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


def _merge_tagged_answers_with_rows(
    key: str,  # pylint: disable=unused-argument
    value: Dict[str, Union[List[Row], List[List[Row]]]]
) -> Row:
  """
  Args:
    key: roundtrip_id, unused
    value:
      {IP_METADATA_PCOLLECTION_NAME: One element list containing a row
       ROWS_PCOLLECION_NAME: One element list of many element
                             list containing tagged answer rows
      }
      ex:
        { IP_METADATA_PCOLLECTION_NAME: [{
            'ip': '1.2.3.4'
            'domain': 'ex.com'
            'received' : [{
                'ip': '4.5.6.7'
              }, {
                'ip': '5.6.7.8'
              },
            ]
          }],
        ROWS_PCOLLECION_NAME: [[{
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
  row: Row = value[BEAM_COGROUP_A_SIDE][0]  # type: ignore

  if len(value[BEAM_COGROUP_B_SIDE]) == 0:  # No tags
    return row
  tagged_answers: List[Row] = value[BEAM_COGROUP_B_SIDE][0]  # type: ignore

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
  # PCollection[Row]
  received_ip_tags = (
      tags | 'filter out vp tags' >> beam.Filter(
          lambda tag: not _is_vantage_point_tag(tag)).with_output_types(Row))

  # PCollection[Tuple[DateIpKey, row]]
  tags_keyed_by_ip_and_date = (
      received_ip_tags | 'tags: key by ips and dates' >>
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
      BEAM_COGROUP_A_SIDE: tags_keyed_by_ip_and_date,
      BEAM_COGROUP_B_SIDE: received_ips_keyed_by_ip_and_date
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
      BEAM_COGROUP_A_SIDE: rows_keyed_by_roundtrip_id,
      BEAM_COGROUP_B_SIDE: received_ips_grouped_by_roundtrip_ip
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

  # PCollection[Row]
  tag_rows = (
      tag_lines | 'tag rows' >>
      beam.FlatMapTuple(_read_satellite_tags).with_output_types(Row))

  rows_with_metadata = _add_satellite_tags(rows, tag_rows)

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
