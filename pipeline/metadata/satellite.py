"""Beam pipeline helper for converting satellite data."""

from __future__ import absolute_import

import datetime
import json
import logging
import re
from typing import List, Dict, Tuple, Iterator, Iterable
import uuid

import apache_beam as beam

from pipeline.metadata.beam_metadata import DateIpKey, DateDomainKey, DomainDateIpKey, IP_METADATA_PCOLLECTION_NAME, ROWS_PCOLLECION_NAME, RECEIVED_IPS_PCOLLECTION_NAME, BLOCKPAGE_PCOLLECTION_NAME, make_date_ip_key, make_date_domain_key, make_domain_date_ip_key, merge_metadata_with_rows, merge_satellite_tags_with_answers, merge_tagged_answers_with_rows, merge_page_fetches_with_answers
from pipeline.metadata.schema import SatelliteRow, SatelliteAnswer, SatelliteAnswerWithKeys, PageFetchRow, IpMetadata, IpMetadataWithKeys, MatchesControl, SCAN_TYPE_PAGE_FETCH
from pipeline.metadata.lookup_country_code import country_name_to_code
from pipeline.metadata import flatten_satellite
from pipeline.metadata import flatten
from pipeline.metadata.add_metadata import MetadataAdder

# Data files for the Satellite pipeline
SATELLITE_RESOLVERS_FILE = 'resolvers.json'  #v1, v2.2

SATELLITE_RESULTS_FILE = 'results.json'  # v2.1, v2.2

SATELLITE_TAGGED_ANSWERS_FILE = 'tagged_answers.json'  # v1
SATELLITE_INTERFERENCE_FILE = 'interference.json'  # v1
SATELLITE_INTERFERENCE_ERR_FILE = 'interference_err.json'  # v1
SATELLITE_ANSWERS_ERR_FILE = 'answers_err.json'  # v1

SATELLITE_TAGGED_RESOLVERS_FILE = 'tagged_resolvers.json'  # v2.1

SATELLITE_TAGGED_RESPONSES = 'tagged_responses.json'  # v2.1

SATELLITE_BLOCKPAGES_FILE = 'blockpages.json'  # v2.2

# Files containing metadata for satellite DNS resolvers
SATELLITE_RESOLVER_TAG_FILES = [
    SATELLITE_RESOLVERS_FILE, SATELLITE_TAGGED_RESOLVERS_FILE
]

# Files containing metadata for satellite receives answer ips
SATELLITE_ANSWER_TAG_FILES = [
    SATELLITE_TAGGED_ANSWERS_FILE, SATELLITE_TAGGED_RESPONSES
]

# Files containing satellite ip metadata
SATELLITE_TAG_FILES = SATELLITE_RESOLVER_TAG_FILES + SATELLITE_ANSWER_TAG_FILES

# Files containing satellite DNS tests
SATELLITE_OBSERVATION_FILES = [
    SATELLITE_INTERFERENCE_FILE, SATELLITE_INTERFERENCE_ERR_FILE,
    flatten_satellite.SATELLITE_ANSWERS_CONTROL_FILE,
    flatten_satellite.SATELLITE_RESPONSES_CONTROL_FILE, SATELLITE_RESULTS_FILE,
    SATELLITE_ANSWERS_ERR_FILE
]

# All satellite files
SATELLITE_FILES = (
    SATELLITE_TAG_FILES + [SATELLITE_BLOCKPAGES_FILE] +
    SATELLITE_OBSERVATION_FILES)

CDN_REGEX = re.compile("AMAZON|Akamai|OPENDNS|CLOUDFLARENET|GOOGLE")
NUM_SATELLITE_INPUT_PARTITIONS = 4


def _get_satellite_v2_date_partition(row: SatelliteRow, _: int) -> int:
  """Partition Satellite data by date, corresponding to v2 format change.

  0 = v1 partition
  1 = v2.1 and v2.2 partition
  """
  date = row.date
  if date is None:
    raise Exception(f"Satellite row with unfilled date field: %{row}")

  if datetime.date.fromisoformat(
      date) < flatten_satellite.SATELLITE_V2_1_START_DATE:
    return 0
  return 1


def _get_satellite_v2p2_date_partition(row: SatelliteRow, _: int) -> int:
  """Partition Satellite data by date, corresponding to v2.2 format change.

  0 = v1 to v2.1 partition
  1 = v2.2 partition
  """
  date = row.date
  if date is None:
    raise Exception(f"Satellite row with unfilled date field: %{row}")

  if datetime.date.fromisoformat(
      date) < flatten_satellite.SATELLITE_V2_2_START_DATE:
    return 0
  return 1


def _set_random_roundtrip_id(row: SatelliteRow) -> Tuple[str, SatelliteRow]:
  """Add a roundtrip_id field to a row."""
  roundtrip_id = uuid.uuid4().hex
  return (roundtrip_id, row)


def get_blockpage_table_name(table_name: str, scan_type: str) -> str:
  """Returns an additional table name for writing the blockpage table.

  Args:
    table_name: dataset.table name like 'base.satellite_scan'
    scan_type: 'satellite'

  Returns:
    name like 'base.satellite_blockpage_scan'
  """
  return table_name.replace(f'.{scan_type}',
                            f'.{scan_type}_{SCAN_TYPE_PAGE_FETCH}')


def _get_received_ips_with_roundtrip_id_and_date(
    row_with_id: Tuple[str, SatelliteRow]
) -> Iterable[Tuple[DateIpKey, Tuple[str, SatelliteAnswer]]]:
  """Get all the individual received ips answers from a row.

  Args:
    row_with_id Tuple[roundtrip_id, SatelliteRow]

  Yields individual recieved answers that also include the roundtrip and date
    ex:
      Tuple(
        Tuple('2021-01-01', '1.2.3.4')
        Tuple('abc' # roundtrip id
              SatelliteAnswer(ip: '1.2.3.4')))
      Tuple(
        Tuple('2021-01-01', '4.5.6.7')
        Tuple('abc' # roundtrip id
              SatelliteAnswer(ip: '4.5.6.7')))
  """
  (roundtrip_id, row) = row_with_id
  date = row.date or ''

  for answer in row.received:
    key: DateIpKey = (date, answer.ip)
    yield (key, (roundtrip_id, answer))


def _get_received_ips_with_roundtrip_id_and_date_domain(
    row_with_id: Tuple[str, SatelliteRow]
) -> Iterable[Tuple[DomainDateIpKey, Tuple[str, SatelliteAnswer]]]:
  """Get all the individual received ips answers from a row.

  Args:
    row_with_id Tuple[roundtrip_id, SatelliteRow]

  Yields individual recieved answers that also include the roundtrip and date
    ex:
      Tuple(
        Tuple('2021-01-01', 'example.com', '1.2.3.4')
        Tuple('abc' # roundtrip id
              SatelliteAnswer(ip: '1.2.3.4')))
      Tuple(
        Tuple('2021-01-01', 'example.com', '4.5.6.7')
        Tuple('abc' # roundtrip id
              SatelliteAnswer(ip: '4.5.6.7')))
  """
  (roundtrip_id, row) = row_with_id
  date = row.date or ''
  domain = row.domain or ''

  for answer in row.received:
    key: DomainDateIpKey = (domain, date, answer.ip)
    yield (key, (roundtrip_id, answer))


def _read_satellite_resolver_tags(filepath: str,
                                  line: str) -> Iterator[IpMetadataWithKeys]:
  """Read data for IP tagging from Satellite.

    Args:
      filepath: source Satellite file
      line: json str (dictionary containing geo tag data)

  Yields:
      A IpMetadataWithKeys of the format
        IpMetadata(
          ip='1.1.1.1',
          date='2020-01-01'
          country='US'  # optional
          name='one.one.one.one'  # optional
        )
  """
  try:
    scan = json.loads(line)
  except json.decoder.JSONDecodeError as e:
    logging.warning('JSONDecodeError: %s\nFilename: %s\n%s\n', e, filepath,
                    line)
    return

  ip: str = scan.get('resolver') or scan.get('vp')
  date = re.findall(r'\d\d\d\d-\d\d-\d\d', filepath)[0]
  tags = IpMetadataWithKeys(ip=ip, date=date)

  if 'name' in scan:
    tags.name = scan['name']

  if 'location' in scan:
    tags.country = scan['location']['country_code']
  if 'country' in scan:
    tags.country = country_name_to_code(scan['country'])

  yield tags


def _read_satellite_answer_tags(filepath: str,
                                line: str) -> Iterator[SatelliteAnswerWithKeys]:
  """Read data for IP tagging from Satellite.

    Args:
      filepath: source Satellite file
      line: json str (dictionary containing geo tag data)

  Yields:
      A SatelliteAnswerMetadata of the format
        SatelliteAnswerMetadata(
          ip='1.1.1.1',
          date='2020-01-01'
          http='e3c1d3...' # optional
          cert='a2fed1...' # optional
          asname='CLOUDFLARENET' # optional
          asnum=13335 # optional
        )
  """
  try:
    scan = json.loads(line)
  except json.decoder.JSONDecodeError as e:
    logging.warning('JSONDecodeError: %s\nFilename: %s\n%s\n', e, filepath,
                    line)
    return

  answer_with_keys = SatelliteAnswerWithKeys(
      ip=scan['ip'],
      date=re.findall(r'\d\d\d\d-\d\d-\d\d', filepath)[0],
      http=scan['http'],
      cert=scan['cert'],
      ip_metadata=IpMetadata(
          as_name=scan['asname'],
          asn=scan['asnum'],
      ))
  yield answer_with_keys


def _add_vantage_point_tags(
    rows: beam.pvalue.PCollection[SatelliteRow],
    tags: beam.pvalue.PCollection[IpMetadataWithKeys]
) -> beam.pvalue.PCollection[SatelliteRow]:
  """Add tags for vantage point IPs - resolver name (hostname/control/special) and country

  Args:
      rows: PCollection of measurement rows
      ips_with_metadata: PCollection of dated ips with geo metadata

    Returns:
      PCollection of measurement rows with tag information added to the ip row
  """
  # PCollection[Tuple[DateIpKey,IpMetadataWithKeys]]
  ips_with_metadata = (
      tags | 'tags key by ips and dates' >>
      beam.Map(lambda metadata:
               (make_date_ip_key(metadata), metadata)).with_output_types(
                   Tuple[DateIpKey, IpMetadataWithKeys]))

  # PCollection[Tuple[DateIpKey,SatelliteRow]]
  rows_keyed_by_ip_and_date = (
      rows | 'add vp tags: key by ips and dates' >>
      beam.Map(lambda row: (make_date_ip_key(row), row)).with_output_types(
          Tuple[DateIpKey, SatelliteRow]))

  # PCollection[Tuple[Tuple[date,ip],Dict[input_name_key,List[SatelliteRow|IpMetadataWithKeys]]]]
  grouped_metadata_and_rows = (({
      IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
      ROWS_PCOLLECION_NAME: rows_keyed_by_ip_and_date
  }) | 'add vp tags: group by keys' >> beam.CoGroupByKey())

  # PCollection[SatelliteRow]
  rows_with_metadata = (
      grouped_metadata_and_rows |
      'add vp tags: merge metadata with rows' >> beam.FlatMapTuple(
          merge_metadata_with_rows).with_output_types(SatelliteRow))

  return rows_with_metadata


def _add_satellite_tags(
    rows: beam.pvalue.PCollection[SatelliteRow],
    resolver_tags: beam.pvalue.PCollection[IpMetadataWithKeys],
    answer_tags: beam.pvalue.PCollection[SatelliteAnswerWithKeys]
) -> beam.pvalue.PCollection[SatelliteRow]:
  """Add tags for resolvers and answer IPs and unflatten the Satellite measurement rows.

    Args:
      rows: PCollection of measurement rows
      resolver_tags: PCollection of tag info for resolvers
      answer_tags: PCollection of geo/asn tag info for received ips

    Returns:
      PCollection of measurement rows containing tag information
  """
  # PCollection[SatelliteRow]
  rows_with_metadata = _add_vantage_point_tags(rows, resolver_tags)

  # Starting with the Satellite v2.2 format, the rows include received ip tags.
  # Received tagging steps are only required prior to v2.2

  # PCollection[SatelliteRow] x2
  rows_pre_v2_2, rows_v2_2 = (
      rows_with_metadata | 'partition by date v2.2' >> beam.Partition(
          _get_satellite_v2p2_date_partition, 2))

  # PCollection[SatelliteRow]
  rows_pre_v2_2_with_tags = add_received_ip_tags(rows_pre_v2_2, answer_tags)

  # PCollection[SatelliteRow]
  rows_with_tags = ((rows_pre_v2_2_with_tags, rows_v2_2) |
                    'combine date partitions v2.2' >> beam.Flatten())

  # PCollection[SatelliteRow]
  return rows_with_tags


def _total_tags(key: DateDomainKey,
                row: SatelliteRow) -> Tuple[DateDomainKey, int]:
  total_tags = 0
  for ans in row.received:
    tag_values = [
        ans.http, ans.cert, ans.ip_metadata.asn, ans.ip_metadata.as_name
    ]
    non_empty_tag_values = [value for value in tag_values if value is not None]
    total_tags = len(list(non_empty_tag_values))
  return (key, total_tags)


def _partition_test_and_controls(row_tuple: Tuple[DateDomainKey, SatelliteRow],
                                 _: int) -> int:
  """Partition tests from domain and ip controls

  Returns
    0 = domain control
    1 = ip control
    2 = test measurement
  """
  row: SatelliteRow = row_tuple[1]
  if row.is_control:
    return 0
  # 'anomaly' is None for control measurements
  if row.is_control_ip or row.anomaly is None:
    return 1
  return 2


def _take_max_ctag(
    ctag: Tuple[DateDomainKey, List[int]]) -> Tuple[DateDomainKey, int]:
  (key, ctags) = ctag
  max_ctag = max(ctags)
  return (key, max_ctag)


def _append_num_controls(
    key_value: Tuple[DateDomainKey, SatelliteRow],
    max_num_ctags: Dict[DateDomainKey, int]) -> Tuple[SatelliteRow, int]:
  key, row = key_value
  if key in max_num_ctags:
    num_ctags = max_num_ctags[key]
    return (row, num_ctags)
  return (row, 0)


def post_processing_satellite(
    rows: beam.pvalue.PCollection[SatelliteRow]
) -> beam.pvalue.PCollection[SatelliteRow]:
  """Run post processing on Satellite v1 data (calculate confidence, verify interference).

    Args:
      rows: PCollection of measurement rows

    Returns:
      PCollection of measurement rows with confidence and verify fields
  """
  # PCollection[Tuple[DateDomainKey, SatelliteRow]]
  rows_keyed_by_date_domains = (
      rows | 'key by dates and domains' >>
      beam.Map(lambda row: (make_date_domain_key(row), row)).with_output_types(
          Tuple[DateDomainKey, SatelliteRow]))

  # PCollection[Tuple[DateDomainKey, SatelliteRow]] x3
  domain_controls, ip_controls, tests = (
      rows_keyed_by_date_domains | 'partition test and control' >>
      beam.Partition(_partition_test_and_controls, 3).with_output_types(
          Tuple[DateDomainKey, SatelliteRow]))

  # PCollection[Tuple[DateDomainKey, int]]
  num_ctags = (
      ip_controls | 'calculate # control tags' >>
      beam.MapTuple(_total_tags).with_output_types(Tuple[DateDomainKey, int]))

  # PCollection[Tuple[DateDomainKey, List[int]]]
  grouped_num_ctags = (
      num_ctags | 'group_ctags' >> beam.GroupByKey().with_output_types(
          Tuple[DateDomainKey, List]))

  # PCollection[Tuple[DateDomainKey, int]]
  max_num_ctags = (
      grouped_num_ctags | 'take max ctag' >>
      beam.Map(_take_max_ctag).with_output_types(Tuple[DateDomainKey, int]))

  # SideInputData[PCollection[Tuple[DateDomainKey, int]]
  max_num_ctags_dict = beam.pvalue.AsDict(max_num_ctags)

  # PCollection[Tuple[SatelliteRow, num_controls]]
  rows_with_num_controls = (
      tests | 'append num controls' >> beam.Map(
          _append_num_controls,
          max_num_ctags=max_num_ctags_dict).with_output_types(
              Tuple[SatelliteRow, int]))

  # PCollection[SatelliteRow]
  confidence = (
      rows_with_num_controls | 'calculate confidence' >>
      beam.MapTuple(_calculate_confidence).with_output_types(SatelliteRow))

  # PCollection[SatelliteRow]
  verified = (
      confidence |
      'verify interference' >> beam.Map(_verify).with_output_types(SatelliteRow)
  )

  # PCollection[SatelliteRow]
  # pylint: disable=no-value-for-parameter
  ip_controls = (
      ip_controls |
      'unkey ip controls' >> beam.Values().with_output_types(SatelliteRow))

  # PCollection[SatelliteRow]
  # pylint: disable=no-value-for-parameter
  domain_controls = (
      domain_controls |
      'unkey domain controls' >> beam.Values().with_output_types(SatelliteRow))

  # PCollection[SatelliteRow]
  post = ((verified, ip_controls, domain_controls) |
          'flatten test and controls' >> beam.Flatten())

  return post


def add_received_ip_tags(
    rows: beam.pvalue.PCollection[SatelliteRow],
    tags: beam.pvalue.PCollection[SatelliteAnswerWithKeys]
) -> beam.pvalue.PCollection[SatelliteRow]:
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
  # PCollection[Tuple[DateIpKey, SatelliteAnswerWithKeys]]
  tags_keyed_by_ip_and_date = (
      tags | 'tags: key by ips and dates' >>
      beam.Map(lambda tag: (make_date_ip_key(tag), tag)).with_output_types(
          Tuple[DateIpKey, SatelliteAnswerWithKeys]))

  # PCollection[Tuple[roundtrip_id, SatelliteRow]]
  rows_with_roundtrip_id = (
      rows | 'add roundtrip_ids' >> beam.Map(
          _set_random_roundtrip_id).with_output_types(Tuple[str, SatelliteRow]))

  # PCollection[Tuple[DateIpKey, Tuple[roundtrip_id, SatelliteAnswer]]]
  received_ips_keyed_by_ip_and_date = (
      rows_with_roundtrip_id | 'get received ips' >> beam.FlatMap(
          _get_received_ips_with_roundtrip_id_and_date).with_output_types(
              Tuple[DateIpKey, Tuple[str, SatelliteAnswer]]))

  grouped_metadata_and_received_ips = (({
      IP_METADATA_PCOLLECTION_NAME: tags_keyed_by_ip_and_date,
      RECEIVED_IPS_PCOLLECTION_NAME: received_ips_keyed_by_ip_and_date
  }) | 'add received ip tags: group by keys' >> beam.CoGroupByKey())

  # PCollection[Tuple[roundtrip_id, SatelliteAnswerWithKeys]] received ip row with
  received_ips_with_metadata = (
      grouped_metadata_and_received_ips |
      'add received ip tags: merge tags with answers' >>
      beam.FlatMapTuple(merge_satellite_tags_with_answers).with_output_types(
          Tuple[str, SatelliteAnswerWithKeys]))

  # PCollection[Tuple[roundtrip_id, List[Tuple[roundtrip_id, SatelliteAnswerWithKeys]]]]
  received_ips_grouped_by_roundtrip_ip = (
      received_ips_with_metadata | 'group received_by roundtrip' >>
      beam.GroupBy(lambda x: x[0]).with_output_types(
          Tuple[str, Iterable[Tuple[str, SatelliteAnswerWithKeys]]]))

  grouped_rows_and_received_ips = (({
      ROWS_PCOLLECION_NAME: rows_with_roundtrip_id,
      RECEIVED_IPS_PCOLLECTION_NAME: received_ips_grouped_by_roundtrip_ip
  }) | 'add received ip tags: group by roundtrip' >> beam.CoGroupByKey())

  # PCollection[SatelliteRow] received ip row with roundtrip id
  rows_with_metadata = (
      grouped_rows_and_received_ips |
      'add received ip tags: add tagged answers back' >> beam.MapTuple(
          merge_tagged_answers_with_rows).with_output_types(SatelliteRow))

  return rows_with_metadata


def process_satellite_with_tags(
    row_lines: beam.pvalue.PCollection[Tuple[str, str]],
    answer_lines: beam.pvalue.PCollection[Tuple[str, str]],
    resolver_lines: beam.pvalue.PCollection[Tuple[str, str]]
) -> beam.pvalue.PCollection[SatelliteRow]:
  """Process Satellite measurements and tags.

  Args:
    row_lines: Tuple[filepath, json_line]
    answer_lines: Tuple[filepath, json_line]
    resolver_lines: Tuple[filepath, json_line]

  Returns:
    PCollection[SatelliteRow] of rows with tag metadata added
  """
  # PCollection[SatelliteRow]
  rows = (
      row_lines | 'flatten json' >> beam.ParDo(
          flatten.FlattenMeasurement()).with_output_types(SatelliteRow))

  # PCollection[IpMetadataWithKeys]
  resolver_tags = (
      resolver_lines | 'resolver tag rows' >> beam.FlatMapTuple(
          _read_satellite_resolver_tags).with_output_types(IpMetadataWithKeys))

  # PCollection[SatelliteAnswerWithKeys]
  answer_tags = (
      answer_lines |
      'answer tag rows' >> beam.FlatMapTuple(_read_satellite_answer_tags).
      with_output_types(SatelliteAnswerWithKeys))

  rows_with_metadata = _add_satellite_tags(rows, resolver_tags, answer_tags)

  return rows_with_metadata


def add_page_fetch_to_answers(
    rows: beam.pvalue.PCollection[SatelliteRow],
    page_fetches: beam.pvalue.PCollection[PageFetchRow]
) -> beam.pvalue.PCollection[SatelliteRow]:
  """Add page fetch rows onto Satellite Answers

  Args:
    rows: Satellite Rows
    page_fetches: PageFetch Rows

  Returns:
    SatelliteRows with page fetch info added to the received answers
  """
  # Page fetches only exist for v2 data
  # PCollection[SatelliteRow] x2
  rows_v1, rows_v2 = (
      rows | 'partition by date v2' >> beam.Partition(
          _get_satellite_v2_date_partition, 2))

  # PCollection[Tuple[DomainDateIpKey, PageFetchRow]
  page_fetches_keyed = (
      page_fetches | 'key page fetch by domain/date/ip' >> beam.Map(
          lambda row: (make_domain_date_ip_key(row), row)).with_output_types(
              Tuple[DomainDateIpKey, PageFetchRow]))

  # PCollection[Tuple[roundtrip_id, SatelliteRow]]
  rows_with_roundtrip_id = (
      rows_v2 | 'add roundtrip_ids: adding page fetch' >> beam.Map(
          _set_random_roundtrip_id).with_output_types(Tuple[str, SatelliteRow]))

  # PCollection[Tuple[DomainDateIpKey, Tuple[roundtrip_id, SatelliteAnswer]]]
  received_ips_keyed_by_ip_domain_and_date = (
      rows_with_roundtrip_id | 'get received ips: adding page fetch' >>
      beam.FlatMap(_get_received_ips_with_roundtrip_id_and_date_domain).
      with_output_types(Tuple[DomainDateIpKey, Tuple[str, SatelliteAnswer]]))

  # cogroup
  grouped_page_fetches_and_answers = (({
      BLOCKPAGE_PCOLLECTION_NAME: page_fetches_keyed,
      RECEIVED_IPS_PCOLLECTION_NAME: received_ips_keyed_by_ip_domain_and_date
  }) | 'cogroup answers and page fetches' >> beam.CoGroupByKey())

  # add page fetches into satellite answers
  # PCollection[Tuple[roundtrip_id, SatelliteAnswerWithKeys]]
  received_ips_with_page_fetches = (
      grouped_page_fetches_and_answers |
      'add page fetches : merge page fetches with answers' >>
      beam.FlatMapTuple(merge_page_fetches_with_answers).with_output_types(
          Tuple[str, SatelliteAnswerWithKeys]))

  # PCollection[Tuple[roundtrip_id, List[Tuple[roundtrip_id, SatelliteAnswerWithKeys]]]]
  received_ips_grouped_by_roundtrip_ip = (
      received_ips_with_page_fetches | 'group answers by roundtrip' >>
      beam.GroupBy(lambda x: x[0]).with_output_types(
          Tuple[str, Iterable[Tuple[str, SatelliteAnswerWithKeys]]]))

  grouped_rows_and_received_ips = (({
      ROWS_PCOLLECION_NAME: rows_with_roundtrip_id,
      RECEIVED_IPS_PCOLLECTION_NAME: received_ips_grouped_by_roundtrip_ip
  }) | 'cogroup by roundtrip id' >> beam.CoGroupByKey())

  # PCollection[SatelliteRow]
  rows_with_page_fetches = (
      grouped_rows_and_received_ips |
      'add blockpages: add tagged answers back' >> beam.MapTuple(
          merge_tagged_answers_with_rows).with_output_types(SatelliteRow))

  # PCollection[SatelliteRow]
  all_rows = ((rows_v1, rows_with_page_fetches) |
              'combine date partitions v2' >> beam.Flatten())

  return all_rows


def process_satellite_page_fetches(
    page_fetches: beam.pvalue.PCollection[Tuple[str, str]]
) -> beam.pvalue.PCollection[PageFetchRow]:
  """Process Satellite measurements and tags.

  Args:
    page_fetches: (filepath, Row) objects

  Returns:
    PCollection[PageFetchRow] of page fetch rows in bigquery format
  """
  rows = (
      page_fetches | 'flatten page fetches' >> beam.ParDo(
          flatten_satellite.FlattenBlockpages()).with_output_types(PageFetchRow)
  )
  return rows


def partition_satellite_input(
    line: Tuple[str, str],
    num_partitions: int = NUM_SATELLITE_INPUT_PARTITIONS) -> int:
  """Partitions Satellite input into answer/resolver_tags, page fetches and rows.

  Args:
    line: an input line Tuple[filepath, line_content]
      filepath is "<path>/name.json"
    num_partitions: number of partitions to use, always 4

  Returns:
    int,
      0 - line is an answer tag file
      1 - line is a resolver tag file
      2 - line is a page fetch
      3 - line is a dns measurement or control

  Raises:
    Exception if num_partitions != NUM_SATELLITE_INPUT_PARTITIONS
    Exception if an unknown filename is used
  """
  if num_partitions != NUM_SATELLITE_INPUT_PARTITIONS:
    raise Exception(
        "Bad input number of partitions; always use NUM_SATELLITE_INPUT_PARTITIONS."
    )

  filename = flatten_satellite.get_filename(line[0])
  if filename in SATELLITE_ANSWER_TAG_FILES:
    return 0
  if filename in SATELLITE_RESOLVER_TAG_FILES:
    return 1
  if filename == SATELLITE_BLOCKPAGES_FILE:
    return 2
  if filename in SATELLITE_OBSERVATION_FILES:
    return 3
  raise Exception(f"Unknown filename in Satellite data: {filename}")


def _calculate_confidence(scan: SatelliteRow,
                          num_control_tags: int) -> SatelliteRow:
  """Calculate confidence for a Satellite measurement.

    Args:
      scan: SatelliteRow containing measurement data
      control_tags: dict containing control tags for the test domain

    Returns:
      scan SatelliteRow with new records:
        'average_confidence':
          average percentage of tags that match control queries
        'untagged_controls': True if all control IPs have no tags
        'untagged_response': True if all answer IPs have no tags

        'match_confidence' in answer records, percentage match per answer IP
  """
  scan.average_confidence = 0
  matches_confidence = []
  scan.untagged_controls = num_control_tags == 0
  scan.untagged_response = True

  for answer in scan.received:
    # check tags for each answer IP
    matches_control = answer.matches_control or MatchesControl()
    total_tags = 0
    matching_tags = 0

    answer_kvs = {
        'http': answer.http,
        'cert': answer.cert,
        'asnum': answer.ip_metadata.asn,
        'asname': answer.ip_metadata.as_name
    }
    non_empty_answer_keys = [
        key for (key, value) in answer_kvs.items() if value is not None
    ]
    # calculate number of tags IP has and how many match controls
    for tag in non_empty_answer_keys:
      total_tags += 1

    matches_control_values = [
        matches_control.http, matches_control.cert, matches_control.asnum,
        matches_control.asname
    ]
    matching_tags = len(
        [value for value in matches_control_values if value is True])

    if total_tags > 0:
      # at least one answer IP has tags
      scan.untagged_response = False

    # calculate percentage of matching tags
    if matches_control.ip:
      # ip is in control response
      ip_match = 100.0
    else:
      if total_tags == 0:
        ip_match = 0.0
      else:
        ip_match = matching_tags * 100 / total_tags
    answer.match_confidence = ip_match
    matches_confidence.append(ip_match)

  if len(matches_confidence) > 0:
    scan.average_confidence = sum(matches_confidence) / len(matches_confidence)
  # Sanity check for untagged responses: do not claim interference
  if scan.untagged_response:
    scan.anomaly = False
  return scan


def _verify(scan: SatelliteRow) -> SatelliteRow:
  """Verify that a Satellite measurement with interference is not a false positive.

    Args:
      scan: SatelliteRow containing measurement data

    Returns:
      scan SatelliteRow with new records:
        'excluded': bool, equals true if interference is a false positive
        'exclude_reason': string, reason(s) for false positive
  """
  scan.excluded = None
  scan.exclude_reason = None

  if scan.anomaly:
    scan.excluded = False
    reasons = []
    # Check received IPs for false positive reasons
    for received in scan.received:
      asname = received.ip_metadata.as_name
      if asname and CDN_REGEX.match(asname):
        # CDN IPs
        scan.excluded = True
        reasons.append('is_CDN')
    scan.exclude_reason = ' '.join(reasons)
  return scan


def process_satellite_lines(
    lines: beam.pvalue.PCollection[Tuple[str, str]],
    metadata_adder: MetadataAdder) -> beam.pvalue.PCollection[SatelliteRow]:
  """Process both satellite and page fetch data files.

  Args:
    lines: input lines from all satellite files. Tuple[filename, line]

  Returns:
    post_processed_satellite: rows of satellite scan data
  """
  # PCollection[Tuple[filename,line]] x4
  answer_lines, resolver_lines, page_fetch_lines, row_lines = lines | beam.Partition(
      partition_satellite_input, NUM_SATELLITE_INPUT_PARTITIONS)

  # PCollection[SatelliteRow]
  tagged_satellite = process_satellite_with_tags(row_lines, answer_lines,
                                                 resolver_lines)

  # PCollection[BlockpageRow]
  page_fetch_rows = process_satellite_page_fetches(page_fetch_lines)

  # PCollection[SatelliteRow]
  satellite_with_page_fetches = add_page_fetch_to_answers(
      tagged_satellite, page_fetch_rows)

  # PCollection[SatelliteRow]
  post_processed_satellite = post_processing_satellite(
      satellite_with_page_fetches)

  rows_with_metadata = metadata_adder.add_metadata(post_processed_satellite)

  return rows_with_metadata
