"""Beam pipeline helper for converting satellite data."""

from __future__ import absolute_import

import datetime
import json
import logging
import re
from typing import List, Dict, Tuple, Iterator, Iterable
import uuid

import apache_beam as beam

from pipeline.metadata import flatten_base
from pipeline.metadata.beam_metadata import SourceDomainKey, SourceIpKey, DomainSourceIpKey, IP_METADATA_PCOLLECTION_NAME, ROWS_PCOLLECION_NAME, RECEIVED_IPS_PCOLLECTION_NAME, BLOCKPAGE_PCOLLECTION_NAME, make_source_ip_key, make_source_domain_key, make_domain_source_ip_key, merge_metadata_with_rows, merge_satellite_tags_with_answers, merge_tagged_answers_with_rows, merge_page_fetches_with_answers
from pipeline.metadata.schema import SatelliteRow, SatelliteAnswer, SatelliteAnswerWithSourceKey, PageFetchRow, IpMetadata, IpMetadataWithSourceKey, MatchesControl, SCAN_TYPE_PAGE_FETCH
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


def _get_received_ips_with_roundtrip_id_and_source(
    row_with_id: Tuple[str, SatelliteRow]
) -> Iterable[Tuple[SourceIpKey, Tuple[str, SatelliteAnswer]]]:
  """Get all the individual received ips answers from a row.

  Args:
    row_with_id Tuple[roundtrip_id, SatelliteRow]

  Yields individual recieved answers that also include the roundtrip and source
    ex:
      Tuple(
        Tuple('CP_Satellite-2022-01-02-12-00-01', '1.2.3.4')
        Tuple('abc' # roundtrip id
              SatelliteAnswer(ip: '1.2.3.4')))
      Tuple(
        Tuple('CP_Satellite-2022-01-02-12-00-01', '4.5.6.7')
        Tuple('abc' # roundtrip id
              SatelliteAnswer(ip: '4.5.6.7')))
  """
  (roundtrip_id, row) = row_with_id
  source = row.source or ''

  for answer in row.received:
    key: SourceIpKey = (source, answer.ip)
    yield (key, (roundtrip_id, answer))


def _get_received_ips_with_roundtrip_id_and_source_domain(
    row_with_id: Tuple[str, SatelliteRow]
) -> Iterable[Tuple[DomainSourceIpKey, Tuple[str, SatelliteAnswer]]]:
  """Get all the individual received ips answers from a row.

  Args:
    row_with_id Tuple[roundtrip_id, SatelliteRow]

  Yields individual recieved answers that also include the roundtrip and source
    ex:
      Tuple(
        Tuple('example.com', 'CP_Satellite-2022-01-02-12-00-01', '1.2.3.4')
        Tuple('abc' # roundtrip id
              SatelliteAnswer(ip: '1.2.3.4')))
      Tuple(
        Tuple('example.com', 'CP_Satellite-2022-01-02-12-00-01', '4.5.6.7')
        Tuple('abc' # roundtrip id
              SatelliteAnswer(ip: '4.5.6.7')))
  """
  (roundtrip_id, row) = row_with_id
  source = row.source or ''
  domain = row.domain or ''

  for answer in row.received:
    key: DomainSourceIpKey = (domain, source, answer.ip)
    yield (key, (roundtrip_id, answer))


def _read_satellite_resolver_tags(
    filepath: str, line: str) -> Iterator[IpMetadataWithSourceKey]:
  """Read data for IP tagging from Satellite.

    Args:
      filepath: source Satellite file
      line: json str (dictionary containing geo tag data)

  Yields:
      A IpMetadataWithSourceKey of the format
        IpMetadata(
          ip='1.1.1.1',
          source='CP_Satellite-2022-01-02-12-00-01'
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
  source = flatten_base.source_from_filename(filepath)
  tags = IpMetadataWithSourceKey(ip=ip, source=source)

  if 'name' in scan:
    tags.name = scan['name']

  if 'location' in scan:
    tags.country = scan['location']['country_code']
  if 'country' in scan:
    tags.country = country_name_to_code(scan['country'])

  yield tags


def _read_satellite_answer_tags(
    filepath: str, line: str) -> Iterator[SatelliteAnswerWithSourceKey]:
  """Read data for IP tagging from Satellite.

    Args:
      filepath: source Satellite file
      line: json str (dictionary containing geo tag data)

  Yields:
      A SatelliteAnswerMetadata of the format
        SatelliteAnswerMetadata(
          ip='1.1.1.1',
          source='2020-01-01'
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

  answer_with_keys = SatelliteAnswerWithSourceKey(
      ip=scan['ip'],
      source=flatten_base.source_from_filename(filepath),
      http=scan['http'],
      cert=scan['cert'],
      ip_metadata=IpMetadata(
          as_name=scan['asname'],
          asn=scan['asnum'],
      ))
  yield answer_with_keys


def add_vantage_point_tags(
    rows: beam.pvalue.PCollection[SatelliteRow],
    tags: beam.pvalue.PCollection[IpMetadataWithSourceKey]
) -> beam.pvalue.PCollection[SatelliteRow]:
  """Add tags for vantage point IPs - resolver name (hostname/control/special) and country

  Args:
      rows: PCollection of measurement rows
      tags: PCollection of source/ips with geo metadata

    Returns:
      PCollection of measurement rows with tag information added to the ip row
  """
  # PCollection[Tuple[SourceIpKey,IpMetadataWithSourceKey]]
  ips_with_metadata = (
      tags | 'tags key by ips and source' >>
      beam.Map(lambda metadata:
               (make_source_ip_key(metadata), metadata)).with_output_types(
                   Tuple[SourceIpKey, IpMetadataWithSourceKey]))

  # PCollection[Tuple[SourceIpKey,SatelliteRow]]
  rows_keyed_by_ip_and_source = (
      rows | 'add vp tags: key by ips and source' >>
      beam.Map(lambda row: (make_source_ip_key(row), row)).with_output_types(
          Tuple[SourceIpKey, SatelliteRow]))

  # PCollection[Tuple[SourceIpKey,Dict[input_name_key,List[SatelliteRow|IpMetadataWithSourceKey]]]]
  grouped_metadata_and_rows = (({
      IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
      ROWS_PCOLLECION_NAME: rows_keyed_by_ip_and_source
  }) | 'add vp tags: group by keys' >> beam.CoGroupByKey())

  # PCollection[SatelliteRow]
  rows_with_metadata = (
      grouped_metadata_and_rows |
      'add vp tags: merge metadata with rows' >> beam.FlatMapTuple(
          merge_metadata_with_rows).with_output_types(SatelliteRow))

  return rows_with_metadata


def add_satellite_answer_tags(
    rows: beam.pvalue.PCollection[SatelliteRow],
    answer_tags: beam.pvalue.PCollection[SatelliteAnswerWithSourceKey]
) -> beam.pvalue.PCollection[SatelliteRow]:
  """Add tags for resolvers and answer IPs and unflatten the Satellite measurement rows.

    Args:
      rows: PCollection of measurement rows
      answer_tags: PCollection of geo/asn tag info for received ips

    Returns:
      PCollection of measurement rows containing answer tag information
  """
  # Starting with the Satellite v2.2 format, the rows include received ip tags.
  # Received tagging steps are only required prior to v2.2

  # PCollection[SatelliteRow] x2
  rows_pre_v2_2, rows_v2_2 = (
      rows | 'partition by date v2.2' >> beam.Partition(
          _get_satellite_v2p2_date_partition, 2))

  # PCollection[SatelliteRow]
  rows_pre_v2_2_with_tags = add_received_ip_tags(rows_pre_v2_2, answer_tags)

  # PCollection[SatelliteRow]
  rows_with_tags = ((rows_pre_v2_2_with_tags, rows_v2_2) |
                    'combine date partitions v2.2' >> beam.Flatten())

  # PCollection[SatelliteRow]
  return rows_with_tags


def add_received_ip_tags(
    rows: beam.pvalue.PCollection[SatelliteRow],
    tags: beam.pvalue.PCollection[SatelliteAnswerWithSourceKey]
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
  # PCollection[Tuple[SourceIpKey, SatelliteAnswerWithSourceKey]]
  tags_keyed_by_ip_and_source = (
      tags | 'tags: key by ips and source' >>
      beam.Map(lambda tag: (make_source_ip_key(tag), tag)).with_output_types(
          Tuple[SourceIpKey, SatelliteAnswerWithSourceKey]))

  # PCollection[Tuple[roundtrip_id, SatelliteRow]]
  rows_with_roundtrip_id = (
      rows | 'add roundtrip_ids' >> beam.Map(
          _set_random_roundtrip_id).with_output_types(Tuple[str, SatelliteRow]))

  # PCollection[Tuple[SourceIpKey, Tuple[roundtrip_id, SatelliteAnswer]]]
  received_ips_keyed_by_ip_and_source = (
      rows_with_roundtrip_id | 'get received ips' >> beam.FlatMap(
          _get_received_ips_with_roundtrip_id_and_source).with_output_types(
              Tuple[SourceIpKey, Tuple[str, SatelliteAnswer]]))

  grouped_metadata_and_received_ips = (({
      IP_METADATA_PCOLLECTION_NAME: tags_keyed_by_ip_and_source,
      RECEIVED_IPS_PCOLLECTION_NAME: received_ips_keyed_by_ip_and_source
  }) | 'add received ip tags: group by keys' >> beam.CoGroupByKey())

  # PCollection[Tuple[roundtrip_id, SatelliteAnswerWithSourceKey]] received ip row with
  received_ips_with_metadata = (
      grouped_metadata_and_received_ips |
      'add received ip tags: merge tags with answers' >>
      beam.FlatMapTuple(merge_satellite_tags_with_answers).with_output_types(
          Tuple[str, SatelliteAnswerWithSourceKey]))

  # PCollection[Tuple[roundtrip_id, List[Tuple[roundtrip_id, SatelliteAnswerWithSourceKey]]]]
  received_ips_grouped_by_roundtrip_ip = (
      received_ips_with_metadata | 'group received_by roundtrip' >>
      beam.GroupBy(lambda x: x[0]).with_output_types(
          Tuple[str, Iterable[Tuple[str, SatelliteAnswerWithSourceKey]]]))

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


def parse_and_flatten_satellite_rows(
    row_lines: beam.pvalue.PCollection[Tuple[str, str]],
) -> beam.pvalue.PCollection[SatelliteRow]:
  """Process Satellite measurements

  Args:
    row_lines: Tuple[filepath, json_line]

  Returns:
    PCollection[SatelliteRow] of rows
  """
  # PCollection[SatelliteRow]
  rows = (
      row_lines | 'flatten json' >> beam.ParDo(
          flatten.FlattenMeasurement()).with_output_types(SatelliteRow))
  return rows


def parse_satellite_resolver_tags(
    resolver_lines: beam.pvalue.PCollection[Tuple[str, str]]
) -> beam.pvalue.PCollection[IpMetadataWithSourceKey]:
  """Process Satellite resolver tags.

  Args:
    resolver_lines: Tuple[filepath, json_line]

  Returns:
    PCollection[IpMetadataWithSourceKey] of resolver tags
  """
  # PCollection[IpMetadataWithSourceKey]
  resolver_tags = (
      resolver_lines |
      'resolver tag rows' >> beam.FlatMapTuple(_read_satellite_resolver_tags).
      with_output_types(IpMetadataWithSourceKey))

  return resolver_tags


def parse_satellite_answer_tags(
    answer_lines: beam.pvalue.PCollection[Tuple[str, str]]
) -> beam.pvalue.PCollection[SatelliteAnswerWithSourceKey]:
  """Process Satellite resolver tags.

  Args:
    answer_lines: Tuple[filepath, json_line]

  Returns:
    PCollection[SatelliteAnswerWithSourceKey] of answer tags
  """
  # PCollection[SatelliteAnswerWithSourceKey]
  answer_tags = (
      answer_lines |
      'answer tag rows' >> beam.FlatMapTuple(_read_satellite_answer_tags).
      with_output_types(SatelliteAnswerWithSourceKey))

  return answer_tags


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

  # PCollection[Tuple[DomainSourceIpKey, PageFetchRow]
  page_fetches_keyed = (
      page_fetches | 'key page fetch by domain/source/ip' >> beam.Map(
          lambda row: (make_domain_source_ip_key(row), row)).with_output_types(
              Tuple[DomainSourceIpKey, PageFetchRow]))

  # PCollection[Tuple[roundtrip_id, SatelliteRow]]
  rows_with_roundtrip_id = (
      rows_v2 | 'add roundtrip_ids: adding page fetch' >> beam.Map(
          _set_random_roundtrip_id).with_output_types(Tuple[str, SatelliteRow]))

  # PCollection[Tuple[DomainSourceIpKey, Tuple[roundtrip_id, SatelliteAnswer]]]
  received_ips_keyed_by_ip_domain_and_source = (
      rows_with_roundtrip_id | 'get received ips: adding page fetch' >>
      beam.FlatMap(_get_received_ips_with_roundtrip_id_and_source_domain).
      with_output_types(Tuple[DomainSourceIpKey, Tuple[str, SatelliteAnswer]]))

  # cogroup
  grouped_page_fetches_and_answers = (({
      BLOCKPAGE_PCOLLECTION_NAME: page_fetches_keyed,
      RECEIVED_IPS_PCOLLECTION_NAME: received_ips_keyed_by_ip_domain_and_source
  }) | 'cogroup answers and page fetches' >> beam.CoGroupByKey())

  # add page fetches into satellite answers
  # PCollection[Tuple[roundtrip_id, SatelliteAnswerWithSourceKey]]
  received_ips_with_page_fetches = (
      grouped_page_fetches_and_answers |
      'add page fetches : merge page fetches with answers' >>
      beam.FlatMapTuple(merge_page_fetches_with_answers).with_output_types(
          Tuple[str, SatelliteAnswerWithSourceKey]))

  # PCollection[Tuple[roundtrip_id, List[Tuple[roundtrip_id, SatelliteAnswerWithSourceKey]]]]
  received_ips_grouped_by_roundtrip_ip = (
      received_ips_with_page_fetches | 'group answers by roundtrip' >>
      beam.GroupBy(lambda x: x[0]).with_output_types(
          Tuple[str, Iterable[Tuple[str, SatelliteAnswerWithSourceKey]]]))

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


def parse_satellite_page_fetches(
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


def process_satellite_lines(
    lines: beam.pvalue.PCollection[Tuple[str, str]],
    metadata_adder: MetadataAdder) -> beam.pvalue.PCollection[SatelliteRow]:
  """Process both satellite and page fetch data files.

  Args:
    lines: input lines from all satellite files. Tuple[filename, line]

  Returns:
    post_processed_satellite: rows of satellite scan data
  """
  # Read in file data

  # PCollection[Tuple[filename,line]] x4
  answer_lines, resolver_lines, page_fetch_lines, row_lines = lines | beam.Partition(
      partition_satellite_input, NUM_SATELLITE_INPUT_PARTITIONS)

  # Parse all file data into schema pcollections

  # PCollection[SatelliteRow]
  rows = parse_and_flatten_satellite_rows(row_lines)

  # PCollection[IpMetadataWithSourceKey]
  resolver_tags = parse_satellite_resolver_tags(resolver_lines)

  # PCollection[SatelliteAnswerWithSourceKey]
  answer_tags = parse_satellite_answer_tags(answer_lines)

  # PCollection[BlockpageRow]
  page_fetch_rows = parse_satellite_page_fetches(page_fetch_lines)

  # Join auxiliary file data onto main row data

  # - join data onto resolver ips

  # PCollection[SatelliteRow]
  resolver_tagged_satellite = add_vantage_point_tags(rows, resolver_tags)

  # PCollection[SatelliteRow]
  rows_with_resolver_ip_annotations = metadata_adder.annotate_row_ip(
      resolver_tagged_satellite)

  # - join data onto answer ips

  # PCollection[SatelliteRow]
  answer_tagged_satellite = add_satellite_answer_tags(
      rows_with_resolver_ip_annotations, answer_tags)

  # PCollection[SatelliteRow]
  rows_with_answer_ip_annotations = metadata_adder.annotate_answer_ips(
      answer_tagged_satellite)

  # PCollection[SatelliteRow]
  satellite_with_page_fetches = add_page_fetch_to_answers(
      rows_with_answer_ip_annotations, page_fetch_rows)

  return satellite_with_page_fetches
