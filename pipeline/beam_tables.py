# Copyright 2020 Jigsaw Operations LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Beam pipeline for converting json scan files into bigquery tables."""

from __future__ import absolute_import

import datetime
import json
import logging
import os
import re
from typing import Optional, Tuple, Dict, List, Any, Iterator, Iterable, Union
import uuid

import apache_beam as beam
import geoip2.database
from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import bigquery as cloud_bigquery
from pipeline.assets import FALSE_POSITIVES, BLOCKPAGES, MAXMIND_CITY, MAXMIND_ASN, COUNTRY_CODES

# Custom Types
#
# All or part of a scan row to be written to bigquery
# ex (only scan data): {'domain': 'test.com', 'ip': '1.2.3.4', 'success' true}
# ex (only ip metadata): {'asn': 13335, 'as_name': 'CLOUDFLAREINC'}
# ex (both): {'domain': 'test.com', 'ip': '1.2.3.4', 'asn': 13335}
Row = Dict[str, Any]
#
# A key containing a date and IP
# ex: ("2020-01-01", '1.2.3.4')
DateIpKey = Tuple[str, str]

# Tables have names like 'echo_scan' and 'http_scan
BASE_TABLE_NAME = 'scan'
# Prod data goes in the `firehook-censoredplanet:base' dataset
PROD_DATASET_NAME = 'base'

# key: (type, mode)
SCAN_BIGQUERY_SCHEMA = {
    # Columns from Censored Planet data
    'domain': ('string', 'nullable'),
    'ip': ('string', 'nullable'),
    'date': ('date', 'nullable'),
    'start_time': ('timestamp', 'nullable'),
    'end_time': ('timestamp', 'nullable'),
    'retries': ('integer', 'nullable'),
    'sent': ('string', 'nullable'),
    'error': ('string', 'nullable'),
    'blocked': ('boolean', 'nullable'),
    'success': ('boolean', 'nullable'),
    'fail_sanity': ('boolean', 'nullable'),
    'stateful_block': ('boolean', 'nullable'),
    'measurement_id': ('string', 'nullable'),
    'source': ('string', 'nullable'),
    'name': ('string', 'nullable'),

    # received columns
    # Column filled in all tables
    'received_status': ('string', 'nullable'),
    # Columns filled only in HTTP/HTTPS tables
    'received_body': ('string', 'nullable'),
    'received_headers': ('string', 'repeated'),
    'blockpage': ('boolean', 'nullable'),
    # Columns filled only in HTTPS tables
    'received_tls_version': ('integer', 'nullable'),
    'received_tls_cipher_suite': ('integer', 'nullable'),
    'received_tls_cert': ('string', 'nullable'),
    # SATELLITE
    'received': ('record', 'repeated', {
        'ip': ('string', 'nullable'),
        'asnum': ('integer', 'nullable'),
        'asname': ('string', 'nullable'),
        'http': ('string', 'nullable'),
        'cert': ('string', 'nullable'),
        'matches_control': ('string', 'nullable')
    }),
    'rcode': ('string', 'repeated'),
    'confidence': ('record', 'nullable', {
        'average': ('float', 'nullable'),
        'matches': ('float', 'repeated'),
        'untagged_controls': ('boolean', 'nullable'),
        'untagged_response': ('boolean', 'nullable'),
    }),
    'verify': ('record', 'nullable', {
        'excluded': ('boolean', 'nullable'),
        'exclude_reason': ('string', 'nullable'),
    }),

    # Columns added from CAIDA data
    'netblock': ('string', 'nullable'),
    'asn': ('integer', 'nullable'),
    'as_name': ('string', 'nullable'),
    'as_full_name': ('string', 'nullable'),
    'as_class': ('string', 'nullable'),
    'country': ('string', 'nullable'),
}
# Future fields
"""
    'domain_category': ('string', 'nullable'),
    'as_traffic': ('integer', 'nullable'),
"""

# Mapping of each scan type to the zone to run its pipeline in.
# This adds more parallelization when running all pipelines.
SCAN_TYPES_TO_ZONES = {
    'https': 'us-east1',  # https has the most data, so it gets the best zone.
    'http': 'us-east4',
    'echo': 'us-west1',
    'discard': 'us-central1',
    'dns': None
}

ALL_SCAN_TYPES = SCAN_TYPES_TO_ZONES.keys()

# PCollection key names used internally by the beam pipeline
IP_METADATA_PCOLLECTION_NAME = 'metadata'
ROWS_PCOLLECION_NAME = 'rows'

SATELLITE_TAGS = {'ip', 'http', 'asnum', 'asname', 'cert'}
CDN_REGEX = re.compile("AMAZON|Akamai|OPENDNS|CLOUDFLARENET|GOOGLE")
VERIFY_THRESHOLD = 2 # 2 or 3 works best to optimize the FP:TP ratio.
INTERFERENCE_IPDOMAIN = {}

class BlockpageMatcher:

  def __init__(self):
    self.false_positives = self._load_signatures(FALSE_POSITIVES)
    self.blockpages = self._load_signatures(BLOCKPAGES)

  def _load_signatures(self, filename: str) -> Dict[str, str]:
    """Load signatures for blockpage matching.

    Args:
      filename: json file containing signatures

    Returns:
      Dictionary mapping fingerprints to signature patterns
    """
    signatures = {}
    with open(filename) as f:
      for line in f:
        try:
          signature = json.loads(line.strip())
          # Patterns stored in BigQuery syntax,
          # so % represents any number of characters
          pattern = signature['pattern']
          # Convert to Python regex
          pattern = re.escape(pattern)
          pattern = pattern.replace('%', '.*')
          signatures[signature["fingerprint"]] = re.compile(pattern, re.DOTALL)
        except:
          pass
    return signatures

  def match_page(self, page: str) -> bool:
    """Check if the input page matches a known blockpage or false positive.

    Args:
      page: a string containing the HTTP body of the potential blockpage

    Returns:
      False if page matches a false positive signature.
      True if page matches a blockpage signature.
      None otherwise.
    """

    # Check false positives
    for fingerprint, pattern in self.false_positives.items():
      if pattern.search(page):
        return False

    # Check blockpages
    for fingerprint, pattern in self.blockpages.items():
      if pattern.search(page):
        return True

    # No signature match
    return

def _maxmind_reader(filepath: str) -> geoip2.database.Reader:
  """Return a reader for the Maxmind database.

  Args:
    filepath: Maxmind .mmdb file

  Returns:
    geoip2.database.Reader
  """
  if not os.path.exists(filepath):
    logging.warning('Path to Maxmind db not found: %s\n', filepath)
    return
  return geoip2.database.Reader(filepath)


def _read_json(filepath) -> Dict[str, str]:
  """Load a JSON file to a dictionary."""
  if os.path.exists(filepath):
    with open(filepath) as f:
      dictionary = json.loads(f.read())
    return dictionary
  return {}


blockpage_matcher = BlockpageMatcher()
maxmind_city = _maxmind_reader(MAXMIND_CITY)
maxmind_asn = _maxmind_reader(MAXMIND_ASN)
country_name_to_code = _read_json(COUNTRY_CODES)


def _get_country_code(vp: str) -> str:
  """Get country code for IP address.

  Args:
    vp: IP address of vantage point (as string)

  Returns:
    2-letter ISO country code
  """
  if maxmind_city:
    try:
      vp_info = maxmind_city.city(vp)
      return vp_info.country.iso_code
    except Exception as e:
      logging.warning('Maxmind: %s\n', e)
  return


def _get_maxmind_asn(vp: str) -> Tuple[str]:
  """Get ASN information for IP address.

  Args:
    vp: IP address of vantage point (as string)

  Returns:
    Tuple containing AS num, AS org, and netblock
  """
  if maxmind_asn:
    try:
      vp_info = maxmind_asn.asn(vp)
      return vp_info.autonomous_system_number, vp_info.autonomous_system_organization, vp_info.network
    except Exception as e:
      logging.warning('Maxmind: %s\n', e)
  return None, None, None


def _get_censys(ips: List[str]) -> cloud_bigquery.table.RowIterator:
  """Get additional IP information from Censys.

    Args:
      ips: list of IP addresses (as strings)

    Returns:
      BigQuery RowIterator for query results
  """
  censys = cloud_bigquery.Client()

  # currently using CP Table
  query = """
    SELECT
      ip,
      asname as asname,
      http as http,
      cert as cert,
      asnum as asnum
    FROM censoredplanet-v1.cp.as_info_from_censys WHERE ip IN UNNEST({})
  """.format(ips)

  job = censys.query(query)
  rows = job.result()
  return rows


def _get_beam_bigquery_schema(
    fields: Dict[str, Tuple[str, str]]) -> beam_bigquery.TableSchema:
  """Return a beam bigquery schema for the output table.

  Args:
    fields: dict of {'field_name': ['column_type', 'column_mode']}

  Returns:
    A bigquery table schema
  """
  table_schema = beam_bigquery.TableSchema()

  for (name, attributes) in fields.items():
    field_type = attributes[0]
    mode = attributes[1]
    field_schema = beam_bigquery.TableFieldSchema()
    field_schema.name = name
    field_schema.type = field_type
    field_schema.mode = mode
    if len(attributes) > 2:
      field_schema.fields = []
      for (n, (t, m)) in attributes[2].items():
        subfield_schema = beam_bigquery.TableFieldSchema()
        subfield_schema.name = n
        subfield_schema.type = t
        subfield_schema.mode = m
        field_schema.fields.append(subfield_schema)
    table_schema.fields.append(field_schema)

  return table_schema


def _source_from_filename(filepath: str) -> str:
  """Get the source string from a scan filename.

  Source represents the .tar.gz container which held this file.

  Args:
    filepath:
    'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'

  Returns:
    Just the 'CP_Quack-echo-2020-08-23-06-01-02' source
  """
  path = os.path.split(filepath)[0]
  path_end = os.path.split(path)[1]
  return path_end


def _get_existing_datasources(table_name: str) -> List[str]:
  """Given a table return all sources that contributed to the table.

  Args:
    table_name: name of a bigquery table like
      'firehook-censoredplanet:echo_results.scan_test'

  Returns:
    List of data sources. ex ['CP_Quack-echo-2020-08-23-06-01-02']
  """
  # This needs to be created locally
  # because bigquery client objects are unpickable.
  # So passing in a client to the class breaks the pickling beam uses
  # to send state to remote machines.
  client = cloud_bigquery.Client()

  # Bigquery table names are of the format project:dataset.table
  # but this library wants the format project.dataset.table
  fixed_table_name = table_name.replace(':', '.')

  query = 'SELECT DISTINCT(source) AS source FROM `{table}`'.format(
      table=fixed_table_name)
  rows = client.query(query)
  sources = [row.source for row in rows]

  return sources


def _make_tuple(line: str, filename: str) -> Tuple[str, str]:
  """Helper method for making a tuple from two args."""
  return (filename, line)


def _read_scan_text(
    p: beam.Pipeline,
    filenames: List[str]) -> beam.pvalue.PCollection[Tuple[str, str]]:
  """Read in all individual lines for the given data sources.

  Args:
    p: beam pipeline object
    filenames: List of files to read from

  Returns:
    A PCollection[Tuple[filename, line]] of all the lines in the files keyed
    by
    filename
  """
  # List[PCollection[Tuple[filename,line]]]
  line_pcollections_per_file: List[beam.PCollection[Tuple[str, str]]] = []

  for filename in filenames:
    # PCollection[line]
    lines = p | 'read file ' + filename >> beam.io.ReadFromText(filename)

    step_name = 'annotate filename ' + filename

    # PCollection[Tuple[filename,line]]
    lines_with_filenames = (
        lines | step_name >> beam.Map(_make_tuple, filename).with_output_types(
            Tuple[str, str]))

    line_pcollections_per_file.append(lines_with_filenames)

  # PCollection[Tuple[filename,line]]
  lines = (
      tuple(line_pcollections_per_file) | 'flatten lines' >>
      beam.Flatten(pipeline=p).with_output_types(Tuple[str, str]))

  return lines


def _between_dates(filename: str,
                   start_date: Optional[datetime.date] = None,
                   end_date: Optional[datetime.date] = None) -> bool:
  """Return true if a filename is between (or matches either of) two dates.

  Args:
    filename: string of the format
    "gs://firehook-scans/http/CP_Quack-http-2020-05-11-01-02-08/results.json"
    start_date: date object or None
    end_date: date object or None

  Returns:
    boolean
  """
  date = datetime.date.fromisoformat(
      re.findall(r'\d\d\d\d-\d\d-\d\d', filename)[0])
  if start_date and end_date:
    return start_date <= date <= end_date
  if start_date:
    return start_date <= date
  if end_date:
    return date <= end_date

  return True


def _parse_received_headers(headers: Dict[str, List[str]]) -> List[str]:
  """Flatten headers from a dictionary of headers to value lists.

  Args:
    headers: Dict from a header key to a list of headers.
      {"Content-Language": ["en", "fr"],
       "Content-Type": ["text/html; charset=iso-8859-1"]}

  Returns:
    A list of key-value headers pairs as flat strings.
    ["Content-Language: en",
     "Content-Language: fr",
     "Content-Type: text/html; charset=iso-8859-1"]
  """
  # TODO decide whether the right approach here is turning each value into its
  # own string, or turning each key into its own string with the values as a
  # comma seperated list.
  # The right answer depends on whether people will be querying mostly for
  # individual values, or for specific combinations of values.
  flat_headers = []
  for key, values in headers.items():
    for value in values:
      flat_headers.append(key + ': ' + value)
  return flat_headers


def _parse_received_data(received: Union[str, Dict[str, Any]], anomaly: bool) -> Row:
  """Parse a received field into a section of a row to write to bigquery.

  Args:
    received: a dict parsed from json data, or a str

  Returns:
    a dict containing the 'received_' keys/values in SCAN_BIGQUERY_SCHEMA
  """
  if isinstance(received, str):
    return {'received_status': received}

  row = {
      'received_status': received['status_line'],
      'received_body': received['body'],
      'received_headers': _parse_received_headers(received.get('headers', {})),
      'blockpage': None,
  }

  if anomaly: # check response for blockpage
    row['blockpage'] = blockpage_matcher.match_page(received['body'])

  tls = received.get('tls', None)
  if tls:
    tls_row = {
        'received_tls_version': tls['version'],
        'received_tls_cipher_suite': tls['cipher_suite'],
        'received_tls_cert': tls['cert']
    }
    row.update(tls_row)

  return row


def _flatten_measurement(filename: str, line: str) -> Iterator[Row]:
  """Flatten a measurement string into several roundtrip rows.

  Args:
    filename: a filepath string
    line: a json string describing a censored planet measurement. example
    {'Keyword': 'test.com',
     'Server': '1.2.3.4',
     'Results': [{'Success': true},
                 {'Success': false}]}

  Yields:
    Dicts containing individual roundtrip information
    {'column_name': field_value}
    examples:
    {'domain': 'test.com', 'ip': '1.2.3.4', 'success': true}
    {'domain': 'test.com', 'ip': '1.2.3.4', 'success': false}
  """

  try:
    scan = json.loads(line)
  except json.decoder.JSONDecodeError as e:
    logging.warning('JSONDecodeError: %s\nFilename: %s\n%s\n', e, filename,
                    line)
    return

  # Add a unique id per-measurement so single retry rows can be reassembled
  random_measurement_id = uuid.uuid4().hex

  if 'Satellite' in filename:
    date = re.findall(r'\d\d\d\d-\d\d-\d\d', filename)[0]
    if date < "2021":
      row = {
        'domain': scan['query'],
        'ip': scan['resolver'],
        'date': date,
        'error': scan.get('error', None),
        'blocked': not scan['passed'] if 'passed' in scan else None,
        'success': 'error' not in scan,
        'received': None,
        'measurement_id': random_measurement_id,
      }
      received_ips = scan.get('answers')
    else:
      row = {
        'domain': scan['test_url'],
        'ip': scan['vp'],
        'country': scan['location']['country_code'],
        'date': scan['start_time'][:10],
        'start_time': scan['start_time'],
        'end_time': scan['end_time'],
        'error': scan.get('error', None),
        'blocked': scan['anomaly'],
        'success': not scan['connect_error'],
        'received': None,
        'measurement_id': random_measurement_id
      }
      received_ips = scan.get('response')

    # separate into one answer ip per row for tagging
    if received_ips:
      if 'rcode' in received_ips:
        row['rcode'] = received_ips['rcode']
      for ip in received_ips:
        if ip != 'rcode':
          row['received'] = {
            'ip': ip
          }
          if row['blocked']:
            # Track domains per IP for interference
            if ip not in INTERFERENCE_IPDOMAIN:
              INTERFERENCE_IPDOMAIN[ip] = set()
            INTERFERENCE_IPDOMAIN[ip].add(row['domain'])
          if type(received_ips) == dict:
            row['received']['matches_control'] = ' '.join([tag for tag in received_ips[ip] if tag in SATELLITE_TAGS])
          yield row
    else:
      yield row
  else:
    for result in scan['Results']:
      if 'Received' in result:
        received = result.get('Received', '')
        received_fields = _parse_received_data(received, scan['Blocked'])
      else:
        received_fields = {}

      if 'Error' in result:
        error_field = {'error': result['Error']}
      else:
        error_field = {}

      date = result['StartTime'][:10]
      row = {
          'domain': scan['Keyword'],
          'ip': scan['Server'],
          'date': date,
          'start_time': result['StartTime'],
          'end_time': result['EndTime'],
          'retries': scan['Retries'],
          'sent': result['Sent'],
          'blocked': scan['Blocked'],
          'success': result['Success'],
          'fail_sanity': scan['FailSanity'],
          'stateful_block': scan['StatefulBlock'],
          'measurement_id': random_measurement_id,
          'source': _source_from_filename(filename),
      }

      row.update(received_fields)
      row.update(error_field)

      yield row


def _read_satellite_tags(filename, line: str) -> Iterator[Dict[str, Any]]:
  """Read data for IP tagging from Satellite.

    Args:
      scan: dictionary containing tag data

    Yields:
      Processed Satellite fields
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
  elif 'name' in scan:
    # from resolvers.json
    tags = {
      'ip': scan.get('resolver', scan.get('vp')),
      'name': scan['name']
    }
  elif 'country' in scan:
    # from v1 tagged_resolvers.json
    # contains resolver's full country name
    # convert to country code
    tags = {
      'ip': scan['resolver'],
      'country': country_name_to_code.get(scan['country'], scan['country'])
    }
  else:
    # from tagged_answers.json
    tags = scan
  tags['date'] = re.findall(r'\d\d\d\d-\d\d-\d\d', filename)[0]
  yield tags


def _add_satellite_tags(rows: beam.pvalue.PCollection[Row], tags: beam.pvalue.PCollection[Tuple[Row]]) -> beam.pvalue.PCollection[Row]:
  """Add tags for resolvers and answer IPs and unflatten the Satellite measurement rows.

    Args:
      rows: PCollection of measurement rows
      tags: PCollection of (filename, tag dictionary) tuples

    Returns:
      PCollection of measurement rows containing tag information
  """

    # 1. Add tags for vantage point IPs - resolver name (hostname/control/special) and country

  def _merge_dicts(dicts):
    merged = {}
    for d in dicts:
      merged.update(d)
    return merged

  # PCollection[Tuple[DateIpKey,Row]]
  rows_keyed_by_ip_and_date = (
      rows
      | 'key by ips and dates' >>
      beam.Map(lambda row: (_make_date_ip_key(row), row)).with_output_types(
          Tuple[DateIpKey, Row]))

  # PCollection[Tuple[DateIpKey,Row]]
  ips_with_metadata = (
      tags
      | 'tags key by ips and dates' >>
      beam.Map(lambda row: (_make_date_ip_key(row), row))
      | 'combine duplicate tags' >>
      beam.CombinePerKey(_merge_dicts).with_output_types(
          Tuple[DateIpKey, Row]))

  # PCollection[Tuple[Tuple[date,ip],Dict[input_name_key,List[Row]]]]
  grouped_metadata_and_rows = (({
      IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
      ROWS_PCOLLECION_NAME: rows_keyed_by_ip_and_date
  }) | 'group by keys' >> beam.CoGroupByKey())

  # PCollection[Row]
  rows_with_metadata = (
      grouped_metadata_and_rows
      | 'merge metadata with rows' >>
      beam.FlatMapTuple(_merge_metadata_with_rows).with_output_types(Row))

  # 2. Add tags for answer ips (field received.ip) - asnum, asname, http, cert

  received_keyed_by_ip_and_date = (
      rows_with_metadata
      | 'key by received ips and dates' >>
      beam.Map(lambda row: ((row['date'], row['received']['ip']), row)).with_output_types(
          Tuple[DateIpKey, Row]))

  grouped_received_metadata_and_rows = (({
      IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
      ROWS_PCOLLECION_NAME: received_keyed_by_ip_and_date
  }) | 'group by received ip keys ' >> beam.CoGroupByKey())

  rows_with_tags = (
      grouped_received_metadata_and_rows
      | 'tag received ips' >>
      beam.FlatMapTuple(lambda k, v: _merge_metadata_with_rows(k, v, field='received')).with_output_types(Row))

  # 3. Measurements are currently flattened to one answer IP per row ->
  #    Unflatten so that each row contains a array of answer IPs

  unflattened_rows = (
    rows_with_tags
    | 'key by measurement id' >>
    beam.Map(lambda row: (row['measurement_id'], row)).with_output_types(
          Tuple[str, Row])
    | 'group by measurement id' >>
    beam.GroupByKey()
    | 'unflatten rows' >>
    beam.FlatMapTuple(lambda k, v: _unflatten_satellite(v)).with_output_types(Row))

  return unflattened_rows

def _post_processing_satellite(rows: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection[Row]:
  """Run post processing on Satellite v1 data (calculate confidence, verify interference).

    Args:
      rows: PCollection of measurement rows

    Returns:
      PCollection of measurement rows with confidence and verify fields
  """
  def _total_tags(key, row):
    total_tags = 0
    for tag_type in SATELLITE_TAGS:
      if tag_type != 'ip':
        type_tags = set([ans[tag_type] for ans in row['received'] if ans.get(tag_type)])
        total_tags += len(type_tags)
    return (key, total_tags)

  def _flat_rows_controls(k, v):
    num_control_tags = 0
    if len(v['control']) > 0:
      num_control_tags = v['control'][0]
    for row in v['test']:
      yield (row, num_control_tags)

  # Partition rows into test measurements and control measurements
  # 'blocked' is None for control measurements
  rows, controls = (
      rows | 'key by dates and domains' >>
      beam.Map(lambda row: ((row['date'], row['domain']), row))
      | 'partition test and control' >>
      beam.Partition(lambda row, p: int(row[1]['blocked'] == None), 2))

  num_ctags = controls | 'calculate # control tags' >> beam.MapTuple(_total_tags)

  post = ({'test': rows, 'control': num_ctags}
    | 'group rows and # control tags by keys' >> beam.CoGroupByKey()
    | 'flat map to (row, # control tags)' >> beam.FlatMapTuple(_flat_rows_controls)
    | 'calculate confidence' >> beam.MapTuple(_calculate_confidence)
    | 'verify interference' >> beam.Map(_verify).with_output_types(Row)
  )

  return post


def _unflatten_satellite(flattened_measurement: List[Dict[str, Any]]) -> Iterator[Row]:
  """Unflatten a Satellite measurement.

  Args:
    flattened_measurment: list of dicts representing a flattened measurement,
    where each contains an unique answer IP and tags in the 'received' field
    (other fields are the same for each dict).
    [{'ip':'1.1.1.1','domain':'x.com','measurement_id':'HASH','received':{'ip':'0.0.0.0','tag':'value1'},...},
     {'ip':'1.1.1.1','domain':'x.com','measurement_id':'HASH','received':{'ip':'0.0.0.1','tag':'value2'},...}]

  Yields:
    Row with common fields remaining the same, 'measurement_id' removed,
    and 'received' mapped to an array of answer IP dictionaries.
    {'ip':'1.1.1.1','domain':'x.com','received':[{'ip':'0.0.0.0','tag':'value1'},{'ip':'0.0.0.1','tag':'value2'}],...}
  """
  if flattened_measurement:
    # Get common fields and update 'received' with array of all answer IPs
    combined = flattened_measurement[0]
    combined['received'] = [answer['received'] for answer in flattened_measurement]
    combined.pop('measurement_id')
    yield combined


def _process_satellite(lines: beam.pvalue.PCollection[Tuple[str, str]],
              lines2: beam.pvalue.PCollection[Tuple[str, str]]):
  """Process Satellite measurements and tags."""
  rows = (
      lines | 'flatten json' >>
      beam.FlatMapTuple(_flatten_measurement).with_output_types(Row))
  tag_rows = (
        lines2 | 'tag rows' >>
        beam.FlatMapTuple(_read_satellite_tags).with_output_types(Row))

  rows_with_metadata = _add_satellite_tags(rows, tag_rows)

  return rows_with_metadata


def _partition_satellite_input(line: Tuple[str, str], num_partitions: int = 2) -> int:
  """Partitions Satellite input into tags (0) and rows (1)."""
  filename = line[0]
  if "tagged" in filename or "resolvers" in filename:
    # {tagged_answers, tagged_resolvers, resolvers}.json contain tags
    return 0
  return 1


def _calculate_confidence(scan: Dict[str, Any], num_control_tags: int) -> Dict[str, Any]:
  """Calculate confidence for a Satellite measurement.

    Args:
      scan: dict containing measurement data
      control_tags: dict containing control tags for the test domain

    Returns:
      scan dict with new 'confidence' record containing:
        'average': average percentage of tags that match control queries
        'matches': array of percentage match per answer IP
        'untagged_controls': True if all control IPs have no tags
        'untagged_response': True if all answer IPs have no tags
  """
  confidence = {
    'matches': [],
    'untagged_controls': num_control_tags == 0,
    'untagged_response': True
  }

  for answer in scan['received']:
    # check tags for each answer IP
    matches_control = answer['matches_control'].split()
    total_tags = 0
    matching_tags = 0

    # calculate number of tags IP has and how many match controls
    for tag in SATELLITE_TAGS:
      if tag != 'ip' and answer.get(tag):
        total_tags += 1
        if tag in matches_control:
          matching_tags += 1

    if confidence['untagged_response'] and total_tags > 0:
      # at least one answer IP has tags
      confidence['untagged_response'] = False

    # calculate percentage of matching tags
    if 'ip' in matches_control:
      # ip is in control response
      ip_match = 100
    else:
      if total_tags == 0:
        ip_match = 0
      else:
        ip_match = matching_tags * 100 / total_tags
    confidence['matches'].append(ip_match)

  confidence['average'] = sum(confidence['matches']) / len(confidence['matches'])
  scan['confidence'] = confidence
  # Sanity check for untagged responses: do not claim interference
  if confidence['untagged_response'] or confidence['untagged_controls']:
    scan['blocked'] = False

  return scan


def _verify(scan: Dict[str, Any]) ->  Dict[str, Any]:
  """Verify that a Satellite measurement with interference is not a false positive.

    Args:
      scan: dict containing measurement data

    Returns:
      scan dict with new 'verify' record containing:
        'excluded': bool, equals true if interference is a false positive
        'exclude_reason': string, reason(s) for false positive
  """
  scan['verify'] = {
    'excluded': None,
    'exclude_reason': None,
  }

  if scan['blocked']:
    scan['verify']['excluded'] = False
    reasons = []
    # Check received IPs for false positive reasons
    for received in scan['received']:
      asname = received.get('asname')
      if asname and CDN_REGEX.match(asname):
        # CDN IPs
        scan['verify']['excluded'] = True
        reasons.append('is_CDN')
      unique_domains = INTERFERENCE_IPDOMAIN.get(received['ip'])
      if unique_domains and len(unique_domains) <= VERIFY_THRESHOLD:
        # IPs that appear <= threshold times across all interference
        scan['verify']['excluded'] = True
        reasons.append('domain_below_threshold')
    scan['verify']['exclude_reason'] = ' '.join(reasons)

  return scan


def _make_date_ip_key(row: Row) -> DateIpKey:
  """Makes a tuple key of the date and ip from a given row dict."""
  return (row['date'], row['ip'])


def _merge_metadata_with_rows(key: DateIpKey,
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
      new_row['received'].update(ip_metadata)
      new_row['received'].pop('date', None)
    else:
      new_row.update(ip_metadata)
    yield new_row


def _get_partition_params() -> Dict[str, Any]:
  """Returns additional partitioning params to pass with the bigquery load.

  Returns: A dict of query params, See:
  https://beam.apache.org/releases/pydoc/2.14.0/apache_beam.io.gcp.bigquery.html#additional-parameters-for-bigquery-tables
  https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource:-table
  """
  partition_params = {
      'timePartitioning': {
          'type': 'DAY',
          'field': 'date'
      },
      'clustering': {
          'fields': ['country', 'asn']
      }
  }
  return partition_params


def get_job_name(table_name: str, incremental_load: bool) -> str:
  """Creates the job name for the beam pipeline.

  Pipelines with the same name cannot run simultaneously.

  Args:
    table_name: a dataset.table name like 'base.scan_echo'
    incremental_load: boolean. whether the job is incremental.

  Returns:
    A string like 'write-base-scan-echo'
  """
  # no underscores or periods are allowed in beam job names
  fixed_table_name = table_name.replace('_', '-').replace('.', '-')

  if incremental_load:
    return 'append-' + fixed_table_name

  return 'write-' + fixed_table_name


def get_table_name(dataset_name: str, scan_type: str,
                   base_table_name: str) -> str:
  """Construct a bigquery table name.

  Args:
    dataset_name: dataset name like 'base' or 'laplante'
    scan_type: data type, one of 'echo', 'discard', 'http', 'https'
    base_table_name: table name like 'scan'

  Returns:
    a dataset.table name like 'base.echo_scan'
  """
  return f'{dataset_name}.{scan_type}_{base_table_name}'


class ScanDataBeamPipelineRunner():
  """A runner to collect cloud values and run a corrosponding beam pipeline."""

  def __init__(self, project: str, schema: Dict[str, Tuple[str, str]],
               bucket: str, staging_location: str, temp_location: str,
               ip_metadata_class: type, ip_metadata_bucket_folder: str) -> None:
    """Initialize a pipeline runner.

    Args:
      project: google cluod project name
      schema: bigquery schema
      bucket: gcs bucket name
      staging_location: gcs bucket name, used for staging beam data
      temp_location: gcs bucket name, used for temp beam data
      ip_metadata_class: an IpMetadataInterface subclass (class, not instance)
      ip_metadata_bucket_folder: gcs folder with ip metadata files
    """
    self.project = project
    self.schema = schema
    self.bucket = bucket
    self.staging_location = staging_location
    self.temp_location = temp_location
    # Because an instantiated IpMetadata object is too big for beam's
    # serlalization to pass around we pass in the class to instantiate instead.
    self.ip_metadata_class = ip_metadata_class
    self.ip_metadata_bucket_folder = ip_metadata_bucket_folder

  def _get_full_table_name(self, table_name: str) -> str:
    """Get a full project:dataset.table name.

    Args:
      table_name: a dataset.table name

    Returns:
      project:dataset.table name
    """
    return self.project + ':' + table_name

  def _data_to_load(self,
                    gcs: GCSFileSystem,
                    scan_type: str,
                    incremental_load: bool,
                    table_name: str,
                    start_date: Optional[datetime.date] = None,
                    end_date: Optional[datetime.date] = None) -> List[str]:
    """Select the right files to read.

    Args:
      gcs: GCSFileSystem object
      scan_type: one of 'echo', 'discard', 'http', 'https', 'dns'
      incremental_load: boolean. If true, only read the latest new data
      table_name: dataset.table name like 'base.scan_echo'
      start_date: date object, only files after or at this date will be read
      end_date: date object, only files at or before this date will be read

    Returns:
      A List of filename strings. ex
       ['gs://firehook-scans/echo/CP_Quack-echo-2020-08-22-06-08-03/results.json',
        'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json']
    """
    if incremental_load:
      full_table_name = self._get_full_table_name(table_name)
      existing_sources = _get_existing_datasources(full_table_name)
    else:
      existing_sources = []

    if scan_type == 'dns':
      # Satellite v1 has several output files
      # TODO: check date for v1 vs. v2
      files_to_load = ['resolvers.json', 'tagged_resolvers.json', 'tagged_answers.json',
                       'answers_control.json', 'interference.json', 'interference_err.json',
                       'tagged_responses.json', 'results.json']
    else:
      files_to_load = ['results.json']

    # Both zipped and unzipped data to be read in
    zipped_regex = self.bucket + scan_type + '/**/{0}.gz'
    unzipped_regex = self.bucket + scan_type + '/**/{0}'

    file_metadata = []
    for file in files_to_load:
      zipped_metadata = [m.metadata_list for m in gcs.match([zipped_regex.format(file)])][0]
      unzipped_metadata = [m.metadata_list for m in gcs.match([unzipped_regex.format(file)])
                          ][0]
      file_metadata += zipped_metadata + unzipped_metadata

    filenames = [metadata.path for metadata in file_metadata]
    file_sizes = [metadata.size_in_bytes for metadata in file_metadata]

    filtered_filenames = [
        filename for (filename, file_size) in zip(filenames, file_sizes)
        if (_between_dates(filename, start_date, end_date) and
            _source_from_filename(filename) not in existing_sources and
            file_size != 0)
    ]
    return filtered_filenames

  def _add_metadata(
      self, rows: beam.pvalue.PCollection[Row]) -> beam.pvalue.PCollection[Row]:
    """Add ip metadata to a collection of roundtrip rows.

    Args:
      rows: beam.PCollection[Row]

    Returns:
      PCollection[Row]
      The same rows as above with with additional metadata columns added.
    """

    # PCollection[Tuple[DateIpKey,Row]]
    rows_keyed_by_ip_and_date = (
        rows | 'key by ips and dates' >>
        beam.Map(lambda row: (_make_date_ip_key(row), row)).with_output_types(
            Tuple[DateIpKey, Row]))

    # PCollection[DateIpKey]
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

    # PCollection[Tuple[DateIpKey,Row]]
    ips_with_metadata = (
        grouped_ips_by_dates | 'get ip metadata' >> beam.FlatMapTuple(
            self._add_ip_metadata).with_output_types(Tuple[DateIpKey, Row]))

    # PCollection[Tuple[Tuple[date,ip],Dict[input_name_key,List[Row]]]]
    grouped_metadata_and_rows = (({
        IP_METADATA_PCOLLECTION_NAME: ips_with_metadata,
        ROWS_PCOLLECION_NAME: rows_keyed_by_ip_and_date
    }) | 'group by keys' >> beam.CoGroupByKey())

    # PCollection[Row]
    rows_with_metadata = (
        grouped_metadata_and_rows | 'merge metadata with rows' >>
        beam.FlatMapTuple(_merge_metadata_with_rows).with_output_types(Row))

    return rows_with_metadata

  def _add_ip_metadata(self, date: str,
                       ips: List[str]) -> Iterator[Tuple[DateIpKey, Row]]:
    """Add Autonymous System metadata for ips in the given rows.

    Args:
      date: a 'YYYY-MM-DD' date key
      ips: a list of ips

    Yields:
      Tuples (DateIpKey, metadata_dict)
      where metadata_dict is a row Dict[column_name, values]
    """
    ip_metadata_db = self.ip_metadata_class(
        datetime.date.fromisoformat(date), self.ip_metadata_bucket_folder, True)
    for ip in ips:
      metadata_key = (date, ip)

      try:
        (netblock, asn, as_name, as_full_name, as_type,
         country) = ip_metadata_db.lookup(ip)
        metadata_values = {
            'netblock': netblock,
            'asn': asn,
            'as_name': as_name,
            'as_full_name': as_full_name,
            'as_class': as_type,
            'country': country,
        }
        if not metadata_values['country']: # try Maxmind
          metadata_values['country'] = _get_country_code(ip)

      except KeyError as e:
        logging.warning('KeyError: %s\n', e)
        metadata_values = {}  # values are missing, but entry should still exist

      yield (metadata_key, metadata_values)

  def _write_to_bigquery(self, rows: beam.pvalue.PCollection[Row],
                         table_name: str, incremental_load: bool) -> None:
    """Write out row to a bigquery table.

    Args:
      rows: PCollection[Row] of data to write.
      table_name: dataset.table name like 'base.echo_scan' Determines which
        tables to write to.
      incremental_load: boolean. If true, only load the latest new data, if
        false reload all data.

    Raises:
      Exception: if any arguments are invalid.
    """
    if incremental_load:
      write_mode = beam.io.BigQueryDisposition.WRITE_APPEND
    else:
      write_mode = beam.io.BigQueryDisposition.WRITE_TRUNCATE

    (rows | 'Write' >> beam.io.WriteToBigQuery(  # pylint: disable=expression-not-assigned
        self._get_full_table_name(table_name),
        schema=_get_beam_bigquery_schema(self.schema),
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=write_mode,
        additional_bq_parameters=_get_partition_params()))

  def _get_pipeline_options(self, scan_type: str,
                            job_name: str) -> PipelineOptions:
    """Sets up pipeline options for a beam pipeline.

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https'
      job_name: a name for the dataflow job

    Returns:
      PipelineOptions
    """
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=self.project,
        region=SCAN_TYPES_TO_ZONES[scan_type],
        staging_location=self.staging_location,
        temp_location=self.temp_location,
        job_name=job_name,
        runtime_type_check=False,  # slow in prod
        setup_file='./pipeline/setup.py')
    pipeline_options.view_as(SetupOptions).save_main_session = True

    return pipeline_options

  def run_beam_pipeline(self, scan_type: str, incremental_load: bool,
                        job_name: str, table_name: str,
                        start_date: Optional[datetime.date],
                        end_date: Optional[datetime.date]) -> None:
    """Run a single apache beam pipeline to load json data into bigquery.

    Args:
      scan_type: one of 'echo', 'discard', 'http', 'https'
      incremental_load: boolean. If true, only load the latest new data, if
        false reload all data.
      job_name: string name for this pipeline job.
      table_name: dataset.table name like 'base.scan_echo'
      start_date: date object, only files after or at this date will be read.
        Mostly only used during development.
      end_date: date object, only files at or before this date will be read.
        Mostly only used during development.

    Raises:
      Exception: if any arguments are invalid or the pipeline fails.
    """
    logging.getLogger().setLevel(logging.INFO)
    pipeline_options = self._get_pipeline_options(scan_type, job_name)
    gcs = GCSFileSystem(pipeline_options)

    new_filenames = self._data_to_load(gcs, scan_type, incremental_load,
                                       table_name, start_date, end_date)
    if not new_filenames:
      logging.info('No new files to load incrementally')
      return

    with beam.Pipeline(options=pipeline_options) as p:
      # PCollection[Tuple[filename,line]]
      lines = _read_scan_text(p, new_filenames)

      if scan_type == 'dns':
        tags, lines = lines | beam.Partition(_partition_satellite_input, 2)

        rows_with_metadata = _process_satellite(lines, tags)
        rows_with_metadata = _post_processing_satellite(rows_with_metadata)
      else:
        # PCollection[Row]
        rows = (
            lines | 'flatten json' >>
            beam.FlatMapTuple(_flatten_measurement).with_output_types(Row))

        # PCollection[Row]
        rows_with_metadata = self._add_metadata(rows)

      self._write_to_bigquery(rows_with_metadata, table_name, incremental_load)
