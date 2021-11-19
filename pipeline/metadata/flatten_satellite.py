"""Mixin class of flattening methods for Satellite data."""

from __future__ import absolute_import

from collections import defaultdict
import json
import pathlib
import re
from typing import Optional, Dict, Any, Iterator, Set
import datetime

from pipeline.metadata import flatten_base
from pipeline.metadata.flatten_base import Row
from pipeline.metadata.blockpage import BlockpageMatcher
from pipeline.metadata.domain_categories import DomainCategoryMatcher

SATELLITE_TAGS = {'ip', 'http', 'asnum', 'asname', 'cert'}
INTERFERENCE_IPDOMAIN: Dict[str, Set[str]] = defaultdict(set)
SATELLITE_V2_1_START_DATE = datetime.date(2021, 3, 1)
SATELLITE_V2_2_START_DATE = datetime.date(2021, 6, 24)

# Data files for the Satellite pipeline
SATELLITE_RESOLVERS_FILE = 'resolvers.json'  #v1, v2.2

SATELLITE_RESULTS_FILE = 'results.json'  # v2.1, v2.2

SATELLITE_TAGGED_ANSWERS_FILE = 'tagged_answers.json'  # v1
SATELLITE_ANSWERS_CONTROL_FILE = 'answers_control.json'  #v1
SATELLITE_INTERFERENCE_FILE = 'interference.json'  # v1
SATELLITE_INTERFERENCE_ERR_FILE = 'interference_err.json'  # v1
SATELLITE_ANSWERS_ERR_FILE = 'answers_err.json'  # v1

SATELLITE_TAGGED_RESOLVERS_FILE = 'tagged_resolvers.json'  # v2.1
SATELLITE_RESPONSES_CONTROL_FILE = 'responses_control.json'  # v2.1
SATELLITE_TAGGED_RESPONSES = 'tagged_responses.json'  # v2.1

SATELLITE_BLOCKPAGES_FILE = 'blockpages.json'  # v2.2

SATELLITE_FILES = [
    SATELLITE_RESOLVERS_FILE,
    SATELLITE_RESULTS_FILE,
    SATELLITE_TAGGED_ANSWERS_FILE,
    SATELLITE_ANSWERS_CONTROL_FILE,
    SATELLITE_INTERFERENCE_FILE,
    SATELLITE_INTERFERENCE_ERR_FILE,
    SATELLITE_ANSWERS_ERR_FILE,
    SATELLITE_TAGGED_RESOLVERS_FILE,
    SATELLITE_RESPONSES_CONTROL_FILE,
    SATELLITE_TAGGED_RESPONSES,
    SATELLITE_BLOCKPAGES_FILE,
]

# Component that will match every satellite filepath
# filepaths look like
# gs://firehook-scans/satellite/CP_Satellite-2018-08-31-00-00-01/file.json
SATELLITE_PATH_COMPONENT = "Satellite"

# For Satellite
CONTROL_IPS = ['1.1.1.1', '8.8.8.8', '8.8.4.4', '64.6.64.6', '64.6.65.6']


def format_timestamp(timestamp: str) -> str:
  """Format the timestamp in ISO8601 format for BigQuery.

  Args:
    timestamp: Satellite format timestamp
      "2021-04-18 14:49:01.62448452 -0400 EDT m=+10140.555964129"

  Returns:
    ISO8601 formatted string "2021-04-18T14:49:01.62448452-04:00"
  """
  elements = timestamp.split()
  date = elements[0]
  time = elements[1]
  timezone_hour = elements[2][0:-2]
  timezone_seconds = elements[2][-2:]
  return f'{date}T{time}{timezone_hour}:{timezone_seconds}'


def _process_received_ips(
    row: Row, received_ips: Optional[Dict[str, str]]) -> Iterator[Row]:
  """Add received_ip metadata to a Satellite row
  Args:
    row: existing row of satellite data
    received_ips: data on the response from the resolver (error or ips)
  Yields:
    Rows
  """
  if not received_ips:
    yield row
    return

  # separate into one answer ip per row for tagging
  if 'rcode' in received_ips:
    row['rcode'] = received_ips.pop('rcode', [])
  if 'error' in received_ips:
    row['error'] = ' | '.join(received_ips.pop('error', []))

  if not received_ips:
    yield row

  for ip in received_ips:
    row['received'] = {'ip': ip}
    if row['anomaly']:
      # Track domains per IP for interference
      INTERFERENCE_IPDOMAIN[ip].add(row['domain'])
    if isinstance(received_ips, dict):
      row['received']['matches_control'] = ' '.join(  # pylint: disable=unsupported-assignment-operation
          [tag for tag in received_ips[ip] if tag in SATELLITE_TAGS])
    yield row.copy()


def _process_satellite_v2p1(row: Row, scan: Any) -> Iterator[Row]:
  """Finish processing a line of Satellite v2.1 data.

  Args:
    row: a partially processed row of satellite data
    scan: a loaded json object containing the parsed content of the line

  Yields:
    Rows
  """
  row['controls_failed'] = not scan['passed_control']
  row['rcode'] = []
  received_ips = scan.get('response')
  yield from _process_received_ips(row, received_ips)


def _process_satellite_v2p2(row: Row, scan: Any) -> Iterator[Row]:
  """Finish processing a line of Satellite v2.2 data.

  Args:
    row: a partially processed row of satellite data
    scan: a loaded json object containing the parsed content of the line

  Yields:
    Rows
  """
  responses = scan.get('response', [])
  row['controls_failed'] = not scan['passed_liveness']
  row['rcode'] = [str(response['rcode']) for response in responses]
  row['confidence'] = scan.get('confidence')
  row['verify'] = {
      'excluded': scan.get('excluded'),
      'exclude_reason': ' '.join(scan.get('exclude_reason', []))
  }
  errors = [
      response['error']
      for response in responses
      if response['error'] and response['error'] != 'null'
  ]
  row['error'] = ' | '.join(errors) if errors else None
  yielded = False
  has_test_domain = False
  for response in responses:
    if response['url'] == row['domain']:
      has_test_domain = True
    # Check responses for test domain with valid answers
    if response['url'] == row['domain'] and (response['rcode'] == 0 and
                                             response['has_type_a']):
      row['has_type_a'] = True
      # Separate into one answer IP per row for tagging
      row['received'] = []
      for ip in response['response']:
        received = {
            'ip': ip,
            'http': response['response'][ip].get('http'),
            'cert': response['response'][ip].get('cert'),
            'asname': response['response'][ip].get('asname'),
            'asnum': response['response'][ip].get('asnum'),
            'matches_control': ''
        }
        matched = response['response'][ip].get('matched', [])
        if matched:
          received['matches_control'] = ' '.join(
              [tag for tag in matched if tag in SATELLITE_TAGS])
        row['received'].append(received)
      yielded = True
      yield row.copy()
    # Do not output rows if there are no trials for the test domain.
  if not yielded and has_test_domain:
    # All trials for the test domain in `scan` are errors
    yield row.copy()


class SatelliteFlattener():
  """Methods for flattening satellite data"""

  def __init__(self, blockpage_matcher: BlockpageMatcher,
               category_matcher: DomainCategoryMatcher):
    self.blockpage_matcher = blockpage_matcher
    self.category_matcher = category_matcher
    self.base_flattener = flatten_base.BaseFlattener(blockpage_matcher,
                                                     category_matcher)

  def process_satellite(self, filename: str, scan: Any,
                        random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Satellite data.

    Args:
      filename: a filepath string
      scan: a loaded json object containing the parsed content of the line
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    date = re.findall(r'\d\d\d\d-\d\d-\d\d', filename)[0]
    if pathlib.PurePosixPath(filename).name == SATELLITE_BLOCKPAGES_FILE:
      yield from self._process_satellite_blockpages(scan, filename)
    elif datetime.date.fromisoformat(date) < SATELLITE_V2_1_START_DATE:
      yield from self._process_satellite_v1(date, scan, filename,
                                            random_measurement_id)
    else:
      if pathlib.PurePosixPath(
          filename).name == SATELLITE_RESPONSES_CONTROL_FILE:
        yield from self._process_satellite_v2_control(scan,
                                                      random_measurement_id)
      else:
        yield from self._process_satellite_v2(scan, random_measurement_id)

  def _process_satellite_v1(self, date: str, scan: Any, filename: str,
                            random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Satellite data.

    Args:
      date: a date string YYYY-mm-DD
      scan: a loaded json object containing the parsed content of the line
      filename: one of
        <path>/answers_control.json
        <path>/interference.json
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    row = {
        'domain':
            scan['query'],
        'is_control':
            False,  # v1 doesn't have domain controls
        'category':
            self.category_matcher.match_url(scan['query']),
        'ip':
            scan.get('resolver', scan.get('ip')),
        'is_control_ip':
            pathlib.PurePosixPath(filename).name ==
            SATELLITE_ANSWERS_CONTROL_FILE,
        'date':
            date,
        'error':
            scan.get('error', None),
        'anomaly':
            not scan['passed'] if 'passed' in scan else None,
        'success':
            'error' not in scan,
        'received':
            None,
        'rcode': ['0'] if 'error' not in scan else ['-1'],
        'measurement_id':
            random_measurement_id,
    }

    if isinstance(row['error'], dict):
      row['error'] = json.dumps(row['error'])

    received_ips = scan.get('answers')
    yield from _process_received_ips(row, received_ips)

  def _process_satellite_v2(self, scan: Any,
                            random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Satellite v2 data.

    Args:
      scan: a loaded json object containing the parsed content of the line
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    is_control_domain = flatten_base.is_control_url(scan['test_url'])

    row = {
        'domain':
            scan['test_url'],
        'is_control':
            is_control_domain,
        'category':
            self.base_flattener.get_category(scan['test_url'],
                                             is_control_domain),
        'ip':
            scan['vp'],
        'is_control_ip':
            scan['vp'] in CONTROL_IPS,
        'country':
            scan.get('location', {}).get('country_code'),
        'date':
            scan['start_time'][:10],
        'start_time':
            format_timestamp(scan['start_time']),
        'end_time':
            format_timestamp(scan['end_time']),
        'error':
            scan.get('error', None),
        'anomaly':
            scan['anomaly'],
        'success':
            not scan['connect_error'],
        'received':
            None,
        'measurement_id':
            random_measurement_id
    }

    if datetime.date.fromisoformat(row['date']) < SATELLITE_V2_2_START_DATE:
      yield from _process_satellite_v2p1(row, scan)
    else:
      yield from _process_satellite_v2p2(row, scan)

  def _process_satellite_blockpages(self, scan: Any,
                                    filename: str) -> Iterator[Row]:
    """Process a line of Satellite blockpage data.

    Args:
      scan: a loaded json object containing the parsed content of the line
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows, usually 2 corresponding to the fetched http and https data respectively
    """
    row = {
        'domain': scan['keyword'],
        'ip': scan['ip'],
        'date': scan['start_time'][:10],
        'start_time': format_timestamp(scan['start_time']),
        'end_time': format_timestamp(scan['end_time']),
        'success': scan['fetched'],
        'source': flatten_base.source_from_filename(filename),
    }

    http = {
        'https': False,
    }
    http.update(row)
    received_fields = self.base_flattener.parse_received_data(
        scan.get('http', ''), True)
    http.update(received_fields)
    yield http

    https = {
        'https': True,
    }
    https.update(row)
    received_fields = self.base_flattener.parse_received_data(
        scan.get('https', ''), True)
    https.update(received_fields)
    yield https

  def _process_satellite_v2_control(
      self, scan: Any, random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Satellite ip control data.

      Args:
        scan: a loaded json object containing the parsed content of the line
        random_measurement_id: a hex id identifying this individual measurement

      Yields:
        Rows
    """
    responses = scan.get('response', [])
    if responses:
      # An overall satellite v2 measurement
      # always contains some non-control trial domains
      is_control_domain = False

      row = {
          'domain':
              scan['test_url'],
          'is_control':
              is_control_domain,
          'category':
              self.base_flattener.get_category(scan['test_url'],
                                               is_control_domain),
          'ip':
              scan['vp'],
          'is_control_ip':
              True,
          'date':
              responses[0]['start_time'][:10],
          'start_time':
              format_timestamp(responses[0]['start_time']),
          'end_time':
              format_timestamp(responses[-1]['end_time']),
          'anomaly':
              None,
          'success':
              not scan['connect_error'],
          'controls_failed':
              not scan['passed_control'],
          'rcode': [str(response['rcode']) for response in responses],
          'measurement_id':
              random_measurement_id
      }
      errors = [
          response['error']
          for response in responses
          if response['error'] and response['error'] != 'null'
      ]
      row['error'] = ' | '.join(errors) if errors else None
      for response in responses:
        if response['url'] == row['domain']:
          # Check response for test domain
          if response['rcode'] == 0 and response['has_type_a']:
            # Valid answers
            row['has_type_a'] = True
            # Separate into one answer IP per row for tagging
            for ip in response['response']:
              row['received'] = {'ip': ip}
              yield row.copy()
