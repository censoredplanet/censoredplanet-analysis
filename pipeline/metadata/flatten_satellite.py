"""Flattening methods for Satellite data."""

from __future__ import absolute_import

import json
import logging
import pathlib
import re
from typing import Optional, Dict, Any, Iterator, List
import datetime

from pipeline.metadata import flatten_base
from pipeline.metadata.flatten_base import Row
from pipeline.metadata.blockpage import BlockpageMatcher
from pipeline.metadata.domain_categories import DomainCategoryMatcher

SATELLITE_TAGS = {'ip', 'http', 'asnum', 'asname', 'cert'}
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


def _process_received_ips_v1(
    row: Row, received_ips: Optional[Dict[str, List[str]]]) -> Iterator[Row]:
  """Add received_ip metadata to a Satellite row

  Args:
    row: existing row of satellite data
    received_ips: data on the response from the resolver
      ex: {<ip>: ["tags"]}

  Yields:
    Row with an additional 'received' array field
  """
  from pprint import pprint

  if received_ips is None:
    row['received'] = []
    pprint(("yielding v1 row", row))
    yield row
    return

  # Convert to dict {<ip>: ["tags"]} with no tags
  if isinstance(received_ips, list):
    received_dict = {}
    for ip in received_ips:
      received_dict[ip] = []
    received_ips = received_dict

  all_received = []
  for (ip, tags) in received_ips.items():
    received = {'ip': ip}
    if tags and len(tags) != 0:
      received['matches_control'] = _append_tags(tags)
    all_received.append(received)

  row['received'] = all_received
  pprint(("yielding v1 row", row))
  yield row


def _append_tags(tags: List[str]) -> str:
  """Return valid received ip tags as a string

  Args:
    tags: List of tags like ["ip", "http", "cert", "invalid"]

  Returns:
    string like "ip http cert"
  """
  return ' '.join([tag for tag in tags if tag in SATELLITE_TAGS])


def _process_satellite_v2p1(row: Row, scan: Any,
                            filepath: str) -> Iterator[Row]:
  """Finish processing a line of Satellite v2.1 data.

  Args:
    row: a partially processed row of satellite data
    scan: a loaded json object containing the parsed content of the line
    filepath: path like "<path>/<filename>.json.gz"

  Yields:
    Rows
  """
  row['controls_failed'] = not scan['passed_control']
  received_ips = scan.get('response').copy()

  from pprint import pprint

  if not received_ips:
    row['received'] = []
    pprint(("yielding v2.1 row", row))
    yield row
    return

  # separate into one answer ip per row for tagging
  rcodes = received_ips.pop('rcode', [])
  errors = received_ips.pop('error', [])

  # v2.1 measurements repeat 4 times or until the test is successful
  if not rcodes:
    successful_test_rcode = []
    failed_test_rcodes = []
  elif rcodes[-1] == "0":
    successful_test_rcode = [rcodes[-1]]
    failed_test_rcodes = rcodes[0:-1]
  else:
    successful_test_rcode = []
    failed_test_rcodes = rcodes

  from pprint import pprint
  pprint(("rcode split", successful_test_rcode, failed_test_rcodes))

  if any([rcode == "0" for rcode in failed_test_rcodes]):
    raise Exception(
        f"Satellite v2.1 measurement has multiple 0 rcodes: {filepath} - {scan}"
    )

  if not successful_test_rcode and received_ips:
    # TODO figure out how to handle this bug case from 2021-05-16
    logging.warning(
        f"Satellite v2.1 measurement has ips but no 0 rcode: {filepath} - {scan}"
    )
    return
  if not successful_test_rcode:
    pass
  else:
    all_received = []
    for (ip, tags) in received_ips.items():
      received = {'ip': ip}
      if tags:
        received['matches_control'] = _append_tags(tags)
      all_received.append(received)

    success_row = row.copy()
    success_row['received'] = all_received
    success_row['rcode'] = successful_test_rcode
    pprint(("yielding v2.1 successful row", success_row))
    yield success_row

  for rcode in failed_test_rcodes:
    failed_row = row.copy()
    failed_row['rcode'] = [rcode]
    # -1 rcodes corrospond to the error messages in order.
    if rcode == "-1":
      failed_row['error'] = errors.pop() if errors else None
    yield failed_row

  # There is a bug in some v2.1 data where -1 rcodes weren't recorded.
  # In that case we add them back in manually
  for error in errors:
    error_row = row.copy()
    error_row['rcode'] = ["-1"]
    error_row['error'] = error
    yield error_row


class SatelliteFlattener():
  """Methods for flattening satellite data"""

  def __init__(self, blockpage_matcher: BlockpageMatcher,
               category_matcher: DomainCategoryMatcher):
    self.blockpage_matcher = blockpage_matcher
    self.category_matcher = category_matcher

  def process_satellite(self, filepath: str, scan: Any,
                        random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Satellite data.

    Args:
      filepath: a filepath string
      scan: a loaded json object containing the parsed content of the line
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    date = re.findall(r'\d\d\d\d-\d\d-\d\d', filepath)[0]

    filename = pathlib.PurePosixPath(filepath).name
    if '.gz' in pathlib.PurePosixPath(filename).suffixes:
      filename = pathlib.PurePosixPath(filename).stem

    if filename == SATELLITE_BLOCKPAGES_FILE:
      yield from self._process_satellite_blockpages(scan, filepath)
    elif datetime.date.fromisoformat(date) < SATELLITE_V2_1_START_DATE:
      yield from self._process_satellite_v1(date, scan, filepath,
                                            random_measurement_id)
    else:
      if filename == SATELLITE_RESPONSES_CONTROL_FILE:
        yield from self._process_satellite_v2_control(scan, filepath,
                                                      random_measurement_id)
      else:
        yield from self._process_satellite_v2(scan, filepath,
                                              random_measurement_id)

  def _process_satellite_v1(self, date: str, scan: Any, filepath: str,
                            random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Satellite data.

    Args:
      date: a date string YYYY-mm-DD
      scan: a loaded json object containing the parsed content of the line
      filepath: one of
        <path>/answers_control.json
        <path>/interference.json
        also potentially .gz files
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    filename = pathlib.PurePosixPath(filepath).name
    if '.gz' in pathlib.PurePosixPath(filename).suffixes:
      filename = pathlib.PurePosixPath(filename).stem

    row = {
        'domain': scan['query'],
        'is_control': False,  # v1 doesn't have domain controls
        'category': self.category_matcher.get_category(scan['query'], False),
        'ip': scan.get('resolver', scan.get('ip')),
        'is_control_ip': filename == SATELLITE_ANSWERS_CONTROL_FILE,
        'date': date,
        'error': scan.get('error', None),
        'anomaly': not scan['passed'] if 'passed' in scan else None,
        'success': 'error' not in scan,
        'received': [],
        'rcode': ['0'] if 'error' not in scan else ['-1'],
        'measurement_id': random_measurement_id,
        'source': flatten_base.source_from_filename(filepath),
    }

    if isinstance(row['error'], dict):
      row['error'] = json.dumps(row['error'])

    received_ips = scan.get('answers')
    yield from _process_received_ips_v1(row, received_ips)

  def _process_satellite_v2(self, scan: Any, filepath: str,
                            random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Satellite v2 data.

    Args:
      scan: a loaded json object containing the parsed content of the line
      filepath: path like "<path>/<filename>.json.gz"
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
            self.category_matcher.get_category(scan['test_url'],
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
        'received': [],
        'measurement_id':
            random_measurement_id,
        'source':
            flatten_base.source_from_filename(filepath),
    }

    if datetime.date.fromisoformat(row['date']) < SATELLITE_V2_2_START_DATE:
      yield from _process_satellite_v2p1(row, scan, filepath)
    else:
      yield from self._process_satellite_v2p2(row, scan)

  def _process_satellite_v2p2(self, row: Row, scan: Any) -> Iterator[Row]:
    """Finish processing a line of Satellite v2.2 data.

    Args:
      row: a partially processed row of satellite data
      scan: a loaded json object containing the parsed content of the line

    Yields:
      Rows
    """
    responses = scan.get('response', [])
    row['controls_failed'] = not scan['passed_liveness']

    row['average_confidence'] = scan.get('confidence')['average']
    row['matches_confidence'] = scan.get('confidence')['matches']
    row['untagged_controls'] = scan.get('confidence')['untagged_controls']
    row['untagged_response'] = scan.get('confidence')['untagged_response']

    row['excluded'] = scan.get('excluded', False)
    row['exclude_reason'] = ' '.join(scan.get('exclude_reason', []))

    for response in responses:

      #redefine domain rows to match individual response
      row['domain'] = response['url']
      is_control_domain = flatten_base.is_control_url(response['url'])
      row['is_control'] = is_control_domain
      row['category'] = self.category_matcher.get_category(
          response['url'], is_control_domain)

      if response['error'] and response['error'] != 'null':
        row['error'] = response['error']

      row['rcode'] = [str(response['rcode'])]

      # Check responses for test domain with valid answers
      if response['url'] == row['domain'] and (response['rcode'] == 0 and
                                               response['has_type_a']):
        row['has_type_a'] = True

      all_received = []
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
          received['matches_control'] = _append_tags(matched)
        all_received.append(received)
      row['received'] = all_received
      yield row.copy()

  def _process_satellite_blockpages(self, scan: Any,
                                    filepath: str) -> Iterator[Row]:
    """Process a line of Satellite blockpage data.

    Args:
      scan: a loaded json object containing the parsed content of the line
      filepath: a filepath string

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
        'source': flatten_base.source_from_filename(filepath),
    }

    http = {
        'https': False,
    }
    http.update(row)
    received_fields = flatten_base.parse_received_data(self.blockpage_matcher,
                                                       scan.get('http', ''),
                                                       True)
    http.update(received_fields)
    yield http

    https = {
        'https': True,
    }
    https.update(row)
    received_fields = flatten_base.parse_received_data(self.blockpage_matcher,
                                                       scan.get('https', ''),
                                                       True)
    https.update(received_fields)
    yield https

  def _process_satellite_v2_control(
      self, scan: Any, filepath: str,
      random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Satellite ip control data.

      Args:
        scan: a loaded json object containing the parsed content of the line
        filepath: path like "<path>/<filename>.json.gz"
        random_measurement_id: a hex id identifying this individual measurement

      Yields:
        Rows
    """
    responses = scan.get('response', [])

    from pprint import pprint

    pprint(("responses for scan", responses, scan))

    for response in responses:
      is_control_domain = flatten_base.is_control_url(response['url'])

      row = {
          'domain':
              response['url'],
          'is_control':
              is_control_domain,
          'category':
              self.category_matcher.get_category(response['url'],
                                                 is_control_domain),
          'ip':
              scan['vp'],
          'is_control_ip':
              True,
          'date':
              response['start_time'][:10],
          'start_time':
              format_timestamp(response['start_time']),
          'end_time':
              format_timestamp(response['end_time']),
          'anomaly':
              None,
          'success':
              not scan['connect_error'],
          'controls_failed':
              not scan['passed_control'],
          'rcode': [str(response['rcode'])],
          'has_type_a':
              response['has_type_a'],
          'measurement_id':
              random_measurement_id,
          'source':
              flatten_base.source_from_filename(filepath),
      }

      if response['error'] == 'null':
        error = None
      else:
        error = response['error']
      row['error'] = error

      received_ips = response['response']

      if not received_ips:
        row['received'] = []
        pprint(("yielding 2.1 control", row))
        yield row

      else:
        all_received = []
        for ip in received_ips:
          received = {'ip': ip}
          all_received.append(received)
        row['received'] = all_received

        pprint(("yielding 2.1 control", row))
        yield row
