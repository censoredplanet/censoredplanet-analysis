"""Flattening methods for Satellite data."""

from __future__ import absolute_import

import datetime
from copy import deepcopy
import json
import logging
import pathlib
import re
from typing import Optional, Dict, Any, Iterator, List, Tuple

import apache_beam as beam

from pipeline.metadata import flatten_base
from pipeline.metadata.schema import SatelliteRow, PageFetchRow, SatelliteAnswer, IpMetadata, MatchesControl
from pipeline.metadata.blockpage import BlockpageMatcher
from pipeline.metadata.domain_categories import DomainCategoryMatcher

# Type definition for input responses, TODO make actual type stricter
ResponsesEntry = Any
# Type definition for blockpage responses
# https://docs.censoredplanet.org/dns.html#id1
BlockpageEntry = Any

SATELLITE_V2_1_START_DATE = datetime.date(2021, 3, 1)
SATELLITE_V2_2_START_DATE = datetime.date(2021, 6, 24)

SATELLITE_ANSWERS_CONTROL_FILE = 'answers_control.json'  #v1
SATELLITE_RESPONSES_CONTROL_FILE = 'responses_control.json'  # v2.1

# Component that will match every satellite filepath
# filepaths look like
# gs://firehook-scans/satellite/CP_Satellite-2018-08-31-00-00-01/file.json
SATELLITE_PATH_COMPONENT = "Satellite"

# For Satellite
CONTROL_IPS = ['1.1.1.1', '8.8.8.8', '8.8.4.4', '64.6.64.6', '64.6.65.6']


def get_filename(filepath: str) -> str:
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


def _annotate_received_ips_v1(
    base_response: SatelliteRow,
    received_ips: Optional[Dict[str, List[str]]]) -> Iterator[SatelliteRow]:
  """Add received_ip metadata to a Satellite row

  Args:
    base_response: existing row of satellite data
    received_ips: data on the response from the resolver
      ex: {<ip>: ["tags"]}

  Yields:
    SatelliteRow with an additional 'received' array field of SatelliteAnswers
  """
  if received_ips is None:
    base_response.received = []
    yield base_response
    return

  # Convert to dict {<ip>: ["tags"]} with no tags
  if isinstance(received_ips, list):
    received_dict = {}
    for ip in received_ips:
      received_dict[ip] = []
    received_ips = received_dict

  all_received = []
  for (ip, tags) in received_ips.items():
    received_answer = SatelliteAnswer(ip)
    if tags and len(tags) != 0:
      received_answer.matches_control = _make_matches_control(tags)
    all_received.append(received_answer)
  base_response.received = all_received

  yield base_response


def _make_matches_control(tags: List[str]) -> MatchesControl:
  """Return valid received ip tags as a string

  Args:
    tags: List of tags like ["ip", "http", "cert", "invalid"]

  Returns:
    string like "ip http cert"
  """
  matches_control = MatchesControl()
  if 'ip' in tags:
    matches_control.ip = True
  if 'http' in tags:
    matches_control.http = True
  if 'cert' in tags:
    matches_control.cert = True
  if 'asnum' in tags:
    matches_control.asnum = True
  if 'asname' in tags:
    matches_control.asname = True
  return matches_control


def split_rcodes(rcodes: List[str]) -> Tuple[List[int], List[int]]:
  """Split rcodes into successful and failed. Also convert to ints

  Args:
    rcodes: an array of rcode strings like
      ex: ["0"], ["2", "-1", "0"], or ["-1", "-1", "-1", "-1"]

  Returns:
    successful_test_rcode: rcodes that succeeded, this should be [0] or []
    failed_test_rcodes: all failure codes, this should never contain 0
  """

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

  int_successful_test_rcode = list(map(int, successful_test_rcode))
  int_failed_test_rcodes = list(map(int, failed_test_rcodes))

  return int_successful_test_rcode, int_failed_test_rcodes


def _process_satellite_v2p1(base_response: SatelliteRow,
                            responses_entry: ResponsesEntry,
                            filepath: str) -> Iterator[SatelliteRow]:
  """Finish processing a line of Satellite v2.1 data.

  Args:
    base_response: a partially processed row of satellite data
    responses_entry: a loaded json object containing the parsed content of the line
    filepath: path like "<path>/<filename>.json.gz"

  Yields:
    SatelliteRow
  """
  base_response.controls_failed = not responses_entry['passed_control']
  input_response_data = responses_entry.get('response').copy()

  if not input_response_data:
    base_response.received = []
    yield base_response
    return

  # separate into one answer ip per row for tagging
  rcodes = input_response_data.pop('rcode', [])
  errors = input_response_data.pop('error', [])

  successful_test_rcode, failed_test_rcodes = split_rcodes(rcodes)

  if any(rcode == 0 for rcode in failed_test_rcodes):
    raise Exception(
        f"Satellite v2.1 measurement has multiple 0 rcodes: {filepath} - {responses_entry}"
    )

  if not successful_test_rcode and input_response_data:
    # TODO figure out how to handle this bug case from 2021-05-16
    # for now drop the measurements
    logging.warning(
        f"Satellite v2.1 measurement has ips but no 0 rcode: {filepath} - {responses_entry}"
    )
    return

  if successful_test_rcode:
    all_received = []
    for (ip, tags) in input_response_data.items():
      received = SatelliteAnswer(ip)
      if tags:
        received.matches_control = _make_matches_control(tags)
      all_received.append(received)

    success_observation = deepcopy(base_response)
    success_observation.received = all_received
    success_observation.rcode = successful_test_rcode[0]
    yield success_observation

  for rcode in failed_test_rcodes:
    failed_observation = deepcopy(base_response)
    failed_observation.rcode = rcode
    # -1 rcodes corrospond to the error messages in order.
    if rcode == -1:
      failed_observation.error = errors.pop() if errors else None
    yield failed_observation

  # There is a bug in some v2.1 data where -1 rcodes weren't recorded.
  # In that case we add them back in manually
  for error in errors:
    error_observation = deepcopy(base_response)
    error_observation.rcode = -1
    error_observation.error = error
    yield error_observation


class SatelliteFlattener():
  """Methods for flattening satellite data"""

  def __init__(self, blockpage_matcher: BlockpageMatcher,
               category_matcher: DomainCategoryMatcher):
    self.blockpage_matcher = blockpage_matcher
    self.category_matcher = category_matcher

  def process_satellite(self, filepath: str, responses_entry: ResponsesEntry,
                        measurement_id: str) -> Iterator[SatelliteRow]:
    """Process a line of Satellite data.

    Args:
      filepath: a filepath string
      responses_entry: a loaded json object containing the parsed content of the line
      measurement_id: a hex id identifying this individual measurement

    Yields:
      SatelliteRow
    """
    date = re.findall(r'\d\d\d\d-\d\d-\d\d', filepath)[0]
    filename = get_filename(filepath)

    if datetime.date.fromisoformat(date) < SATELLITE_V2_1_START_DATE:
      yield from self._process_satellite_v1(date, responses_entry, filepath,
                                            measurement_id)
    else:
      if filename == SATELLITE_RESPONSES_CONTROL_FILE:
        yield from self._process_satellite_v2_control(responses_entry, filepath,
                                                      measurement_id)
      else:
        yield from self._process_satellite_v2(responses_entry, filepath,
                                              measurement_id)

  def _process_satellite_v1(self, date: str, responses_entry: ResponsesEntry,
                            filepath: str,
                            measurement_id: str) -> Iterator[SatelliteRow]:
    """Process a line of Satellite data.

    Args:
      date: a date string YYYY-mm-DD
      responses_entry: a loaded json object containing the parsed content of the line
      filepath: one of
        <path>/answers_control.json
        <path>/interference.json
        also potentially .gz files
      measurement_id: a hex id identifying this individual measurement

    Yields:
      SatelliteRow
    """
    filename = pathlib.PurePosixPath(filepath).name
    if '.gz' in pathlib.PurePosixPath(filename).suffixes:
      filename = pathlib.PurePosixPath(filename).stem

    row = SatelliteRow(
        domain=responses_entry['query'],
        is_control=False,  # v1 doesn't have domain controls
        controls_failed=False,  # v1 doesn't have domain controls
        category=self.category_matcher.get_category(responses_entry['query'],
                                                    False),
        ip=responses_entry.get('resolver', responses_entry.get('ip')),
        is_control_ip=filename == SATELLITE_ANSWERS_CONTROL_FILE,
        date=date,
        anomaly=not responses_entry['passed']
        if 'passed' in responses_entry else None,
        success='error' not in responses_entry,
        received=[],
        rcode=0 if 'error' not in responses_entry else -1,
        measurement_id=measurement_id,
        source=flatten_base.source_from_filename(filepath),
    )

    error = responses_entry.get('error', None)
    if isinstance(error, dict):
      row.error = json.dumps(error)
    else:
      row.error = error

    received_ips = responses_entry.get('answers')
    yield from _annotate_received_ips_v1(row, received_ips)

  def _process_satellite_v2(self, responses_entry: ResponsesEntry,
                            filepath: str,
                            measurement_id: str) -> Iterator[SatelliteRow]:
    """Process a line of Satellite v2 data.

    Args:
      responses_entry: a loaded json object containing the parsed content of the line
      filepath: path like "<path>/<filename>.json.gz"
      measurement_id: a hex id identifying this individual measurement

    Yields:
      SatelliteRow
    """
    is_control_domain = flatten_base.is_control_url(responses_entry['test_url'])
    date = responses_entry['start_time'][:10]

    row = SatelliteRow(
        domain=responses_entry['test_url'],
        is_control=is_control_domain,
        category=self.category_matcher.get_category(responses_entry['test_url'],
                                                    is_control_domain),
        ip=responses_entry['vp'],
        is_control_ip=responses_entry['vp'] in CONTROL_IPS,
        date=date,
        start_time=format_timestamp(responses_entry['start_time']),
        end_time=format_timestamp(responses_entry['end_time']),
        error=responses_entry.get('error', None),
        anomaly=responses_entry['anomaly'],
        success=not responses_entry['connect_error'],
        received=[],
        measurement_id=measurement_id,
        source=flatten_base.source_from_filename(filepath),
        ip_metadata=IpMetadata(
            country=responses_entry.get('location', {}).get('country_code'),))

    if datetime.date.fromisoformat(date) < SATELLITE_V2_2_START_DATE:
      yield from _process_satellite_v2p1(row, responses_entry, filepath)
    else:
      yield from self._process_satellite_v2p2(row, responses_entry)

  def _process_satellite_v2p2(
      self, base_row: SatelliteRow,
      responses_entry: ResponsesEntry) -> Iterator[SatelliteRow]:
    """Finish processing a line of Satellite v2.2 data.

    Args:
      base_row: a partially processed row of satellite data
      responses_entry: a loaded json object containing the parsed content of the line

    Yields:
      Rows
    """
    responses = responses_entry.get('response', [])
    base_row.controls_failed = not responses_entry['passed_liveness']
    base_row.untagged_controls = responses_entry.get(
        'confidence')['untagged_controls']
    base_row.untagged_response = responses_entry.get(
        'confidence')['untagged_response']
    base_row.excluded = responses_entry.get('excluded', False)
    base_row.exclude_reason = ' '.join(
        responses_entry.get('exclude_reason', []))

    # We yield a row for each individual roundrip in the measurement
    for roundtrip in responses:
      roundtrip_row = deepcopy(base_row)

      #redefine domain rows to match individual response
      # TODO normalize this to a domain from a url
      roundtrip_row.domain = roundtrip['url']
      is_control_domain = flatten_base.is_control_url(roundtrip['url'])
      roundtrip_row.is_control = is_control_domain
      roundtrip_row.category = self.category_matcher.get_category(
          roundtrip['url'], is_control_domain)

      if roundtrip['error'] and roundtrip['error'] != 'null':
        roundtrip_row.error = roundtrip['error']

      roundtrip_row.rcode = roundtrip['rcode']
      roundtrip_row.success = roundtrip['rcode'] == 0

      # Check responses for test domain with valid answers
      if roundtrip['url'] == roundtrip_row.domain and (
          roundtrip['rcode'] == 0 and roundtrip['has_type_a']):
        roundtrip_row.has_type_a = True

      answers: Dict[str, Dict[str, Any]] = roundtrip['response']
      # confidence only corresponds to fields with received ips
      if len(answers) == 0:
        yield roundtrip_row
      else:
        roundtrip_row.average_confidence = responses_entry.get(
            'confidence')['average']
        matches_confidence: Optional[List[
            Optional[float]]] = responses_entry.get('confidence')['matches']
        all_received = []

        # Sometimes matches_confidence field is not present
        if matches_confidence is None:
          matches_confidence = [None] * len(answers.keys())

        for ((ip, answer), match_confidence) in zip(answers.items(),
                                                    matches_confidence):
          received = SatelliteAnswer(
              ip=ip,
              http=answer.get('http'),
              cert=answer.get('cert'),
              match_confidence=match_confidence,
              ip_metadata=IpMetadata(
                  as_name=answer.get('asname'),
                  asn=answer.get('asnum'),
              ))
          matched = answer.get('matched', [])
          if matched:
            received.matches_control = _make_matches_control(matched)
          all_received.append(received)
        roundtrip_row.received = all_received
        yield roundtrip_row

  def _process_satellite_v2_control(
      self, responses_entry: ResponsesEntry, filepath: str,
      measurement_id: str) -> Iterator[SatelliteRow]:
    """Process a line of Satellite ip control data.

      Args:
        responses_entry: a loaded json object containing the parsed content of the line
        filepath: path like "<path>/responses_control.json.gz"
        measurement_id: a hex id identifying this individual measurement

      Yields:
        SatelliteRow
    """
    responses = responses_entry.get('response', [])

    for response in responses:
      is_control_domain = flatten_base.is_control_url(response['url'])

      row = SatelliteRow(
          domain=response['url'],
          is_control=is_control_domain,
          category=self.category_matcher.get_category(response['url'],
                                                      is_control_domain),
          ip=responses_entry['vp'],
          is_control_ip=True,
          date=response['start_time'][:10],
          start_time=format_timestamp(response['start_time']),
          end_time=format_timestamp(response['end_time']),
          anomaly=None,
          success=not responses_entry['connect_error'],
          controls_failed=not responses_entry['passed_control'],
          rcode=response['rcode'],
          has_type_a=response['has_type_a'],
          measurement_id=measurement_id,
          source=flatten_base.source_from_filename(filepath),
      )

      if response['error'] == 'null':
        error = None
      else:
        error = response['error']
      row.error = error

      received_ips = response['response']

      if not received_ips:
        yield row

      else:
        all_received = []
        for ip in received_ips:
          received = SatelliteAnswer(ip)
          all_received.append(received)
        row.received = all_received

        yield row


class FlattenBlockpages(beam.DoFn):
  """DoFn class for flattening lines of blockpage text into Rows."""

  def setup(self) -> None:
    #pylint: disable=attribute-defined-outside-init
    self.blockpage_matcher = BlockpageMatcher()
    #pylint: enable=attribute-defined-outside-init

  def process(self, element: Tuple[str, str]) -> Iterator[PageFetchRow]:
    (filename, line) = element

    try:
      scan = json.loads(line)
    except json.decoder.JSONDecodeError as e:
      logging.warning('JSONDecodeError: %s\nFilename: %s\n%s\n', e, filename,
                      line)
      return

    yield from self._process_satellite_blockpages(scan, filename)

  def _process_satellite_blockpages(self, blockpage_entry: BlockpageEntry,
                                    filepath: str) -> Iterator[PageFetchRow]:
    """Process a line of Satellite blockpage data.

    Args:
      blockpage_entry: a loaded json object containing the parsed content of the line
      filepath: a filepath string like "<path>/blockpages.json.gz"

    Yields:
      PageFetchRow, usually 2 corresponding to the fetched http and https data
    """
    domain = blockpage_entry['keyword']

    row = PageFetchRow(
        domain=domain,
        ip=blockpage_entry['ip'],
        date=blockpage_entry['start_time'][:10],
        start_time=format_timestamp(blockpage_entry['start_time']),
        end_time=format_timestamp(blockpage_entry['end_time']),
        success=blockpage_entry['fetched'],
        source=flatten_base.source_from_filename(filepath),
    )

    http_row = deepcopy(row)
    http_row.https = False
    received = blockpage_entry.get('http', None)
    if isinstance(received, str):
      http_row.error = received
    if isinstance(received, dict):
      http_row.received = flatten_base.parse_received_data(
          self.blockpage_matcher, received, domain, 'satellite', True)
    yield http_row

    https_row = deepcopy(row)
    https_row.https = True
    received = blockpage_entry.get('https', None)
    if isinstance(received, str):
      https_row.error = received
    if isinstance(received, dict):
      https_row.received = flatten_base.parse_received_data(
          self.blockpage_matcher, received, domain, 'satellite', True)
      https_row.received.tls_cert_has_trusted_ca = blockpage_entry.get(
          'trusted_cert', None)
      https_row.received.tls_cert_matches_domain = blockpage_entry.get(
          'cert_hostname_match', None)
    yield https_row
