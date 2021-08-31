"""Beam pipeline helper for flattening measurements lines into bq format."""

from __future__ import absolute_import

from collections import defaultdict
import json
import logging
import os
import re
from typing import Optional, Tuple, Dict, List, Any, Iterator, Union, Set
import uuid

import apache_beam as beam

from pipeline.metadata.blockpage import BlockpageMatcher
from pipeline.metadata.domain_categories import DomainCategoryMatcher

# Custom Type
# All or part of a scan row to be written to bigquery
# ex (only scan data): {'domain': 'test.com', 'ip': '1.2.3.4', 'success' true}
# ex (only ip metadata): {'asn': 13335, 'as_name': 'CLOUDFLAREINC'}
# ex (both): {'domain': 'test.com', 'ip': '1.2.3.4', 'asn': 13335}
Row = Dict[str, Any]

SATELLITE_TAGS = {'ip', 'http', 'asnum', 'asname', 'cert'}
INTERFERENCE_IPDOMAIN: Dict[str, Set[str]] = defaultdict(set)

# For Hyperquack v1
# echo/discard domain and url content
SENT_PATTERN = "GET (.*) HTTP/1.1\r\nHost: (.*)\r\n"

# For Hyperquack v1
CONTROL_URLS = [
    'example5718349450314.com',  # echo/discard
    'rtyutgyhefdafioasfjhjhi.com'  # HTTP/S
]


def parse_received_headers(headers: Dict[str, List[str]]) -> List[str]:
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


def source_from_filename(filepath: str) -> str:
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


def format_timestamp(timestamp: str) -> str:
  """Format the timestamp in ISO8601 format for BigQuery.

  Args:
    timestamp: Satellite format timestamp
      "2021-04-18 14:49:01.62448452 -0400 EDT m=+10140.555964129"

  Returns:
    ISO8601 formatted string "2021-04-18T14:49:01.62448452-04:00"
  """
  elements = timestamp.split()
  return '{0}T{1}{2}:{3}'.format(elements[0], elements[1], elements[2][0:-2],
                                 elements[2][-2:])


def _extract_domain_from_sent_field(sent: str) -> Optional[str]:
  """Get the url out of a 'sent' field in a measurement.

  Args:
    sent: string like either

      "" meaning the sent packet wasn't recorded.
      "GET / HTTP/1.1\r\nHost: example5718349450314.com\r\n" (echo/discard)
      "GET www.bbc.co.uk HTTP/1.1\r\nHost: /content.html\r\n" (discard error)
      or just "www.apple.com" (HTTP/S)

    Returns: just the url or None
  """
  if sent == '':
    return None

  match = re.search(SENT_PATTERN, sent)
  if match:
    path = match.group(1)
    domain = match.group(2)

    # This is a bug where the domain and path were reversed in content sent.
    # We do our best to reconstruct the intended url
    # by swapping them to their intended position
    # TODO should we do something else instead because the test is invalid?
    if domain[0] == '/':
      domain, path = path, domain

    if path == '/':
      return domain
    return domain + path

  if ' ' not in sent:
    return sent

  raise Exception(f"unknown sent field format: {sent}")


def _is_control_url(url: Optional[str]) -> bool:
  return url in CONTROL_URLS


class FlattenMeasurement(beam.DoFn):
  """DoFn class for flattening lines of json text into Rows."""

  def setup(self) -> None:
    self.blockpage_matcher = BlockpageMatcher()  #pylint: disable=attribute-defined-outside-init
    self.category_matcher = DomainCategoryMatcher()  #pylint: disable=attribute-defined-outside-init

  def process(self, element: Tuple[str, str]) -> Iterator[Row]:
    """Flatten a measurement string into several roundtrip Rows.

    Args:
      element: Tuple(filepath, line)
        filename: a filepath string
        line: a json string describing a censored planet measurement. example
        {'Keyword': 'test.com',
        'Server': '1.2.3.4',
        'Results': [{'Success': true},
                    {'Success': false}]}

    Yields:
      Row dicts containing individual roundtrip information
      {'column_name': field_value}
      examples:
      {'domain': 'test.com', 'ip': '1.2.3.4', 'success': true}
      {'domain': 'test.com', 'ip': '1.2.3.4', 'success': false}
    """
    (filename, line) = element

    # pylint: disable=too-many-branches
    try:
      scan = json.loads(line)
    except json.decoder.JSONDecodeError as e:
      logging.warning('JSONDecodeError: %s\nFilename: %s\n%s\n', e, filename,
                      line)
      return

    # Add a unique id per-measurement so single retry rows can be reassembled
    random_measurement_id = uuid.uuid4().hex

    if 'Satellite' in filename:
      yield from self._process_satellite(filename, scan, random_measurement_id)
    else:
      yield from self._process_hyperquack(filename, scan, random_measurement_id)

  def _process_hyperquack(self, filename: str, scan: Any,
                          random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Echo/Discard/HTTP/S data.

    Args:
      filename: a filepath string
      scan: a loaded json object containing the parsed content of the line
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    if 'Server' in scan:
      yield from self._process_hyperquack_v1(filename, scan,
                                             random_measurement_id)
    elif 'vp' in scan:
      yield from self._process_hyperquack_v2(filename, scan,
                                             random_measurement_id)
    else:
      raise Exception(f"Line with unknown hyperquack format:\n{scan}")

  def _process_hyperquack_v1(self, filename: str, scan: Any,
                             random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Echo/Discard/HTTP/S data in HyperQuack V1 format.

    https://github.com/censoredplanet/censoredplanet/blob/master/docs/hyperquackv1.rst

    Args:
      filename: a filepath string
      scan: a loaded json object containing the parsed content of the line
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    for index, result in enumerate(scan.get('Results', [])):
      date = result['StartTime'][:10]

      sent_domain = _extract_domain_from_sent_field(result['Sent'])
      is_control = _is_control_url(sent_domain)
      # Due to a bug the sent field sometimes isn't populated
      # when the measurement failed due to network timeout.
      if not sent_domain:
        # Control measurements come at the end, and are not counted as retries.
        is_control = index > scan['Retries']
        if is_control:
          domain = ""
        else:
          domain = scan['Keyword']
      else:
        domain = sent_domain

      row = {
          'domain': domain,
          'category': self._get_category(domain, is_control),
          'ip': scan['Server'],
          'date': date,
          'start_time': result['StartTime'],
          'end_time': result['EndTime'],
          'anomaly': scan['Blocked'],
          'success': result['Success'],
          'stateful_block': scan['StatefulBlock'],
          'is_control': is_control,
          'controls_failed': scan['FailSanity'],
          'measurement_id': random_measurement_id,
          'source': source_from_filename(filename),
      }

      if 'Received' in result:
        received = result.get('Received', '')
        received_fields = self._parse_received_data(received, scan['Blocked'])
        row.update(received_fields)

      if 'Error' in result:
        row['error'] = result['Error']

      yield row

  def _process_hyperquack_v2(self, filename: str, scan: Any,
                             random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Echo/Discard/HTTP/S data in HyperQuack V2 format.

    https://github.com/censoredplanet/censoredplanet/blob/master/docs/hyperquackv2.rst

    Args:
      filename: a filepath string
      scan: a loaded json object containing the parsed content of the line
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    for response in scan.get('response', []):
      date = response['start_time'][:10]
      domain: str = response.get('control_url', scan['test_url'])
      is_control = 'control_url' in response

      row = {
          'domain': domain,
          'category': self._get_category(domain, is_control),
          'ip': scan['vp'],
          'date': date,
          'start_time': response['start_time'],
          'end_time': response['end_time'],
          'anomaly': scan['anomaly'],
          'success': response['matches_template'],
          'stateful_block': scan['stateful_block'],
          'is_control': is_control,
          'controls_failed': scan.get('controls_failed', None),
          'measurement_id': random_measurement_id,
          'source': source_from_filename(filename),
      }

      if 'response' in response:
        received = response.get('response', '')
        received_fields = self._parse_received_data(received, scan['anomaly'])
        row.update(received_fields)

      if 'error' in response:
        row['error'] = response['error']

      yield row

  def _process_satellite(self, filename: str, scan: Any,
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
    if date < "2021-03":
      yield from self._process_satellite_v1(date, scan, random_measurement_id)
    else:
      if "responses_control" in filename:
        yield from self._process_satellite_v2_responses(scan,
                                                        random_measurement_id)
      else:
        yield from self._process_satellite_v2(scan, random_measurement_id)

  def _process_satellite_v1(  # pylint: disable=no-self-use
      self, date: str, scan: Any, random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Satellite data.

    Args:
      date: a date string YYYY-mm-DD
      scan: a loaded json object containing the parsed content of the line
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    row = {
        'domain': scan['query'],
        'category': self.category_matcher.match_url(scan['query']),
        'ip': scan['resolver'],
        'date': date,
        'error': scan.get('error', None),
        'anomaly': not scan['passed'] if 'passed' in scan else None,
        'success': 'error' not in scan,
        'received': None,
        'measurement_id': random_measurement_id,
    }
    received_ips = scan.get('answers')
    yield from self._process_received_ips(row, received_ips)

  def _process_satellite_v2(self, scan: Any,
                            random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Satellite data.

    Args:
      scan: a loaded json object containing the parsed content of the line
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    row = {
        'domain': scan['test_url'],
        'category': self.category_matcher.match_url(scan['test_url']),
        'ip': scan['vp'],
        'country': scan.get('location', {}).get('country_code'),
        'date': scan['start_time'][:10],
        'start_time': format_timestamp(scan['start_time']),
        'end_time': format_timestamp(scan['end_time']),
        'error': scan.get('error', None),
        'anomaly': scan['anomaly'],
        'success': not scan['connect_error'],
        'controls_failed': not scan['passed_control'],
        'received': None,
        'rcode': [],
        'measurement_id': random_measurement_id
    }
    received_ips = scan.get('response')
    yield from self._process_received_ips(row, received_ips)

  def _process_satellite_v2_responses(
      self, scan: Any, random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Satellite response data.

      Args:
        scan: a loaded json object containing the parsed content of the line
        random_measurement_id: a hex id identifying this individual measurement

      Yields:
        Rows
    """
    responses = scan.get('response', [])
    if responses:
      row = {
          'domain': scan['test_url'],
          'category': self.category_matcher.match_url(scan['test_url']),
          'ip': scan['vp'],
          'date': responses[0]['start_time'][:10],
          'start_time': format_timestamp(responses[0]['start_time']),
          'end_time': format_timestamp(responses[-1]['end_time']),
          'anomaly': None,
          'success': not scan['connect_error'],
          'controls_failed': not scan['passed_control'],
          'rcode': [str(response['rcode']) for response in responses],
          'measurement_id': random_measurement_id
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

  def _process_received_ips(  # pylint: disable=no-self-use
      self, row: Row, received_ips: Optional[Dict[str, str]]) -> Iterator[Row]:
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

  def _parse_received_data(self, received: Union[str, Dict[str, Any]],
                           anomaly: bool) -> Row:
    """Parse a received field into a section of a row to write to bigquery.

    Args:
      received: a dict parsed from json data, or a str
      anomaly: whether data may indicate blocking

    Returns:
      a dict containing the 'received_' keys/values in SCAN_BIGQUERY_SCHEMA
    """
    if isinstance(received, str):
      row: Row = {'received_status': received}
      self.add_blockpage_match(received, anomaly, row)
      return row

    row = {
        'received_status': received['status_line'],
        'received_body': received['body'],
        'received_headers': parse_received_headers(received.get('headers', {})),
    }

    self.add_blockpage_match(received['body'], anomaly, row)

    # hyperquack v1 TLS format
    tls = received.get('tls', None)
    if tls:
      tls_row = {
          'received_tls_version': tls['version'],
          'received_tls_cipher_suite': tls['cipher_suite'],
          'received_tls_cert': tls['cert']
      }
      row.update(tls_row)

    # hyperquack v2 TLS format
    if 'TlsVersion' in received:
      tls_row = {
          'received_tls_version': received['TlsVersion'],
          'received_tls_cipher_suite': received['CipherSuite'],
          'received_tls_cert': received['Certificate']
      }
      row.update(tls_row)

    return row

  def add_blockpage_match(self, content: str, anomaly: bool, row: Row) -> None:
    """If there's an anomaly check the content for a blockpage match and add to row

    content: the string to check for blockpage matches.
      For HTTP/S this is the HTTP body
      For echo/discard this is the entire recieved content
    anomaly: whether there was an anomaly in the measurement
    row: existing row to add blpckpage info to.
    """
    if anomaly:
      blockpage, signature = self.blockpage_matcher.match_page(content)
      row['blockpage'] = blockpage
      row['page_signature'] = signature

  def _get_category(self, domain: str, is_control: bool) -> Optional[str]:
    if is_control:
      return "Control"
    return self.category_matcher.match_url(domain)
