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
      yield from self._process_non_satellite(filename, scan,
                                             random_measurement_id)

  def _process_non_satellite(self, filename: str, scan: Any,
                             random_measurement_id: str) -> Iterator[Row]:
    """Process a line of Echo/Discard/HTTP/S data.

    Args:
      filename: a filepath string
      scan: a loaded json object containing the parsed content of the line
      random_measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    for result in scan.get('Results', []):
      date = result['StartTime'][:10]
      row = {
          'domain': scan['Keyword'],
          'category': self.category_matcher.match_url(scan['Keyword']),
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
          'source': source_from_filename(filename),
      }

      if 'Received' in result:
        received = result.get('Received', '')
        received_fields = self._parse_received_data(received, scan['Blocked'])
        row.update(received_fields)

      if 'Error' in result:
        row['error'] = result['Error']

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
        'blocked': not scan['passed'] if 'passed' in scan else None,
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
    yield from self._process_received_ips(row, received_ips)

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
      row['rcode'] = received_ips.pop('rcode', None)
    for ip in received_ips:
      row['received'] = {'ip': ip}
      if row['blocked']:
        # Track domains per IP for interference
        INTERFERENCE_IPDOMAIN[ip].add(row['domain'])
      if isinstance(received_ips, dict):
        row['received']['matches_control'] = ' '.join(  # pylint: disable=unsupported-assignment-operation
            [tag for tag in received_ips[ip] if tag in SATELLITE_TAGS])
      yield row

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
      return {'received_status': received}

    row = {
        'received_status': received['status_line'],
        'received_body': received['body'],
        'received_headers': parse_received_headers(received.get('headers', {})),
    }

    if anomaly:  # check response for blockpage
      blockpage, signature = self.blockpage_matcher.match_page(received['body'])
      row['blockpage'] = blockpage
      row['page_signature'] = signature

    tls = received.get('tls', None)
    if tls:
      tls_row = {
          'received_tls_version': tls['version'],
          'received_tls_cipher_suite': tls['cipher_suite'],
          'received_tls_cert': tls['cert']
      }
      row.update(tls_row)

    return row
