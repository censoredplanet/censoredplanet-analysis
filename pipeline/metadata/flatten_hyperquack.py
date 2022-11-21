"""Flattening methods for Hyperquack data."""

from __future__ import absolute_import

import re
from typing import Optional, Any, Iterator

from pipeline.metadata import flatten_base
from pipeline.metadata.schema import HyperquackRow
from pipeline.metadata.blockpage import BlockpageMatcher
from pipeline.metadata.domain_categories import DomainCategoryMatcher

# For Hyperquack v1
# echo/discard domain and url content
SENT_PATTERN = "GET (.*) HTTP/1.1\r\nHost: (.*)\r\n"


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


class HyperquackFlattener():
  """Methods for flattening hyperquack data"""

  def __init__(self, blockpage_matcher: BlockpageMatcher,
               category_matcher: DomainCategoryMatcher):
    self.blockpage_matcher = blockpage_matcher
    self.category_matcher = category_matcher

  def process_hyperquack(self, filename: str, scan: Any,
                         measurement_id: str) -> Iterator[HyperquackRow]:
    """Process a line of Echo/Discard/HTTP/S data.

    Args:
      filename: a filepath string
      scan: a loaded json object containing the parsed content of the line
      measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    if 'Server' in scan:
      yield from self._process_hyperquack_v1(filename, scan, measurement_id)
    elif 'vp' in scan:
      yield from self._process_hyperquack_v2(filename, scan, measurement_id)
    else:
      raise Exception(f"Line with unknown hyperquack format:\n{scan}")

  def _process_hyperquack_v1(self, filename: str, scan: Any,
                             measurement_id: str) -> Iterator[HyperquackRow]:
    """Process a line of Echo/Discard/HTTP/S data in HyperQuack V1 format.

    https://github.com/censoredplanet/censoredplanet/blob/master/docs/hyperquackv1.rst

    Args:
      filename: a filepath string
      scan: a loaded json object containing the parsed content of the line
      measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    for index, result in enumerate(scan.get('Results', [])):
      date = result['StartTime'][:10]

      sent_domain = _extract_domain_from_sent_field(result['Sent'])
      is_control = flatten_base.is_control_url(sent_domain)
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

      row = HyperquackRow(
          domain=domain,
          category=self.category_matcher.get_category(domain, is_control),
          ip=scan['Server'],
          date=date,
          start_time=result['StartTime'],
          end_time=result['EndTime'],
          retry=None if is_control else index,
          anomaly=scan['Blocked'],
          success=result['Success'],
          stateful_block=scan['StatefulBlock'],
          is_control=is_control,
          controls_failed=scan['FailSanity'],
          measurement_id=measurement_id,
          source=flatten_base.source_from_filename(filename))

      if 'Received' in result:
        received = result.get('Received', '')
        scan_type = flatten_base.get_scan_type_from_filename(filename)
        received_fields = flatten_base.parse_received_data(
            self.blockpage_matcher, received, domain, scan_type,
            scan['Blocked'])
        row.received = received_fields

      if 'Error' in result:
        row.error = result['Error']

      yield row

  def _process_hyperquack_v2(self, filename: str, scan: Any,
                             measurement_id: str) -> Iterator[HyperquackRow]:
    """Process a line of Echo/Discard/HTTP/S data in HyperQuack V2 format.

    https://github.com/censoredplanet/censoredplanet/blob/master/docs/hyperquackv2.rst

    Args:
      filename: a filepath string
      scan: a loaded json object containing the parsed content of the line
      measurement_id: a hex id identifying this individual measurement

    Yields:
      Rows
    """
    # Retries don't include controls,
    # which can come at the beginning and end of measurements
    retry_index = 0

    for response in scan.get('response', []):
      date = response['start_time'][:10]
      domain: str = response.get('control_url', scan['test_url'])
      is_control = 'control_url' in response

      row = HyperquackRow(
          domain=domain,
          category=self.category_matcher.get_category(domain, is_control),
          ip=scan['vp'],
          date=date,
          start_time=response['start_time'],
          end_time=response['end_time'],
          retry=None if is_control else retry_index,
          anomaly=scan['anomaly'],
          success=response['matches_template'],
          stateful_block=scan['stateful_block'],
          is_control=is_control,
          controls_failed=scan.get('controls_failed', None),
          measurement_id=measurement_id,
          source=flatten_base.source_from_filename(filename),
      )

      if 'response' in response:
        received = response.get('response', '')
        scan_type = flatten_base.get_scan_type_from_filename(filename)
        received_fields = flatten_base.parse_received_data(
            self.blockpage_matcher, received, domain, scan_type,
            scan['anomaly'])
        row.received = received_fields

      if 'error' in response:
        row.error = response['error']

      yield row
      if not is_control:
        retry_index += 1
