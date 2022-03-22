"""Beam pipeline helper for flattening measurements lines into bq format."""

from __future__ import absolute_import

import json
import logging
from typing import Tuple, Iterator
import uuid

import apache_beam as beam

from pipeline.metadata.schema import BigqueryRow
from pipeline.metadata.blockpage import BlockpageMatcher
from pipeline.metadata.domain_categories import DomainCategoryMatcher
from pipeline.metadata.flatten_satellite import SatelliteFlattener, SATELLITE_PATH_COMPONENT
from pipeline.metadata.flatten_hyperquack import HyperquackFlattener

# Randomly chosen UUID used as a namespace for generating further UUIDs
CENSORED_PLANET_NAMESPACE = uuid.UUID('23fd299b-6547-4dca-b630-cb05dec70e0e')


class FlattenMeasurement(beam.DoFn):
  """DoFn class for flattening lines of json text into BigqueryRow."""

  def setup(self) -> None:
    blockpage_matcher = BlockpageMatcher()
    category_matcher = DomainCategoryMatcher()
    #pylint: disable=attribute-defined-outside-init
    self.hyperquack_flattener = HyperquackFlattener(blockpage_matcher,
                                                    category_matcher)
    self.satellite_flattener = SatelliteFlattener(blockpage_matcher,
                                                  category_matcher)
    #pylint: enable=attribute-defined-outside-init

  def process(self, element: Tuple[str, str]) -> Iterator[BigqueryRow]:
    """Flatten a measurement string into several roundtrip BigqueryRow.

    Args:
      element: Tuple(filepath, line)
        filename: a filepath string
        line: a json string describing a censored planet measurement. example
        {'Keyword': 'test.com',
        'Server': '1.2.3.4',
        'Results': [{'Success': true},
                    {'Success': false}]}

    Yields:
      BigqueryRow dicts containing individual roundtrip information
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
    measurement_id = uuid.uuid5(CENSORED_PLANET_NAMESPACE, filename + line).hex

    if SATELLITE_PATH_COMPONENT in filename:
      yield from self.satellite_flattener.process_satellite(
          filename, scan, measurement_id)
    else:
      yield from self.hyperquack_flattener.process_hyperquack(
          filename, scan, measurement_id)
