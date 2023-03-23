"""Names for global pipeline metrics."""

import logging
from typing import Any

import apache_beam as beam
from apache_beam.metrics.metric import Metrics

NAMESPACE = 'censoredplanet'

# Number of json lines that we fail to parse due to json errors
MEASUREMENT_JSON_PARSE_ERRORS_COUNTER = Metrics.counter(
    NAMESPACE, 'measurement_json_parse_errors')

# Number of (successfully parsed) json lines we read in
MEASUREMENT_LINES_COUNTER = Metrics.counter(NAMESPACE, 'measurement_lines')

# Number of roundtrip rows output from the flattening stage
ROWS_FLATTENED_COUNTER = Metrics.counter(NAMESPACE, 'rows_flattened')

# Number of roundtrips rows output after the metadata addition stage
ROWS_WITH_METADATA_COUNTER = Metrics.counter(NAMESPACE, 'rows_with_metadata')


# pylint: disable=invalid-name
def countPCollection(pcol: beam.pvalue.PCollection[Any], name: str) -> None:
  """Count a PCollection and log the count."""

  def _log_count(num: int) -> None:
    logging.info(f'Global count of {name} is {num}')

  # pylint: disable=expression-not-assigned
  (pcol | f"Count collection {name}" >> beam.combiners.Count.Globally() |
   f"Log count of collection {name}" >> beam.Map(_log_count))
