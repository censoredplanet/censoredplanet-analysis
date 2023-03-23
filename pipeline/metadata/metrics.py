"""Names for global pipeline metrics."""

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
