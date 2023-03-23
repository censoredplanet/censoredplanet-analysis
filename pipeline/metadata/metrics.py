"""Names for global pipeline metrics."""

NAMESPACE = 'censoredplanet'

JSON_MEASUREMENT_PARSE_ERRORS = 'json_measurement_parse_errors'  # Number of json lines that we fail to parse due to json errors
MEASUREMENT_LINES = 'measurement_lines'  # Number of (successfully parsed) json lines we read in
ROWS_FLATTENED = 'rows_flattened'  # Number of roundtrip rows output from the flattening stage
ROWS_WITH_METADATA = 'rows_with_metadata'  # Number of roundtrips rows output after the metadata addition stage
