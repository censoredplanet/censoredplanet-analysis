"""Various cross-file constants and project-specific initializers."""

PROJECT_NAME = 'firehook-censoredplanet'

# Buckets that store scanfiles
U_MICH_BUCKET = 'censoredplanetscanspublic'
TARRED_BUCKET = 'firehook-censoredplanetscanspublic'
UNTARRED_BUCKET = 'firehook-scans'
INPUT_BUCKET = f'gs://{UNTARRED_BUCKET}/'

# Buckets that store CAIDA information
CAIDA_BUCKET = 'censoredplanet_geolocation'
CAIDA_FILE_LOCATION = f'gs://{CAIDA_BUCKET}/caida/'
ROUTEVIEW_PATH = 'caida/routeviews/'

# Temp Buckets
BEAM_STAGING_LOCATION = 'gs://firehook-dataflow-test/staging'
BEAM_TEMP_LOCATION = 'gs://firehook-dataflow-test/temp'
