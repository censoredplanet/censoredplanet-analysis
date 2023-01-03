# Copyright 2020 Jigsaw Operations LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Various cross-file constants and project-specific initializers."""

PROD_PROJECT_NAME = 'censoredplanet-analysisv1'
DEV_PROJECT_NAME = 'firehook-censoredplanet'

# Bucket that stores scanfiles
SCAN_BUCKET = 'censoredplanetscanspublic'
INPUT_BUCKET = f'gs://{SCAN_BUCKET}/'

ROUTEVIEW_PATH = 'caida/routeviews/'

# Buckets that store METADATA information
DEV_METADATA_BUCKET = 'censoredplanet_geolocation'
DEV_CAIDA_FILE_LOCATION = f'gs://{DEV_METADATA_BUCKET}/caida/'
DEV_MAXMIND_FILE_LOCATION = f'gs://{DEV_METADATA_BUCKET}/maxmind/'
DEV_DBIP_FILE_LOCATION = f'gs://{DEV_METADATA_BUCKET}/dbip/'

PROD_METADATA_BUCKET = 'censoredplanet_ip_metadata'
PROD_CAIDA_FILE_LOCATION = f'gs://{PROD_METADATA_BUCKET}/caida/'
PROD_MAXMIND_FILE_LOCATION = f'gs://{PROD_METADATA_BUCKET}/maxmind/'
PROD_DBIP_FILE_LOCATION = f'gs://{PROD_METADATA_BUCKET}/dbip/'

# Output GCS Buckets
DEV_OUTPUT_BUCKET = 'firehook-test'
PROD_OUTPUT_BUCKET = 'censoredplanetraw'

# Temp Buckets
DEV_BEAM_STAGING_LOCATION = 'gs://firehook-dataflow-test/staging'
DEV_BEAM_TEMP_LOCATION = 'gs://firehook-dataflow-test/temp'

PROD_BEAM_STAGING_LOCATION = 'gs://censoredplanet-analysis-beam-staging/staging'
PROD_BEAM_TEMP_LOCATION = 'gs://censoredplanet-analysis-beam-staging/temp'
