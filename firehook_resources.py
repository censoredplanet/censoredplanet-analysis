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
