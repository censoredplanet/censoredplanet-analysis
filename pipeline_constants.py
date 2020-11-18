"""Various cross-file constants and project-specific initializers."""

import datetime
from google.cloud import storage

from mirror.data_transfer import setup_transfer_service
from mirror.routeviews.bulk_download import download_manual_routeviews
from mirror.routeviews.sync import RouteviewMirror
from mirror.untar_files.sync import ScanfileMirror
from pipeline.beam_tables import ScanDataBeamPipelineRunner
from pipeline.beam_tables import SCAN_BIGQUERY_SCHEMA
from pipeline.metadata import ip_metadata

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

# Output table name pieces
# Tables have names like 'scan' and 'scan_test'
SCAN_TABLE_NAME = 'scan'
# Datasets have names like 'echo_results', and 'https_results'
DATASET_SUFFIX = '_results'


# Initializers for specific pipeline steps with Firehook values.
def setup_firehook_data_transfer():
  transfer_job_start = datetime.date.today()
  setup_transfer_service(PROJECT_NAME, U_MICH_BUCKET, TARRED_BUCKET,
                         transfer_job_start)


def download_manual_routeviews_firehook():
  download_manual_routeviews(CAIDA_BUCKET)


def get_firehook_routeview_mirror():
  """Factory function to get a RouteviewUpdater with our project values."""
  client = storage.Client()
  bucket = client.get_bucket(CAIDA_BUCKET)

  return RouteviewMirror(bucket, ROUTEVIEW_PATH)


def get_firehook_scanfile_mirror():
  """Factory function to get a Untarrer with our project values/paths."""
  client = storage.Client()

  tarred_bucket = client.get_bucket(TARRED_BUCKET)
  untarred_bucket = client.get_bucket(UNTARRED_BUCKET)

  return ScanfileMirror(tarred_bucket, untarred_bucket)


def get_firehook_beam_pipeline_runner():
  """Factory function to get a beam pipeline class with firehook values."""

  return ScanDataBeamPipelineRunner(PROJECT_NAME, SCAN_TABLE_NAME,
                                    DATASET_SUFFIX, SCAN_BIGQUERY_SCHEMA,
                                    INPUT_BUCKET, BEAM_STAGING_LOCATION,
                                    BEAM_TEMP_LOCATION, ip_metadata.IpMetadata,
                                    CAIDA_BUCKET)
