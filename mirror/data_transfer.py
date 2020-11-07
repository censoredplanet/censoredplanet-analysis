# Copyright 2020 Google LLC
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
"""Data transfer job to copy over all scan.tar.gz files.

Runs daily and transfers all files created that day in the
gs://censoredplanetscanspublic bucket into the
gs://firehook-censoredplanetscanspublic bucket.


To update this data transfer job edit this file.

Then go to
https://console.cloud.google.com/transfer/cloud
and delete any existing daily scheduled jobs named
"Transfer scan data from UMich to Firehook".

Then run
  python transfer/scans.py
to create a new scheduled transfer job.
"""

import datetime
import json

import googleapiclient.discovery

PROJECT_NAME = 'firehook-censoredplanet'
SOURCE_BUCKET = 'censoredplanetscanspublic'
SINK_BUCKET = 'firehook-censoredplanetscanspublic'


def setup_transfer_service(project_name: str, source_bucket: str,
                           sink_bucket: str, start_date: datetime.date):
  storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

  # Transfer any files created in the last day
  transfer_data_since = datetime.timedelta(days=1)

  transfer_job = {
      'description': 'Transfer scan data from UMich to Firehook',
      'status': 'ENABLED',
      'projectId': project_name,
      'schedule': {
          'scheduleStartDate': {
              'day': start_date.day,
              'month': start_date.month,
              'year': start_date.year
          },
          # No scheduled end date, job runs indefinitely.
      },
      'transferSpec': {
          'gcsDataSource': {
              'bucketName': source_bucket
          },
          'gcsDataSink': {
              'bucketName': sink_bucket
          },
          'objectConditions': {
              'maxTimeElapsedSinceLastModification':
                  str(transfer_data_since.total_seconds()) + 's'
          },
          'transferOptions': {
              'overwriteObjectsAlreadyExistingInSink': 'false',
              'deleteObjectsFromSourceAfterTransfer': 'false'
          }
      }
  }

  result = storagetransfer.transferJobs().create(body=transfer_job).execute()
  print(f'Returned transferJob: {json.dumps(result, indent=4)}')


if __name__ == '__main__':
  transfer_job_start = datetime.date.today()
  setup_transfer_service(PROJECT_NAME, SOURCE_BUCKET, SINK_BUCKET,
                         transfer_job_start)
