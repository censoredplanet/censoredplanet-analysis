"""Data transfer job to copy over all scan.tar.gz files.

To update this data transfer job edit this file.

Then go to
https://pantheon.corp.google.com/transfer/cloud?project=firehook-censoredplanet
and delete any existing daily scheduled jobs named
"Transfer scan data from UMich to Firehook".

Then run
  python transfer/scans.py
to create a new scheduled transfer job.
"""

import datetime
import json

import googleapiclient.discovery


def run():
  storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

  start_date = datetime.date.today()
  # Transfer any files created in the last day
  transfer_data_since = datetime.timedelta(days=1)

  transfer_job = {
      'description': 'Transfer scan data from UMich to Firehook',
      'status': 'ENABLED',
      'projectId': 'firehook-censoredplanet',
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
              'bucketName': 'censoredplanetscanspublic'
          },
          'gcsDataSink': {
              'bucketName': 'firehook-censoredplanetscanspublic'
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
  print('Returned transferJob: {}'.format(json.dumps(result, indent=4)))


if __name__ == '__main__':
  run()
