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
"""Mirror the latest CAIDA routeview files into a cloud bucket."""

import io
import os
import pathlib
from pprint import pprint
from typing import List

import httpio
from google.cloud import storage

import firehook_resources

CAIDA_ROUTEVIEW_DIR_URL = "http://data.caida.org/datasets/routing/routeviews-prefix2as/"
# This file contains only the last 30 routeview files created.
# To backfill beyond 30 days use bulk_download.py
CAIDA_CREATION_FILE = "pfx2as-creation.log"


def _get_latest_generated_routeview_files() -> List[str]:
  """Get a list of recently created files CAIDA routeview files on their server.

  Returns:
    A list of filename strings
    ex ["routeviews-rv2-20200720-1200.pfx2as.gz",
        "routeviews-rv2-20200719-1200.pfx2as.gz"]
  """
  url = CAIDA_ROUTEVIEW_DIR_URL + CAIDA_CREATION_FILE
  output = io.TextIOWrapper(httpio.open(url), encoding="utf-8")

  files = []
  for line in output:
    if line[0] != "#":  # ignore comment lines
      # Line format:
      # 4492	1595262269	2020/07/routeviews-rv2-20200719-1200.pfx2as.gz
      # we only want the routeviews-rv2-20200719-1200.pfx2as.gz portion
      filename = pathlib.PurePosixPath(line.split()[2]).name
      files.append(filename)
  return files


class RouteviewMirror():
  """Syncer to look for any recent routeview files and mirror them in cloud."""

  def __init__(self, bucket: storage.bucket.Bucket, bucket_routeview_path: str):
    """Initialize a client for updating routeviews.

    Args:
      bucket: GCS bucket
      bucket_routeview_path: path to write routeview files in the bucket
    """
    self.caida_bucket = bucket
    self.bucket_routeview_path = bucket_routeview_path

  def _get_caida_files_in_bucket(self):
    """Get a list of all caida files stored in our bucket.

    Returns:
      A list of filename strings
      ex ["routeviews-rv2-20200720-1200.pfx2as.gz",
          "routeviews-rv2-20200719-1200.pfx2as.gz"]
    """
    if self.caida_bucket:
      blobs = self.caida_bucket.list_blobs()
      filenames = [os.path.basename(blob.name) for blob in blobs]
      return filenames
    else:
      # Check local directory
      filenames = [obj.name for obj in os.scandir(self.bucket_routeview_path) if obj.is_file()]
      return filenames

  def _transfer_new_file(self, filename: str):
    """Transfer a routeview file into the cloud bucket.

    Args:
      filename: string of the format "routeviews-rv2-20200720-1200.pfx2as.gz"
    """
    year = filename[15:19]
    month = filename[19:21]

    url = CAIDA_ROUTEVIEW_DIR_URL + year + "/" + month + "/" + filename

    if self.caida_bucket:
      output_blob = self.caida_bucket.blob(
          os.path.join(self.bucket_routeview_path, filename))

      with httpio.open(url) as output:
        output_blob.upload_from_file(output)
    else:
      # Download locally
      with httpio.open(url) as output, open(os.path.join(self.bucket_routeview_path, filename), 'wb') as f:
        f.write(output.read())

  def sync(self):
    """Look for new routeview files and transfer them into the cloud bucket."""
    latest_files = _get_latest_generated_routeview_files()
    existing_files = self._get_caida_files_in_bucket()
    new_files = list(set(latest_files) - set(existing_files))

    if not new_files:
      pprint("no new CAIDA files to transfer")

    for new_file in new_files:
      pprint(("transferring file: ", new_file))
      self._transfer_new_file(new_file)
      pprint(("transferred file: ", new_file))


def get_firehook_routeview_mirror():
  """Factory function to get a RouteviewUpdater with our project values."""
  client = storage.Client()
  bucket = client.get_bucket(firehook_resources.CAIDA_BUCKET)

  return RouteviewMirror(bucket, firehook_resources.ROUTEVIEW_PATH)


def get_local_routeview_mirror():
  project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
  directory = os.path.join(project_root, 'pipeline', 'assets', 'routeviews')
  return RouteviewMirror(None, directory)


if __name__ == "__main__":
  # Called manually when running a backfill.
  # get_firehook_routeview_mirror().sync()
  get_local_routeview_mirror().sync()