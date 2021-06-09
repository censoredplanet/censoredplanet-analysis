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
"""Mirror the latest CAIDA routeview files into a cloud bucket."""

import os
import pathlib
from pprint import pprint
from typing import List
import urllib.request

from google.cloud import storage  # type: ignore

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
  output = map(lambda line: line.decode('utf8').rstrip(),
               urllib.request.urlopen(url).readlines())

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

  def __init__(self, bucket: storage.bucket.Bucket,
               bucket_routeview_path: str) -> None:
    """Initialize a client for updating routeviews.

    Args:
      bucket: GCS bucket
      bucket_routeview_path: path to write routeview files in the bucket
    """
    self.caida_bucket = bucket
    self.bucket_routeview_path = bucket_routeview_path

  def _get_caida_files_in_bucket(self) -> List[str]:
    """Get a list of all caida files stored in our bucket.

    Returns:
      A list of filename strings
      ex ["routeviews-rv2-20200720-1200.pfx2as.gz",
          "routeviews-rv2-20200719-1200.pfx2as.gz"]
    """
    blobs = self.caida_bucket.list_blobs()
    filenames = [os.path.basename(blob.name) for blob in blobs]
    return filenames

  def _transfer_new_file(self, filename: str) -> None:
    """Transfer a routeview file into the cloud bucket.

    Args:
      filename: string of the format "routeviews-rv2-20200720-1200.pfx2as.gz"
    """
    year = filename[15:19]
    month = filename[19:21]

    url = CAIDA_ROUTEVIEW_DIR_URL + year + "/" + month + "/" + filename

    output_blob = self.caida_bucket.blob(
        os.path.join(self.bucket_routeview_path, filename))

    output = urllib.request.urlopen(url).read()
    output_blob.upload_from_string(output)

  def sync(self) -> None:
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


def get_firehook_routeview_mirror() -> RouteviewMirror:
  """Factory function to get a RouteviewUpdater with our project values."""
  client = storage.Client()
  bucket = client.get_bucket(firehook_resources.METADATA_BUCKET)

  return RouteviewMirror(bucket, firehook_resources.ROUTEVIEW_PATH)


if __name__ == "__main__":
  # Called manually when running a backfill.
  get_firehook_routeview_mirror().sync()
