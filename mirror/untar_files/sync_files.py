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
r"""Untar scan files automatically on create.

Automatically untar files in the gs://firehook-censoredplanetscanspublic/
bucket into the gs://firehook-scans/ bucket.

Files in the tarred bucket are of the form:
gs://firehook-censoredplanetscanspublic/CP_Quack-discard-2018-07-28-03-11-21.tar.gz
or
gs://firehook-censoredplanetscanspublic/CP_Satellite-2018-08-07-17-24-41.tar.gz

They should be untarred but recompressed into scan type specific directories:
gs://firehook-scans/discard/CP_Quack-discard-2018-07-28-03-11-21/results.json.gz
or
gs://firehook-scans/satellite/CP_Satellite-2018-08-07-17-24-41/results.json.gz
"""

import gzip
import os
import pathlib
from pprint import pprint
import shutil
import tarfile
from typing import List

import requests
from retry import retry
from google.cloud import storage

import firehook_resources

TIMEOUT_5_MINUTES = 300

# Key - a substring that will be in the filename
# Value - the scan type of that file
SCAN_TYPE_IDENTIFIERS = {
    'Satellite-': 'satellite',
    'Quack-echo-': 'echo',
    'Quack-discard-': 'discard',
    'Quack-https-': 'https',
    'Quack-http-': 'http'
}


def _get_missing_tarred_files(tarred_files: List[str],
                              untarred_files: List[str]) -> List[str]:
  """Get all files in the tarred list that are not in the untarred list."""
  diff = set(tarred_files) - set(untarred_files)
  return list(diff)


class ScanfileMirror():
  """Look for any tarred files in a given bucket and untar them."""

  def __init__(self, tarred_bucket: storage.bucket.Bucket,
               untarred_bucket: storage.bucket.Bucket):
    """Initialize an untarrer.

    Args:
      tarred_bucket: gcs bucket which stores tarred files
      untarred_bucket: gcs bucket which stores untarred files
    """
    self.tarred_bucket = tarred_bucket
    self.untarred_bucket = untarred_bucket

  @retry(requests.exceptions.ConnectionError, tries=3, delay=1)
  def _untar_file(self, tar_name: str) -> None:
    """Untar a given scan file.

    Downloads the file from GCS, untars on disk,
    and uploads the untarred version to a different location in GCS.

    Args:
      tar_name: filename like CP_Quack-discard-2020-08-17-08-41-15.tar.gz

    Raises:
      Exception: when the file has an unknown scan type
    """
    scan_type = None
    for type_identifier, potential_scan_type in SCAN_TYPE_IDENTIFIERS.items():
      if type_identifier in tar_name:
        scan_type = potential_scan_type

    if not scan_type:
      raise Exception(f'Couldn\'t determine scan type for filename {tar_name}')

    tmp_filepath = os.path.join('/tmp', tar_name)
    tar_folder = tar_name[:-7]  # remove the extensions
    tmp_folder = os.path.join('/tmp', tar_folder)
    os.makedirs(tmp_folder, exist_ok=True)

    self.tarred_bucket.get_blob(tar_name).download_to_filename(
        tmp_filepath, timeout=TIMEOUT_5_MINUTES)

    # Un-gzip and untar the folder
    tfile = tarfile.open(tmp_filepath, 'r:gz')
    for entry in tfile:
      if entry.isfile():
        unzipped_file = tfile.extractfile(entry)
        if unzipped_file is None:
          raise Exception(f'No data associated with member {entry}')
        with unzipped_file:
          # Re-zip the individual files for upload
          filename_rezipped = pathlib.PurePosixPath(entry.name).name + '.gz'
          filepath_rezipped = os.path.join(tmp_folder, filename_rezipped)

          with gzip.open(filepath_rezipped, mode='wb') as rezipped_file:
            shutil.copyfileobj(unzipped_file, rezipped_file)
          output_blob = self.untarred_bucket.blob(
              os.path.join(scan_type, tar_folder, filename_rezipped))
          output_blob.upload_from_filename(
              filepath_rezipped, timeout=TIMEOUT_5_MINUTES)
          os.remove(filepath_rezipped)

    os.remove(tmp_filepath)
    shutil.rmtree(tmp_folder)

  def _get_all_tarred_filenames(self) -> List[str]:
    """Get a list of all tarred filenames, minus the file extension.

    Returns:
      a list of filename strings
      ex ["CP_Quack-discard-2020-08-17-08-41-15",
          "CP_Satellite-2020-08-16-17-07-54"]
    """
    # CP_Satellite-2020-08-16-17-07-54.tar.gz
    blobs = list(self.tarred_bucket.list_blobs())
    # CP_Satellite-2020-08-16-17-07-54
    filenames = [  # remove both .tar and .gz
        pathlib.PurePosixPath(pathlib.PurePosixPath(blob.name).stem).stem
        for blob in blobs
        if pathlib.PurePosixPath(blob.name).suffixes == ['.tar', '.gz']
    ]
    return filenames

  def _get_all_untarred_filepaths(self) -> List[str]:
    """Get a list of all directories with untarredfilenames.

    Returns:
      a list of filename strings
      ex ["CP_Quack-discard-2020-08-17-08-41-15",
          "CP_Satellite-2020-08-16-17-07-54"]
    """
    # discard/CP_Quack-discard-2020-08-17-08-41-15/results.json
    blobs = list(self.untarred_bucket.list_blobs())
    # CP_Quack-discard-2020-08-17-08-41-15/
    path_ends = [
        pathlib.PurePosixPath(blob.name).parts[1]
        for blob in blobs
        if len(pathlib.PurePosixPath(blob.name).parts) == 3
    ]
    return path_ends

  def sync(self) -> None:
    """Untar all files that exist only in the tarred bucket.

    Used for backfilling data.
    """
    tarred_files = self._get_all_tarred_filenames()
    untarred_files = self._get_all_untarred_filepaths()
    new_files = _get_missing_tarred_files(tarred_files, untarred_files)

    files_with_extensions = [f'{filename}.tar.gz' for filename in new_files]

    if not files_with_extensions:
      pprint('no new scan files to untar')

    for filename in files_with_extensions:
      pprint(('untarring file: ', filename))
      self._untar_file(filename)
      pprint(('untarred file: ', filename))


def get_firehook_scanfile_mirror() -> ScanfileMirror:
  """Factory function to get a Untarrer with our project values/paths."""
  client = storage.Client()

  tarred_bucket = client.get_bucket(firehook_resources.TARRED_BUCKET)
  untarred_bucket = client.get_bucket(firehook_resources.UNTARRED_BUCKET)

  return ScanfileMirror(tarred_bucket, untarred_bucket)


if __name__ == '__main__':
  # Called manually when running a backfill.
  get_firehook_scanfile_mirror().sync()
