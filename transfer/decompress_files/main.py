r"""Decompress scan files automatically on create.

Automatically decompress files in the gs://firehook-censoredplanetscanspublic/
bucket into the gs://firehook-scans/ bucket.

Files in the compressed bucket are of the form
gs://firehook-censoredplanetscanspublic/CP_Quack-discard-2018-07-28-03-11-21.tar.gz
or
gs://firehook-censoredplanetscanspublic/CP_Satellite-2018-08-07-17-24-41.tar.gz

They should be decompressed into scan type specific directories like
gs://firehook-scans/discard/CP_Quack-discard-2018-07-28-03-11-21/results.json
or
gs://firehook-scans/satellite/CP_Satellite-2018-08-07-17-24-41.tar.gz
"""

import io
import os
import tarfile
from pprint import pprint

from google.cloud import storage

COMPRESSED_BUCKET_NAME = 'firehook-censoredplanetscanspublic'
UNCOMPRESSED_BUCKET_NAME = 'firehook-scans'

client = storage.Client()
compressed_bucket = client.get_bucket(COMPRESSED_BUCKET_NAME)
uncompressed_bucket = client.get_bucket(UNCOMPRESSED_BUCKET_NAME)

# Key - a substring that will be in the filename
# Value - the scan type of that file
scan_type_identifiers = {
    'Satellite-': 'satellite',
    'Quack-echo-': 'echo',
    'Quack-discard-': 'discard',
    'Quack-https-': 'https',
    'Quack-http-': 'http'
}


def decompress_file(tar_name):
  """Decompress a given scan file.

  Args:
    tar_name: filename like CP_Quack-discard-2020-08-17-08-41-15.tar.gz

  Raises:
    Exception: when the file has an unknown scan type
  """
  scan_type = None
  for type_identifier, potential_scan_type in scan_type_identifiers.items():
    if type_identifier in tar_name:
      scan_type = potential_scan_type

  if not scan_type:
    raise Exception("Couldn't determine scan type for filename " + tar_name)

  input_blob = compressed_bucket.get_blob(tar_name).download_as_string()
  tar = tarfile.open(fileobj=io.BytesIO(input_blob))

  for file_path in tar.getnames():
    file_object = tar.extractfile(file_path)

    # skip directories
    if file_object:
      output_blob = uncompressed_bucket.blob(os.path.join(scan_type, file_path))
      output_blob.upload_from_string(file_object.read())


def get_all_compressed_filenames():
  """Get a list of all compressed filenames, minus the file extension.

  Returns:
    a list of filename strings
    ex ["CP_Quack-discard-2020-08-17-08-41-15",
        "CP_Satellite-2020-08-16-17-07-54"]
  """
  # CP_Satellite-2020-08-16-17-07-54.tar.gz
  blobs = list(client.list_blobs(COMPRESSED_BUCKET_NAME))
  # CP_Satellite-2020-08-16-17-07-54
  filenames = [  # remove both .tar and .gz
      os.path.splitext(os.path.splitext(blob.name)[0])[0] for blob in blobs
  ]
  return filenames


def get_all_uncompressed_filepaths():
  """Get a list of all directories with uncompressed filenames.

  Returns:
    a list of filename strings
    ex ["CP_Quack-discard-2020-08-17-08-41-15",
        "CP_Satellite-2020-08-16-17-07-54"]
  """
  # discard/CP_Quack-discard-2020-08-17-08-41-15/results.json
  blobs = list(client.list_blobs(UNCOMPRESSED_BUCKET_NAME))
  # discard/CP_Quack-discard-2020-08-17-08-41-15/
  paths = [os.path.split(blob.name)[0] for blob in blobs]
  # CP_Quack-discard-2020-08-17-08-41-15/
  path_ends = [os.path.split(path)[1] for path in paths]
  return path_ends


def get_missing_compressed_files(compressed_files, uncompressed_files):
  """Get all files in the compressed list that are not in the uncompressed list."""
  diff = set(compressed_files) - set(uncompressed_files)
  return list(diff)


def decompress_all_missing_files():
  """Decompress all files that exist only in the compressed bucket.

  Used for backfilling data.
  """
  compressed_files = get_all_compressed_filenames()
  uncompressed_files = get_all_uncompressed_filepaths()
  new_files = get_missing_compressed_files(compressed_files, uncompressed_files)

  files_with_extensions = [filename + '.tar.gz' for filename in new_files]

  if not files_with_extensions:
    pprint('no new scan files to decompress')

  for filename in files_with_extensions:
    pprint(('decompressing file: ', filename))
    decompress_file(filename)
    pprint(('decompressed file: ', filename))


if __name__ == '__main__':
  # Called manually when running a backfill.
  decompress_all_missing_files()
