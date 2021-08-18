"""Method to read in mmdb files"""

import tempfile

import geoip2.database
from geoip2.database import MODE_MEMORY
import apache_beam.io.filesystems as apache_filesystems


def mmdb_reader(filepath: str) -> geoip2.database.Reader:
  """Return a reader for an MMDB database.

    Args:
      filepath: gcs or local fileststem path to .mmdb file

    Returns:
      geoip2.database.Reader
  """
  f = apache_filesystems.FileSystems.open(filepath)

  # geoip2 reader will only take a filepath,
  # so we need to write the file to local disk
  with tempfile.NamedTemporaryFile() as disk_file:
    disk_file.write(f.read())
    disk_filename = disk_file.name
    database = geoip2.database.Reader(disk_filename, mode=MODE_MEMORY)
    return database
