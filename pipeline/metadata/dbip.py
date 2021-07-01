import logging
import os
from typing import Optional
from pprint import pprint

import geoip2.database
from geoip2.database import MODE_MEMORY
import apache_beam.io.filesystems as apache_filesystems

DBIP_ISP = 'dbip-isp-2021-07-01.mmdb'


def _mmdb_reader(filepath: str) -> geoip2.database.Reader:
  """Return a reader for an MMDB database.

    Args:
      filepath: gcs or local fileststem path to .mmdb file

    Returns:
      geoip2.database.Reader
  """
  f = apache_filesystems.FileSystems.open(filepath)

  # geoip2 reader will only take a filepath,
  # so we need to write the file to local disk
  disk_filename = os.path.join('/tmp', os.path.basename(filepath))
  with open(disk_filename, 'wb') as disk_file:
    disk_file.write(f.read())
  database = geoip2.database.Reader(disk_filename, mode=MODE_MEMORY)
  os.remove(disk_filename)
  return database


class DbipMetadata():
  """Lookup database for DBIP ASN and country metadata."""

  def __init__(self, dbip_folder: str) -> None:
    """Create a DBIP Database.

      Args:
        dbip_folder: a folder containing a dbip file.
          Either a gcs filepath or a local system folder.
    """
    dbip_path = os.path.join(dbip_folder, DBIP_ISP)
    self.dbip_isp = _mmdb_reader(dbip_path)

  def get_org(self, ip: str) -> Optional[str]:
    """Lookup the organization for an ip

    Args:
      ip: ip like 1.1.1.1

    Returns:
      organization name or None
    """
    try:
      ip_info = self.dbip_isp.enterprise(ip)
      return ip_info.traits.organization

    except (ValueError, geoip2.errors.AddressNotFoundError) as e:
      logging.warning('DBIP: %s\n', e)
    return None


class FakeDbipMetadata():
  """A fake lookup table for testing DbipMetadata."""

  def __init__(self, _: str) -> None:
    super()

  # pylint: disable=no-self-use
  def get_org(self, _: str) -> Optional[str]:
    return "Test Org Name"
