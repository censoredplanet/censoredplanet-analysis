"""IP classificaation using the dbip database"""

import logging
import os
from typing import Optional, NamedTuple

import geoip2.database

from pipeline.metadata.mmdb_reader import mmdb_reader

DBIP_ISP = 'dbip-isp-2021-07-01.mmdb'

# Tuple(organization_name, asn)
# ex: ("Boranet", 3786)
DbipReturnValues = NamedTuple('DbipReturnValues', [('org_name', Optional[str]),
                                                   ('asn', Optional[int])])


class DbipIpMetadataInterface:
  """Interface for an CAIDA IP Metadata lookup database."""

  def __init__(self, dbip_folder: str) -> None:
    pass

  def lookup(self, ip: str) -> DbipReturnValues:
    pass


class DbipMetadata(DbipIpMetadataInterface):
  """Lookup database for DBIP ASN and organization metadata."""

  def __init__(self, dbip_folder: str) -> None:
    """Create a DBIP Database.

      Args:
        dbip_folder: a folder containing a dbip file.
          Either a gcs filepath or a local system folder.
    """
    super().__init__(dbip_folder)
    dbip_path = os.path.join(dbip_folder, DBIP_ISP)
    self.dbip_isp = mmdb_reader(dbip_path)

  def lookup(self, ip: str) -> DbipReturnValues:
    """Lookup the organization for an ip

    Args:
      ip: ip like 1.1.1.1

    Returns:
      Tuple of organization name and asn
    """
    try:
      ip_info = self.dbip_isp.enterprise(ip)
      return DbipReturnValues(ip_info.traits.organization,
                              ip_info.traits.autonomous_system_number)

    except (ValueError, geoip2.errors.AddressNotFoundError) as e:
      logging.warning('DBIP: %s\n', e)
    return DbipReturnValues(None, None)


class FakeDbipMetadata(DbipIpMetadataInterface):
  """A fake lookup table for testing DbipMetadata."""

  def __init__(self, dbip_folder: str) -> None:
    super().__init__(dbip_folder)

  # pylint: disable=no-self-use
  def lookup(self, _: str) -> DbipReturnValues:
    return DbipReturnValues("Fake Cloudflare Sub-Org", 13335)
