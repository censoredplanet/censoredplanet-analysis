import os.path
import logging
from typing import Optional, Tuple

import geoip2.database
from pipeline.assets import MAXMIND_CITY, MAXMIND_ASN

class MaxmindMetadata():

  def __init__(self, city_path: str, asn_path: str):
    """Initialize readers for Maxmind database lookup.
    
    Args:
      city_path: path to Maxmind City .mmdb file
      asn_path: path to Maxmind ASN .mmdb file
    """
    self.maxmind_city = self._maxmind_reader(city_path)
    self.maxmind_asn = self._maxmind_reader(asn_path)

  def _maxmind_reader(self, filepath: str) -> Optional[geoip2.database.Reader]:
    """Return a reader for the Maxmind database.

    Args:
      filepath: Maxmind .mmdb file

    Returns:
      geoip2.database.Reader
    """
    if not os.path.exists(filepath):
      logging.warning('Path to Maxmind db not found: %s\n', filepath)
      return
    return geoip2.database.Reader(filepath)

  def get_country_code(self, ip: str) -> Optional[str]:
    """Get country code for IP address.

    Args:
      ip: IP address (as string)

    Returns:
      2-letter ISO country code
      None if IP not in database
    """
    if self.maxmind_city:
      try:
        ip_info = self.maxmind_city.city(ip)
        return ip_info.country.iso_code
      except Exception as e:
        logging.warning('Maxmind: %s\n', e)
    return None

  def get_asn(self, ip: str) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """Get ASN information for IP address.

    Args:
      ip: IP address (as string)

    Returns:
      Tuple(asn, as_name, netblock)
      Fields are None if IP not in database
    """
    if self.maxmind_asn:
      try:
        ip_info = self.maxmind_asn.asn(ip)
        return (ip_info.autonomous_system_number, ip_info.autonomous_system_organization, ip_info.network)
      except Exception as e:
        logging.warning('Maxmind: %s\n', e)
    return (None, None, None)

  def lookup(self, ip: str) -> Tuple[Optional[str], Optional[int], Optional[str], Optional[str]]:
    """Lookup IP in Maxmind databases.

    Args:
      ip: IP address (as string)

    Returns:
      Tuple(netblock, asn, as_name, country)
      Fields are None if IP not in respective database
    """
    asn, as_name, netblock = self.get_asn(ip)
    country = self.get_country_code(ip)
    return (netblock, asn, as_name, country)


def get_maxmind_db():
  """Return MaxmindMetadata for local Maxmind database lookup."""
  return MaxmindMetadata(MAXMIND_CITY, MAXMIND_ASN)