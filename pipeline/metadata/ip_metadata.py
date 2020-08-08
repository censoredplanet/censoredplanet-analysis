"""IP Metadata is a class to add network metadata to IPs."""

import csv
import datetime
import logging
from pprint import pprint
import re
from typing import Tuple, Dict, Optional
import pyasn

from apache_beam.io.filesystems import FileSystems

CLOUD_DATA_LOCATION = "gs://censoredplanet_geolocation/caida/"


class IpMetadata(object):
  """A lookup table which contains network metadata about IPs."""

  def __init__(self, date: str):
    """Create an IP Metadata object by reading/parsing all needed data.

    Args:
      date: the "YYYY-MM-DD" date string to initialize the asn database to
    """
    self.date = date

    org_to_country_map = self.get_org_name_to_country_map()
    self.as_to_org_map = self.get_as_to_org_map(org_to_country_map)
    self.as_to_type_map = self.get_as_to_type_map()
    self.asn_db = self.get_asn_db(date)

  def lookup(
      self, ip: str
  ) -> Tuple[str, int, Optional[str], Optional[str], Optional[str],
             Optional[str]]:
    """Lookup metadata infomation about an IP.

    Args:
      ip: string of the format 1.1.1.1 (ipv4 only)

    Returns:
      Tuple(netblock, asn, as_name, as_full_name, as_type, country)
      ("1.0.0.1/24", 13335, "CLOUDFLARENET", "Cloudflare Inc.", "Content", "US")
      The final 4 fields may be None

    Raises:
      KeyError: when the IP's ASN can't be found
    """
    asn, netblock = self.asn_db.lookup(ip)

    if not asn:
      raise KeyError("Missing IP {} at {}".format(ip, self.date))

    if asn not in self.as_to_org_map:
      logging.warn("Missing asn %s in org name map", asn)
    as_name, as_full_name, country = self.as_to_org_map.get(
        asn, (None, None, None))

    if asn not in self.as_to_type_map:
      logging.warn("Missing asn %s in type map", asn)
    as_type = self.as_to_type_map.get(asn, None)

    return (netblock, asn, as_name, as_full_name, as_type, country)

  def get_asn_db(self, date: str) -> pyasn.pyasn:
    """Creates an ASN db for a given date.

    Args:
      date: "YYYY-MM-DD" date string to initialize the database to

    Returns:
      pyasn database object

    Raises:
      FileNotFoundError: when no matching routeview file is found
    """
    formatted_date = date.replace("-", "")
    file_pattern = "routeviews-rv2-" + formatted_date + "*.pfx2as.gz"
    filepath_pattern = CLOUD_DATA_LOCATION + "routeviews/" + file_pattern
    match = FileSystems.match([filepath_pattern], limits=[1])

    if len(match) == 0:
      raise FileNotFoundError(file_pattern)

    filepath = match[0].metadata_list[0].path
    f = FileSystems.open(filepath)

    # ipasn_string arg does not yet exist in pyasn 1.6.0b1,
    # so we need to write a local file.
    tmp_filename = "/tmp/routeview" + date + ".pfx2as"
    tmp_file = open(tmp_filename, mode="w+")

    line = f.readline()
    while line:
      # CAIDA file lines are stored in the format
      # 1.0.0.0\t24\t13335
      # but pyasn wants lines in the format
      # 1.0.0.0/24\t13335
      decoded_line = line.decode("utf-8")
      formatted_line = re.sub(r"(.*)\t(.*)\t(.*)", r"\1/\2\t\3", decoded_line)
      tmp_file.write(formatted_line)

      line = f.readline()
    tmp_file.close()
    f.close()

    return pyasn.pyasn(tmp_filename)

  def get_org_name_to_country_map(self) -> Dict[str, Tuple[str, str]]:
    """Reads in and returns a mapping of AS org short names to country info.

    Returns:
      Dict {as_name -> ("readable name", country_code)}
      ex: {"8X8INC-ARIN": ("8x8, Inc.","US")}
    """
    filepath = CLOUD_DATA_LOCATION + "as-organizations/as-org2countryinfo.txt"
    orgid2country = FileSystems.open(filepath).read()
    orgid2country_content = orgid2country.decode("utf-8").split("\n")[:-1]
    org_country_data = list(csv.reader(orgid2country_content, delimiter="|"))

    org_name_to_country_map: Dict[str, Tuple[str, str]] = {}
    for line in org_country_data:
      org_id, changed_date, org_name, country, source = line
      org_name_to_country_map[org_id] = (org_name, country)

    return org_name_to_country_map

  def get_as_to_org_map(
      self, org_id_to_country_map: Dict[str, Tuple[str, str]]
  ) -> Dict[int, Tuple[str, Optional[str], Optional[str]]]:
    """Reads in and returns a mapping of ASNs to organization info.

    Args:
      org_id_to_country_map: Dict {as_name -> ("readable name", country_code)}

    Returns:
      Dict {asn -> (asn_name, readable_name, country)}
      ex {204867 : ("LIGHTNING-WIRE-LABS", "Lightning Wire Labs GmbH", "DE")}
      The final 2 fields may be None
    """
    filepath = CLOUD_DATA_LOCATION + "as-organizations/as-org2info.txt"
    as2orgid = FileSystems.open(filepath).read()
    as2orgid_content = as2orgid.decode("utf-8").split("\n")[:-1]
    org_id_data = list(csv.reader(as2orgid_content, delimiter="|"))

    asn_to_org_info_map: Dict[int, Tuple[str, Optional[str],
                                         Optional[str]]] = {}
    for line in org_id_data:
      asn, changed_date, asn_name, org_id, opaque_id, source = line
      try:
        readable_name, country = org_id_to_country_map[org_id]
        asn_to_org_info_map[int(asn)] = (asn_name, readable_name, country)
      except KeyError as e:
        logging.warn("Missing org country info for asn", asn, e)
        asn_to_org_info_map[int(asn)] = (asn_name, None, None)

    return asn_to_org_info_map

  def get_as_to_type_map(self) -> Dict[int, str]:
    """Reads in and returns a mapping of ASNs to org type info.

    Returns:
      Dict {asn -> network_type}
      ex {398243 : "Enterprise", 13335: "Content", 4: "Transit/Access"}
    """
    filepath = CLOUD_DATA_LOCATION + "as-classifications/as2types.txt"
    as2type = FileSystems.open(filepath).read()
    as2type_content = as2type.decode("utf-8").split("\n")[:-1]
    type_data = list(csv.reader(as2type_content, delimiter="|"))

    as_to_type_map: Dict[int, str] = {}
    for line in type_data:
      asn, source, org_type = line
      as_to_type_map[int(asn)] = org_type

    return as_to_type_map
