"""TODO(laplante): DO NOT SUBMIT without one-line documentation for classify_ips.

TODO(laplante): DO NOT SUBMIT without a detailed description of classify_ips.
"""

import csv
import datetime
from pprint import pprint
from typing import Tuple, Dict
import radix

CLOUD_DATA_LOCATION = "gs://censoredplanet_geolocation/caida/"


class IpMetadata(object):

  def __init__(self, gcs, date: datetime.date):
    """...

    Args:
      gcs: GCSFileSystem
      date: the historical date to initialize the asn database for
    """
    self.gcs = gcs
    self.date = date

    org_to_country_map = self.get_org_name_to_country_map()
    self.as_to_org_map = self.get_as_to_org_map(org_to_country_map)
    self.asn_db = self.get_asn_db(date)

  def lookup(self, ip: str) -> Tuple[str, int, str, str, str]:
    """Lookup metadata infomation about an IP.

    Args:
      ip: string of the format 1.1.1.1 (ipv4 only)

    Returns:
      Tuple(netblock, asn, as_name, as_full_name, country)
      ("1.0.0.1/24", 13335, "CLOUDFLARENET", "Cloudflare Inc.", "US")

    Raises
      KeyError
    """
    rnode = self.asn_db.search_best(ip)

    if rnode:
      asn = rnode.data["asn"]
      netblock = rnode.data["netblock"]
    else:
      raise KeyError("Missing IP %s at %s", ip, self.date)

    as_name, as_full_name, country = self.as_to_org_map[str(asn)]

    return (netblock, asn, as_name, as_full_name, country)

  def get_asn_db(self, date: datetime.date) -> radix.Radix:
    """Creates an ASN radix for a given date.

    Args:
      date: the historical date to initialize the asn database for

    Returns:
      pyasn database object
    """
    # TODO: read in zipped?
    # filename = "routeviews-rv2-" + date.strftime("%Y%m%d") + ".pfx2as"
    filename = "routeviews-rv2-20180727-1200.pfx2as"
    filepath = CLOUD_DATA_LOCATION + "routeviews/" + filename
    routeview = self.gcs.open(filepath).read()
    routeview_content = routeview.decode("utf-8").split("\n")[:-1]

    rtree = radix.Radix()

    for entry in routeview_content:
      (ip, mask, asns) = entry.split("\t")
      asn = asns.replace("_", ",").split(",")[0]
      rnode = rtree.add(ip, int(mask))
      rnode.data["asn"] = int(asn)
      rnode.data["netblock"] = ip + "/" + mask

    return rtree

  def get_org_name_to_country_map(self) -> Dict[str, Tuple[str, str]]:
    """Reads in and returns a mapping of AS org short names to country info.

    Returns:
      Dict {as_name -> ("readable name", country_code)}
      ex: {"8X8INC-ARIN": ("8x8, Inc.","US")}
    """

    orgid2country = self.gcs.open(
        CLOUD_DATA_LOCATION + "as-organizations/as-org2countryinfo.txt").read()
    orgid2country_content = orgid2country.decode("utf-8").split("\n")[:-1]
    org_country_data = list(csv.reader(orgid2country_content, delimiter="|"))

    org_name_to_country_map = {}
    for line in org_country_data:
      org_id, changed_date, org_name, country, source = line
      org_name_to_country_map[org_id] = (org_name, country)

    return org_name_to_country_map

  def get_as_to_org_map(
      self, org_id_to_country_map) -> Dict[str, Tuple[str, str, str]]:
    """Reads in and returns a mapping of ASNs to organization info.

    Args:
      org_id_to_country_map: Dict {as_name -> ("readable name", country_code)}

    Returns:
      Dict {asn -> (asn_name, readable_name, country)}
      ex {"204867" : ("LIGHTNING-WIRE-LABS", "Lightning Wire Labs GmbH", "DE")}
    """

    as2orgid = self.gcs.open(CLOUD_DATA_LOCATION +
                             "as-organizations/as-org2info.txt").read()
    as2orgid_content = as2orgid.decode("utf-8").split("\n")[:-1]
    org_id_data = list(csv.reader(as2orgid_content, delimiter="|"))

    asn_to_org_info_map = {}
    for line in org_id_data:
      asn, changed_date, asn_name, org_id, opaque_id, source = line
      readable_name, country = org_id_to_country_map[org_id]
      asn_to_org_info_map[asn] = (asn_name, readable_name, country)

    return asn_to_org_info_map
