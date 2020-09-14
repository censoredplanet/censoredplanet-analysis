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
"""IP Metadata is a class to add network metadata to IPs."""

import csv
import logging
import os
from pprint import pprint
import re
from typing import Dict, List, Optional, Tuple

from apache_beam.io.filesystems import FileSystems
import pyasn

CLOUD_DATA_LOCATION = "gs://censoredplanet_geolocation/caida/"

# The as-org2info.txt file contains two tables
# Comment lines with these headers divide the tables.
ORG_TO_COUNTRY_HEADER = "# format:org_id|changed|org_name|country|source"
AS_TO_ORG_HEADER = "# format:aut|changed|aut_name|org_id|opaque_id|source"


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
      logging.warning("Missing asn %s in org name map", asn)
    as_name, as_full_name, country = self.as_to_org_map.get(
        asn, (None, None, None))

    if asn not in self.as_to_type_map:
      logging.warning("Missing asn %s in type map", asn)
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

    try:
      filepath = match[0].metadata_list[0].path
      lines = self.read_gcs_compressed_file_as_list(filepath)
    except IndexError:
      raise FileNotFoundError(filepath_pattern)

    # CAIDA file lines are stored in the format
    # 1.0.0.0\t24\t13335
    # but pyasn wants lines in the format
    # 1.0.0.0/24\t13335
    formatted_lines = [
        re.sub(r"(.*)\t(.*)\t(.*)", r"\1/\2\t\3", line) for line in lines
    ]

    # ipasn_string arg does not yet exist in pyasn 1.6.0b1,
    # so we need to write a local file.
    tmp_filename = "/tmp/routeview" + date + ".pfx2as"
    tmp_file = open(tmp_filename, mode="w+")
    for line in formatted_lines:
      tmp_file.write(line + "\n")
    tmp_file.close()

    as_db = pyasn.pyasn(tmp_filename)
    os.remove(tmp_filename)
    return as_db

  def get_org_name_to_country_map(self) -> Dict[str, Tuple[str, str]]:
    """Reads in and returns a mapping of AS org short names to country info.

    Returns:
      Dict {as_name -> ("readable name", country_code)}
      ex: {"8X8INC-ARIN": ("8x8, Inc.","US")}
    """
    filepath = CLOUD_DATA_LOCATION + "as-organizations/20200701.as-org2info.txt.gz"
    lines = self.read_gcs_compressed_file_as_list(filepath)

    data_start_index = lines.index(ORG_TO_COUNTRY_HEADER) + 1
    data_end_index = lines.index(AS_TO_ORG_HEADER)
    org_country_lines = lines[data_start_index:data_end_index]
    org_country_data = list(csv.reader(org_country_lines, delimiter="|"))

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
    filepath = CLOUD_DATA_LOCATION + "as-organizations/20200701.as-org2info.txt.gz"
    lines = self.read_gcs_compressed_file_as_list(filepath)

    data_start_index = lines.index(AS_TO_ORG_HEADER) + 1
    as_to_org_lines = lines[data_start_index:]
    org_id_data = list(csv.reader(as_to_org_lines, delimiter="|"))

    asn_to_org_info_map: Dict[int, Tuple[str, Optional[str],
                                         Optional[str]]] = {}
    for line in org_id_data:
      asn, changed_date, asn_name, org_id, opaque_id, source = line
      try:
        readable_name, country = org_id_to_country_map[org_id]
        asn_to_org_info_map[int(asn)] = (asn_name, readable_name, country)
      except KeyError as e:
        logging.warning("Missing org country info for asn %s %s", asn, e)
        asn_to_org_info_map[int(asn)] = (asn_name, None, None)

    return asn_to_org_info_map

  def get_as_to_type_map(self) -> Dict[int, str]:
    """Reads in and returns a mapping of ASNs to org type info.

    Returns:
      Dict {asn -> network_type}
      ex {398243 : "Enterprise", 13335: "Content", 4: "Transit/Access"}
    """
    filepath = CLOUD_DATA_LOCATION + "as-classifications/20200801.as2types.txt.gz"
    lines = self.read_gcs_compressed_file_as_list(filepath)

    # filter comments
    data_lines = [line for line in lines if line[0] != "#"]
    type_data = list(csv.reader(data_lines, delimiter="|"))

    as_to_type_map: Dict[int, str] = {}
    for line in type_data:
      asn, source, org_type = line
      as_to_type_map[int(asn)] = org_type

    return as_to_type_map

  def read_gcs_compressed_file_as_list(self, filepath: str) -> List[str]:
    """Read in a compressed GCS file as a list of strings.

    We have to read the whole file into memory because some operations
    (removing comments, using only the second half of the file)
    require being able to manipulate particular lines.

    Args:
      filepath: a path to a compressed file. ex
        gs://censoredplanet_geolocation/caida/as-classifications/as2types.txt.gz

    Returns:
      A list of strings representing each line in the file
    """
    f = FileSystems.open(filepath)
    lines = []

    line = f.readline()
    while line:
      decoded_line = line.decode("utf-8").strip()
      lines.append(decoded_line)
      line = f.readline()
    f.close()

    return lines
