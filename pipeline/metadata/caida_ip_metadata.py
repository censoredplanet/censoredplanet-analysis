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
"""IP Metadata is a class to add network metadata to IPs."""

import csv
import datetime
import logging
import re
from typing import Dict, Optional, Tuple, Iterator

import apache_beam.io.filesystem as apache_filesystem
import apache_beam.io.filesystems as apache_filesystems
import pyasn

# These are the latest CAIDA files stored in CLOUD_DATA_LOCATION
# TODO: Add a feature to update.py that updates these files automatically
#       and get the latest file here instead.
LATEST_AS2ORG_FILEPATH = "as-organizations/20200701.as-org2info.txt.gz"
LATEST_AS2CLASS_FILEPATH = "as-classifications/20200801.as2types.txt.gz"

# The as-org2info.txt file contains two tables
# Comment lines with these headers divide the tables.
ORG_TO_COUNTRY_HEADER = "# format:org_id|changed|org_name|country|source"
AS_TO_ORG_HEADER = "# format:aut|changed|aut_name|org_id|opaque_id|source"


class CaidaIpMetadataInterface:
  """Interface for an CAIDA IP Metadata lookup database."""

  def __init__(
      self,
      date: datetime.date,
      cloud_data_location: str,
      allow_previous_day: bool,
  ) -> None:
    pass

  def lookup(
      self, ip: str
  ) -> Tuple[Optional[str], int, Optional[str], Optional[str], Optional[str],
             Optional[str]]:
    pass


def _read_compressed_file(filepath: str) -> Iterator[str]:
  """Read in a compressed file as a decompressed string iterator.

  Args:
    filepath: a path to a compressed file. Could be either local like
      '/tmp/text.txt.gz' or a gcs file like
      'gs://censoredplanet_geolocation/caida/as-classifications/as2types.txt.gz'

  Returns:
    An generator per-line reader for the file
  """
  f: apache_filesystem.CompressedFile = apache_filesystems.FileSystems.open(
      filepath)

  while True:
    line = f.readline()
    if not line:
      f.close()
      return
    # Remove the newline char
    yield str(line, "utf-8")[:-1]


def _parse_asn_db(f: Iterator[str]) -> pyasn.pyasn:
  """Returns a pyasn db from a routeview file.

  Args:
    f: an routeview file Iterator

  Returns:
    pyasn database object
  """
  # CAIDA file lines are stored in the format
  # 1.0.0.0\t24\t13335
  # but pyasn wants lines in the format
  # 1.0.0.0/24\t13335
  formatted_lines = map(
      lambda line: re.sub(r"(.*)\t(.*)\t(.*)", r"\1/\2\t\3", line), f)
  as_str = "\n".join(formatted_lines)
  del formatted_lines

  asn_db = pyasn.pyasn(None, ipasn_string=as_str)
  return asn_db


def _parse_as_to_org_map(
    f: Iterator[str]) -> Dict[int, Tuple[str, Optional[str], Optional[str]]]:
  org2country_map = _parse_org_name_to_country_map(f)
  as2org_map = _parse_as_to_org_map_remainder(f, org2country_map)

  return as2org_map


def _parse_org_name_to_country_map(
    f: Iterator[str]) -> Dict[str, Tuple[str, str]]:
  # pyformat: disable
  """Returns a mapping of AS org short names to country info from a file.

  This function only reads up to the AS_TO_ORG_HEADER and leaves the rest of the
  iterator still readable.

  Args:
    f: an iterator containing the content of a as-org2info.txt file
    File is of the format
      # some comment lines
      ORG_TO_COUNTRY_HEADER
      1|20120224|LVLT-1|LVLT-ARIN|e5e3b9c13678dfc483fb1f819d70883c_ARIN|ARIN
      ...
      AS_TO_ORG_HEADER
      LVLT-ARIN|20120130|Level 3 Communications, Inc.|US|ARIN
      ...

  Returns:
    Dict {as_name -> ("readable name", country_code)}
    ex: {"8X8INC-ARIN": ("8x8, Inc.","US")}
  """
  # pyformat: enable
  line = next(f)

  while line != ORG_TO_COUNTRY_HEADER:
    # Throw away starter comment lines
    line = next(f)

  org_name_to_country_map: Dict[str, Tuple[str, str]] = {}

  while line != AS_TO_ORG_HEADER:
    org_id, changed_date, org_name, country, source = line.split("|")
    org_name_to_country_map[org_id] = (org_name, country)

    line = next(f)

  return org_name_to_country_map


def _parse_as_to_org_map_remainder(
    f: Iterator[str], org_id_to_country_map: Dict[str, Tuple[str, str]]
) -> Dict[int, Tuple[str, Optional[str], Optional[str]]]:
  # pyformat: disable
  """Returns a mapping of ASNs to organization info from a file.

  Args:
    f: an iterator containing the content of an as-org2info.txt file which
      has already been iterated over by _parse_org_name_to_country_map so the
      only remaining line are of the format
        LVLT-ARIN|20120130|Level 3 Communications, Inc.|US|ARIN
    org_id_to_country_map: Dict {as_name -> ("readable name", country_code)}

  Returns:
    Dict {asn -> (asn_name, readable_name, country)}
    ex {204867 : ("LIGHTNING-WIRE-LABS", "Lightning Wire Labs GmbH", "DE")}
    The final 2 fields may be None
  """
  # pyformat: enable
  asn_to_org_info_map: Dict[int, Tuple[str, Optional[str], Optional[str]]] = {}

  for line in f:
    asn, changed_date, asn_name, org_id, opaque_id, source = line.split("|")
    try:
      readable_name, country = org_id_to_country_map[org_id]
      asn_to_org_info_map[int(asn)] = (asn_name, readable_name, country)
    except KeyError as e:
      logging.warning("Missing org country info for asn %s, %s", asn, e)
      asn_to_org_info_map[int(asn)] = (asn_name, None, None)

  return asn_to_org_info_map


def _parse_as_to_type_map(f: Iterator[str]) -> Dict[int, str]:
  """Returns a mapping of ASNs to org type info from a file.

  Args:
    f: as2type file object

  Returns:
    Dict {asn -> network_type}
    ex {398243 : "Enterprise", 13335: "Content", 4: "Transit/Access"}
  """
  # filter comments
  data_lines = [line for line in f if line[0] != "#"]
  type_data = list(csv.reader(data_lines, delimiter="|"))

  as_to_type_map: Dict[int, str] = {}
  for line in type_data:
    asn, source, org_type = line
    as_to_type_map[int(asn)] = org_type

  return as_to_type_map


class CaidaIpMetadata(CaidaIpMetadataInterface):
  """A lookup table which contains CAIDA metadata about IPs."""

  def __init__(
      self,
      date: datetime.date,
      cloud_data_location: str,
      allow_previous_day: bool,
  ) -> None:
    """Create an CAIDA IP Metadata object by reading/parsing all needed data.

    Args:
      date: a date to initialize the asn database to
      cloud_data_location: GCS bucket folder name like "gs://bucket/folder/"
      allow_previous_day: If the given date's routeview file doesn't exist,
        allow the one from the previous day instead. This is useful when
        processing very recent data where the newest file may not yet exist.
    """
    super().__init__(date, cloud_data_location, allow_previous_day)
    self.cloud_data_location = cloud_data_location

    self.as_to_org_map = self._get_asn2org_map()
    self.as_to_type_map = self._get_asn2type_map()
    self.asn_db = self._get_asn_db(date, allow_previous_day)

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
      raise KeyError("Missing IP {} at {}".format(ip, self.date.isoformat()))

    if asn not in self.as_to_org_map:
      logging.warning("Missing asn %s in org name map", asn)
    as_name, as_full_name, country = self.as_to_org_map.get(
        asn, (None, None, None))

    if asn not in self.as_to_type_map:
      logging.warning("Missing asn %s in type map", asn)
    as_type = self.as_to_type_map.get(asn, None)

    return (netblock, asn, as_name, as_full_name, as_type, country)

  def _get_asn2org_map(
      self) -> Dict[int, Tuple[str, Optional[str], Optional[str]]]:
    as_to_org_filename = self.cloud_data_location + LATEST_AS2ORG_FILEPATH
    as_to_org_file = _read_compressed_file(as_to_org_filename)
    return _parse_as_to_org_map(as_to_org_file)

  def _get_asn2type_map(self) -> Dict[int, str]:
    as_to_type_filename = self.cloud_data_location + LATEST_AS2CLASS_FILEPATH
    as_to_type_file = _read_compressed_file(as_to_type_filename)
    return _parse_as_to_type_map(as_to_type_file)

  def _get_asn_db(self, date: datetime.date,
                  allow_previous_day: bool) -> pyasn.pyasn:
    """Return an ASN database object.

    Args:
      date: a date to initialize the asn database to
      allow_previous_day: allow using previous routeview file

    Returns:
      pyasn database

    Raises:
      FileNotFoundError: when no allowable routeview file is found
    """
    try:
      self.date = date
      return self._get_dated_asn_db(self.date)
    except FileNotFoundError as ex:
      if allow_previous_day:
        self.date = date - datetime.timedelta(days=1)
        return self._get_dated_asn_db(self.date)

      raise ex

  def _get_dated_asn_db(self, date: datetime.date) -> pyasn.pyasn:
    """Finds the right routeview file for a given date and returns an ASN DB.

    Args:
      date: date object to initialize the database to

    Returns:
      A pyasn DB for the dated routeview file.

    Raises:
      FileNotFoundError: when no exactly matching routeview file is found
    """
    file_pattern = f"routeviews-rv2-{date:%Y%m%d}*.pfx2as.gz"
    filepath_pattern = self.cloud_data_location + "routeviews/" + file_pattern
    match = apache_filesystems.FileSystems.match([filepath_pattern], limits=[1])

    try:
      filepath = match[0].metadata_list[0].path
      return _parse_asn_db(_read_compressed_file(filepath))
    except IndexError as ex:
      raise FileNotFoundError(filepath_pattern) from ex


class FakeCaidaIpMetadata(CaidaIpMetadataInterface):
  """A fake lookup table for testing CaidaIpMetadata."""

  def __init__(
      self,
      date: datetime.date,
      cloud_data_location: str,
      allow_previous_day: bool,
  ) -> None:
    super().__init__(date, cloud_data_location, allow_previous_day)
    # A little example data for testing.
    self.lookup_table = {
        "1.1.1.1": ("1.0.0.1/24", 13335, "CLOUDFLARENET", "Cloudflare Inc.",
                    "Content", "US"),
        "8.8.8.8":
            ("8.8.8.0/24", 15169, "GOOGLE", "Google LLC", "Content", "US"),
        "1.1.1.3": ("1.0.0.1/24", 13335, "CLOUDFLARENET", "Cloudflare Inc.",
                    "Content", None),
    }

  def lookup(
      self, ip: str
  ) -> Tuple[str, int, Optional[str], Optional[str], Optional[str],
             Optional[str]]:
    return self.lookup_table[ip]


def get_firehook_caida_ip_metadata_db(
    date: datetime.date,
    allow_previous_day: bool = False,
) -> CaidaIpMetadata:
  """Factory to return an CaidaIpMetadata object which reads in firehook files.

  Args:
    date: a date to initialize the asn database to
    allow_previous_day: If the given date's routeview file doesn't exist, allow
      the one from the previous day instead. This is useful when processing very
      recent data where the newest file may not yet exist.

  Returns:
    an IpMetadata for the given date.
  """
  # import here to avoid beam pickling issues
  import firehook_resources  # pylint: disable=import-outside-toplevel
  return CaidaIpMetadata(date, firehook_resources.CAIDA_FILE_LOCATION,
                         allow_previous_day)
