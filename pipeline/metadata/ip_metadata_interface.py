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
"""Interface for CaidaIPMetadata class and its fake."""

from typing import Optional, Tuple


class IpMetadataInterface:
  """Interface for an CAIDA IP Metadata lookup database."""

  def lookup(
      self, ip: str
  ) -> Tuple[Optional[str], int, Optional[str], Optional[str], Optional[str],
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
