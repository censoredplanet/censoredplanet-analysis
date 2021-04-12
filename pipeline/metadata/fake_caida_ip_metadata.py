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
"""Fake CaidaIpMetadata implementation for use in testing."""

from typing import Any, List, Optional, Tuple

from pipeline.metadata.ip_metadata_interface import IpMetadataInterface


class FakeCaidaIpMetadata(IpMetadataInterface):
  """A fake lookup table for testing CaidaIpMetadata."""

  def __init__(self, *_: List[Any]) -> None:
    super()
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
