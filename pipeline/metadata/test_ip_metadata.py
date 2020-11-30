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

from typing import Iterable
import unittest

from pipeline.metadata import ip_metadata


class IpMetadataTest(unittest.TestCase):

  def test_read_compressed_file(self):
    filepath = "pipeline/metadata/test_file.txt.gz"
    lines = [line for line in ip_metadata._read_compressed_file(filepath)]
    self.assertListEqual(lines, ["test line 1", "test line 2"])

  def test_parse_asn_db(self):
    # Sample content for a routeviews-rv2-*.pfx2as file
    # pyformat: disable
    routeview_file_content = iter([
        "1.0.0.0\t24\t13335",
        "8.8.8.0\t24\t15169",
    ])
    # pyformat: enable
    asn_db = ip_metadata._parse_asn_db(routeview_file_content)

    self.assertEqual(asn_db.lookup("1.0.0.1"), (13335, "1.0.0.0/24"))
    self.assertEqual(asn_db.lookup("8.8.8.8"), (15169, "8.8.8.0/24"))

  def test_parse_org_map(self):
    # Sample content for an as-org2info.txt file.
    # pyformat: disable
    as2org_file_content = iter([
        "# name: AS Org",
        "# some random",
        "# comment lines",
        "# format:org_id|changed|org_name|country|source",
        "01CO-ARIN|20170128|O1.com|US|ARIN",
        "LVLT-ARIN|20120130|Level 3 Communications, Inc.|US|ARIN",
        "# format:aut|changed|aut_name|org_id|opaque_id|source",
        "1|20120224|LVLT-1|LVLT-ARIN|e5e3b9c13678dfc483fb1f819d70883c_ARIN|ARIN",
        "19864|20120320|O1COMM|01CO-ARIN|928772fc737205dea9e069438acaae36_ARIN|ARIN",
        "394811|20160111|O1FIBER|01CO-ARIN|928772fc737205dea9e069438acaae36_ARIN|ARIN"
    ])
    # pyformat: enable

    as2org_map = ip_metadata._parse_as_to_org_map(as2org_file_content)
    self.assertEqual(
        as2org_map, {
            1: ("LVLT-1", "Level 3 Communications, Inc.", "US"),
            19864: ("O1COMM", "O1.com", "US"),
            394811: ("O1FIBER", "O1.com", "US")
        })

  def test_parse_as_to_type_map(self):
    # Sample content for an as2types.txt file
    # pyformat: disable
    as2type_file_content = iter([
        "# format: as|source|type",
        "# date: 20201001",
        "# name: as2type",
        "# exe: type-convert-amogh.pl",
        "# files: 20201001.merged.class.txt",
        "# types: Content|Enterprise|Transit/Access",
        "1|CAIDA_class|Transit/Access",
        "4|CAIDA_class|Content"
    ])
    # pyformat: enable

    as2type_map = ip_metadata._parse_as_to_type_map(as2type_file_content)
    self.assertEqual(as2type_map, {1: "Transit/Access", 4: "Content"})


if __name__ == "__main__":
  unittest.main()
