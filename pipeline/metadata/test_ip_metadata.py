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

import unittest
from pipeline.metadata.ip_metadata import IpMetadata


class IpMetadataTest(unittest.TestCase):

  def test_init_and_lookup(self):
    # This E2E test requires the user to have get access to the
    # gs://censoredplanet_geolocation bucket.
    ip_metadata = IpMetadata("2018-07-27")
    metadata = ip_metadata.lookup("1.1.1.1")

    self.assertEqual(metadata, ("1.1.1.0/24", 13335, "CLOUDFLARENET",
                                "Cloudflare, Inc.", "Content", "US"))

  def test_previous_day(self):
    day = "2020-01-02"
    previous_day = IpMetadata.previous_day(day)
    self.assertEqual(previous_day, "2020-01-01")

  def test_read_compressed_file(self):
    filepath = "pipeline/metadata/test_file.txt.gz"
    lines = IpMetadata.read_compressed_file(filepath)
    self.assertListEqual(lines, ["test line 1", "test line 2"])


if __name__ == "__main__":
  unittest.main()
