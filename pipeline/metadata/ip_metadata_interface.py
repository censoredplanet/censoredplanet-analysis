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

import datetime
from typing import Optional, Tuple


class IpMetadataInterface(object):
  """Interface for an IP Metadata lookup database."""

  def __init__(
      self,
      date: datetime.date,
      cloud_data_location: str,
      latest_as2org_filepath: str,
      latest_as2class_filepath: str,
      allow_previous_day: bool,
  ):
    pass

  def lookup(
      self, ip: str
  ) -> Tuple[str, int, Optional[str], Optional[str], Optional[str],
             Optional[str]]:
    pass
