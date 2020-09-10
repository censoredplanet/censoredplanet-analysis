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

import subprocess
import pandas as pd
from datetime import datetime


def download_manual_routeviews():
  # 2018-07-27 is the earliest scan file date
  datelist = pd.date_range(start="2018-07-27", end=datetime.today().isoformat())

  for date in datelist:
    year, month, day = date.isoformat()[:10].split("-")
    print(year, month, day)
    path = "http://data.caida.org/datasets/routing/routeviews-prefix2as/" + year + "/" + month + "/"
    # possible times are 0000 to 2200 in intervals of 200
    times = [
        "0000", "0200", "0400", "0600", "0800", "1000", "1200", "1400", "1600",
        "1800", "2000", "2200"
    ]
    for time in times:
      try:
        filename = "routeviews-rv2-" + year + month + day + "-" + time + ".pfx2as.gz"

        print(path + filename)

        subprocess.run(["wget", path + filename, "-P", "/tmp/"])
        subprocess.run([
            "gsutil", "cp", "/tmp/" + filename,
            "gs://censoredplanet_geolocation/caida/routeviews/" + filename
        ])
      except:
        pass


if __name__ == "__main__":
  download_manual_routeviews()
