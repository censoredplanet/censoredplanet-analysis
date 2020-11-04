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

from datetime import datetime

from google.cloud import storage
import httpio
import pandas as pd
import requests

earliest_data = "2018-07-27"


def download_manual_routeviews():
  datelist = pd.date_range(
      start=earliest_data, end=datetime.today().isoformat())
  client = storage.Client()
  bucket = client.get_bucket("censoredplanet_geolocation")

  for date in datelist:
    print("checking date {}".format(date))

    year = str(date.year)
    month = str(date.month).zfill(2)
    day = str(date.day).zfill(2)

    path = "http://data.caida.org/datasets/routing/routeviews-prefix2as/{}/{}/".format(
        year, month)
    # possible times are 0000 to 2200 in intervals of 200
    times = [
        "0000", "0200", "0400", "0600", "0800", "1000", "1200", "1400", "1600",
        "1800", "2000", "2200"
    ]
    for time in times:
      try:
        filename = "routeviews-rv2-{}{}{}-{}.pfx2as.gz".format(
            year, month, day, time)
        url = path + filename
        cloud_filepath = "caida/routeviews/" + filename

        # This call will fail for most urls,
        # since we don't know which timestamp is correct.
        # In that case we just move on to our next guess.
        f = httpio.open(url)

        print("mirroring {} to {}".format(
            url, "gs://censoredplanet_geolocation/" + cloud_filepath))

        blob = bucket.blob(cloud_filepath)
        blob.upload_from_file(f)
      except requests.exceptions.HTTPError as ex:
        if ex.response.status_code != 404:
          raise ex


if __name__ == "__main__":
  download_manual_routeviews()
