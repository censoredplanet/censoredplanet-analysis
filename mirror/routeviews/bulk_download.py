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

import datetime

import httpio
import requests

from google.cloud import storage

import firehook_resources


def download_manual_routeviews(bucket: storage.bucket.Bucket):
  first_date = datetime.date(2018, 7, 27)  # Date of earliest data
  last_date = datetime.date.today()
  datelist = [
      first_date + datetime.timedelta(days=x)
      for x in range(0, (last_date - first_date).days + 1)
  ]

  for date in datelist:
    print("checking date {}".format(date))
    year, month, day = date.year, date.month, date.day

    path = f"http://data.caida.org/datasets/routing/routeviews-prefix2as/{year}/{month:02}/"
    # possible times are 0000 to 2200 in intervals of 200
    times = [
        "0000", "0200", "0400", "0600", "0800", "1000", "1200", "1400", "1600",
        "1800", "2000", "2200"
    ]
    for time in times:
      try:
        filename = f"routeviews-rv2-{year}{month:02}{day:02}-{time}.pfx2as.gz"
        url = path + filename
        cloud_filepath = "caida/routeviews/" + filename

        # This call will fail for most urls,
        # since we don't know which timestamp is correct.
        # In that case we just move on to our next guess.
        f = httpio.open(url)

        print(f"mirroring {url} to gs://{bucket.name}/{cloud_filepath}")

        blob = bucket.blob(cloud_filepath)
        blob.upload_from_file(f)
      except requests.exceptions.HTTPError as ex:
        if ex.response.status_code != 404:
          raise ex


def download_manual_routeviews_firehook():
  client = storage.Client()
  bucket = client.get_bucket(firehook_resources.CAIDA_BUCKET)

  download_manual_routeviews(bucket)


if __name__ == "__main__":
  download_manual_routeviews_firehook()
