# to use this script run
# python3 caida_routeviews/bulk_download.py

import subprocess
import pandas as pd
from datetime import datetime


def run():
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
  run()
