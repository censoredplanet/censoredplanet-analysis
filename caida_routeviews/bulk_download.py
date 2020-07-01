# to use this script run
# python3 caida_routeviews/bulk_download.py
# then run
# gunzip data/caida/routeviews/*.pfx2as.gz
# sed -i "s/\t/\//" data/caida/routeviews/*.pfx2as

import subprocess

# create an all_dates.csv file with all the dates you want to download data for
with open("./all_dates.csv") as dates:
  for date in dates:
    year, month, day = date.strip().split("-")
    print(day, month, year)

    path = "http://data.caida.org/datasets/routing/routeviews-prefix2as/" + year + "/" + month + "/"
    # possible times are 0000 to 2200 in intervals of 200
    for time in [
        "0000", #removing times already looked for
        "0200",
        "0400",
        "0600",
        "0800",
        "1000",
        "1200",
        "1400",
        "1600",
        "1800",
        "2000",
        "2200"
    ]:
      try:
        filename = "routeviews-rv2-" + year + month + day + "-" + time + ".pfx2as.gz"
        filename_no_time = "routeviews-rv2-" + year + month + day + ".pfx2as.gz"

        print(path + filename)

        subprocess.run(["wget", path + filename, "-P", "/tmp/"])
        subprocess.run(["mv", "/tmp/" + filename, "data/caida/routeviews/" + filename_no_time])
      except:
        pass
