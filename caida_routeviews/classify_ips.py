# To use
# create the files
# all_ips.csv and all_date.csv
# then run
# python3 caida_routeviews/classify_ips.py

import pyasn
import csv
import pprint

# User must create these files
ips_file = list(
    open("./all_ips.csv"))
dates = list(
    open("./all_dates.csv"))

orgid2country = open("data/caida/as-organizations/as-org2countryinfo.txt")
org_country_data = list(csv.reader(orgid2country, delimiter="|"))

# org_id -> ("readable name", CC),
# {"8X8INC-ARIN":("8x8, Inc.","US")}
org_id_to_country_map = {}

for line in org_country_data:
  org_id, changed_date, org_name, country, source = line
  org_id_to_country_map[org_id] = (org_name, country)

as2orgid = open("data/caida/as-organizations/as-org2info.txt")
org_id_data = list(csv.reader(as2orgid, delimiter="|"))
# asn -> (asn_name, readable_name, country)
# {"204867" : "LIGHTNING-WIRE-LABS", "Lightning Wire Labs GmbH", "DE"}
asn_to_org_info_map = {}
for line in org_id_data:
  asn, changed_date, asn_name, org_id, opaque_id, source = line
  readable_name, country = org_id_to_country_map[org_id]
  asn_to_org_info_map[asn] = (asn_name, readable_name, country)

for date in dates:
  year, month, day = date.strip().split("-")
  flat_date = year + month + day
  split_date = date.strip()
  filename = "routeviews-rv2-" + flat_date + ".pfx2as"

  asn_db = pyasn.pyasn("data/caida/routeviews/" + filename)
  ip_info = []

  for ip in ips_file:
    ip = ip.strip()
    asn = asn_db.lookup(ip)[0]

    if asn:
      try:
        asn_name, readable_name, country = asn_to_org_info_map[str(asn)]
        ip_info.append([ip, asn, asn_name, readable_name, country, split_date])
      except KeyError:
        ip_info.append([ip, "", "", "", "", flat_date])
    else:
      ip_info.append([ip, "", "", "", "", flat_date])

  with open("data/caida/routeviews/asns_" + flat_date + ".csv", "w") as f:
    output_csv = csv.writer(f)
    for entry in ip_info:
      output_csv.writerow(entry)
