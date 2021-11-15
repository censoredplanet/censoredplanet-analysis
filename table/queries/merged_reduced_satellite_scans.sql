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

# Classify all errors into a small set of enums
#
# Input is a nullable error string and rcode string from the raw data
#
# Output is a string of the format "stage/outcome"
# Documentation of this enum is at
# https://github.com/censoredplanet/censoredplanet-analysis/blob/master/docs/merged_reduced_satelllite_scans_table.md#outcome-classification
CREATE TEMP FUNCTION ClassifySatelliteError(rcode STRING, error STRING) AS (
  CASE
    WHEN rcode = "-1" AND (error IS NULL OR error = "" OR error = "null")  THEN "read/udp.timeout"
    WHEN rcode = "0" THEN "dns/dns.name_not_resolved"
    WHEN rcode = "1" THEN "dns/dns.formerr"
    WHEN rcode = "2" THEN "dns/dns.servfail"
    WHEN rcode = "3" THEN "dns/dns.nxdomain"
    WHEN rcode = "4" THEN "dns/dns.notimp"
    WHEN rcode = "5" THEN "dns/dns.refused"
    WHEN rcode = "6" THEN "dns/dns.yxdomain"
    WHEN rcode = "7" THEN "dns/dns.yxrrset"
    WHEN rcode = "8" THEN "dns/dns.nxrrset"
    WHEN rcode = "9" THEN "dns/dns.notauth"
    WHEN rcode = "10" THEN "dns/dns.notzone"
    WHEN rcode = "16" THEN "dns/dns.badsig"
    WHEN rcode = "17" THEN "dns/dns.badkey"
    WHEN rcode = "18" THEN "dns/dns.badtime"
    WHEN rcode = "19" THEN "dns/dns.badmode"
    WHEN rcode = "20" THEN "dns/dns.badname"
    WHEN rcode = "21" THEN "dns/dns.badalg"
    WHEN rcode = "22" THEN "dns/dns.badtrunc"
    WHEN rcode = "23" THEN "dns/dns.badcookie"
    # Satellite v1
    WHEN REGEXP_CONTAINS(error, '"Err": {}') THEN "read/udp.timeout"
    WHEN REGEXP_CONTAINS(error, '"Err": 90') THEN "read/dns.msgsize"
    WHEN REGEXP_CONTAINS(error, '"Err": 111') THEN "read/udp.refused"
    WHEN REGEXP_CONTAINS(error, '"Err": 113') THEN "read/ip.host_no_route"
    WHEN error = "{}" THEN "dns/unknown"
    WHEN error = "no_answer" THEN "dns/dns.name_not_resolved"
    #Satellite v2
    WHEN ENDS_WITH(error, "i/o timeout") THEN "read/udp.timeout"
    WHEN ENDS_WITH(error, "message too long") THEN "read/dns.msgsize"
    WHEN ENDS_WITH(error, "connection refused") THEN "read/udp.refused"
    WHEN ENDS_WITH(error, "no route to host") THEN "read/ip.host_no_route"
    WHEN ENDS_WITH(error, "short read") THEN "read/dns.msgsize"
    ELSE "dns/unknown"
  END
);

CREATE TEMP FUNCTION LastRCode(rcode ARRAY<STRING>) AS (
  rcode[ORDINAL(ARRAY_LENGTH(rcode))]
);

CREATE TEMP FUNCTION LastError(error STRING) AS (
  SPLIT(error, " | ")[ORDINAL(ARRAY_LENGTH(SPLIT(error, " | ")))]
);

# Input: array of received IP information, array of rcodes, error,
#        controls failed, and anomaly fields from the raw data
# Output is a string of the format "stage/outcome"
CREATE TEMP FUNCTION SatelliteOutcome(received ANY TYPE, rcode ARRAY<STRING>, error STRING, controls_failed BOOL, anomaly BOOL) AS (
  CASE
    WHEN controls_failed THEN "setup/controls"
    WHEN ARRAY_LENGTH(received) > 0 THEN
      CASE
        WHEN anomaly THEN "dns/dns.ipmismatch"
        ELSE "expected/match"
      END
    WHEN ARRAY_LENGTH(rcode) > 0 THEN ClassifySatelliteError(LastRCode(rcode),LastError(error))
    ELSE ClassifySatelliteError("", LastError(error))
  END
);

# BASE_DATASET and DERIVED_DATASET are reserved dataset placeholder names
# which will be replaced when running the query

# Increment the version of this table if you change the table in a backwards-incomatible way.

# Rely on the table name firehook-censoredplanet.derived.merged_reduced_scans_vN
# if you would like to see a clear breakage when there's a backwards-incompatible change.
# Old table versions will be deleted.
CREATE OR REPLACE TABLE `firehook-censoredplanet.DERIVED_DATASET.merged_reduced_satellite_scans_v2`
PARTITION BY date
# Column `country_name` is always used for filtering and must come first.
# `network`, `subnetwork`, and `domain` are useful for filtering and grouping.
CLUSTER BY country_name, network, subnetwork, domain
OPTIONS (
  friendly_name="Reduced Satellite Scans",
  description="Filtered and pre-aggregated table of Satellite scans to use with the Censored Planed Dashboard"
)
AS (
WITH Grouped AS (
    SELECT
        date,

        source,
        country AS country_code,
        as_full_name AS network,
        IF(is_control, "CONTROL", domain) AS domain,

        SatelliteOutcome(received, rcode, error, controls_failed, anomaly) as outcome,
        CONCAT("AS", asn, IF(organization IS NOT NULL, CONCAT(" - ", organization), "")) AS subnetwork,
        IFNULL(category, "Uncategorized") AS category,
        IF(ARRAY_LENGTH(received) > 1, received[OFFSET(0)].ip, "") as received_ip,

        COUNT(1) AS count
    FROM `firehook-censoredplanet.BASE_DATASET.satellite_scan`
    # Filter on controls_failed to potentially reduce the number of output rows (less dimensions to group by).
    WHERE NOT controls_failed
    GROUP BY date, source, country_code, network, subnetwork, outcome, domain, category, received_ip
    # Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
    HAVING NOT STARTS_WITH(outcome, "setup/")
)
SELECT
    Grouped.* EXCEPT (country_code),
    IFNULL(country_name, country_code) AS country_name,
    CASE
        WHEN (STARTS_WITH(outcome, "expected/")) THEN 0
        WHEN (STARTS_WITH(outcome, "dial/") OR STARTS_WITH(outcome, "setup/") OR ENDS_WITH(outcome, "/invalid")) THEN NULL
        ELSE count
    END AS unexpected_count
    FROM Grouped
    LEFT JOIN `firehook-censoredplanet.metadata.country_names` USING (country_code)
    WHERE country_code IS NOT NULL
);

# Drop the temp function before creating the view
# Since any temp functions in scope block view creation.
DROP FUNCTION ClassifySatelliteError;
DROP FUNCTION SatelliteOutcome;
DROP FUNCTION LastRCode;
DROP FUNCTION LastError;

# This view is the stable name for the table above.
# Rely on the table name firehook-censoredplanet.derived.merged_reduced_scans
# if you would like to continue pointing to the table even when there is a breaking change.
CREATE OR REPLACE VIEW `firehook-censoredplanet.DERIVED_DATASET.merged_reduced_satellite_scans`
AS (
  SELECT *
  FROM `firehook-censoredplanet.DERIVED_DATASET.merged_reduced_satellite_scans_v2`
)
