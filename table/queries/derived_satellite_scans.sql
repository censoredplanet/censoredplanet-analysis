# https://www.iana.org/assignments/dns-parameters/dns-parameters.xhtml#dns-parameters-6
CREATE TEMP FUNCTION ClassifySatelliteRCode(rcode STRING) AS (
  CASE
    WHEN rcode = "0" THEN "dns/answer:empty"
    WHEN rcode = "1" THEN "dns/rcode:FormErr"
    WHEN rcode = "2" THEN "dns/rcode:ServFail"
    WHEN rcode = "3" THEN "dns/rcode:NXDomain"
    WHEN rcode = "4" THEN "dns/rcode:NotImp"
    WHEN rcode = "5" THEN "dns/rcode:Refused"
    WHEN rcode = "6" THEN "dns/rcode:YXDomain"
    WHEN rcode = "7" THEN "dns/rcode:YXRRSet"
    WHEN rcode = "8" THEN "dns/rcode:NXRRSet"
    WHEN rcode = "9" THEN "dns/rcode:NotAuth"
    WHEN rcode = "10" THEN "dns/rcode:NotZone"
    WHEN rcode = "11" THEN "dns/rcode:DSOTYPENI"
    WHEN rcode = "12" THEN "dns/rcode:Unassigned"
    WHEN rcode = "13" THEN "dns/rcode:Unassigned"
    WHEN rcode = "14" THEN "dns/rcode:Unassigned"
    WHEN rcode = "15" THEN "dns/rcode:Unassigned"
    WHEN rcode = "16" THEN "dns/rcode:BadVers"
    WHEN rcode = "17" THEN "dns/rcode:BadSig"
    WHEN rcode = "18" THEN "dns/rcode:BadKey"
    WHEN rcode = "19" THEN "dns/rcode:BadTime"
    WHEN rcode = "20" THEN "dns/rcode:BadMode"
    WHEN rcode = "21" THEN "dns/rcode:BadAlg"
    WHEN rcode = "22" THEN "dns/rcode:BadTrunc"
    WHEN rcode = "23" THEN "dns/rcode:BadCookie"
    ELSE CONCAT("dns/unknown_rcode:", rcode)
  END
);

CREATE TEMP FUNCTION ClassifySatelliteError(error STRING) AS (
  CASE
    # Satellite v1
    WHEN REGEXP_CONTAINS(error, '"Err": {}') THEN "read/udp.timeout"
    WHEN REGEXP_CONTAINS(error, '"Err": 90') THEN "read/dns.msgsize"
    WHEN REGEXP_CONTAINS(error, '"Err": 111') THEN "read/udp.refused"
    WHEN REGEXP_CONTAINS(error, '"Err": 113') THEN "read/ip.host_no_route"
    WHEN REGEXP_CONTAINS(error, '"Err": 24') THEN "setup/system_failure" # Too many open files
    WHEN error = "{}" THEN "dns/unknown" #TODO figure out origin
    WHEN error = "no_answer" THEN "dns/rcode.name_not_resolved"
    #Satellite v2
    WHEN ENDS_WITH(error, "i/o timeout") THEN "read/udp.timeout"
    WHEN ENDS_WITH(error, "message too long") THEN "read/dns.msgsize"
    WHEN ENDS_WITH(error, "connection refused") THEN "read/udp.refused"
    WHEN ENDS_WITH(error, "no route to host") THEN "read/ip.host_no_route"
    WHEN ENDS_WITH(error, "short read") THEN "read/dns.msgsize"
    WHEN ENDS_WITH(error, "read: protocol error") THEN "read/protocol_error"
    WHEN ENDS_WITH(error, "socket: too many open files") THEN "setup/system_failure"
    ELSE CONCAT("dns/unknown_error:", error)
  END
);

CREATE TEMP FUNCTION ClassifySatelliteErrorNegRCode(error STRING) AS (
  CASE
    WHEN (error IS NULL OR error = "" OR error = "null")  THEN "read/udp.timeout"
    ELSE ClassifySatelliteError(error)
  END
);

# Input: array of received IP information, array of rcodes, error,
#        controls failed, and anomaly fields from the raw data
# Output is a string of the format "stage/outcome"
CREATE TEMP FUNCTION SatelliteOutcome(received ANY TYPE, rcode ARRAY<STRING>, error STRING, controls_failed BOOL, anomaly BOOL) AS (
  CASE
    WHEN controls_failed THEN "setup/controls"
    WHEN ARRAY_LENGTH(received) > 0 THEN
      CASE
        WHEN anomaly THEN "dns/answer:ipmismatch"
        ELSE "expected/match"
      END
    WHEN ARRAY_LENGTH(rcode) = 0 THEN ClassifySatelliteError(error)
    WHEN rcode[OFFSET(0)] = "-1" THEN ClassifySatelliteErrorNegRCode(error)
    ELSE ClassifySatelliteRCode(rcode[OFFSET(0)])
  END
);

# BASE_DATASET and DERIVED_DATASET are reserved dataset placeholder names
# which will be replaced when running the query

# Increment the version of this table if you change the table in a backwards-incomatible way.

# Rely on the table name firehook-censoredplanet.derived.merged_reduced_scans_vN
# if you would like to see a clear breakage when there's a backwards-incompatible change.
# Old table versions will be deleted.
CREATE OR REPLACE TABLE `firehook-censoredplanet.DERIVED_DATASET.reduced_satellite_scans_v1`
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

        country AS country_code,
        as_full_name AS network,
        IF(is_control, "CONTROL", domain) AS domain,

        SatelliteOutcome(received, rcode, error, controls_failed, anomaly) as outcome,
        CONCAT("AS", asn, IF(organization IS NOT NULL, CONCAT(" - ", organization), "")) AS subnetwork,
        IFNULL(category, "Uncategorized") AS category,

        COUNT(1) AS count
    FROM `firehook-censoredplanet.BASE_DATASET.satellite_scan_flattened_no_received_ip_tagging`
    # Filter on controls_failed to potentially reduce the number of output rows (less dimensions to group by).
    WHERE controls_failed = FALSE OR controls_failed IS NULL # compensating for null controls_failed in v1 data
    GROUP BY date, country_code, network, subnetwork, outcome, domain, category
    # Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
    HAVING NOT STARTS_WITH(outcome, "setup/")
)
SELECT
    Grouped.* EXCEPT (country_code),
    IFNULL(country_name, country_code) AS country_name,
    CASE
        WHEN (STARTS_WITH(outcome, "expected/")) THEN 0
        WHEN (outcome = "read/udp.timeout" # timeouts are common in dns
              OR STARTS_WITH(outcome, "dial/")
              OR STARTS_WITH(outcome, "setup/")
              OR ENDS_WITH(outcome, "/invalid")) THEN NULL
        ELSE count
    END AS unexpected_count
    FROM Grouped
    LEFT JOIN `firehook-censoredplanet.metadata.country_names` USING (country_code)
    WHERE country_code IS NOT NULL
);

# Drop the temp function before creating the view
# Since any temp functions in scope block view creation.
DROP FUNCTION ClassifySatelliteRCode;
DROP FUNCTION ClassifySatelliteError;
DROP FUNCTION ClassifySatelliteErrorNegRCode;
DROP FUNCTION SatelliteOutcome;

# This view is the stable name for the table above.
# Rely on the table name firehook-censoredplanet.derived.merged_reduced_scans
# if you would like to continue pointing to the table even when there is a breaking change.
CREATE OR REPLACE VIEW `firehook-censoredplanet.DERIVED_DATASET.reduced_satellite_scans`
AS (
  SELECT *
  FROM `firehook-censoredplanet.DERIVED_DATASET.reduced_satellite_scans_v1`
)