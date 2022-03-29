# https://www.iana.org/assignments/dns-parameters/dns-parameters.xhtml#dns-parameters-6
CREATE TEMP FUNCTION ClassifySatelliteRCode(rcode INTEGER) AS (
  CASE
    WHEN rcode = 0 THEN "dns/answer:no_answer"
    WHEN rcode = 1 THEN "dns/rcode:FormErr"
    WHEN rcode = 2 THEN "dns/rcode:ServFail"
    WHEN rcode = 3 THEN "dns/rcode:NXDomain"
    WHEN rcode = 4 THEN "dns/rcode:NotImp"
    WHEN rcode = 5 THEN "dns/rcode:Refused"
    WHEN rcode = 6 THEN "dns/rcode:YXDomain"
    WHEN rcode = 7 THEN "dns/rcode:YXRRSet"
    WHEN rcode = 8 THEN "dns/rcode:NXRRSet"
    WHEN rcode = 9 THEN "dns/rcode:NotAuth"
    WHEN rcode = 10 THEN "dns/rcode:NotZone"
    WHEN rcode = 11 THEN "dns/rcode:DSOTYPENI"
    WHEN rcode = 12 THEN "dns/rcode:Unassigned"
    WHEN rcode = 13 THEN "dns/rcode:Unassigned"
    WHEN rcode = 14 THEN "dns/rcode:Unassigned"
    WHEN rcode = 15 THEN "dns/rcode:Unassigned"
    WHEN rcode = 16 THEN "dns/rcode:BadVers"
    WHEN rcode = 17 THEN "dns/rcode:BadSig"
    WHEN rcode = 18 THEN "dns/rcode:BadKey"
    WHEN rcode = 19 THEN "dns/rcode:BadTime"
    WHEN rcode = 20 THEN "dns/rcode:BadMode"
    WHEN rcode = 21 THEN "dns/rcode:BadAlg"
    WHEN rcode = 22 THEN "dns/rcode:BadTrunc"
    WHEN rcode = 23 THEN "dns/rcode:BadCookie"
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
    WHEN error = "{}" THEN "dns/unknown" # TODO figure out origin
    WHEN error = "no_answer" THEN "dns/answer:no_answer"
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


CREATE TEMP FUNCTION InvalidIp(received STRUCT<ip STRING,
                                               asnum INT, 
                                               asname STRING, 
                                               http STRING, 
                                               cert STRING, 
                                               matches_control STRING>) AS (
  CASE
    WHEN STARTS_WITH(received.ip, "0.") THEN TRUE
    WHEN STARTS_WITH(received.ip, "127.") then TRUE
    WHEN STARTS_WITH(received.ip, "10.")
         OR STARTS_WITH(received.ip, "192.168.") then TRUE
    ELSE FALSE
  END
);

CREATE TEMP FUNCTION InvalidIpType(received STRUCT<ip STRING, 
                                                   asnum INT, 
                                                   asname STRING, 
                                                   http STRING, 
                                                   cert STRING, 
                                                   matches_control STRING>) AS (
  CASE
    WHEN STARTS_WITH(received.ip, "0.") then "ip_invalid:zero"
    WHEN STARTS_WITH(received.ip, "127.") then "ip_invalid:local_host"
    WHEN STARTS_WITH(received.ip, "10.")
         OR STARTS_WITH(received.ip, "192.168.") then "ip_invalid:local_net"
    ELSE "invalid_ip_parse_error"
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
CREATE TEMP FUNCTION SatelliteOutcome(received ANY TYPE,
                                      rcode INTEGER, 
                                      error STRING, 
                                      controls_failed BOOL, 
                                      anomaly BOOL) AS (
  CASE
    WHEN controls_failed THEN "setup/controls"
    WHEN ARRAY_LENGTH(received) > 0 THEN
      CASE
        # assuming no one will mix valid and invalid ips
        WHEN InvalidIp(received[OFFSET(0)]) THEN InvalidIpType(received[OFFSET(0)])
        WHEN anomaly THEN CONCAT("dns/ipmismatch:", received[OFFSET(0)].asname)
        ELSE "expected/match"
      END
    WHEN rcode IS NULL THEN ClassifySatelliteError(error)
    WHEN rcode = -1 THEN ClassifySatelliteErrorNegRCode(error)
    ELSE ClassifySatelliteRCode(rcode)
  END
);


CREATE OR REPLACE TABLE `firehook-censoredplanet.DERIVED_DATASET.satellite_scan_left_joined_inline_blockpages`
PARTITION BY date
CLUSTER BY country, asn
AS (
  SELECT a.* EXCEPT (received),
         r.ip as received_ip,
         r.asnum as received_asnum,
         r.asname as received_asname,
         r.http as received_http,
         r.cert as received_cert,
         r.matches_control as received_matches_control,
         r.http_response_is_known_blockpage as received_http_response_is_known_blockpage,
         r.http_response_page_signature as received_http_response_page_signature,
         r.http_response_status as received_http_response_status,
         r.http_response_body as received_http_response_body,
         r.http_response_headers as received_http_response_headers,
         r.https_response_is_known_blockpage as received_https_response_is_known_blockpage,
         r.https_response_page_signature as received_https_response_page_signature,
         r.https_response_status as received_https_response_status,
         r.https_response_body as received_https_response_body,
         r.https_response_headers	as received_https_response_headers,
         r.https_response_tls_version as received_https_response_tls_version,
         r.https_response_tls_cipher_suite as received_https_response_tls_cipher_suite,
         r.https_response_tls_cert as received_https_response_tls_cert,
  FROM `firehook-censoredplanet.BASE_DATASET.satellite_scan_inline_blockpages` as a
       LEFT JOIN UNNEST(received) as r
);





# BASE_DATASET and DERIVED_DATASET are reserved dataset placeholder names
# which will be replaced when running the query

# Increment the version of this table if you change the table in a backwards-incomatible way.

# Rely on the table name firehook-censoredplanet.derived.merged_reduced_scans_vN
# if you would like to see a clear breakage when there's a backwards-incompatible change.
# Old table versions will be deleted.
CREATE OR REPLACE TABLE `firehook-censoredplanet.DERIVED_DATASET.reduced_satellite_scans_inline_blockpages`
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

        IF(name="special","special",NET.REG_DOMAIN(name)) as reg_hostname,
        name as hostname,
        as_full_name AS network,
        CONCAT("AS", asn, IF(organization IS NOT NULL, CONCAT(" - ", organization), "")) AS subnetwork,
        country AS country_code,

        IF(is_control, "CONTROL", domain) AS domain,
        IFNULL(category, "Uncategorized") AS category,

        SatelliteOutcome(received, rcode, error, controls_failed, anomaly) as outcome,
        
        COUNT(1) AS count
    FROM `firehook-censoredplanet.BASE_DATASET.satellite_scan_inline_blockpages`
    # Filter on controls_failed to potentially reduce the number of output rows (less dimensions to group by).
    WHERE controls_failed = FALSE
    GROUP BY date, hostname, country_code, network, subnetwork, outcome, domain, category
    # Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
    HAVING NOT STARTS_WITH(outcome, "setup/")
)
SELECT
    Grouped.* EXCEPT (country_code),
    IFNULL(country_name, country_code) AS country_name,
    CASE
        WHEN STARTS_WITH(outcome, "expected/") THEN 0
        WHEN outcome = "read/udp.timeout" THEN NULL # timeouts are common in dns
        ELSE count
    END AS unexpected_count,
    CASE
        WHEN STARTS_WITH(outcome, "expected/") THEN count
        ELSE 0
    END AS expected_count,
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
DROP FUNCTION InvalidIp;
DROP FUNCTION InvalidIpType;