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

CREATE TEMP FUNCTION CleanError(error STRING) AS (
  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    IF(error = "", "null", IFNULL(error, "null")),
    "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+", "[IP]"),
    "\\[IP\\]:[0-9]+", "[IP]:[PORT]"),
    "length [0-9]+", "length [LENGTH]"),
    "port\\.[0-9]+", "port.[PORT]"),
    "\"Port\": [0-9]+", "\"Port\": [PORT]")
);

# Classify all errors into a small set of enums
#
# Input is a nullable error string from the raw data
# Source is one of "ECHO", "DISCARD, "HTTP", "HTTPS"
#
# Output is a string of the format "stage/outcome"
# Documentation of this enum is at
# https://github.com/censoredplanet/censoredplanet-analysis/blob/master/docs/tables.md#outcome-classification
CREATE TEMP FUNCTION ClassifyError(error STRING,
                                   source STRING,
                                   template_match BOOL,
                                   blockpage_match BOOL,
                                   blockpage_id STRING) AS (
  CASE
    WHEN blockpage_match then CONCAT("content/blockpage:", blockpage_id)

    # Content mismatch for hyperquack v2 which doesn't write
    # content verification failures in the error field.
    WHEN (NOT template_match AND (error is NULL OR error = "")) then "content/template_mismatch"

    # Success
    WHEN (error is NULL OR error = "") then "complete/success"

    # System failures
    WHEN ENDS_WITH(error, "address already in use") THEN "setup/system_failure"
    WHEN ENDS_WITH(error, "protocol error") THEN "setup/system_failure"
    WHEN ENDS_WITH(error, "protocol not available") THEN "setup/system_failure" # ipv4 vs 6 error
    WHEN ENDS_WITH(error, "too many open files") THEN "setup/system_failure"

    # Dial failures
    WHEN ENDS_WITH(error, "network is unreachable") THEN "dial/ip.network_unreachable"
    WHEN ENDS_WITH(error, "no route to host") THEN "dial/ip.host_no_route"
    WHEN ENDS_WITH(error, "connection refused") THEN "dial/tcp.refused"
    WHEN ENDS_WITH(error, "context deadline exceeded") THEN "dial/timeout"
    WHEN ENDS_WITH(error, "connect: connection timed out") THEN "dial/timeout"
    WHEN STARTS_WITH(error, "connection reset by peer") THEN "dial/tcp.reset" #no read: or write: prefix in error
    WHEN ENDS_WITH(error, "connect: connection reset by peer") THEN "dial/tcp.reset"
    WHEN ENDS_WITH(error, "getsockopt: connection reset by peer") THEN "dial/tcp.reset"

    # TLS failures
    WHEN REGEXP_CONTAINS(error, "tls:") THEN "tls/tls.failed"
    WHEN REGEXP_CONTAINS(error, "remote error:") THEN "tls/tls.failed"
    WHEN REGEXP_CONTAINS(error, "local error:") THEN "tls/tls.failed"
    WHEN ENDS_WITH(error, "readLoopPeekFailLocked: <nil>") THEN "tls/tls.failed"
    WHEN ENDS_WITH(error, "missing ServerKeyExchange message") THEN "tls/tls.failed"
    WHEN ENDS_WITH(error, "no mutual cipher suite") THEN "tls/tls.failed"
    WHEN REGEXP_CONTAINS(error, "TLS handshake timeout") THEN "tls/timeout"

    # Write failures
    WHEN ENDS_WITH(error, "write: connection reset by peer") THEN "write/tcp.reset"
    WHEN ENDS_WITH(error, "write: broken pipe") THEN "write/system"

    # Read failures
    WHEN REGEXP_CONTAINS(error, "request canceled") THEN "read/timeout"
    WHEN ENDS_WITH(error, "i/o timeout") THEN "read/timeout"
    WHEN ENDS_WITH(error, "shutdown: transport endpoint is not connected") THEN "read/system"
    # TODO: for HTTPS this error could potentially also be SNI blocking in the tls stage
    # find a way to diffentiate this case.
    WHEN REGEXP_CONTAINS(error, "read: connection reset by peer") THEN "read/tcp.reset"

    # HTTP content verification failures
    WHEN (source != "ECHO" AND REGEXP_CONTAINS(error, "unexpected EOF")) THEN "read/http.truncated_response"
    WHEN (source != "ECHO" AND REGEXP_CONTAINS(error, "EOF")) THEN "read/http.empty"
    WHEN REGEXP_CONTAINS(error, "http: server closed idle connection") THEN "read/http.truncated_response"
    WHEN ENDS_WITH(error, "trailer header without chunked transfer encoding") THEN "http/http.invalid"
    WHEN ENDS_WITH(error, "response missing Location header") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "bad Content-Length") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "failed to parse Location header") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "malformed HTTP") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "malformed MIME") THEN "http/http.invalid"

    # Content verification failures
    WHEN (source = "ECHO" AND ENDS_WITH(error, "EOF")) THEN "content/response_mismatch" # Echo
    # Hyperquack v1 errors
    WHEN (error = "Incorrect echo response") THEN "content/response_mismatch" # Echo
    WHEN (error = "Received response") THEN "content/response_mismatch" # Discard
    WHEN (error = "Incorrect web response: status lines don't match") THEN "content/status_mismatch" # HTTP/S
    WHEN (error = "Incorrect web response: bodies don't match") THEN "content/body_mismatch" # HTTP/S
    WHEN (error = "Incorrect web response: certificates don't match") THEN "content/tls_mismatch" # HTTPS
    WHEN (error = "Incorrect web response: cipher suites don't match") THEN "content/tls_mismatch" # HTTPS
    WHEN (error = "Incorrect web response: TLS versions don't match") THEN "content/tls_mismatch" # HTTPS
    # Hyperquack v2 errors
    WHEN (error = "echo response does not match echo request") THEN "content/response_mismatch" # Echo
    WHEN (error = "discard response is not empty") THEN "content/response_mismatch" # Discard
    WHEN (error = "Status lines does not match") THEN "content/status_mismatch" # HTTP/S
    WHEN (error = "Bodies do not match") THEN "content/body_mismatch" # HTTP/S
    WHEN (error = "Certificates do not match") THEN "content/tls_mismatch" # HTTPS
    WHEN (error = "Cipher suites do not match") THEN "content/tls_mismatch" # HTTPS
    WHEN (error = "TLS versions do not match") THEN "content/tls_mismatch" # HTTPS

    # Unknown errors
    ELSE "unknown/unknown"
  END
);

CREATE TEMP FUNCTION ClassifySatelliteError(rcode STRING, error STRING) AS (
  CASE
    WHEN rcode = "-1" AND (error IS NULL OR error = "" OR error = "null")  THEN "dns/dns.connerr"
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

CREATE TEMP FUNCTION SatelliteOutcome(received ANY TYPE, rcode ARRAY<STRING>, error STRING, controls_failed BOOL, anomaly BOOL) AS (
  CASE
    WHEN controls_failed THEN "setup/controls"
    WHEN ARRAY_LENGTH(received) > 0 THEN
      CASE
        WHEN anomaly THEN "dns/dns.ipmismatch"
        ELSE "complete/success"
      END
    WHEN ARRAY_LENGTH(rcode) > 0 THEN MapSatelliteError(rcode[ORDINAL(ARRAY_LENGTH(rcode))], SPLIT(error, " | ")[ORDINAL(ARRAY_LENGTH(SPLIT(error, " | ")))])
    ELSE MapSatelliteError("", SPLIT(error, " | ")[ORDINAL(ARRAY_LENGTH(SPLIT(error, " | ")))])
  END
);

CREATE TEMP FUNCTION SatelliteResult(received ANY TYPE, rcode ARRAY<STRING>, error STRING) AS (
  CASE
    WHEN ARRAY_LENGTH(received) > 0 THEN CONCAT(ARRAY_LENGTH(received), " answer IPs")
    WHEN ARRAY_LENGTH(rcode) > 0 THEN
      CASE
        WHEN rcode[ORDINAL(ARRAY_LENGTH(rcode))] = "-1" AND error IS NOT NULL AND error != "" AND error != "null" THEN CleanError(SPLIT(error, " | ")[ORDINAL(ARRAY_LENGTH(SPLIT(error, " | ")))])
        ELSE CONCAT("rcode: ", rcode[ORDINAL(ARRAY_LENGTH(rcode))])
      END
    ELSE CleanError(SPLIT(error, " | ")[ORDINAL(ARRAY_LENGTH(SPLIT(error, " | ")))])
  END
);

# BASE_DATASET and DERIVED_DATASET are reserved dataset placeholder names
# which will be replaced when running the query

CREATE OR REPLACE TABLE `firehook-censoredplanet.DERIVED_DATASET.merged_net_as`
PARTITION BY date
CLUSTER BY netblock, as_name, asn
AS (
  SELECT DISTINCT
    date,
    netblock,
    asn,
    as_full_name AS as_name
  FROM (
    SELECT * FROM `firehook-censoredplanet.BASE_DATASET.discard_scan` UNION ALL
    SELECT * FROM `firehook-censoredplanet.BASE_DATASET.echo_scan` UNION ALL
    SELECT * FROM `firehook-censoredplanet.BASE_DATASET.http_scan` UNION ALL
    SELECT * FROM `firehook-censoredplanet.BASE_DATASET.https_scan` UNION ALL
    SELECT * FROM `firehook-censoredplanet.BASE_DATASET.satellite_scan`
  )
);

CREATE OR REPLACE TABLE `firehook-censoredplanet.DERIVED_DATASET.merged_reduced_scans_no_as`
PARTITION BY date
CLUSTER BY source, country_name, organization, domain
AS (
WITH
  AllScans AS (

  SELECT
    date,
    IF(is_control, "CONTROL", domain) as domain,
    category,
    "DISCARD" AS source,
    country,
    netblock,
    organization,
    controls_failed,
    CleanError(error) AS result,
    ClassifyError(error, "DISCARD", success, blockpage, page_signature) as outcome,
    count(1) AS count
  FROM `firehook-censoredplanet.BASE_DATASET.discard_scan`
  GROUP BY date, source, country, category, domain, netblock, organization, controls_failed, result, outcome

  UNION ALL
  SELECT
    date,
    IF(is_control, "CONTROL", domain) as domain,
    category,
    "ECHO" AS source,
    country,
    netblock,
    organization,
    controls_failed,
    CleanError(error) AS result,
    ClassifyError(error, "ECHO", success, blockpage, page_signature) as outcome,
    count(1) AS count
  FROM `firehook-censoredplanet.BASE_DATASET.echo_scan`
  GROUP BY date, source, country, category, domain, netblock, organization, controls_failed, result, outcome

  UNION ALL
  SELECT
    date,
    IF(is_control, "CONTROL", domain) as domain,
    category,
    "HTTP" AS source,
    country,
    netblock,
    organization,
    controls_failed,
    CleanError(error) AS result,
    ClassifyError(error, "HTTP", success, blockpage, page_signature) as outcome,
    count(1) AS count
  FROM `firehook-censoredplanet.BASE_DATASET.http_scan`
  GROUP BY date, source, country, category, domain, netblock, organization, controls_failed, result, outcome

  UNION ALL
  SELECT
    date,
    IF(is_control, "CONTROL", domain) as domain,
    category,
    "HTTPS" AS source,
    country,
    netblock,
    organization,
    controls_failed,
    CleanError(error) AS result,
    ClassifyError(error, "HTTPS", success, blockpage, page_signature) as outcome,
    count(1) AS count
  FROM `firehook-censoredplanet.BASE_DATASET.https_scan`
  GROUP BY date, source, country, category, domain, netblock, organization, controls_failed, result, outcome

  UNION ALL
  SELECT
    date,
    IF(is_control, "CONTROL", domain) as domain,
    category,
    "SATELLITE" AS source,
    country,
    netblock,
    organization,
    controls_failed,
    SatelliteResult(received, rcode, error) AS result,
    SatelliteOutcome(received, rcode, error, controls_failed, anomaly) as outcome,
    count(1) AS count
  FROM `firehook-censoredplanet.BASE_DATASET.satellite_scan`
  GROUP BY date, source, country, category, domain, netblock, organization, controls_failed, result, outcome
)
SELECT
  AllScans.* except (country),
  IFNULL(country_name, country) as country_name
FROM AllScans
LEFT JOIN `firehook-censoredplanet.metadata.country_names` ON country_code = country
# Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
WHERE NOT STARTS_WITH(outcome, "setup/")
);

# Drop the temp function before creating the view
# Since any temp functions in scope block view creation.
DROP FUNCTION CleanError;
DROP FUNCTION ClassifyError;

CREATE OR REPLACE VIEW `firehook-censoredplanet.DERIVED_DATASET.merged_reduced_scans`
OPTIONS(
  friendly_name="Reduced Scan View",
  description="A join of reduced scans with ASN info."
)
AS (
  SELECT
    date,
    domain,
    category,
    source,
    country_name,
    netblock,
    asn,
    as_name,
    organization,
    controls_failed,
    result,
    outcome,
    count
  FROM `firehook-censoredplanet.DERIVED_DATASET.merged_reduced_scans_no_as`
  LEFT JOIN `firehook-censoredplanet.DERIVED_DATASET.merged_net_as`
  USING (date, netblock)
);
