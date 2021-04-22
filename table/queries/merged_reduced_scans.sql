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
  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    IF(error = "", "null", IFNULL(error, "null")),
    "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+", "[IP]"),
    "\\[IP\\]:[0-9]+", "[IP]:[PORT]"),
    "length [0-9]+", "length [LENGTH]"),
    "port\\.[0-9]+", "port.[PORT]")
);

# Classify all errors into a small set of enums
#
# Input is a nullable error string from the raw data
# Source is one of "ECHO", "Discard, "HTTP", "HTTPS"
#
# Output is a string of the format "stage/outcome"
# Documentation of this enum is at
# https://github.com/censoredplanet/censoredplanet-analysis/blob/master/docs/tables.md#outcome-classification
CREATE TEMP FUNCTION ClassifyError(error STRING, source STRING) AS (
  CASE
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

    # Write failures
    WHEN ENDS_WITH(error, "write: connection reset by peer") THEN "write/tcp.reset"
    WHEN ENDS_WITH(error, "write: broken pipe") THEN "write/system"

    # Read failures
    WHEN REGEXP_CONTAINS(error, "request canceled") THEN "read/timeout"
    WHEN ENDS_WITH(error, "i/o timeout") THEN "read/timeout"
    WHEN ENDS_WITH(error, "shutdown: transport endpoint is not connected") THEN "read/system"
    # TODO: for HTTPS this error could potentially also be SNI blocking in the tls stage
    # find a way to diffentiate this case.
    WHEN ENDS_WITH(error, "read: connection reset by peer") THEN "read/tcp.reset"

    # HTTP content verification failures
    WHEN (source != "ECHO" AND REGEXP_CONTAINS(error, "unexpected EOF")) THEN "read/http.truncated_response"
    WHEN (source != "ECHO" AND REGEXP_CONTAINS(error, "EOF")) THEN "read/http.empty"
    WHEN ENDS_WITH(error, "http: server closed idle connection") THEN "read/http.truncated_response"
    WHEN ENDS_WITH(error, "trailer header without chunked transfer encoding") THEN "http/http.invalid"
    WHEN ENDS_WITH(error, "response missing Location header") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "bad Content-Length") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "failed to parse Location header") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "malformed HTTP") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "malformed MIME") THEN "http/http.invalid"

    # Content verification failures
    WHEN (source = "ECHO" AND ENDS_WITH(error, "EOF")) THEN "content/response_mismatch" # Echo
    WHEN (error = "Incorrect echo response") THEN "content/response_mismatch" # Echo
    WHEN (error = "Received response") THEN "content/response_mismatch" # Discard
    WHEN (error = "Incorrect web response: status lines don't match") THEN "content/status_mismatch" # HTTP/S
    WHEN (error = "Incorrect web response: bodies don't match") THEN "content/body_mismatch" # HTTP/S
    WHEN (error = "Incorrect web response: certificates don't match") THEN "content/tls_mismatch" # HTTPS
    WHEN (error = "Incorrect web response: cipher suites don't match") THEN "content/tls_mismatch" # HTTPS
    WHEN (error = "Incorrect web response: TLS versions don't match") THEN "content/tls_mismatch" # HTTPS

    # Unknown errors
    ELSE "unknown/unknown"
  END
);


CREATE OR REPLACE TABLE `firehook-censoredplanet.derived.merged_net_as`
PARTITION BY date
CLUSTER BY netblock
AS (
  SELECT DISTINCT
    date,
    netblock,
    asn,
    as_full_name AS as_full_name
  FROM (
    SELECT * FROM `firehook-censoredplanet.base.discard_scan` UNION ALL
    SELECT * FROM `firehook-censoredplanet.base.echo_scan` UNION ALL
    SELECT * FROM `firehook-censoredplanet.base.http_scan` UNION ALL
    SELECT * FROM `firehook-censoredplanet.base.https_scan`
  )
);

CREATE OR REPLACE TABLE `firehook-censoredplanet.derived.merged_reduced_scans_no_as`
PARTITION BY date
CLUSTER BY source, country, domain, netblock
AS (
WITH
  AllScans AS (
  SELECT
    date,
    if(sent != "", TRIM(REGEXP_EXTRACT(sent, "Host: (.*)")), domain) as domain,
    "DISCARD" AS source,
    country,
    netblock,
    CleanError(error) AS result,
    ClassifyError(error, "DISCARD") as outcome,
    count(1) AS count
  FROM `firehook-censoredplanet.base.discard_scan`
  GROUP BY date, source, country, domain, netblock, result, outcome

  UNION ALL
  SELECT
    date,
    if(sent != "", TRIM(REGEXP_EXTRACT(sent, "Host: (.*)")), domain) as domain,
    "ECHO" AS source,
    country,
    netblock,
    CleanError(error) AS result,
    ClassifyError(error, "ECHO") as outcome,
    count(1) AS count
  FROM `firehook-censoredplanet.base.echo_scan`
  GROUP BY date, source, country, domain, netblock, result, outcome

  UNION ALL
  SELECT
    date,
    IF(domain != sent AND sent != "", sent, domain) AS domain,
    "HTTP" AS source,
    country,
    netblock,
    CleanError(error) AS result,
    ClassifyError(error, "HTTP") as outcome,
    count(1) AS count
  FROM `firehook-censoredplanet.base.http_scan`
  GROUP BY date, source, country, domain, netblock, result, outcome

  UNION ALL
  SELECT
    date,
    IF(domain != sent AND sent != "", sent, domain) AS domain,
    "HTTPS" AS source,
    country,
    netblock,
    CleanError(error) AS result,
    ClassifyError(error, "HTTPS") as outcome,
    count(1) AS count
  FROM `firehook-censoredplanet.base.https_scan`
  GROUP BY date, source, country, domain, netblock, result, outcome
)
SELECT *
FROM AllScans
);

# Drop the temp function before creating the view
# Since any temp functions in scope block view creation.
DROP FUNCTION CleanError;
DROP FUNCTION ClassifyError;

CREATE OR REPLACE VIEW `firehook-censoredplanet.derived.merged_reduced_scans`
OPTIONS(
  friendly_name="Reduced Scan View",
  description="A join of reduced scans with ASN info."
)
AS (
  SELECT
    date,
    domain,
    source,
    country,
    netblock,
    asn,
    as_full_name AS as_name,
    result,
    outcome,
    count
  FROM `firehook-censoredplanet.derived.merged_reduced_scans_no_as`
  LEFT JOIN `firehook-censoredplanet.derived.merged_net_as`
  USING (date, netblock)
);
