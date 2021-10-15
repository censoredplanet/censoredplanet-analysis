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
                                   blockpage_id STRING,
                                   server_header STRING) AS (
  CASE
    WHEN blockpage_match THEN CONCAT("content/blockpage:", blockpage_id)

    # Trust headers from from Akamai servers.
    WHEN (server_header = 'AkamaiGHost') OR (server_header = 'GHost') THEN "expected/trusted_host:akamai"

    # Content mismatch for hyperquack v2 which didn't write
    # content verification failures in the error field from 2021-04-26 to 2021-07-21
    WHEN (NOT template_match AND (error IS NULL OR error = "")) THEN "content/mismatch"

    # Success
    WHEN (error IS NULL OR error = "") THEN "expected/match"

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
    WHEN (source = "ECHO" AND ENDS_WITH(error, "EOF")) THEN "content/mismatch" # Echo
    # Hyperquack v1 errors
    WHEN (error = "Incorrect echo response") THEN "content/mismatch" # Echo
    WHEN (error = "Received response") THEN "content/mismatch" # Discard
    WHEN (error = "Incorrect web response: status lines don't match") THEN "content/status_mismatch" # HTTP/S
    WHEN (error = "Incorrect web response: bodies don't match") THEN "content/body_mismatch" # HTTP/S
    WHEN (error = "Incorrect web response: certificates don't match") THEN "content/tls_mismatch" # HTTPS
    WHEN (error = "Incorrect web response: cipher suites don't match") THEN "content/tls_mismatch" # HTTPS
    WHEN (error = "Incorrect web response: TLS versions don't match") THEN "content/tls_mismatch" # HTTPS
    # Hyperquack v2 errors
    WHEN (error = "echo response does not match echo request") THEN "content/mismatch" # Echo
    WHEN (error = "discard response is not empty") THEN "content/mismatch" # Discard
    WHEN (error = "Status lines does not match") THEN "content/status_mismatch" # HTTP/S
    WHEN (error = "Bodies do not match") THEN "content/body_mismatch" # HTTP/S
    WHEN (error = "Certificates do not match") THEN "content/tls_mismatch" # HTTPS
    WHEN (error = "Cipher suites do not match") THEN "content/tls_mismatch" # HTTPS
    WHEN (error = "TLS versions do not match") THEN "content/tls_mismatch" # HTTPS

    # Unknown errors
    ELSE "unknown/unknown"
  END
);

# Returns "abc" from the "Server: abc" header if it exists. Otherwise NULL.
CREATE TEMP FUNCTION ExtractServerHeader(received_headers ARRAY<STRING>) AS (
  (SELECT REGEXP_EXTRACT(header, "Server: (.*)")
   FROM UNNEST(received_headers) AS header
   WHERE STARTS_WITH(header, "Server: ") LIMIT 1)
);

# BASE_DATASET and DERIVED_DATASET are reserved dataset placeholder names
# which will be replaced when running the query

# Increment the version of this table if you change the table in a backwards-incomatible way.

# Rely on the table name firehook-censoredplanet.derived.merged_reduced_scans_vN
# if you would like to see a clear breakage when there's a backwards-incompatible change.
# Old table versions will be deleted.
CREATE OR REPLACE TABLE `firehook-censoredplanet.DERIVED_DATASET.merged_reduced_scans_v2`
PARTITION BY date
# Columns `source` and `country_name` are always used for filtering and must come first.
# `network` and `domain` are useful for filtering and grouping.
CLUSTER BY source, country_name, network, domain
OPTIONS (
  friendly_name="Reduced Scans",
  description="Filtered and pre-aggregated table of scans to use with the Censored Planed Dashboard"
)
AS (
WITH AllScans AS (
  SELECT * EXCEPT (source), "DISCARD" AS source
  FROM `firehook-censoredplanet.BASE_DATASET.discard_scan`
  UNION ALL
  SELECT * EXCEPT (source), "ECHO" AS source
  FROM `firehook-censoredplanet.BASE_DATASET.echo_scan`
  UNION ALL
  SELECT * EXCEPT (source), "HTTP" AS source
  FROM `firehook-censoredplanet.BASE_DATASET.http_scan`
  UNION ALL
  SELECT * EXCEPT (source), "HTTPS" AS source
  FROM `firehook-censoredplanet.BASE_DATASET.https_scan`
), Grouped AS (
    SELECT
        date,

        source,
        country AS country_code,
        as_full_name AS network,
        IF(is_control, "CONTROL", domain) AS domain,

        ClassifyError(error, source, success, blockpage, page_signature, ExtractServerHeader(received_headers)) AS outcome,
        CONCAT("AS", asn, IF(organization IS NOT NULL, CONCAT(" - ", organization), "")) AS subnetwork,
        IFNULL(category, "Uncategorized") AS category,

        COUNT(*) AS count
    FROM AllScans
    # Filter on controls_failed to potentially reduce the number of output rows (less dimensions to group by).
    WHERE NOT controls_failed
    GROUP BY date, source, country_code, network, outcome, domain, category, subnetwork
    # Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
    HAVING NOT STARTS_WITH(outcome, "setup/")
)
SELECT
    Grouped.* EXCEPT (country_code),
    IFNULL(country_name, country_code) AS country_name,
    CASE
        WHEN (STARTS_WITH(outcome, "expected/")) THEN 0
        WHEN (STARTS_WITH(outcome, "dial/") OR STARTS_WITH(outcome, "setup/") OR ENDS_WITH(outcome, "/invalid")) THEN NULL
        WHEN (ENDS_WITH(outcome, "unknown")) THEN count / 2.0
        ELSE count
    END AS unexpected_count
    FROM Grouped
    LEFT JOIN `firehook-censoredplanet.metadata.country_names` USING (country_code)
    WHERE country_code IS NOT NULL
);

# Drop the temp function before creating the view
# Since any temp functions in scope block view creation.
DROP FUNCTION ClassifyError;
DROP FUNCTION ExtractServerHeader;

# This view is the stable name for the table above.
# Rely on the table name firehook-censoredplanet.derived.merged_reduced_scans
# if you would like to continue pointing to the table even when there is a breaking change.
CREATE OR REPLACE VIEW `firehook-censoredplanet.DERIVED_DATASET.merged_reduced_scans`
AS (
  SELECT *
  FROM `firehook-censoredplanet.DERIVED_DATASET.merged_reduced_scans_v2`
)