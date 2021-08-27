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

# BASE_DATASET and DERIVED_DATASET are reserved dataset placeholder names
# which will be replaced when running the query

CREATE OR REPLACE TABLE `firehook-censoredplanet.DERIVED_DATASET.merged_reduced_scans`
PARTITION BY date
CLUSTER BY source, country_name, network, domain
AS (
WITH AllScans AS (
  SELECT * except (source), "DISCARD" AS source
  FROM `firehook-censoredplanet.BASE_DATASET.discard_scan`
  UNION ALL
  SELECT * except (source), "ECHO" AS source
  FROM `firehook-censoredplanet.BASE_DATASET.echo_scan`
  UNION ALL
  SELECT * except (source), "HTTP" AS source
  FROM `firehook-censoredplanet.BASE_DATASET.http_scan`
  UNION ALL
  SELECT * except (source), "HTTPS" AS source
  FROM `firehook-censoredplanet.BASE_DATASET.https_scan`
), Grouped AS (
    SELECT
        date,

        source,
        country as country_code,
        as_full_name as network,
        IF(is_control, "CONTROL", domain) as domain,

        ClassifyError(error, source, success, blockpage, page_signature) as outcome,
        CONCAT("AS", asn, IF(organization is not null, CONCAT(" - ", organization), "")) as subnetwork,
        category,

        count(*) as count
    FROM AllScans
    # Filter on controls_failed to potentially reduce the number of output rows (less dimensions to group by).
    WHERE NOT controls_failed
    GROUP BY date, source, country_code, network, outcome, domain, category, subnetwork
    # Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
    HAVING NOT STARTS_WITH(outcome, "setup/")
)
SELECT
    Grouped.* except (country_code),
    IFNULL(country_name, country_code) as country_name,
    CASE
        WHEN (outcome = "complete/success") THEN 0
        WHEN (STARTS_WITH(outcome, "dial/") OR STARTS_WITH(outcome, "setup/") OR ENDS_WITH(outcome, "/invalid")) THEN NULL
        WHEN (ENDS_WITH(outcome, "unknown")) THEN count / 2.0
        ELSE count
    END as unexpected_count
    FROM Grouped
    LEFT JOIN `firehook-censoredplanet.metadata.country_names` using (country_code)
);
