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

DECLARE earliest_date DATE;
SET earliest_date = DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH);

CREATE TEMP FUNCTION AddHyperquackOutcomeEmoji(outcome STRING) AS (
  CASE
    WHEN STARTS_WITH(outcome, "setup/") THEN CONCAT("❔", outcome)
    WHEN STARTS_WITH(outcome, "unknown/") THEN CONCAT("❔", outcome)
    WHEN STARTS_WITH(outcome, "dial/") THEN CONCAT("❔", outcome)
    WHEN STARTS_WITH(outcome, "read/system") THEN CONCAT("❔", outcome)
    WHEN STARTS_WITH(outcome, "expected/") THEN CONCAT("✅", SUBSTR(outcome, 10))
    WHEN STARTS_WITH(outcome, "content/blockpage") THEN CONCAT("❗️", outcome)
    WHEN STARTS_WITH(outcome, "read/") THEN CONCAT("❗️", outcome)
    WHEN STARTS_WITH(outcome, "write/") THEN CONCAT("❗️", outcome)
    ELSE CONCAT("❓", outcome)
  END
);

# https://www.iana.org/assignments/dns-parameters/dns-parameters.xhtml#dns-parameters-6
CREATE TEMP FUNCTION ClassifySatelliteRCode(rcode INTEGER) AS (
  CASE
    WHEN rcode = 1 THEN "❗️dns.error:FormErr"
    WHEN rcode = 2 THEN "❓dns.error:ServFail"
    WHEN rcode = 3 THEN "❗️dns.error:NXDomain"
    WHEN rcode = 4 THEN "❗️dns.error:NotImp"
    WHEN rcode = 5 THEN "❗️dns.error:Refused"
    WHEN rcode = 6 THEN "❗️dns.error:YXDomain"
    WHEN rcode = 7 THEN "❗️dns.error:YXRRSet"
    WHEN rcode = 8 THEN "❗️dns.error:NXRRSet"
    WHEN rcode = 9 THEN "❗️dns.error:NotAuth"
    WHEN rcode = 10 THEN "❗️dns.error:NotZone"
    WHEN rcode = 11 THEN "❗️dns.error:DSOTYPENI"
    WHEN rcode = 12 THEN "❗️dns.error:Unassigned"
    WHEN rcode = 13 THEN "❗️dns.error:Unassigned"
    WHEN rcode = 14 THEN "❗️dns.error:Unassigned"
    WHEN rcode = 15 THEN "❗️dns.error:Unassigned"
    WHEN rcode = 16 THEN "❗️dns.error:BadVers"
    WHEN rcode = 17 THEN "❗️dns.error:BadSig"
    WHEN rcode = 18 THEN "❗️dns.error:BadKey"
    WHEN rcode = 19 THEN "❗️dns.error:BadTime"
    WHEN rcode = 20 THEN "❗️dns.error:BadMode"
    WHEN rcode = 21 THEN "❗️dns.error:BadAlg"
    WHEN rcode = 22 THEN "❗️dns.error:BadTrunc"
    WHEN rcode = 23 THEN "❗️dns.error:BadCookie"
    ELSE CONCAT("❗️dns.unknown_rcode:", rcode)
  END
);

CREATE TEMP FUNCTION ClassifySatelliteError(error STRING) AS (
  CASE
    # Satellite v1
    WHEN REGEXP_CONTAINS(error, '"Err": {}') THEN "❓dns.timedout"
    WHEN REGEXP_CONTAINS(error, '"Err": 90') THEN "❗️dns.msgsize"
    WHEN REGEXP_CONTAINS(error, '"Err": 111') THEN "❗️dns.connrefused"
    WHEN REGEXP_CONTAINS(error, '"Err": 113') THEN "❔dns.hostunreach"
    WHEN REGEXP_CONTAINS(error, '"Err": 24') THEN "❔setup.system_failure" # Too many open files
    WHEN error = "{}" THEN "❗️dns.unknown" # TODO figure out origin
    WHEN error = "no_answer" THEN "❗️dns.no_answer"
    #Satellite v2
    WHEN ENDS_WITH(error, "i/o timeout") THEN "❓dns.timedout"
    WHEN ENDS_WITH(error, "message too long") THEN "❗️dns.msgsize"
    WHEN ENDS_WITH(error, "connection refused") THEN "❗️dns.connrefused"
    WHEN ENDS_WITH(error, "no route to host") THEN "❔dns.hostunreach"
    WHEN ENDS_WITH(error, "short read") THEN "❗️dns.msgsize"
    WHEN ENDS_WITH(error, "read: protocol error") THEN "❗️dns.protocol_error"
    WHEN ENDS_WITH(error, "socket: too many open files") THEN "❔setup.system_failure"
    ELSE CONCAT("❗️unknown_error:", error)
  END
);

CREATE TEMP FUNCTION InvalidIpType(ip STRING) AS (
  CASE
    WHEN STARTS_WITH(ip, "0.") THEN "❗️ip.invalid:zero"
    WHEN STARTS_WITH(ip, "127.") THEN "❗️ip.invalid:local_host"
    WHEN STARTS_WITH(ip, "10.") THEN "❗️ip.invalid:local_net"
    WHEN NET.IP_TO_STRING(NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(ip), 12)) = "172.16.0.0"  THEN "❗️ip.invalid:local_net"
    WHEN STARTS_WITH(ip, "192.168.") THEN "❗️ip.invalid:local_net"
    ELSE NULL
  END
);

CREATE TEMP FUNCTION AnswersSignature(answers ANY TYPE) AS (
  ARRAY_TO_STRING(ARRAY(
    SELECT DISTINCT
      CASE
        WHEN answer.as_name != "" THEN answer.as_name
        WHEN answer.asn IS NOT NULL THEN CONCAT("AS", answer.asn)
        ELSE "missing_as_info"
      END
    FROM UNNEST(answers) answer
  ), ",")
);

# resolver_*_rate fields measure various error rates for each resolver across the whole scan
# They should be a value from 0-1 calculated as
# number_of_unique_domains_with_error_on_resolver_per_scan
#  /total_number_of_unique_domains_on_resolver_per_scan
# resolvers which consistantly return too many errors are filtered out.
CREATE TEMP FUNCTION BadResolver(
  # Error rate in creating the DNS connection, including timeouts
  resolver_connect_error_rate FLOAT64,
  # Failure to get a valid certificate when fetching the page from a returned IP.
  resolver_invalid_cert_rate FLOAT64,
  resolver_non_zero_rcode_rate FLOAT64
) AS (
  resolver_connect_error_rate > .75 OR
  resolver_invalid_cert_rate > .75 OR
  resolver_non_zero_rcode_rate > .75 
);

CREATE TEMP FUNCTION SatelliteOutcomeString(domain_name STRING,
                                            dns_error STRING,
                                            rcode INTEGER,
                                            answers ANY TYPE) AS (
    CASE 
        WHEN (dns_error is NOT NULL
              AND dns_error != ""
              AND dns_error != "null"
              AND dns_error != "SERVFAIL") THEN ClassifySatelliteError(dns_error)
        # TODO fix -1 rcodes in v1 data in the pipeline
        WHEN rcode = -1 THEN "❓dns.timedout"
        WHEN rcode != 0 THEN ClassifySatelliteRCode(rcode)
        WHEN ARRAY_LENGTH(answers) = 0 THEN "❗️ip.empty"
        ELSE IFNULL(
            (SELECT InvalidIpType(answer.ip) FROM UNNEST(answers) answer LIMIT 1),
            CASE
                WHEN (SELECT LOGICAL_OR(answer.matches_control.ip)
                      FROM UNNEST(answers) answer)
                      THEN "✅ip.matchip"
                WHEN (SELECT LOGICAL_OR(a.https_tls_cert_matches_domain AND a.https_tls_cert_has_trusted_ca)
                      FROM UNNEST(answers) a)
                      THEN "✅tls.validcert"
                WHEN (SELECT LOGICAL_OR(a.https_tls_cert_matches_domain AND NOT a.https_tls_cert_has_trusted_ca)
                      FROM UNNEST(answers) a)
                      THEN CONCAT("❗️tls.badca:", answers[OFFSET(0)].https_tls_cert_issuer)
                WHEN (SELECT LOGICAL_AND(NOT a.https_tls_cert_matches_domain)
                      FROM UNNEST(answers) a)
                      THEN CONCAT("❗️️tls.baddomain:", answers[OFFSET(0)].https_tls_cert_common_name)
                WHEN (SELECT LOGICAL_OR(answer.http_analysis_is_known_blockpage)
                      FROM UNNEST(answers) answer)
                      THEN CONCAT("❗️http.blockpage:", answers[OFFSET(0)].http_analysis_page_signature)
                -- We check AS after cert/blockpage because we've seen (rare) cases of blockpages hosted on the ISP that also hosts Akamai servers.
                WHEN (SELECT LOGICAL_OR(answer.matches_control.asn)
                      FROM UNNEST(answers) answer)
                      THEN "✅ip.matchasn"
                ELSE CONCAT("❓tls.connerror:", AnswersSignature(answers))
            END
        )
    END
);


# When some roundtrips in the DNS scans fail they retry.
# These retried are identified in bigquery by having the same measurement id
# Once a roundtrip succeeds we stop retrying
# To avoid overcounting retries (ex: measuring x4 timeouts on failure but only x1 success when succeeding)
# We only want to include the last retry in any set
CREATE OR REPLACE TABLE `PROJECT_NAME.DERIVED_DATASET.satellite_last_measurement_ids`
# More correctly this would be by source, but we can't partition on strings
PARTITION BY date
AS (
  SELECT date,
         measurement_id,
         MAX(retry) AS retry
  FROM `PROJECT_NAME.BASE_DATASET.satellite_scan`
  GROUP BY date, measurement_id
);

# TODO move this to the pipeline instead of the query
# Get all domains that have even a single valid HTTPS certificate resolution per scan
CREATE OR REPLACE TABLE `PROJECT_NAME.DERIVED_DATASET.https_capable_domains`
AS (
  SELECT domain, source
  FROM `PROJECT_NAME.BASE_DATASET.satellite_scan`,
       UNNEST(answers) as a
  WHERE a.https_tls_cert_matches_domain AND a.https_tls_cert_has_trusted_ca
  GROUP BY domain, source
);

# BASE_DATASET and DERIVED_DATASET are reserved dataset placeholder names
# which will be replaced when running the query

# Increment the version of this table if you change the table in a backwards-incomatible way.

# Rely on the table name firehook-censoredplanet.derived.merged_reduced_scans_vN
# if you would like to see a clear breakage when there's a backwards-incompatible change.
# Old table versions will be deleted.
CREATE OR REPLACE TABLE `PROJECT_NAME.DERIVED_DATASET.merged_reduced_scans_v3`
PARTITION BY date
# Columns `source` and `country_name` are always used for filtering and must come first.
# `network` and `domain` are useful for filtering and grouping.
CLUSTER BY source, country_name, domain, network
OPTIONS (
  friendly_name="Reduced Scans",
  description="Filtered and pre-aggregated table of scans to use with the Censored Planed Dashboard"
)
AS (
WITH 
  FilteredDiscard AS (SELECT * FROM `PROJECT_NAME.BASE_DATASET.discard_scan` WHERE date >= earliest_date AND NOT controls_failed),
  FilteredEcho AS (SELECT * FROM `PROJECT_NAME.BASE_DATASET.echo_scan` WHERE date >= earliest_date AND NOT controls_failed),
  FilteredHTTP AS (SELECT * FROM `PROJECT_NAME.BASE_DATASET.http_scan` WHERE date >= earliest_date AND NOT controls_failed),
  FilteredHTTPS AS (SELECT * FROM `PROJECT_NAME.BASE_DATASET.https_scan` WHERE date >= earliest_date AND NOT controls_failed),
  FilteredSatellite AS (
    SELECT * EXCEPT (date), 
           a.date AS date
    FROM `PROJECT_NAME.BASE_DATASET.satellite_scan` AS a
    # Only include the last measurement in any set of retries
    JOIN `PROJECT_NAME.DERIVED_DATASET.satellite_last_measurement_ids` AS b
        ON (a.date = b.date AND a.measurement_id = b.measurement_id AND (a.retry = b.retry OR a.retry IS NULL))
    INNER JOIN `PROJECT_NAME.DERIVED_DATASET.https_capable_domains`
        USING (domain, source)
    # Filter on controls_failed to potentially reduce the number of output rows (less dimensions to group by).
    WHERE a.date >= earliest_date 
          AND domain_controls_failed = FALSE
          AND NOT BadResolver(resolver_connect_error_rate,
                              resolver_invalid_cert_rate,
                              resolver_non_zero_rcode_rate)
  ),
  AllScans AS (
  SELECT "DISCARD" AS source,
         date,
         server_country,
         server_as_full_name,
         NULL AS server_name,
         NULL AS reg_server_hostname,
         domain_is_control,
         domain,
         AddHyperquackOutcomeEmoji(outcome) as outcome,
         server_asn,
         server_organization,
         domain_category,
  FROM FilteredDiscard
  UNION ALL
  SELECT "ECHO" AS source,
         date,
         server_country,
         server_as_full_name,
         NULL AS server_name,
         NULL AS reg_server_hostname,
         domain_is_control,
         domain,
         AddHyperquackOutcomeEmoji(outcome) as outcome,
         server_asn,
         server_organization,
         domain_category,
  FROM FilteredEcho
  UNION ALL
  SELECT "HTTP" AS source,
         date,
         server_country,
         server_as_full_name,
         NULL AS server_name,
         NULL AS reg_server_hostname,
         domain_is_control,
         domain,
         AddHyperquackOutcomeEmoji(outcome) as outcome,
         server_asn,
         server_organization,
         domain_category,
  FROM FilteredHTTP
  UNION ALL
  SELECT "HTTPS" AS source,
         date,
         server_country,
         server_as_full_name,
         NULL AS server_name,
         NULL AS reg_server_hostname,
         domain_is_control,
         domain,
         AddHyperquackOutcomeEmoji(outcome) as outcome,
         server_asn,
         server_organization,
         domain_category,
  FROM FilteredHTTPS
  UNION ALL
  SELECT "DNS" AS source,
         date,
         resolver_country as server_country,
         resolver_as_full_name as server_as_full_name,
         resolver_name AS server_name,
         # As per https://docs.censoredplanet.org/dns.html#id2, some resolvers are named `special` instead of the real hostname.
         IF(resolver_name="special","special",NET.REG_DOMAIN(resolver_name)) as reg_server_hostname,
         domain_is_control,
         domain,
         SatelliteOutcomeString(domain, received_error, received_rcode, answers) as outcome,
         resolver_asn as server_asn,
         resolver_organization as server_organization,
         domain_category,
  FROM FilteredSatellite
), Grouped AS (
    SELECT
        date,
        source,
        server_country AS country_code,
        server_name as hostname,
        reg_server_hostname AS reg_hostname,
        server_as_full_name AS network,
        CONCAT("AS", server_asn, IF(server_organization IS NOT NULL, CONCAT(" - ", server_organization), "")) AS subnetwork,
        IF(domain_is_control, "CONTROL", domain) AS domain,
        IFNULL(domain_category, "Uncategorized") AS category,
        outcome,
        COUNT(*) AS count
    FROM AllScans
    GROUP BY date, source, country_code, network, outcome, domain, category, subnetwork, hostname, reg_hostname
    # Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
    HAVING (NOT STARTS_WITH(outcome, "❔setup/")
            AND NOT outcome = "❔read/system")
)
SELECT
    Grouped.* EXCEPT (country_code),
    IFNULL(country_name, country_code) AS country_name,
    CASE
        WHEN (STARTS_WITH(outcome, "✅")) THEN 0
        WHEN (STARTS_WITH(outcome, "❗️")) THEN count
        # Timeouts are not counted in Satellite because they're too common
        WHEN outcome = "❓read/udp.timeout" THEN NULL
        WHEN (STARTS_WITH(outcome, "❓")) THEN count
        WHEN (STARTS_WITH(outcome, "❔")) THEN NULL
    END AS unexpected_count
    FROM Grouped
    LEFT JOIN `PROJECT_NAME.metadata.country_names` USING (country_code)
    WHERE country_code IS NOT NULL
);

DROP TABLE `PROJECT_NAME.DERIVED_DATASET.satellite_last_measurement_ids`;
DROP TABLE `PROJECT_NAME.DERIVED_DATASET.https_capable_domains`;

# Drop the temp function before creating the view
# Since any temp functions in scope block view creation.
DROP FUNCTION AddHyperquackOutcomeEmoji;
DROP FUNCTION ClassifySatelliteRCode;
DROP FUNCTION ClassifySatelliteError;
DROP FUNCTION SatelliteOutcomeString;
DROP FUNCTION InvalidIpType;
DROP FUNCTION AnswersSignature;
DROP FUNCTION BadResolver;

# This view is the stable name for the table above.
# Rely on the table name firehook-censoredplanet.derived.merged_reduced_scans
# if you would like to continue pointing to the table even when there is a breaking change.
CREATE OR REPLACE VIEW `PROJECT_NAME.DERIVED_DATASET.merged_reduced_scans`
AS (
  SELECT *
  FROM `PROJECT_NAME.DERIVED_DATASET.merged_reduced_scans_v3`
)