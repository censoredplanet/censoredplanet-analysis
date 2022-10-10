# https://www.iana.org/assignments/dns-parameters/dns-parameters.xhtml#dns-parameters-6
CREATE TEMP FUNCTION ClassifySatelliteRCode(rcode INTEGER) AS (
  CASE
    WHEN rcode = 1 THEN "❗️dns/rcode:FormErr"
    WHEN rcode = 2 THEN "❗️dns/rcode:ServFail"
    WHEN rcode = 3 THEN "❗️dns/rcode:NXDomain"
    WHEN rcode = 4 THEN "❗️dns/rcode:NotImp"
    WHEN rcode = 5 THEN "❗️dns/rcode:Refused"
    WHEN rcode = 6 THEN "❗️dns/rcode:YXDomain"
    WHEN rcode = 7 THEN "❗️dns/rcode:YXRRSet"
    WHEN rcode = 8 THEN "❗️dns/rcode:NXRRSet"
    WHEN rcode = 9 THEN "❗️dns/rcode:NotAuth"
    WHEN rcode = 10 THEN "❗️dns/rcode:NotZone"
    WHEN rcode = 11 THEN "❗️dns/rcode:DSOTYPENI"
    WHEN rcode = 12 THEN "❗️dns/rcode:Unassigned"
    WHEN rcode = 13 THEN "❗️dns/rcode:Unassigned"
    WHEN rcode = 14 THEN "❗️dns/rcode:Unassigned"
    WHEN rcode = 15 THEN "❗️dns/rcode:Unassigned"
    WHEN rcode = 16 THEN "❗️dns/rcode:BadVers"
    WHEN rcode = 17 THEN "❗️dns/rcode:BadSig"
    WHEN rcode = 18 THEN "❗️dns/rcode:BadKey"
    WHEN rcode = 19 THEN "❗️dns/rcode:BadTime"
    WHEN rcode = 20 THEN "❗️dns/rcode:BadMode"
    WHEN rcode = 21 THEN "❗️dns/rcode:BadAlg"
    WHEN rcode = 22 THEN "❗️dns/rcode:BadTrunc"
    WHEN rcode = 23 THEN "❗️dns/rcode:BadCookie"
    ELSE CONCAT("⚠️dns/unknown_rcode:", rcode)
  END
);

CREATE TEMP FUNCTION ClassifySatelliteError(error STRING) AS (
  CASE
    # Satellite v1
    WHEN REGEXP_CONTAINS(error, '"Err": {}') THEN "⚠️read/udp.timeout"
    WHEN REGEXP_CONTAINS(error, '"Err": 90') THEN "⚠️read/dns.msgsize"
    WHEN REGEXP_CONTAINS(error, '"Err": 111') THEN "⚠️read/udp.refused"
    WHEN REGEXP_CONTAINS(error, '"Err": 113') THEN "⚠️read/ip.host_no_route"
    WHEN REGEXP_CONTAINS(error, '"Err": 24') THEN "⚠️setup/system_failure" # Too many open files
    WHEN error = "{}" THEN "⚠️dns/unknown" # TODO figure out origin
    WHEN error = "no_answer" THEN "⚠️dns/answer:no_answer"
    #Satellite v2
    WHEN ENDS_WITH(error, "i/o timeout") THEN "⚠️read/udp.timeout"
    WHEN ENDS_WITH(error, "message too long") THEN "⚠️read/dns.msgsize"
    WHEN ENDS_WITH(error, "connection refused") THEN "⚠️read/udp.refused"
    WHEN ENDS_WITH(error, "no route to host") THEN "⚠️read/ip.host_no_route"
    WHEN ENDS_WITH(error, "short read") THEN "⚠️read/dns.msgsize"
    WHEN ENDS_WITH(error, "read: protocol error") THEN "⚠️read/protocol_error"
    WHEN ENDS_WITH(error, "socket: too many open files") THEN "⚠️setup/system_failure"
    ELSE CONCAT("⚠️dns/unknown_error:", error)
  END
);

CREATE TEMP FUNCTION InvalidIpType(ip STRING) AS (
  CASE
    WHEN STARTS_WITH(ip, "0.") THEN "❗️ip_invalid:zero"
    WHEN STARTS_WITH(ip, "127.") THEN "❗️ip_invalid:local_host"
    WHEN STARTS_WITH(ip, "10.") THEN "❗️ip_invalid:local_net"
    WHEN NET.IP_TO_STRING(NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(ip), 12)) = "172.16.0.0"  THEN "❗️ip_invalid:local_net"
    WHEN STARTS_WITH(ip, "192.168.") THEN "❗️ip_invalid:local_net"
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


CREATE TEMP FUNCTION OutcomeString(domain_name STRING,
                                   dns_error STRING,
                                   rcode INTEGER,
                                   answers ANY TYPE) AS (
    CASE 
        WHEN dns_error is NOT NULL AND dns_error != "" AND dns_error != "null" THEN ClassifySatelliteError(dns_error)
        # TODO fix -1 rcodes in v1 data in the pipeline
        WHEN rcode = -1 THEN "⚠️read/udp.timeout"
        WHEN rcode != 0 THEN ClassifySatelliteRCode(rcode)
        WHEN ARRAY_LENGTH(answers) = 0 THEN "⚠️answer:no_answer"
        ELSE IFNULL(
            (SELECT InvalidIpType(answer.ip) FROM UNNEST(answers) answer LIMIT 1),
            CASE
                WHEN (SELECT LOGICAL_OR(answer.matches_control.ip)
                      FROM UNNEST(answers) answer)
                      THEN "✅answer:matches_ip"
                WHEN (SELECT LOGICAL_OR(a.https_tls_cert_matches_domain AND a.https_tls_cert_has_trusted_ca)
                      FROM UNNEST(answers) a)
                      THEN "✅answer:valid_cert"
                WHEN (SELECT LOGICAL_OR(answer.http_analysis_is_known_blockpage)
                      FROM UNNEST(answers) answer)
                      THEN CONCAT("❗️page:http_blockpage:", answers[OFFSET(0)].http_analysis_page_signature)
                WHEN (SELECT LOGICAL_OR(answer.https_analysis_is_known_blockpage)
                      FROM UNNEST(answers) answer)
                      THEN CONCAT("❗️page:https_blockpage:", answers[OFFSET(0)].https_analysis_page_signature)
                WHEN (SELECT LOGICAL_OR(a.https_tls_cert_matches_domain AND NOT a.https_tls_cert_has_trusted_ca)
                      FROM UNNEST(answers) a)
                      THEN CONCAT("❗️answer:invalid_ca_valid_domain:", answers[OFFSET(0)].https_tls_cert_issuer)
                WHEN (SELECT LOGICAL_AND(NOT a.https_tls_cert_matches_domain)
                      FROM UNNEST(answers) a)
                      THEN CONCAT("❗️answer:cert_not_for_domain:", answers[OFFSET(0)].https_tls_cert_common_name)
                -- We check AS after cert because we've seen (rare) cases of blockpages hosted on the ISP that also hosts Akamai servers.
                WHEN (SELECT LOGICAL_OR(answer.matches_control.asn)
                      FROM UNNEST(answers) answer)
                      THEN "✅answer:matches_asn"
                ELSE CONCAT("⚠️answer:not_validated:", AnswersSignature(answers))
            END
        )
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

        # As per https://docs.censoredplanet.org/dns.html#id2, some resolvers are named `special` instead of the real hostname.
        IF(resolver_name="special","special",NET.REG_DOMAIN(resolver_name)) as reg_hostname,
        resolver_name as hostname,
        resolver_as_full_name AS network,
        CONCAT("AS", resolver_asn, IF(resolver_organization IS NOT NULL, CONCAT(" - ", resolver_organization), "")) AS subnetwork,
        resolver_country AS country_code,

        IF(domain_is_control, "CONTROL", domain) AS domain,
        IFNULL(domain_category, "Uncategorized") AS category,

        OutcomeString(domain, received_error, received_rcode, answers) as outcome,
        
        COUNT(1) AS count
    FROM `firehook-censoredplanet.BASE_DATASET.satellite_scan`
    # Filter on controls_failed to potentially reduce the number of output rows (less dimensions to group by).
    WHERE domain_controls_failed = FALSE
    GROUP BY date, hostname, country_code, network, subnetwork, outcome, domain, category
    # Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
    HAVING NOT STARTS_WITH(outcome, "setup/")
)
SELECT
    Grouped.* EXCEPT (country_code),
    IFNULL(country_name, country_code) AS country_name,
    CASE
        WHEN STARTS_WITH(outcome, "✅") THEN 0
        WHEN outcome = "⚠️read/udp.timeout" THEN NULL # timeouts are common in dns
        ELSE count
    END AS unexpected_count,
    CASE
        WHEN STARTS_WITH(outcome, "✅") THEN count
        WHEN outcome = "⚠️read/udp.timeout" THEN NULL
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
DROP FUNCTION OutcomeString;
DROP FUNCTION InvalidIpType;
DROP FUNCTION AnswersSignature;