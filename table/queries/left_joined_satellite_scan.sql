CREATE OR REPLACE TABLE `firehook-censoredplanet.DERIVED_DATASET.satellite_scan_left_joined`
PARTITION BY date
CLUSTER BY resolver_country, resolver_asn
AS (
  SELECT a.* EXCEPT (answers),
         r.ip as received_ip,
         r.asn as received_asnum,
         r.as_name as received_asname,
         r.censys_http_body_hash as received_censys_http_body_hash,
         r.censys_ip_cert as received_censys_ip_cert,
         r.matches_control as received_matches_control,
         r.http_error as received_http_error,
         r.http_analysis_is_known_blockpage as received_http_analysis_is_known_blockpage,
         r.http_analysis_page_signature as received_http_analysis_page_signature,
         r.http_response_status as received_http_response_status,
         r.http_response_body as received_http_response_body,
         r.http_response_headers as received_http_response_headers,
         r.https_error as received_https_error,
         r.https_analysis_is_known_blockpage as received_https_analysis_is_known_blockpage,
         r.https_analysis_page_signature as received_https_analysis_page_signature,
         r.https_response_status as received_https_response_status,
         r.https_response_body as received_https_response_body,
         r.https_response_headers	as received_https_response_headers,
         r.https_tls_version as received_https_response_tls_version,
         r.https_tls_cipher_suite as received_https_response_tls_cipher_suite,
         r.https_tls_cert as received_https_response_tls_cert,
         r.https_tls_cert_common_name as received_https_response_tls_cert_common_name,
         r.https_tls_cert_issuer as received_https_response_tls_cert_issuer,
  FROM `firehook-censoredplanet.BASE_DATASET.satellite_scan` as a
       LEFT JOIN UNNEST(answers) as r
);