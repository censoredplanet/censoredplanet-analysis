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