# Base Tables

These tables are created using the original censored planet json data, plus some
[additional data sources](../pipeline/metadata/).

## Table names

There is one table for each scan type.

- `censoredplanet-analysisv1:base.echo_scan`
- `censoredplanet-analysisv1:base.discard_scan`
- `censoredplanet-analysisv1:base.http_scan`
- `censoredplanet-analysisv1:base.https_scan`
- `censoredplanet-analysisv1:base.satellite_scan`

## Partitioning and Clustering

The tables are time-partitioned along the `date` field.

The tables are clustered along the `[server|resolver]_country` and then `[server|resolver]_asn` fields.

## Hyperquack Table Format

The json data is processed into a flat table format which looks like this.

| Field Name                | Type         | Contains |
| ------------------------- | ------------ | -------- |
|                           |
| **Measured Domain**       |
|                           |
| domain                    | STRING       | The domain being tested, eg. `example.com` |
| domain_is_control         | STRING       | If the measured domain a control domain? |
| domain_category           | STRING       | The [category](domain_categories.md) of the domain being tested, eg. `Social Networking`, `None` if unknown |
|                           |
| **Time**                  |
|                           |
| date                      | DATE         | Date that an individual measurement was taken |
| start_time                | TIMESTAMP    | Start time of the individual measurement |
| end_time                  | TIMESTAMP    | End time of the individual measurement |
| retry                     | INTEGER      | Number 0-N (usually not > 4) indicating which retry in the measurement_id set this roundtrip was. Domain control measurements have a retry index of `None`. |
|                           |
| **Vantage Point Server**  |
|                           |
| server_ip                 | STRING       | The ip address of the server being tested, eg. `1.1.1.1` |
| server_netblock           | STRING       | Netblock of the IP, eg. `1.1.1.0/24` |
| server_asn                | INTEGER      | Autonomous system number, eg. `13335` |
| server_as_name            | STRING       | Autonomous system short name, eg. `CLOUDFLARENET` |
| server_as_full_name       | STRING       | Autonomous system long name, eg. `Cloudflare, Inc.` |
| server_as_class           | STRING       | The type of AS eg. `Transit/Access`, `Content` (for CDNs) or `Enterprise` |
| server_country            | STRING       | Autonomous system country, eg. `US` |
| server_organization       | STRING       | The IP organization, eg. `United Technical Services` |
|                           |
| **Received Fields**       |              | :warning: These fields differ between scan types |
|                           |
| received_error            | STRING       | Any error, eg. `Network Timeout` |
| received_tls_version      | INTEGER      | The TLS version number eg. `771` (meaning TLS 1.2) </br> :warning: only present in HTTPS tables |
| received_tls_cipher_suite | INTEGER      | The TLS cipher suite number </br> eg. `49199` (meaning TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256) </br> :warning: only present in HTTPS tables |
| received_tls_cert         | STRING       | The TLS certificate eg. `MIIG1DCCBb...` </br> :warning: only present in HTTPS tables |
| received_tls_cert_common_name | STRING   | Common name of the TLS certificate `example.com` </br> :warning: only present in HTTPS tables |
| received_tls_cert_issuer  | STRING       | Issuer of the TLS certificate `Verisign` </br> :warning: only present in HTTPS tables |
| received_tls_cert_alternative_names | STRING ARRAY | Alternative names from the TLS certificate `www.example.com` </br> :warning: only present in HTTPS tables |
| received_status           | STRING       | In Echo/Discard, any content received on the wire, eg. `HTTP/1.1 403 Forbidden` </br> In the HTTP/S, the http response status, eg. `301 Moved Permanently` |
| received_headers          | STRING ARRAY | Each HTTP header in the response eg. `Content-Type: text/html` </br> :warning: only present in HTTP/S tables |
| received_body             | STRING       | The HTTP response body </br> eg. `<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD></HTML>` </br> Truncated to 64k. </br> :warning: only present in HTTP/S tables |
|                           |
| **Blockpages**            |
|                           |
| is_known_blockpage        | BOOLEAN      | True if the received page matches a blockpage, False if it matches a known false positive blockpage, None otherwise. |
| page_signature            | STRING      | A string describing the matched page </br> ex: `a_prod_cisco` (a know blockpage) or `x_document_moved` (a known false positive). </br> To see the pattern a signature matches check [blockpage signatures](https://github.com/censoredplanet/censoredplanet-analysis/blob/master/pipeline/metadata/data/blockpage_signatures.json) or [false positive signatures](https://github.com/censoredplanet/censoredplanet-analysis/blob/master/pipeline/metadata/data/false_positive_signatures.json) |
|                           |
| **Analysis**              |
|                           |
| outcome                   | STRING       | What was the [outcome](outcome.md) of the individual measurement |
| matches_template          | BOOLEAN      | Did the individual roundtrip measurement match the control template? |
| no_response_in_measurement_matches_template | BOOLEAN | No roundtrip in the overall measurement matches the template |
| stateful_block            | BOOLEAN      | Was stateful interference detected? |
| controls_failed           | BOOLEAN      | Did all the control measurements connected to this measurement fail? |
|                           |
| **Internal**              |
|                           |
| measurement_id            | STRING       | A uuid which is the same for observations which are part of the same measurement. </br> If there are 5 retries of a scan they will all have the same id. </br> eg. `a08df2fe70d54092916b8df87e330f47` |
| source                    | STRING       | The name of the .tar.gz scan file this row came from. </br> eg. `CP_Quack-discard-2020-08-20-05-58-35` </br> Used internally and for debugging |

We intend to add more columns in the future.

## Original Hyperquack Data Format

The Censored Planet data is stored in .json files with one measurement per line.

Data from before 2021-04-25 is parsed from the [Hyperquack V1 format](https://github.com/censoredplanet/censoredplanet/blob/master/docs/hyperquackv1.rst). Data after that is parsed from the [HyperQuack V2 format](https://github.com/censoredplanet/censoredplanet/blob/master/docs/hyperquackv2.rst). To see the exact dates when the raw data changed check the versions in the [raw data directory](https://censoredplanet.org/data/raw).

[HyperQuack V1 measurements](https://github.com/censoredplanet/censoredplanet/blob/master/docs/hyperquackv1.rst) look like this:

```
{ "Server": "1.1.1.1",
  "Keyword": "example.com",
  "Retries": 4,
  "Results": [
    {
      "Sent": "GET / HTTP/1.1 Host: example.com",
      "Received": "HTTP/1.1 503 Service Unavailable",
      "Success": false,
      "Error": "Incorrect echo response",
      "StartTime": "2020-04-29T07:29:46.139500633-04:00",
      "EndTime": "2020-04-29T07:29:46.490678827-04:00"
    },
    ...
  ],
  "Blocked": true,
  "FailSanity": false,
  "StatefulBlock": false
}
```

[HyperQuack V2 measurements](https://github.com/censoredplanet/censoredplanet/blob/master/docs/hyperquackv2.rst) look like this:

```
{ "vp": "146.112.255.132",
  "location": {
    "country_name": "United States",
    "country_code": "US"
  },
  "service": "echo",
  "test_url": "104.com.tw",
  "response": [
    {
      "matches_template": true,
      "control_url": "control-9f26cf1579e1e31d.com",
      "start_time": "2021-05-30T01:01:03.783547451-04:00",
      "end_time": "2021-05-30T01:01:03.829470254-04:00"
    },
    {
      "matches_template": true,
      "start_time": "2021-05-30T01:01:03.829473355-04:00",
      "end_time": "2021-05-30T01:01:03.855786298-04:00"
    },
  ],
  "anomaly": true,
  "controls_failed": false,
  "stateful_block": false,
  "tag": "2021-05-30T01:01:01"
}
```

## DNS Table Format

The DNS (Satellite) data included the following alternative set of columns. (Many are identical to Hyperquack.)

| Field Name                  | Type         | Contains |
| --------------------------- | ------------ | -------- |
|                             |
| **Measured Domain**         |
|                             |
| domain                      | STRING       | The domain being tested, eg. `example.com` |
| domain_is_control           | STRING       | If the measured domain a control domain? |
| domain_category             | STRING       | The [category](domain_categories.md) of the domain being tested, eg. `Social Networking`, `None` if unknown |
| domain_controls_failed      | BOOLEAN      | Did the other control tests for this domain fail? |
|                             |
| **Time**                    |
|                             |
| date                        | DATE         | Date that an individual measurement was taken |
| start_time                  | TIMESTAMP    | Start time of the individual measurement |
| end_time                    | TIMESTAMP    | End time of the individual measurement |
| retry                       | INTEGER      | Number 0-N (usually not > 4) indicating which retry in the measurement_id set this roundtrip was. Domain control measurements have a retry index of `None`. |
|                             |
| **DNS Resolver**            |
|                             |
| resolver_ip                 | STRING       | The ip address of the resolver being tested, eg. `1.1.1.1` |
| resolver_netblock           | STRING       | Netblock of the IP, eg. `1.1.1.0/24` |
| resolver_name               | STRING       | The domain name of the resolver. ex: 'ns2.tower.com.ar.` |
| resolver_is_trusted         | BOOLEAN      | Whether the resolver is considered a 'trusted' resolver, ie '1.1.1.1', '8.8.8.8', '9.9.9.9' |
| resolver_asn                | INTEGER      | Autonomous system number, eg. `13335` |
| resolver_as_name            | STRING       | Autonomous system short name, eg. `CLOUDFLARENET` |
| resolver_as_full_name       | STRING       | Autonomous system long name, eg. `Cloudflare, Inc.` |
| resolver_as_class           | STRING       | The type of AS eg. `Transit/Access`, `Content` (for CDNs) or `Enterprise` |
| resolver_country            | STRING       | Autonomous system country, eg. `US` |
| resolver_organization       | STRING       | The IP organization, eg. `United Technical Services` |
|                             |
| **DNS Resolver Properties** |
|                             |
| resolver_non_zero_rcode_rate  | FLOAT        | The rate of rcode errors returned by this resolver |
| resolver_private_ip_rate      | FLOAT        | The rate of private-use ips (eg. `10.10.1.1`) returned by this resolver |
| resolver_zero_ip_rate         | FLOAT        | The rate of ip `0.0.0.0` returned by this resolver |
| resolver_connect_error_rate   | FLOAT        | The rate of conection errors returned by this resolver |
| resolver_invalid_cert_rate    | FLOAT        | The rate of invalid certificates returned by IP answers given by this resolver |
|                             |
| **DNS Responses**           |
|                             |
| received_error              | STRING       | Any error recieved from the resolver |
| received_rcode              | INTEGER      | Any [RCode](https://datatracker.ietf.org/doc/html/rfc5395#section-2.3) response recieved from the resolver. In the case of an error this is `-1`. ex: `2` representing `SERVFAIL` |
|                           |
| **Analysis**              | These analysis fields are generally obselete |
|                           |
| success                   | BOOLEAN        | Did the overall test succeed? |
| anomaly                   | BOOLEAN        | Was there an anomaly in the test responses? |
| average_confidence        | FLOAT          | The calculated average confidence in this resolver |
| untagged_controls         | BOOLEAN        | Are the IP controls for this test missing ASN metadata |
| untagged_response         | BOOLEAN        | Are the responses for this test missing ASN metadata? |
| excluded                  | BOOLEAN        | Should the test be excluded from analysis? |
| exclude_reason            | STRING         | The reason for the exclusion from analysis |
| has_type_a                | BOOLEAN        | Does the response contai a Type-A DNS record? |
|                           |
| **Internal**              |
|                           |
| measurement_id            | STRING       | A uuid which is the same for observations which are part of the same measurement. </br> If there are 5 retries of a scan they will all have the same id. </br> eg. `a08df2fe70d54092916b8df87e330f47` |
| source                    | STRING       | The name of the .tar.gz scan file this row came from. </br> eg. `CP_Satellite-2020-08-20-05-58-35` </br> Used internally and for debugging |
|                               |
| **Answers**                   | Each answer represents an IP address answer received from the resolver, and subsequent metadata for that IP. |
|                               |
| answers                       | REPEATED STRUCT |  |
| answers.ip                    | STRING          | IP address recieved from the resolver eg. `1.2.3.4` |
| answers.asn                   | INTEGER         | Autonomous system number for the received iP address eg. `13335` |
| answers.as_name               | STRING          | Name of the autonomous system eg. `CLOUDFLARENET` |
| answers.ip_organization       | STRING          | IP organization of the IP address eg. `United Technical Services` |
| answers.censys_http_body_hash | STRING          | The hash of the HTTP body taken from Censys |
| answers.censys_ip_cert        | STRING          | The IP cert taken from Censys |
|                                               |
| **Matches Control**                           | Whether the metadata of the returned IP matches the expected metadata of a control measurement | 
|                                               |
| answers.matches_control                       | REPEATED RECORD |  |
| answers.matches_control.ip                    | BOOLEAN         | Whether the IP matches an expected control IP |
| answers.matches_control.censys_http_body_hash | BOOLEAN         | Whether the HTTP body hash matches an expected control |
| answers.matches_control.censys_ip_cert        | BOOLEAN         | Whether the Censys IP cert matches a control |
| answers.matches_control.asn                   | BOOLEAN         | Whether the ASN matches an expected control ASN |
| answers.matches_control.as_name               | BOOLEAN         | Whether the AS name matches an expected control AS name |
| answers.match_confidence                      | FLOAT           | Value from 0-1. Confidence that this IP response matches a control measurement |
|                                           |
| **HTTP Request**                          | Metadata from the HTTP request made to the returned IP | 
|                                           |
| answers.http_error                        | STRING          | Any recieved error, eg. `Network Timeout` |
| answers.http_response_status              | STRING          | The HTTP response status, eg. `301 Moved Permanently` |
| answers.http_response_headers             | REPEATED STRING | Each HTTP header in the response eg. `Content-Type: text/html` |
| answers.http_response_body                | STRING          | The HTTP response body </br> eg. `<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD></HTML>` </br> Truncated to 64k. |
| answers.http_analysis_is_known_blockpage  | BOOLEAN         | True if the received page matches a blockpage, False if it matches a known false positive blockpage, None otherwise.  |
| answers.http_analysis_page_signature      | STRING          | A string describing the matched page </br> ex: `a_prod_cisco` (a know blockpage) or `x_document_moved` (a known false positive). </br> To see the pattern a signature matches check [blockpage signatures](https://github.com/censoredplanet/censoredplanet-analysis/blob/master/pipeline/metadata/data/blockpage_signatures.json) or [false positive signatures](https://github.com/censoredplanet/censoredplanet-analysis/blob/master/pipeline/metadata/data/false_positive_signatures.json) |
|                                           |
| **HTTPS Request**                         | Metadata from the HTTPS request made to the returned IP | 
|                                           |
| answers.https_error                       | STRING          | Any recieved error, eg. `TLS error` |
| answers.https_tls_version                 | INTEGER         | The TLS version number eg. `771` (meaning TLS 1.2) |
| answers.https_tls_cipher_suite            | STRING          | The TLS cipher suite number </br> eg. `49199` (meaning TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256) |
| answers.https_tls_cert                    | BYTES           | The TLS certificate eg. `MIIG1DCCBb...` |
| answers.https_tls_cert_common_name        | STRING          | Common name of the TLS certificate eg. `example.com` |
| answers.https_tls_cert_issuer             | STRING          | Issuer of the TLS certificate eg. `Verisign` |
| answers.https_tls_cert_start_date         | TIMESTAMP       | The issue data of the certificate |
| answers.https_tls_cert_end_date           | TIMESTAMP       | The expiration data of the certificate |
| answers.https_tls_cert_alternative_names  | REPEATED STRING | Alternative names from the TLS certificate eg. `www.example.com` |
| answers.https_tls_cert_has_trusted_ca     | BOOLEAN         | Whether the issuing CA was trusted by the [Mozilla root CA list](https://wiki.mozilla.org/CA/Included_Certificates) when the request was made |
| answers.https_tls_cert_matches_domain     | BOOLEAN         | Whether the certificate is valid for the test domain |
| answers.https_response_status             | STRING          | The HTTP response status, eg. `301 Moved Permanently` |
| answers.https_response_headers            | REPEATED STRING | Each HTTP header in the response eg. `Content-Type: text/html` |
| answers.https_response_body               | STRING          | The HTTP response body </br> eg. `<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD></HTML>` </br> Truncated to 64k. |
| answers.https_analysis_is_known_blockpage | BOOLEAN         | True if the received page matches a blockpage, False if it matches a known false positive blockpage, None otherwise.  |
| answers.https_analysis_page_signature     | STRING          | A string describing the matched page </br> ex: `a_prod_cisco` (a know blockpage) or `x_document_moved` (a known false positive). </br> To see the pattern a signature matches check [blockpage signatures](https://github.com/censoredplanet/censoredplanet-analysis/blob/master/pipeline/metadata/data/blockpage_signatures.json) or [false positive signatures](https://github.com/censoredplanet/censoredplanet-analysis/blob/master/pipeline/metadata/data/false_positive_signatures.json) |