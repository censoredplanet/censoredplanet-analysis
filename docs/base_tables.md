# Base Tables

These tables are created using the original censored planet json data, plus some
[additional data sources](../pipeline/metadata/).

## Table names

There is one table for each scan type.

- `firehook-censoredplanet:base.echo_scan`
- `firehook-censoredplanet:base.discard_scan`
- `firehook-censoredplanet:base.http_scan`
- `firehook-censoredplanet:base.https_scan`

## Partitioning and Clustering

The tables are time-partitioned along the `date` field.

The tables are clustered along the `country` and then `asn` fields.

## Table Format

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
| server_organization       | STRING       | The IP organization, eg. `US` |
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
| overall_measurement_does_not_match_template | BOOLEAN | Did any of the roundtrips in the overall measurement match the template? |
| stateful_block            | BOOLEAN      | Was stateful interference detected? |
| controls_failed           | BOOLEAN      | Did all the control measurements connected to this measurement fail? |
|                           |
| **Internal**              |
|                           |
| measurement_id            | STRING       | A uuid which is the same for observations which are part of the same measurement. </br> If there are 5 retries of a scan they will all have the same id. </br> eg. `a08df2fe70d54092916b8df87e330f47` |
| source                    | STRING       | The name of the .tar.gz scan file this row came from. </br> eg. `CP_Quack-discard-2020-08-20-05-58-35` </br> Used internally and for debugging |

We intend to add more columns in the future.

## Original Data Format

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