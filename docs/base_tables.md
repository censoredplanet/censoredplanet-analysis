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

## Original Data Format

The Censored Planet data is stored in .json files with one measurement per line.
The measurements look like this:

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

## Table Format

The json data is processed into a flat table format which looks like this.

| Field Name                | Type         | Contains |
| ------------------------- | ------------ | -------- |
|                           |
| **Measured Domain**       |
|                           |
| domain                    | STRING       | The domain being tested, eg. `example.com` |
|                           |
| **Vantage Point Server**  |
|                           |
| ip                        | STRING       | The ip address of the server being tested, eg. `1.1.1.1` |
| netblock                  | STRING       | Netblock of the IP, eg. `1.1.1.0/24` |
| asn                       | INTEGER      | Autonomous system number, eg. `13335` |
| as_name                   | STRING       | Autonomous system short name, eg. `CLOUDFLARENET` |
| as_full_name              | STRING       | Autonomous system long name, eg. `Cloudflare, Inc.` |
| as_class                  | STRING       | The type of AS eg. `Transit/Access`, `Content` (for CDNs) or `Enterprise` |
| country                   | STRING       | Autonomous system country, eg. `US` |
|                           |
| **Observation**           |
|                           |
| date                      | DATE         | Date that an individual measurement was taken |
| start_time                | TIMESTAMP    | Start time of the individual measurement |
| end_time                  | TIMESTAMP    | End time of the individual measurement |
| retries                   | INTEGER      | Number of times this scan was retried in a measurement |
| measurement_id            | STRING       | A uuid which is the same for observations which are part of the same measurement. </br> If there are 5 retries of a scan they will all have the same id. </br> eg. `a08df2fe70d54092916b8df87e330f47` |
| sent                      | STRING       | The content sent over the wire, eg. `GET / HTTP/1.1 Host: example.com` |
| error                     | STRING       | Any error, eg. `Network Timeout` |
|                           |
| **Received Fields**       |              | :warning: These fields differ between scan types |
|                           |
| received_status           | STRING       | In Echo/Discard, any content received on the wire, eg. `HTTP/1.1 403 Forbidden` </br> In the HTTP/S, the http response status, eg. `301 Moved Permanently` |
| received_body             | STRING       | The HTTP response body </br> eg. `<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD></HTML>` </br> :warning: only present in HTTP/S tables |
| received_headers          | STRING ARRAY | Each HTTP header in the response eg. `Content-Type: text/html` </br> :warning: only present in HTTP/S tables |
| received_tls_version      | INTEGER      | The TLS version number eg. `771` (meaning TLS 1.2) </br> :warning: only present in HTTPS tables |
| received_tls_cipher_suite | INTEGER      | The TLS cipher suite number </br> eg. `49199` (meaning TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256) </br> :warning: only present in HTTPS tables |
| received_tls_cert         | STRING       | The TLS certificate eg. `MIIG1DCCBb...` (truncated) </br> :warning: only present in HTTPS tables |
|                           |
| **Blockpages**            |
|                           |
| blockpage                 | BOOLEAN      | True if the received page matches a blockpage, False if it matches a known false positive blockpage, None otherwise. |
| page_signature            | STRING      | A string describing the matched page </br> ex: `a_prod_cisco` (a know blockpage) or `x_document_moved` (a known false positive). </br> To see the pattern a signature matches check [blockpage signatures](https://github.com/censoredplanet/censoredplanet-analysis/blob/master/pipeline/metadata/data/blockpage_signatures.json) or [false positive signatures](https://github.com/censoredplanet/censoredplanet-analysis/blob/master/pipeline/metadata/data/false_positive_signatures.json) |
|                           |
| **Analysis**              |
|                           |
| success                   | BOOLEAN      | Did the individual roundtrip measurement succeed? |
| blocked                   | BOOLEAN      | Was interference detected in the overall measurement? |
| fail_sanity               | BOOLEAN      | Was the ip being tested malfunctioning/down? |
| stateful_block            | BOOLEAN      | Was stateful interference detected? |
|                           |
| **Internal**              |
|                           |
| source                    | STRING       | The name of the .tar.gz scan file this row came from. </br> eg. `CP_Quack-discard-2020-08-20-05-58-35` </br> Used internally and for debugging |

We intend to add more columns in the future.