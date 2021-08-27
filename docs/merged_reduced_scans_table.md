# Reduced Table

The table `firehook-censoredplanet.derived.merged_reduced_scans` contains filtered and pre-aggregated
data to be used in the Censored Planet dashboard.

The table is created by the script
[merged_reduced_scans.sql](../table/queries/merged_reduced_scans.sql).

## Partitioning and Clustering

The table is [time-partitioned](https://cloud.google.com/bigquery/docs/partitioned-tables) along the `date` field.
This allows queries to skip any data outside the desired date range.

The table also uses [clustering](https://cloud.google.com/bigquery/docs/clustered-tables) to make filtering and aggregation
more efficient. The table is clustered along the `source`, `country_name`, `network` and `domain` columns.
The columns `source` and `country_name` are always used for filtering, so they come first.

## Table Format

Reduced Scans

| Field Name | Type    | Contains |
| ---------- | ------- | -------- |
| date       | DATE    | Date that an individual measurement was taken |
| source       | STRING    | What probe type the measurement came from ("HTTPS", "HTTP", "DISCARD", "ECHO") |
| domain     | STRING  | The domain being tested (eg. `example.com`) or `CONTROL` for control measurements |
| category   | STRING  | The [category](domain_categories.md) of the domain being tested, eg. `Social Networking`, `None` if unknown |
| country_name    | STRING  | The country of the autonomous system, eg. `US`  |
| network        | STRING | The Autonomous System long name, eg. `Cloudflare, Inc.` |
| subnetwork | STRING | The combinarion of the AS number and the IP organization, e.g. `AS6697 - Reliable Software, Ltd` |
| outcome    | STRING  | An outcome classification, explained below. eg `read/timeout` |
| count      | INTEGER | How many measurements fit the exact pattern of this row? |
| unexpected_count      | INTEGER | Count of measurements with an unexpected outcome |

## Outcome Classification

The `outcome` field classifies the result of a test into an enumeration of the different types of high-level outcomes. Outcome strings are of the format `stage/outcome`. For example `read/timeout` means the test received a TCP timeout during the read stage. `complete/success` means a test finished successfully.

### Stages

Stages are listed here in order. If a test reaches a later stage like `content` then it successfully passed the earlier stages like `dial` and `read`.

| Stage       | Explanation |
| ----------- | ----------- |
| setup       | The initial setup phase for the test (mustering resources, opening ports, etc.) |
| dial        | The initial TCP dial connection to the remote |
| tls         | The TLS handshake. `HTTPS` only. The [SNI header](https://en.wikipedia.org/wiki/Server_Name_Indication) containing the test domain is sent in this stage |
| write       | Writing to the remote. For non-`HTTPS` tests this is where the domain is sent |
| read        | Reading from the remote |
| http        | Verification of HTTP headers. `HTTP/S` only |
| content     | Verification that the returned content matches the expected content. These are the most common types of errors and represent things like blockpages |
| complete    | Reaching the final stage without encountering any problems in the previous stages |
| unknown     | Unknown stage. Usually these are new outcomes which should be investigated and classified |

#### Stages per Probe

Not all tests include every stage depending on the type of test. For example since the Echo protocol does not involve TLS Echo tests will never fail with outcomes classified as `tls`. Here are the stages for each test type.

##### Discard

![discard connection diagram](diagrams/discard.svg)

##### Echo

![echo connection diagram](diagrams/echo.svg)

##### HTTP

![http connection diagram](diagrams/http.svg)

##### HTTPS

![https connection diagram](diagrams/https.svg)

### Outcome Classes

Basic outcomes represent simplest types of errors, as well as the `success` case (no error detected).

Protocol errors are similar but not identical to the Network Error Logging standard's [Predefined Network Error Types](https://www.w3.org/TR/network-error-logging/#predefined-network-error-types). These errors may represent normal network failures and noise, but they may also expose interference by a middlebox to disrupt a connection. For example China and Iran are both known to use [TCP Reset Attacks](https://en.wikipedia.org/wiki/TCP_reset_attack) as a censorship method.

Mismatch Errors are used when the connection is successful, but the content received does not match the content expected. This can happen in the case of blockpages, remote servers with unusual behavior, or complicated CDN networks serving many sites.

| Outcome Class           | Explanation |
| ----------------------- | ----------- |
|                         |
| **Basic Outcomes**      |
|                         |
| success                 | The test completed successfully and no interference was detected |
| system_failure          | There was a test system failure, rendering the test invalid |
| unknown                 | The class of the outcome was not known. Usually these are new errors which should be investigated and classified |
|                         |
| **Protocol Errors**     | There were errors in the connection protocol |
|                         |
| ip.network_unreachable  | The network was unreachable |
| ip.host_no_route        | No route to the host could be found |
| timeout                 | The connection timed out. Could indicate packets being dropped by a middlebox |
| tcp.refused             | The TCP connection was refused by the server |
| tcp.reset               | The TCP connection was reset. Could indicate an inserted `RST` packet |
| tls.failed              | The TLS connection failed, usually due to a TLS protocol error |
| http.invalid            | The HTTP response could not be parsed. Possibly because the response has a content-length mismatch, has improper encoding, or other conditions |
| http.empty              | Received no content when HTTP content was expected |
| http.truncated_response | The HTTP response content was unexpectedly truncated |
|                         |
| **Mismatch Errors**     | The connection completed successfully, but the content returned didn't match the content expected for the domain. |
|                         |
| HyperQuack v1 only      |
| response_mismatch       | Received a different response from the one expected. </br> For Discard no response is expected and any response is a mismatch, </br> for Echo a mirrored response is expected and anything else is a mismatch. |
| status_mismatch         | The HTTP status code didn't match, eg. `403` instead of `200` |
| body_mismatch           | The HTTP body didn't match, potentially a blockpage |
| tls_mismatch            | An element of the TLS connection (certificate, cipher suite, or TLS version) didn't match |
| HyperQuack v1 and v2    |
| template_mismatch       | Some element of the response did not match the expected template |
| blockpage               | The response was unexpected and matched a [known blockpage]((https://github.com/censoredplanet/censoredplanet-analysis/blob/master/pipeline/metadata/data/blockpage_signatures.json)) |
