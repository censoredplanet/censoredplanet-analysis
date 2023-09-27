## Outcome Classification

The `outcome` field classifies the result of a test into an enumeration of the different types of high-level outcomes. Outcome strings are of the format `stage/outcome`. For example `❗️read/timeout` means the test received a TCP timeout during the read stage. `✅match` means a test finished successfully.

### Stages

Stages are listed here in order. If a test reaches a later stage like `❗️content` then it successfully passed the earlier stages like `❗️dial` and `❗️read`.

Stages include emoji to make them easier to differentiate at a glance.

| Stage         | Explanation |
| ------------- | ----------- |
| ❔setup       | The initial setup phase for the test (mustering resources, opening ports, etc.) |
| ❗️dial        | The initial TCP dial connection to the remote |
| ❗️tls         | The TLS handshake. `HTTPS` only. The [SNI header](https://en.wikipedia.org/wiki/Server_Name_Indication) containing the test domain is sent in this stage |
| ❗️write       | Writing to the remote. For non-`HTTPS` tests this is where the domain is sent |
| ❗️read        | Reading from the remote |
| ❗️http        | Verification of HTTP headers. `HTTP/S` only |
| ❗️content     | Verification that the returned content matches the expected content. These are the most common types of errors and represent things like blockpages |
| ✅            | Verified an expected response |
| ❔unknown     | Unknown stage. Usually these are new outcomes which should be investigated and classified |

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

##### DNS

![dns connection diagram](diagrams/dns.svg)

### Outcome Classes

Basic outcomes represent simplest types of errors, as well as the `match` case (no error detected).

Protocol errors are similar but not identical to the Network Error Logging standard's [Predefined Network Error Types](https://www.w3.org/TR/network-error-logging/#predefined-network-error-types). These errors may represent normal network failures and noise, but they may also expose interference by a middlebox to disrupt a connection. For example China and Iran are both known to use [TCP Reset Attacks](https://en.wikipedia.org/wiki/TCP_reset_attack) as a censorship method.

Mismatch Errors are used when the connection is successful, but the content received does not match the content expected. This can happen in the case of blockpages, remote servers with unusual behavior, or complicated CDN networks serving many sites.

| Outcome Class           | Additional Outcome Information Included | Explanation |
| ----------------------- | --------------------------------------- | ----------- |
|                         |
| **Basic Outcomes**      |
|                         |
| match                   |  | The test completed successfully and no interference was detected |
| system_failure          |  | There was a test system failure, rendering the test invalid |
| unknown                 |  | The class of the outcome was not known. Usually these are new errors which should be investigated and classified |
|                         |
| **Protocol Errors**     |  | There were errors in the connection protocol |
|                         |
| ip.network_unreachable  |  | The network was unreachable |
| ip.host_no_route        |  | No route to the host could be found |
| timeout                 |  | The connection timed out. Could indicate packets being dropped by a middlebox |
| tcp.refused             |  | The TCP connection was refused by the server |
| tcp.reset               |  | The TCP connection was reset. Could indicate an inserted `RST` packet |
| tls.failed              |  | The TLS connection failed, usually due to a TLS protocol error |
| http.invalid            |  | The HTTP response could not be parsed. Possibly because the response has a content-length mismatch, has improper encoding, or other conditions |
| http.empty              |  | Received no content when HTTP content was expected |
| http.truncated_response |  | The HTTP response content was unexpectedly truncated |
|                         |
| **Mismatched Content**  |  | The connection completed successfully, but the content returned didn't match the content expected for the domain. |
|                         |
| mismatch                |  | Received a different response from the one expected. </br> For Discard no response is expected and any response is a mismatch, </br> for Echo a mirrored response is expected and anything else is a mismatch. </br> For HTTP/S the expected response is determined by sending multiple control domains to the server and building an expected template. This response is returned when more detail about the exact part of the template mismatched (eg. status, body) is not available. |
| status_mismatch         | `:http_status_code` eg. `:301` | The HTTP status code didn't match, eg. `403` instead of `200` |
| body_mismatch           |  | The HTTP body didn't match, potentially a blockpage |
| tls_mismatch            |  | An element of the TLS connection (certificate, cipher suite, or TLS version) didn't match |
| blockpage               | `:<blockpage_id>` eg. `:b_nat_ir_national_1` | The response was unexpected and matched a [known blockpage](https://github.com/censoredplanet/censoredplanet-analysis/blob/master/pipeline/metadata/data/blockpage_signatures.json) |
| trusted_host            | `:<trusted_host_name>` eg. `:akamai` | The response didn't match the expected response for the template. But it did match a common known server pattern, and is likely not censorship. This outcome is used for CDNs that respond in network-specific ways to domains they host. |

## DNS Outcomes

The Satellite data uses its own unique set of outcomes, and does not use stages. The outcomes are based on DNS errors and POSIX TCP/IP socket return codes.

| Outcome                | Additional Outcome Information Included | Explanation |
| ---------------------- | --------------------------------------- | ----------- |
|                        |
| **DNS Failures**       |  | The DNS request failed |
|                        |
| ❗️dns.error            | `:<rcode_error_string>` eg. `:NXDomain` | The DNS request returned an [RCode](https://datatracker.ietf.org/doc/html/rfc5395#section-2.3) error |
| ❗️dns.connrefused      |  | The DNS request was refused |
| ❗️dns.timedout         |  | The DNS request timed out |
| ❗️dns.msgsize          |  | The DNS request returned a message size error |
| ❗️dns.protocol_error   |  | The DNS request failed with a protocol error |
| ❔dns.hostunreach      |  | The DNS resolver was unreachable |
|                        |
| **DNS Response Analysis** |  | The DNS request responded successfully. Outcome is based on the returned content/IP addresses |
|                        |
| ✅ip.matchip           |  | The DNS request returned an expected (matching) IP address for the domain |
| ✅ip.matchasn          |  | The DNS request returned an IP address matching the ASN of an expected IP address |
| ❗️ip.invalid           | One of `:zero`, `:local_host`, `:local_net` | The DNS request returned an IP that could never be valid. eg. `0.0.0.0`, `127.0.0.1`, `10.10.0.0`, `172.16.0.0` |
| ❗️ip.empty             |  | The DNS request returned an empty response |
|                        |
| **HTTP/S Response Analysis**   |  | The DNS request successfully returned IP addresses. Analysis is based on follow-up HTTP/S requests sent to those IP addresses requesting the test domain |
|                        |
| ❗️tls.connerror        | `:<returned_ip_autonomous_system_name>` eg. `:ERTELECOM_DS_AS` </br> or `:AS<as_number>` eg. `:AS15169` </br> or `:missing_as_info` | Attempting to connect to all returned IP addresses over HTTPS failed |
| ✅tls.validcert        |  | An HTTPS connection to a returned IP address returned a valid certificate which matched the expected test domain |
| ❗️tls.baddomain        | `:<certificate_common_domain_name>` eg. `:dnsfilter.net` | An HTTPS connection to a returned IP returned a certificate for an unexpected domain. Could indicate a MITM attempt |
| ❗️tls.badca            | `:<certificate_authority_issuer_name>` eg. `:Fortiguard SDNS Blocked Page` | An HTTPS connection to a returned IP returned an invalid certificate. Could indicate a MITM attempt |
| ❗️http.blockpage       | `:<blockpage_id>` eg. `:f_gen_id_1_satellite` | An HTTPS request to the returned IP address failed, but an HTTP request returned a [known blockpage](https://github.com/censoredplanet/censoredplanet-analysis/blob/master/pipeline/metadata/data/blockpage_signatures.json) |
|                        |
| **Other Errors**       |
|                        |
| ❔setup.system_failure |  | There was a test system failure, rendering the test invalid |
| ❗️unknown_error        |  | An unknown error occured |