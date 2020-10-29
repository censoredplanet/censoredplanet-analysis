# Bigquery Tables

## Base Tables

These tables are created using the original censored planet json data, plus some
[additional data sources](../pipeline/metadata/).

#### Table names

There is one table for each scan type.

- `firehook-censoredplanet:echo_results.scan`
- `firehook-censoredplanet:discard_results.scan`
- `firehook-censoredplanet:http_results.scan`
- `firehook-censoredplanet:https_results.scan`

#### Original Data Format

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

#### Table Format

The json data is processed into a flat table format which looks like this.

| Field Name               | Type      | Contains |
| ------------------------ | --------- | -------- |
|                          |
| **Measured Domain**      |
|                          |
| domain                   | STRING    | The domain being tested, eg. `example.com` |
|                          |
| **Vantage Point Server** |
|                          |
| ip                       | STRING    | The ip address of the server being tested, eg. `1.1.1.1` |
| netblock                 | STRING    | Netblock of the IP, eg. `1.1.1.0/24` |
| asn                      | INTEGER   | Autonomous system number, eg. `13335` |
| as_name                  | STRING    | Autonomous system short name, eg. `CLOUDFLARENET` |
| as_full_name             | STRING    | Autonomous system long name, eg. `Cloudflare, Inc.` |
| as_class                 | STRING    | The type of AS eg. `Transit/Access`, `Content` (for CDNs) or `Enterprise` |
| country                  | STRING    | Autonomous system country, eg. `US` |
|                          |
| **Observation**          |
|                          |
| date                     | DATE      | Date that an individual measurement was taken |
| start_time               | TIMESTAMP | Start time of the individual measurement |
| end_time                 | TIMESTAMP | End time of the individual measurement |
| retries                  | INTEGER   | Number of times this scan was retried in a measurement |
| measurement_id           | STRING    | A uuid which is the same for observations which are part of the same measurement. If there are 5 retries of a scan they will all have the same id. eg. `a08df2fe70d54092916b8df87e330f47` |
| sent                     | STRING    | The content sent over the wire, eg. `GET / HTTP/1.1 Host: example.com` |
| received                 | STRING    | Any content received on the wire, eg. `503 Service Unavailable` </br> :warning:  In the HTTP/HTTPS datasets this field contains more json, but currently is it flattened into a string |
| error                    | STRING    | Any error, eg. `Network Timeout` |
|                          |
| **Analysis**             |
|                          |
| success                  | BOOLEAN   | Did the individual roundtrip measurement succeed? |
| blocked                  | BOOLEAN   | Was interference detected in the overall measurement? |
| fail_sanity              | BOOLEAN   | Was the ip being tested malfunctioning/down? |
| stateful_block           | BOOLEAN   | Was stateful interference detected? |
|                          |
| **Internal**             |
|                          |
| source                   | STRING    | The name of the .tar.gz scan file this row came from. eg. `CP_Quack-discard-2020-08-20-05-58-35` Used internally and for debugging |

We intend to add more columns in the future.

## Derived Tables

These tables are created from the base tables. They drop some data but also do
some common pre-processing and are partitioned, making them faster, easier and
less expensive to use.

### Merged Table

This table contains data from all 4 scan types together in one place.

This table is created by the script
[merged_scans.sql](../table/queries/merged_scans.sql).

#### Table Name

- `firehook-censoredplanet.merged_results.cleaned_error_scans`

#### Table format

| Field Name | Type    | Contains |
| ---------- | ------- | -------- |
| date       | DATE    | Date that an individual measurement was taken |
| domain     | STRING  | The domain being tested, eg. `example.com` |
| country    | STRING  | Autonomous system country, eg. `US`  |
| asn        | INTEGER | Autonomous system number, eg. `13335` |
| as_name    | STRING  | Autonomous system short name, eg. `CLOUDFLARENET` |
| ip         | STRING  | The ip address of the server being tested, eg. `1.1.1.1` |
| netblock   | STRING  | Netblock of the IP, eg. `1.1.1.0/24` |
| as_class   | STRING  | The type of AS eg. `Transit/Access`, `Content` (for CDNs) or `Enterprise`  |
| source     | STRING  | The type of measurement, one of `ECHO`, `DISCARD`, `HTTP`, `HTTPS` |
| result     | STRING  | The source type, followed by the `: null` (meaning success) or error returned. eg. `ECHO: null`, `HTTPS: Incorrect web response: status lines don't match`, `HTTP: Get http://[IP]: net/http: request canceled (Client.Timeout exceeded while awaiting headers)` `DISCARD: Received response` |
| count      | INTEGER | How many measurements fit the exact pattern of this row? |

### Reduced Table

These two tables are intended to be used together, they contain scan information
in one table `reduced_scans` and AS information in another `net_as`.

To use these tables together join on the `date` and `netblock` fields.

These tables contain only HTTPS scan data.

These tables are created by the script
[https_reduced_scans.sql](../table/queries/https_reduced_scans.sql).

#### Table names

- `firehook-censoredplanet.https.reduced_scans`
- `firehook-censoredplanet.https.net_as`

#### Table Formats

Reduced Scans

| Field Name | Type    | Contains |
| ---------- | ------- | -------- |
| date       | DATE    | Date that an individual measurement was taken |
| domain     | STRING  | The domain being tested, eg. `example.com` |
| country    | STRING  | Autonomous system country, eg. `US`  |
| netblock   | STRING  | Netblock of the IP, eg. `1.1.1.0/24`  |
| result     | STRING  | The source type, followed by the `: null` (meaning success) or error returned. eg. `HTTPS: null`, `HTTPS: Incorrect web response: status lines don't match` |
| count      | INTEGER | How many measurements fit the exact pattern of this row? |

Net AS

| Field Name   | Type    | Contains |
| ------------ | ------- | -------- |
| date         | DATE    | Date that an individual measurement was taken |
| netblock     | STRING  | Netblock of the IP, eg. `1.1.1.0/24` |
| asn          | INTEGER | Autonomous system number, eg. `13335` |
| as_full_name | STRING  | Autonomous system long name, eg. `Cloudflare, Inc.` |
