# Merged Table

This table contains data from all 4 scan types together in one place.

This table is created by the script
[merged_scans.sql](../table/queries/merged_scans.sql).

## Table Name

- `firehook-censoredplanet.derived.merged_error_scans`

## Partitioning and Clustering

The tables are time-partitioned along the `date` field.

The tables are clustered along the `source`, `country`, `domain`, and then
`result` fields.

## Table format

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
