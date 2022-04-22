# Reduced Table

The table `firehook-censoredplanet.derived.merged_reduced_scans_vN` (where N is a version number)
contains filtered and pre-aggregated data to be used in the Censored Planet dashboard.

This table is evolving and will change in backwards incompatible ways. When a backwards-incompatible
change happens the version number will increment. Older version of the table will be deleted.

The view `firehook-censoredplanet.derived.merged_reduced_scans` points to the latest version of
the table above. If you would like to deal with backwards-incompatible changes yourself rely on
this view instead of the table directly.

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

| Field Name       | Type    | Contains |
| ---------------- | ------- | -------- |
| date             | DATE    | Date that an individual measurement was taken |
| source           | STRING  | What probe type the measurement came from ("HTTPS", "HTTP", "DISCARD", "ECHO") |
| domain           | STRING  | The domain being tested (eg. `example.com`) or `CONTROL` for control measurements |
| category         | STRING  | The [category](domain_categories.md) of the domain being tested, eg. `Social Networking`, `None` if unknown |
| country_name     | STRING  | The country of the autonomous system, eg. `United States`  |
| network          | STRING  | The Autonomous System long name, eg. `China Telecom` |
| subnetwork       | STRING  | The combination of the AS number and the IP organization. The subnetworks can represent multiple autonomous systems owned by a single organization. They can also show IP space which has been sub-leased from one organization to another. e.g. `AS4812 - Apple technology services (Shanghai) Co., Ltd.` which represents IP space sub-leased from `China Telecom`'s network `AS4812` |
| outcome          | STRING  | What was the [outcome](outcome.md) of the individual measurement eg `read/timeout` |
| count            | INTEGER | How many measurements fit the exact pattern of this row? |
| unexpected_count | INTEGER | Count of measurements with an unexpected outcome |

