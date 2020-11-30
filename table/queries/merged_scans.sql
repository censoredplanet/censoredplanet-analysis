# Copyright 2020 Jigsaw Operations LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CREATE TEMP FUNCTION CleanError(error string) AS (
  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    IF(error = "", "null", IFNULL(error, "null")),
    "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+", "[IP]"),
    "\\[IP\\]:[0-9]+", "[IP]:[PORT]"),
    "length [0-9]+", "length [LENGTH]"),
    "port\\.[0-9]+", "port.[PORT]")
);

CREATE OR REPLACE TABLE `firehook-censoredplanet.derived.merged_error_scans`
PARTITION BY date
CLUSTER BY source, country, domain, result
as (
WITH
  AllScans AS (
  SELECT
    date,
    if(sent != "", TRIM(REGEXP_EXTRACT(sent, "Host: (.*)")), domain) as domain,
    ip,
    "DISCARD" AS source,
    error,
    country,
    asn,
    as_full_name,
    netblock,
    as_class
  FROM `firehook-censoredplanet.base.discard_scan`
  UNION ALL
  SELECT
    date,
    if(sent != "", TRIM(REGEXP_EXTRACT(sent, "Host: (.*)")), domain) as domain,
    ip,
    "ECHO" AS source,
    error,
    country,
    asn,
    as_full_name,
    netblock,
    as_class
  FROM `firehook-censoredplanet.base.echo_scan`
  UNION ALL
  SELECT
    date,
    IF(domain != sent AND sent != "", sent, domain) AS domain,
    ip,
    "HTTP" AS source,
    error,
    country,
    asn,
    as_full_name,
    netblock,
    as_class
  FROM `firehook-censoredplanet.base.http_scan`
  UNION ALL
  SELECT
    date,
    IF(domain != sent AND sent != "", sent, domain) AS domain,
    ip,
    "HTTPS" AS source,
    error,
    country,
    asn,
    as_full_name,
    netblock,
    as_class
  FROM `firehook-censoredplanet.base.https_scan`
)
SELECT
  date,
  domain,
  country,
  asn,
  as_full_name AS as_name,
  ip,
  netblock,
  as_class,
  source,
  CONCAT(source, ": ", CleanError(error)) AS result,
  COUNT(1) AS count
FROM
  AllScans
GROUP BY
  date,
  domain,
  country,
  asn,
  as_name,
  ip,
  netblock,
  as_class,
  source,
  result
);
