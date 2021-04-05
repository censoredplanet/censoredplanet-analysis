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

CREATE TEMP FUNCTION CleanError(error STRING) AS (
  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    IF(error = "", "null", IFNULL(error, "null")),
    "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+", "[IP]"),
    "\\[IP\\]:[0-9]+", "[IP]:[PORT]"),
    "length [0-9]+", "length [LENGTH]"),
    "port\\.[0-9]+", "port.[PORT]")
);

CREATE OR REPLACE TABLE `firehook-censoredplanet.derived.merged_net_as`
PARTITION BY date
CLUSTER BY netblock
AS (
  SELECT DISTINCT
    date,
    netblock,
    asn,
    as_full_name AS as_full_name
  FROM (
    SELECT * FROM `firehook-censoredplanet.base.discard_scan` UNION ALL
    SELECT * FROM `firehook-censoredplanet.base.echo_scan` UNION ALL
    SELECT * FROM `firehook-censoredplanet.base.http_scan` UNION ALL
    SELECT * FROM `firehook-censoredplanet.base.https_scan`
  )
);

CREATE OR REPLACE TABLE `firehook-censoredplanet.derived.merged_reduced_scans_no_as`
PARTITION BY date
CLUSTER BY source, country, domain, netblock
AS (
WITH
  AllScans AS (
  SELECT
    date,
    if(sent != "", TRIM(REGEXP_EXTRACT(sent, "Host: (.*)")), domain) as domain,
    "DISCARD" AS source,
    country,
    netblock,
    CleanError(error) AS result,
    count(1) AS count
  FROM `firehook-censoredplanet.base.discard_scan`
  WHERE
    error IS NULL OR
    NOT SAFE.REGEXP_CONTAINS(error, "too many open files|address already in use|no route to host|connection refused|connect: connection timed out")
  GROUP BY date, source, country, domain, netblock, result

  UNION ALL
  SELECT
    date,
    if(sent != "", TRIM(REGEXP_EXTRACT(sent, "Host: (.*)")), domain) as domain,
    "ECHO" AS source,
    country,
    netblock,
    CleanError(error) AS result,
    count(1) AS count
  FROM `firehook-censoredplanet.base.echo_scan`
  WHERE
    error IS NULL OR
    NOT SAFE.REGEXP_CONTAINS(error, "too many open files|address already in use|no route to host|connection refused|connect: connection timed out")
  GROUP BY date, source, country, domain, netblock, result

  UNION ALL
  SELECT
    date,
    IF(domain != sent AND sent != "", sent, domain) AS domain,
    "HTTP" AS source,
    country,
    netblock,
    CleanError(error) AS result,
    count(1) AS count
  FROM `firehook-censoredplanet.base.http_scan`
  WHERE
    error IS NULL OR
    NOT SAFE.REGEXP_CONTAINS(error, "too many open files|address already in use|no route to host|connection refused|connect: connection timed out")
  GROUP BY date, source, country, domain, netblock, result

  UNION ALL
  SELECT
    date,
    IF(domain != sent AND sent != "", sent, domain) AS domain,
    "HTTPS" AS source,
    country,
    netblock,
    CleanError(error) AS result,
    count(1) AS count
  FROM `firehook-censoredplanet.base.https_scan`
  WHERE
    error IS NULL OR
    NOT SAFE.REGEXP_CONTAINS(error, "too many open files|address already in use|no route to host|connection refused|connect: connection timed out")
  GROUP BY date, source, country, domain, netblock, result
)
SELECT *
FROM AllScans
);

# Drop the temp function before creating the view
# Since any temp functions in scope block view creation.
DROP FUNCTION CleanError;

CREATE OR REPLACE VIEW `firehook-censoredplanet.derived.merged_reduced_scans`
OPTIONS(
  friendly_name="Reduced Scan View",
  description="A join of reduced scans with ASN info."
)
AS (
  SELECT
    date,
    domain,
    source,
    country,
    netblock,
    asn,
    as_full_name AS as_name,
    result,
    count
  FROM `firehook-censoredplanet.derived.merged_reduced_scans_no_as`
  LEFT JOIN `firehook-censoredplanet.derived.merged_net_as`
  USING (date, netblock)
);
