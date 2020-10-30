# Copyright 2020 Google LLC
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

# This must be defined function and not a temp function
# because it is disallowed to have a view creation after making a temp function
# we don't actually want this function permenantly
# so it is dropped at the end of this script.
CREATE OR REPLACE FUNCTION `firehook-censoredplanet`.https_results.CleanErrorTemp(error STRING) AS (
  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    IF(error = "", "null", IFNULL(error, "null")),
    "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+", "[IP]"),
    "\\[IP\\]:[0-9]+", "[IP]:[PORT]"),
    "length [0-9]+", "length [LENGTH]"),
    "port\\.[0-9]+", "port.[PORT]")
);

CREATE OR REPLACE TABLE `firehook-censoredplanet.https_results.net_as`
PARTITION BY date
CLUSTER BY netblock
AS (
  SELECT DISTINCT
    date,
    netblock,
    asn,
    as_full_name AS as_full_name
  FROM `firehook-censoredplanet.https_results.scan`
);

CREATE OR REPLACE TABLE `firehook-censoredplanet.https_results.reduced_scans`
PARTITION BY date
CLUSTER BY country, domain, netblock
AS (
  SELECT
    date,
    IF(domain != sent AND sent != "", sent, domain) AS domain,
    country,
    netblock,
    `firehook-censoredplanet`.https_results.CleanErrorTemp(error) AS result,
    count(1) AS count
  FROM `firehook-censoredplanet.https_results.scan`
  WHERE
    NOT SAFE.REGEXP_CONTAINS(error, "too many open files|address already in use|no route to host|connection refused|connect: connection timed out")
  GROUP BY date, country, domain, netblock, result
);

CREATE OR REPLACE VIEW `firehook-censoredplanet.https_results.reduced_scans_geolocated`
OPTIONS(
  friendly_name="Reduced Scan View",
  description="a join of reduced scans with ASN info"
)
AS (
  SELECT
    date,
    domain,
    country,
    netblock,
    asn,
    as_full_name AS as_name,
    result,
    count
  FROM `firehook-censoredplanet.https_results.reduced_scans`
  LEFT JOIN `firehook-censoredplanet.https_results.net_as`
  USING (date, netblock)
);

DROP FUNCTION `firehook-censoredplanet`.https_results.CleanErrorTemp;
