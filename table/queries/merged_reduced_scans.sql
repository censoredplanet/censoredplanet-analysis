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

CREATE TEMP FUNCTION AddOutcomeEmoji(outcome STRING) AS (
  CASE
    WHEN STARTS_WITH(outcome, "setup/") THEN CONCAT("❔", outcome)
    WHEN STARTS_WITH(outcome, "unknown/") THEN CONCAT("❔", outcome)
    WHEN STARTS_WITH(outcome, "expected/") THEN CONCAT("✅", SUBSTR(outcome, 10))
    ELSE CONCAT("❗️", outcome)
  END
);

# BASE_DATASET and DERIVED_DATASET are reserved dataset placeholder names
# which will be replaced when running the query

# Increment the version of this table if you change the table in a backwards-incomatible way.

# Rely on the table name firehook-censoredplanet.derived.merged_reduced_scans_vN
# if you would like to see a clear breakage when there's a backwards-incompatible change.
# Old table versions will be deleted.
CREATE OR REPLACE TABLE `PROJECT_NAME.DERIVED_DATASET.merged_reduced_scans_with_mids`
PARTITION BY date
# Columns `source` and `country_name` are always used for filtering and must come first.
# `network` and `domain` are useful for filtering and grouping.
CLUSTER BY source, country_name, network, a_domain
OPTIONS (
  friendly_name="Reduced Scans",
  description="Filtered and pre-aggregated table of scans to use with the Censored Planed Dashboard"
)
AS (
WITH AllScans AS (
  SELECT * EXCEPT (source), "DISCARD" AS source
  FROM `PROJECT_NAME.BASE_DATASET.discard_scan`
  UNION ALL
  SELECT * EXCEPT (source), "ECHO" AS source
  FROM `PROJECT_NAME.BASE_DATASET.echo_scan`
  UNION ALL
  SELECT * EXCEPT (source), "HTTP" AS source
  FROM `PROJECT_NAME.BASE_DATASET.http_scan`
  UNION ALL
  SELECT * EXCEPT (source), "HTTPS" AS source
  FROM `PROJECT_NAME.BASE_DATASET.https_scan`
), Grouped AS (
    SELECT
        date,
        source,
        server_country AS country_code,
        server_as_full_name AS network,
        IF(domain_is_control, "CONTROL", domain) AS a_domain,
        AddOutcomeEmoji(outcome) AS outcome,
        CONCAT("AS", server_asn, IF(server_organization IS NOT NULL, CONCAT(" - ", server_organization), "")) AS subnetwork,
        IFNULL(domain_category, "Uncategorized") AS category,
        IF(domain = 'example.com' OR domain_is_control,
           ARRAY<STRING>[],
           ARRAY_AGG(DISTINCT measurement_id IGNORE NULLS)) as measurement_ids,
        COUNT(*) AS count
    FROM AllScans
    # Filter on controls_failed to potentially reduce the number of output rows (less dimensions to group by).
    WHERE NOT controls_failed
    GROUP BY date, source, country_code, network, outcome, domain_is_control, domain, category, subnetwork
    # Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
    HAVING NOT STARTS_WITH(outcome, "❔setup/")
)
SELECT
    Grouped.* EXCEPT (country_code, measurement_ids),
    IFNULL(country_name, country_code) AS country_name,
    CONCAT(
      "https://datastudio.google.com/c/reporting/3f783159-f9ed-4a60-81c8-26ecba7bd3bf/page/uUfAD?params=%7B%22df2%22:%22include%25EE%2580%25800%25EE%2580%2580IN%25EE%2580%2580",
      ARRAY_TO_STRING(measurement_ids, "%25EE%2580%2580"),
      "%22%7D") as measurement_id_url,
    CASE
        WHEN (STARTS_WITH(outcome, "✅")) THEN 0
        WHEN (STARTS_WITH(outcome, "❗️")) THEN count
        WHEN (STARTS_WITH(outcome, "❔")) THEN NULL
    END AS unexpected_count
    FROM Grouped
    LEFT JOIN `PROJECT_NAME.metadata.country_names` USING (country_code)
    WHERE country_code IS NOT NULL
);

# Drop the temp function before creating the view
# Since any temp functions in scope block view creation.
DROP FUNCTION AddOutcomeEmoji;
