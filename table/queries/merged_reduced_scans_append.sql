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
    WHEN STARTS_WITH(outcome, "dial/") THEN CONCAT("❔", outcome)
    WHEN STARTS_WITH(outcome, "read/system") THEN CONCAT("❔", outcome)
    WHEN STARTS_WITH(outcome, "expected/") THEN CONCAT("✅", SUBSTR(outcome, 10))
    WHEN STARTS_WITH(outcome, "content/blockpage") THEN CONCAT("❗️", outcome)
    WHEN STARTS_WITH(outcome, "read/") THEN CONCAT("❗️", outcome)
    WHEN STARTS_WITH(outcome, "write/") THEN CONCAT("❗️", outcome)
    ELSE CONCAT("❓", outcome)
  END
);

# BASE_DATASET and DERIVED_DATASET are reserved dataset placeholder names
# which will be replaced when running the query

INSERT INTO `PROJECT_NAME.DERIVED_DATASET.merged_reduced_scans_v2`
(
WITH AllScans AS (
  SELECT * EXCEPT (source), "DISCARD" AS source
  FROM `PROJECT_NAME.BASE_DATASET.discard_scan`
  WHERE date >= 'EARLIEST_DATE'
  UNION ALL
  SELECT * EXCEPT (source), "ECHO" AS source
  FROM `PROJECT_NAME.BASE_DATASET.echo_scan`
  WHERE date >= 'EARLIEST_DATE'
  UNION ALL
  SELECT * EXCEPT (source), "HTTP" AS source
  FROM `PROJECT_NAME.BASE_DATASET.http_scan`
  WHERE date >= 'EARLIEST_DATE'
  UNION ALL
  SELECT * EXCEPT (source), "HTTPS" AS source
  FROM `PROJECT_NAME.BASE_DATASET.https_scan`
  WHERE date >= 'EARLIEST_DATE'
), Grouped AS (
    SELECT
        date,
        source,
        server_country AS country_code,
        server_as_full_name AS network,
        IF(domain_is_control, "CONTROL", domain) AS domain,
        AddOutcomeEmoji(outcome) AS outcome,
        CONCAT("AS", server_asn, IF(server_organization IS NOT NULL, CONCAT(" - ", server_organization), "")) AS subnetwork,
        IFNULL(domain_category, "Uncategorized") AS category,
        COUNT(*) AS count
    FROM AllScans
    # Filter on controls_failed to potentially reduce the number of output rows (less dimensions to group by).
    WHERE NOT controls_failed
    GROUP BY date, source, country_code, network, outcome, domain, category, subnetwork
    # Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
    HAVING (NOT STARTS_WITH(outcome, "❔setup/")
            AND NOT outcome = "❔read/system")
)
SELECT
    Grouped.* EXCEPT (country_code),
    IFNULL(country_name, country_code) AS country_name,
    CASE
        WHEN (STARTS_WITH(outcome, "✅")) THEN 0
        WHEN (STARTS_WITH(outcome, "❗️")) THEN count
        WHEN (STARTS_WITH(outcome, "❓")) THEN count
        WHEN (STARTS_WITH(outcome, "❔")) THEN NULL
    END AS unexpected_count
    FROM Grouped
    LEFT JOIN `PROJECT_NAME.metadata.country_names` USING (country_code)
    WHERE country_code IS NOT NULL
);
