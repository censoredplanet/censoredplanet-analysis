SELECT
        ddate,
        source,
        server_country AS country_name,
        server_as_full_name AS network,
        IF(domain_is_control, "CONTROL", domain) AS domain,
        outcome AS outcome,
        CONCAT("AS", server_asn, IF(server_organization IS NOT NULL, CONCAT(" - ", server_organization), "")) AS subnetwork,
        IFNULL(domain_category, "Uncategorized") AS category,
        COUNT(*) AS count,
        COUNT(*) AS unexpected_count
    FROM PCOLLECTION
    # Filter on controls_failed to potentially reduce the number of output rows (less dimensions to group by).
    WHERE NOT controls_failed
    GROUP BY date, source, country_code, network, outcome, domain, category, subnetwork
    # Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
    HAVING (NOT STARTS_WITH(outcome, "setup/")
            AND NOT outcome = "read/system")