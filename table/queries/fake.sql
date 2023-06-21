SELECT
        ddate,
        source,
        server_country AS country_name,
        server_as_full_name AS network,
        domain,
        outcome AS outcome,
        server_asn AS subnetwork,
        COALESCE(domain_category, 'Uncategorized') AS category,
        COUNT(*),
        1 AS unexpected_count
    FROM PCOLLECTION
    WHERE NOT controls_failed
    GROUP BY ddate, source, server_country, server_as_full_name, outcome, domain, domain_category, server_asn, server_organization
    HAVING (NOT STARTS_WITH(outcome, 'setup/')
            AND NOT outcome = 'read/system')