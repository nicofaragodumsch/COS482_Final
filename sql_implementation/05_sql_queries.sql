-- ============================================================================
-- NASA Exoplanet Analysis Queries
-- ============================================================================
-- Project: K-Means Clustering Analysis of NASA Exoplanets
-- Purpose: Demonstrate SQL query capabilities for data analysis
-- ============================================================================

-- ============================================================================
-- QUERY 1: JOIN - Planets with Star Characteristics
-- ============================================================================
-- Find planets with their host star information, ordered by distance from Earth

SELECT 
    p.pl_name AS planet_name,
    p.pl_masse AS mass_earth,
    p.pl_rade AS radius_earth,
    p.pl_orbper AS orbital_period_days,
    s.hostname AS host_star,
    s.sy_dist AS distance_parsecs,
    d.discoverymethod AS discovery_method,
    d.disc_year AS discovery_year
FROM planets p
JOIN stars s ON p.star_id = s.star_id
JOIN discoveries d ON p.planet_id = d.planet_id
WHERE s.sy_dist IS NOT NULL
ORDER BY s.sy_dist ASC
LIMIT 10;

-- Export to CSV (run with \copy in psql):
-- \copy (SELECT p.pl_name, p.pl_masse, p.pl_rade, s.hostname, s.sy_dist, d.discoverymethod FROM planets p JOIN stars s ON p.star_id = s.star_id JOIN discoveries d ON p.planet_id = d.planet_id WHERE s.sy_dist IS NOT NULL ORDER BY s.sy_dist ASC LIMIT 10) TO 'nearest_planets.csv' CSV HEADER

-- ============================================================================
-- QUERY 2: AGGREGATE - Count Planets by Discovery Method
-- ============================================================================
-- Count the number of planets discovered by each method

SELECT 
    d.discoverymethod,
    COUNT(*) AS planet_count,
    ROUND(AVG(p.pl_masse), 2) AS avg_mass,
    ROUND(AVG(p.pl_rade), 2) AS avg_radius,
    ROUND(AVG(p.pl_orbper), 2) AS avg_period
FROM discoveries d
JOIN planets p ON d.planet_id = p.planet_id
WHERE d.discoverymethod IS NOT NULL
GROUP BY d.discoverymethod
ORDER BY planet_count DESC;

-- Export to CSV:
-- \copy (SELECT d.discoverymethod, COUNT(*) AS planet_count, ROUND(AVG(p.pl_masse), 2) AS avg_mass, ROUND(AVG(p.pl_rade), 2) AS avg_radius FROM discoveries d JOIN planets p ON d.planet_id = p.planet_id WHERE d.discoverymethod IS NOT NULL GROUP BY d.discoverymethod ORDER BY planet_count DESC) TO 'planets_by_method.csv' CSV HEADER

-- ============================================================================
-- QUERY 3: GROUP BY Year - Discovery Timeline
-- ============================================================================
-- Analyze planet discoveries over time

SELECT 
    d.disc_year AS year,
    COUNT(*) AS planets_discovered,
    COUNT(DISTINCT s.star_id) AS unique_stars,
    ROUND(AVG(p.pl_masse), 2) AS avg_mass,
    ROUND(AVG(s.sy_dist), 2) AS avg_distance
FROM discoveries d
JOIN planets p ON d.planet_id = p.planet_id
JOIN stars s ON p.star_id = s.star_id
WHERE d.disc_year IS NOT NULL
GROUP BY d.disc_year
ORDER BY d.disc_year ASC;

-- Export to CSV:
-- \copy (SELECT d.disc_year AS year, COUNT(*) AS planets_discovered, COUNT(DISTINCT s.star_id) AS unique_stars FROM discoveries d JOIN planets p ON d.planet_id = p.planet_id JOIN stars s ON p.star_id = s.star_id WHERE d.disc_year IS NOT NULL GROUP BY d.disc_year ORDER BY d.disc_year ASC) TO 'discoveries_by_year.csv' CSV HEADER

-- ============================================================================
-- QUERY 4: COMPLEX FILTERING - Recent Discoveries with Specific Criteria
-- ============================================================================
-- Find planets discovered after 2015 with mass > 1 Earth mass and radius < 10 Earth radii

SELECT 
    p.pl_name,
    p.pl_masse,
    p.pl_rade,
    p.density,
    s.hostname,
    d.disc_year,
    d.discoverymethod,
    CASE 
        WHEN p.density > 0.8 AND p.density < 1.2 THEN 'Rocky'
        WHEN p.density < 0.5 THEN 'Gas Giant'
        ELSE 'Other'
    END AS planet_type
FROM planets p
JOIN stars s ON p.star_id = s.star_id
JOIN discoveries d ON p.planet_id = d.planet_id
WHERE d.disc_year > 2015
    AND p.pl_masse > 1.0
    AND p.pl_rade < 10.0
    AND p.density IS NOT NULL
ORDER BY d.disc_year DESC, p.pl_masse DESC;

-- Export to CSV:
-- \copy (SELECT p.pl_name, p.pl_masse, p.pl_rade, d.disc_year, d.discoverymethod FROM planets p JOIN stars s ON p.star_id = s.star_id JOIN discoveries d ON p.planet_id = d.planet_id WHERE d.disc_year > 2015 AND p.pl_masse > 1.0 AND p.pl_rade < 10.0 ORDER BY d.disc_year DESC) TO 'recent_massive_planets.csv' CSV HEADER

-- ============================================================================
-- QUERY 5: SUBQUERY - Most Massive Planet for Each Discovery Method
-- ============================================================================
-- Find the most massive planet discovered by each method

SELECT 
    d.discoverymethod,
    p.pl_name,
    p.pl_masse AS max_mass,
    s.hostname,
    d.disc_year
FROM planets p
JOIN stars s ON p.star_id = s.star_id
JOIN discoveries d ON p.planet_id = d.planet_id
WHERE p.pl_masse = (
    SELECT MAX(p2.pl_masse)
    FROM planets p2
    JOIN discoveries d2 ON p2.planet_id = d2.planet_id
    WHERE d2.discoverymethod = d.discoverymethod
        AND p2.pl_masse IS NOT NULL
)
ORDER BY p.pl_masse DESC;

-- Export to CSV:
-- \copy (SELECT d.discoverymethod, p.pl_name, p.pl_masse AS max_mass, s.hostname FROM planets p JOIN stars s ON p.star_id = s.star_id JOIN discoveries d ON p.planet_id = d.planet_id WHERE p.pl_masse = (SELECT MAX(p2.pl_masse) FROM planets p2 JOIN discoveries d2 ON p2.planet_id = d2.planet_id WHERE d2.discoverymethod = d.discoverymethod AND p2.pl_masse IS NOT NULL) ORDER BY p.pl_masse DESC) TO 'most_massive_by_method.csv' CSV HEADER

-- ============================================================================
-- QUERY 6: WINDOW FUNCTION - Ranking Planets by Mass within Each Star System
-- ============================================================================
-- Rank planets by mass within each star system

SELECT 
    s.hostname,
    p.pl_name,
    p.pl_masse,
    RANK() OVER (PARTITION BY s.star_id ORDER BY p.pl_masse DESC NULLS LAST) AS mass_rank_in_system,
    COUNT(*) OVER (PARTITION BY s.star_id) AS planets_in_system
FROM planets p
JOIN stars s ON p.star_id = s.star_id
WHERE p.pl_masse IS NOT NULL
ORDER BY s.hostname, mass_rank_in_system;

-- ============================================================================
-- QUERY 7: HAVING CLAUSE - Stars with Multiple Planets
-- ============================================================================
-- Find star systems with more than 2 known planets

SELECT 
    s.hostname,
    COUNT(p.planet_id) AS planet_count,
    ROUND(AVG(p.pl_orbper), 2) AS avg_orbital_period,
    ROUND(AVG(s.sy_dist), 2) AS distance_parsecs
FROM stars s
JOIN planets p ON s.star_id = p.star_id
GROUP BY s.star_id, s.hostname
HAVING COUNT(p.planet_id) > 2
ORDER BY planet_count DESC;

-- Export to CSV:
-- \copy (SELECT s.hostname, COUNT(p.planet_id) AS planet_count, ROUND(AVG(p.pl_orbper), 2) AS avg_orbital_period FROM stars s JOIN planets p ON s.star_id = p.star_id GROUP BY s.star_id, s.hostname HAVING COUNT(p.planet_id) > 2 ORDER BY planet_count DESC) TO 'multi_planet_systems.csv' CSV HEADER

-- ============================================================================
-- QUERY 8: CORRELATED SUBQUERY - Planets Larger than System Average
-- ============================================================================
-- Find planets that are larger than the average planet in their star system

SELECT 
    p.pl_name,
    s.hostname,
    p.pl_rade AS planet_radius,
    (SELECT AVG(p2.pl_rade)
     FROM planets p2
     WHERE p2.star_id = p.star_id
         AND p2.pl_rade IS NOT NULL) AS system_avg_radius
FROM planets p
JOIN stars s ON p.star_id = s.star_id
WHERE p.pl_rade > (
    SELECT AVG(p2.pl_rade)
    FROM planets p2
    WHERE p2.star_id = p.star_id
        AND p2.pl_rade IS NOT NULL
)
ORDER BY s.hostname, p.pl_rade DESC;

-- ============================================================================
-- QUERY 9: CASE STATEMENT - Planet Classification
-- ============================================================================
-- Classify planets into categories based on mass and radius

SELECT 
    p.pl_name,
    p.pl_masse,
    p.pl_rade,
    CASE 
        WHEN p.pl_masse < 2.0 AND p.pl_rade < 1.5 THEN 'Super-Earth'
        WHEN p.pl_masse < 10.0 AND p.pl_rade < 4.0 THEN 'Neptune-like'
        WHEN p.pl_masse >= 10.0 AND p.pl_rade > 8.0 THEN 'Jupiter-like'
        WHEN p.pl_masse < 0.5 AND p.pl_rade < 1.0 THEN 'Sub-Earth'
        ELSE 'Other'
    END AS planet_category,
    d.discoverymethod
FROM planets p
JOIN discoveries d ON p.planet_id = d.planet_id
WHERE p.pl_masse IS NOT NULL AND p.pl_rade IS NOT NULL
ORDER BY p.pl_masse DESC;

-- ============================================================================
-- QUERY 10: CTE (Common Table Expression) - Complex Analysis
-- ============================================================================
-- Analyze discovery methods with good detection of Earth-like planets

WITH earth_like_planets AS (
    SELECT 
        p.planet_id,
        p.pl_name,
        p.pl_masse,
        p.pl_rade,
        d.discoverymethod
    FROM planets p
    JOIN discoveries d ON p.planet_id = d.planet_id
    WHERE p.pl_masse BETWEEN 0.5 AND 2.0
        AND p.pl_rade BETWEEN 0.8 AND 1.5
),
method_stats AS (
    SELECT 
        discoverymethod,
        COUNT(*) AS earth_like_count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
    FROM earth_like_planets
    GROUP BY discoverymethod
)
SELECT 
    discoverymethod,
    earth_like_count,
    ROUND(percentage, 2) AS percentage_of_earth_like
FROM method_stats
ORDER BY earth_like_count DESC;

-- ============================================================================
-- QUERY 11: PLACEHOLDER - For Clustering Results (After K-Means)
-- ============================================================================
-- This query will be used after clustering analysis to find the most massive
-- planet in each cluster. First, you'll need to add a cluster_id column:

-- ALTER TABLE planets ADD COLUMN cluster_id INTEGER;

-- Then run this query after populating cluster_id:
/*
SELECT 
    p.cluster_id,
    p.pl_name,
    p.pl_masse AS max_mass_in_cluster,
    s.hostname,
    d.discoverymethod
FROM planets p
JOIN stars s ON p.star_id = s.star_id
JOIN discoveries d ON p.planet_id = d.planet_id
WHERE p.cluster_id IS NOT NULL
    AND p.pl_masse = (
        SELECT MAX(p2.pl_masse)
        FROM planets p2
        WHERE p2.cluster_id = p.cluster_id
            AND p2.pl_masse IS NOT NULL
    )
ORDER BY p.cluster_id, p.pl_masse DESC;
*/

-- ============================================================================
-- UTILITY QUERIES
-- ============================================================================

-- Quick table summaries
SELECT 'Stars' AS table_name, COUNT(*) AS record_count FROM stars
UNION ALL
SELECT 'Planets' AS table_name, COUNT(*) AS record_count FROM planets
UNION ALL
SELECT 'Discoveries' AS table_name, COUNT(*) AS record_count FROM discoveries;

-- Data quality check
SELECT 
    'Planets with all features' AS category,
    COUNT(*) AS count
FROM planets
WHERE pl_masse IS NOT NULL 
    AND pl_rade IS NOT NULL 
    AND pl_orbper IS NOT NULL 
    AND pl_eqt IS NOT NULL
UNION ALL
SELECT 
    'Planets missing mass' AS category,
    COUNT(*) AS count
FROM planets
WHERE pl_masse IS NULL;

-- ============================================================================
-- END OF QUERIES
-- ============================================================================