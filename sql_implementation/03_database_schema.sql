-- ============================================================================
-- NASA Exoplanet Database Schema
-- ============================================================================
-- Project: K-Means Clustering Analysis of NASA Exoplanets
-- Author: [Professor Name]
-- Date: December 2025
--
-- Purpose: Create a normalized relational database for exoplanet data
-- Database: PostgreSQL
--
-- Entity-Relationship Design:
--   - Stars (1) -> (M) Planets (one star hosts many planets)
--   - Planets (1) -> (1) Discoveries (one planet has one discovery record)
-- ============================================================================

-- Drop existing tables if they exist (for clean re-runs)
DROP TABLE IF EXISTS discoveries CASCADE;
DROP TABLE IF EXISTS planets CASCADE;
DROP TABLE IF EXISTS stars CASCADE;

-- ============================================================================
-- STARS TABLE
-- ============================================================================
-- Stores information about host stars
CREATE TABLE stars (
    star_id SERIAL PRIMARY KEY,
    hostname VARCHAR(100) NOT NULL UNIQUE,  -- Host star name (unique identifier)
    sy_dist NUMERIC(10, 4),                 -- Distance from Earth (parsecs)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments for documentation
COMMENT ON TABLE stars IS 'Host stars that have confirmed exoplanets';
COMMENT ON COLUMN stars.star_id IS 'Primary key - auto-incrementing star identifier';
COMMENT ON COLUMN stars.hostname IS 'Name of the host star (must be unique)';
COMMENT ON COLUMN stars.sy_dist IS 'Distance from Earth to star system in parsecs';

-- Create index for faster lookups
CREATE INDEX idx_stars_hostname ON stars(hostname);
CREATE INDEX idx_stars_distance ON stars(sy_dist);

-- ============================================================================
-- PLANETS TABLE
-- ============================================================================
-- Stores physical and orbital characteristics of exoplanets
CREATE TABLE planets (
    planet_id SERIAL PRIMARY KEY,
    pl_name VARCHAR(100) NOT NULL UNIQUE,   -- Planet name (unique identifier)
    star_id INTEGER NOT NULL,               -- Foreign key to stars table
    pl_masse NUMERIC(12, 6),                -- Planet mass (Earth masses)
    pl_rade NUMERIC(12, 6),                 -- Planet radius (Earth radii)
    pl_orbper NUMERIC(15, 6),               -- Orbital period (days)
    pl_eqt NUMERIC(10, 2),                  -- Equilibrium temperature (Kelvin)
    density NUMERIC(12, 6),                 -- Derived density (relative to Earth)
    in_stage1 BOOLEAN DEFAULT FALSE,        -- Is planet in Stage 1 dataset?
    in_stage1c BOOLEAN DEFAULT FALSE,       -- Is planet in Stage 1c dataset?
    in_stage2 BOOLEAN DEFAULT FALSE,        -- Is planet in Stage 2 dataset?
    in_stage2c BOOLEAN DEFAULT FALSE,       -- Is planet in Stage 2c dataset?
    cluster_id_s1 INTEGER,                  -- Cluster assignment for Stage 1
    cluster_id_s1c INTEGER,                 -- Cluster assignment for Stage 1c
    cluster_id_s2 INTEGER,                  -- Cluster assignment for Stage 2
    cluster_id_s2c INTEGER,                 -- Cluster assignment for Stage 2c
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraint
    CONSTRAINT fk_planet_star 
        FOREIGN KEY (star_id) 
        REFERENCES stars(star_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    
    -- Check constraints for data validity
    CONSTRAINT chk_mass_positive 
        CHECK (pl_masse IS NULL OR pl_masse > 0),
    CONSTRAINT chk_radius_positive 
        CHECK (pl_rade IS NULL OR pl_rade > 0),
    CONSTRAINT chk_period_positive 
        CHECK (pl_orbper IS NULL OR pl_orbper > 0),
    CONSTRAINT chk_temp_valid 
        CHECK (pl_eqt IS NULL OR (pl_eqt > 0 AND pl_eqt < 5000)),
    CONSTRAINT chk_density_valid 
        CHECK (density IS NULL OR (density > 0.01 AND density < 100))
);

-- Add comments for documentation
COMMENT ON TABLE planets IS 'Confirmed exoplanets with physical and orbital properties';
COMMENT ON COLUMN planets.planet_id IS 'Primary key - auto-incrementing planet identifier';
COMMENT ON COLUMN planets.pl_name IS 'Official name of the exoplanet (must be unique)';
COMMENT ON COLUMN planets.star_id IS 'Foreign key linking to host star';
COMMENT ON COLUMN planets.pl_masse IS 'Planet mass in Earth masses (NULL if unknown)';
COMMENT ON COLUMN planets.pl_rade IS 'Planet radius in Earth radii (NULL if unknown)';
COMMENT ON COLUMN planets.pl_orbper IS 'Orbital period in days';
COMMENT ON COLUMN planets.pl_eqt IS 'Equilibrium temperature in Kelvin';
COMMENT ON COLUMN planets.density IS 'Calculated density relative to Earth (mass/radius^3)';

-- Create indexes for faster queries
CREATE INDEX idx_planets_name ON planets(pl_name);
CREATE INDEX idx_planets_star ON planets(star_id);
CREATE INDEX idx_planets_mass ON planets(pl_masse);
CREATE INDEX idx_planets_radius ON planets(pl_rade);
CREATE INDEX idx_planets_period ON planets(pl_orbper);
CREATE INDEX idx_planets_temp ON planets(pl_eqt);

-- ============================================================================
-- DISCOVERIES TABLE
-- ============================================================================
-- Stores discovery information for each planet
CREATE TABLE discoveries (
    discovery_id SERIAL PRIMARY KEY,
    planet_id INTEGER NOT NULL UNIQUE,      -- Foreign key to planets (one-to-one)
    discoverymethod VARCHAR(100),           -- Method used to discover the planet
    disc_year INTEGER,                      -- Year of discovery
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraint (one-to-one relationship)
    CONSTRAINT fk_discovery_planet 
        FOREIGN KEY (planet_id) 
        REFERENCES planets(planet_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    
    -- Check constraints
    CONSTRAINT chk_year_valid 
        CHECK (disc_year IS NULL OR (disc_year >= 1990 AND disc_year <= 2030))
);

-- Add comments for documentation
COMMENT ON TABLE discoveries IS 'Discovery information for each confirmed exoplanet';
COMMENT ON COLUMN discoveries.discovery_id IS 'Primary key - auto-incrementing discovery identifier';
COMMENT ON COLUMN discoveries.planet_id IS 'Foreign key linking to planet (one-to-one relationship)';
COMMENT ON COLUMN discoveries.discoverymethod IS 'Detection method (e.g., Transit, Radial Velocity)';
COMMENT ON COLUMN discoveries.disc_year IS 'Year the planet was discovered';

-- Create indexes for faster queries
CREATE INDEX idx_discoveries_planet ON discoveries(planet_id);
CREATE INDEX idx_discoveries_method ON discoveries(discoverymethod);
CREATE INDEX idx_discoveries_year ON discoveries(disc_year);

-- ============================================================================
-- VIEWS FOR CONVENIENT QUERYING
-- ============================================================================

-- Complete planet information with star and discovery data
CREATE OR REPLACE VIEW vw_planets_complete AS
SELECT 
    p.planet_id,
    p.pl_name,
    p.pl_masse,
    p.pl_rade,
    p.pl_orbper,
    p.pl_eqt,
    p.density,
    s.star_id,
    s.hostname,
    s.sy_dist,
    d.discovery_id,
    d.discoverymethod,
    d.disc_year
FROM planets p
LEFT JOIN stars s ON p.star_id = s.star_id
LEFT JOIN discoveries d ON p.planet_id = d.planet_id;

COMMENT ON VIEW vw_planets_complete IS 'Complete view joining planets with their stars and discovery information';

-- Summary statistics by discovery method
CREATE OR REPLACE VIEW vw_discovery_stats AS
SELECT 
    d.discoverymethod,
    COUNT(*) as planet_count,
    AVG(p.pl_masse) as avg_mass,
    AVG(p.pl_rade) as avg_radius,
    AVG(p.pl_orbper) as avg_period,
    AVG(p.pl_eqt) as avg_temp,
    MIN(d.disc_year) as first_discovery,
    MAX(d.disc_year) as latest_discovery
FROM discoveries d
JOIN planets p ON d.planet_id = p.planet_id
WHERE d.discoverymethod IS NOT NULL
GROUP BY d.discoverymethod
ORDER BY planet_count DESC;

COMMENT ON VIEW vw_discovery_stats IS 'Summary statistics grouped by discovery method';

-- ============================================================================
-- FUNCTIONS FOR DATA INTEGRITY
-- ============================================================================

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers to automatically update timestamps
CREATE TRIGGER update_stars_modtime
    BEFORE UPDATE ON stars
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_planets_modtime
    BEFORE UPDATE ON planets
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_discoveries_modtime
    BEFORE UPDATE ON discoveries
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check table structure
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
    AND table_name IN ('stars', 'planets', 'discoveries')
ORDER BY table_name, ordinal_position;

-- ============================================================================
-- GRANT PERMISSIONS (adjust as needed for your setup)
-- ============================================================================

-- Example: Grant all privileges to a specific user
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_username;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO your_username;

-- ============================================================================
-- SCHEMA CREATION COMPLETE
-- ============================================================================

-- Display confirmation
SELECT 
    'Schema created successfully!' as status,
    COUNT(*) as table_count
FROM information_schema.tables
WHERE table_schema = 'public'
    AND table_name IN ('stars', 'planets', 'discoveries');