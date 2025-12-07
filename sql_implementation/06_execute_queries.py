"""
Execute SQL Queries and Export Results
======================================
Project: K-Means Clustering Analysis of NASA Exoplanets

This script executes predefined SQL queries and exports results to CSV files.
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
import os
import getpass
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'exoplanet_db',
    'user': 'postgres'
    # Password will be prompted at runtime
}

OUTPUT_DIR = 'query_results'

# ============================================================================
# SQL QUERIES DICTIONARY
# ============================================================================

QUERIES = {
    # ---------------------------------------------------------
    # ANALYTICAL QUERIES (Targeting Complete Data - Stage 2/2c)
    # ---------------------------------------------------------
    'recent_massive_planets': {
        'description': 'Planets discovered after 2015 with mass > 1 Earth mass (Stage 2c Only)',
        'sql': """
            SELECT 
                p.pl_name, p.pl_masse, p.pl_rade, p.density, 
                s.hostname, d.disc_year, d.discoverymethod,
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
              AND p.in_stage2c = TRUE  -- ADDED: Filter for high-quality data
            ORDER BY d.disc_year DESC, p.pl_masse DESC
        """
    },
    'most_massive_by_method': {
        'description': 'Most massive planet for each discovery method (Stage 2 Only)',
        'sql': """
            SELECT d.discoverymethod, p.pl_name, p.pl_masse AS max_mass, s.hostname, d.disc_year
            FROM planets p
            JOIN stars s ON p.star_id = s.star_id
            JOIN discoveries d ON p.planet_id = d.planet_id
            WHERE p.pl_masse = (
                SELECT MAX(p2.pl_masse)
                FROM planets p2
                JOIN discoveries d2 ON p2.planet_id = d2.planet_id
                WHERE d2.discoverymethod = d.discoverymethod
                  AND p2.in_stage2 = TRUE -- ADDED: Ensure comparison is within Stage 2
            )
            AND p.in_stage2 = TRUE -- ADDED: Filter outer query
            ORDER BY p.pl_masse DESC
        """
    },
    'earth_like_by_method': {
        'description': 'Earth-like planets (0.8-1.2 Earth radii/mass) by method (Stage 2c)',
        'sql': """
            WITH earth_like AS (
                SELECT p.planet_id, p.pl_name, d.discoverymethod
                FROM planets p
                JOIN discoveries d ON p.planet_id = d.planet_id
                WHERE p.pl_rade BETWEEN 0.8 AND 1.2
                  AND p.pl_masse BETWEEN 0.8 AND 1.2
                  AND p.in_stage2c = TRUE -- ADDED: strict filter for "Earth-like" candidates
            )
            SELECT discoverymethod, COUNT(*) as count, string_agg(pl_name, ', ') as planets
            FROM earth_like
            GROUP BY discoverymethod
            ORDER BY count DESC
        """
    },

    # ---------------------------------------------------------
    # GENERAL QUERIES (Can run on All Data - Stage 1)
    # ---------------------------------------------------------
    'planets_by_method': {
        'description': 'Count of planets by discovery method (All Stages)',
        'sql': """
            SELECT d.discoverymethod, COUNT(*) as count, 
                   ROUND(AVG(p.pl_masse)::numeric, 2) as avg_mass
            FROM discoveries d
            JOIN planets p ON d.planet_id = p.planet_id
            -- No stage filter here: we want to count EVERYTHING discovered
            GROUP BY d.discoverymethod
            ORDER BY count DESC
        """
    },
    'discoveries_by_year': {
        'description': 'Number of planets discovered per year (All Stages)',
        'sql': """
            SELECT d.disc_year, COUNT(*) as count
            FROM discoveries d
            -- No stage filter: counts all discoveries regardless of data completeness
            GROUP BY d.disc_year
            ORDER BY d.disc_year ASC
        """
    },
    
    # ---------------------------------------------------------
    # NEW QUERIES (Stage Comparison)
    # ---------------------------------------------------------
    'stage_summary': {
        'description': 'Comparison of planet counts across different data stages',
        'sql': """
            SELECT 
                COUNT(*) as total_planets,
                COUNT(CASE WHEN in_stage1 THEN 1 END) as stage1_count,
                COUNT(CASE WHEN in_stage2 THEN 1 END) as stage2_complete_data,
                COUNT(CASE WHEN in_stage2c THEN 1 END) as stage2c_high_quality
            FROM planets
        """
    },
    # ---------------------------------------------------------
    # RESTORED & UPDATED QUERIES (PostgreSQL Version)
    # ---------------------------------------------------------
    'multi_planet_systems': {
        'description': 'Star systems with > 2 confirmed planets (Stage 2c)',
        'sql': """
            SELECT 
                s.hostname, 
                COUNT(p.planet_id) AS planet_count, 
                ROUND(AVG(p.pl_orbper)::numeric, 2) AS avg_orbital_period, 
                ROUND(AVG(s.sy_dist)::numeric, 2) AS distance_parsecs
            FROM stars s
            JOIN planets p ON s.star_id = p.star_id
            WHERE p.in_stage2c = TRUE
            GROUP BY s.star_id, s.hostname
            HAVING COUNT(p.planet_id) > 2
            ORDER BY planet_count DESC
        """
    },
    'planet_classification': {
        'description': 'Classify planets by mass/radius (Stage 2 Only)',
        'sql': """
            SELECT 
                p.pl_name, p.pl_masse, p.pl_rade,
                CASE 
                    WHEN p.pl_masse < 2.0 AND p.pl_rade < 1.25 THEN 'Rocky'
                    WHEN p.pl_masse < 10.0 AND p.pl_rade < 4.0 THEN 'Neptune-like'
                    WHEN p.pl_masse >= 10.0 AND p.pl_rade > 8.0 THEN 'Gas Giant'
                    ELSE 'Other/Unknown'
                END AS classification
            FROM planets p
            WHERE p.in_stage2c = TRUE
            ORDER BY p.pl_masse DESC
        """
    },
    'nearest_confirmed_planets': {
        'description': 'The 5 nearest confirmed planets to Earth',
        'sql': """
            SELECT p.pl_name, s.hostname, s.sy_dist, p.pl_masse
            FROM planets p
            JOIN stars s ON p.star_id = s.star_id
            WHERE s.sy_dist IS NOT NULL
              AND p.in_stage1c = TRUE
            ORDER BY s.sy_dist ASC
            LIMIT 5
        """
    },
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_output_directory():
    """Create output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"✓ Created output directory: {OUTPUT_DIR}")
    else:
        print(f"✓ Output directory exists: {OUTPUT_DIR}")


def execute_query(conn, query_name, query_info):
    """
    Execute a SQL query and return results as DataFrame.
    
    Parameters:
    ----------
    conn : psycopg2 connection
        Database connection
    query_name : str
        Name identifier for the query
    query_info : dict
        Dictionary containing 'description' and 'sql' keys
    
    Returns:
    -------
    pd.DataFrame or None
    """
    print(f"\n{'='*80}")
    print(f"Query: {query_name}")
    print(f"Description: {query_info['description']}")
    print(f"{'='*80}")
    
    try:
        # Execute query and load into DataFrame
        df = pd.read_sql_query(query_info['sql'], conn)
        
        print(f"✓ Query executed successfully")
        print(f"  Rows returned: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")
        
        # Display first few rows
        if len(df) > 0:
            print(f"\nFirst 5 rows:")
            print(df.head().to_string())
        else:
            print("\n⚠ Query returned no results")
        
        return df
        
    except Exception as e:
        print(f"✗ Error executing query: {e}")
        return None


def export_to_csv(df, filename):
    """
    Export DataFrame to CSV file.
    
    Parameters:
    ----------
    df : pd.DataFrame
        DataFrame to export
    filename : str
        Output filename
    """
    if df is None or len(df) == 0:
        print(f"⚠ Skipping export - no data to export")
        return
    
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    try:
        df.to_csv(filepath, index=False)
        file_size = os.path.getsize(filepath)
        print(f"\n✓ Exported to: {filepath}")
        print(f"  File size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
        
    except Exception as e:
        print(f"✗ Error exporting to CSV: {e}")


def create_summary_report(results):
    """
    Create a summary report of all executed queries.
    
    Parameters:
    ----------
    results : dict
        Dictionary of query results
    """
    print(f"\n{'='*80}")
    print("QUERY EXECUTION SUMMARY")
    print(f"{'='*80}")
    
    report = []
    
    for query_name, df in results.items():
        if df is not None:
            report.append({
                'Query': query_name,
                'Rows': len(df),
                'Columns': len(df.columns),
                'Status': '✓ Success'
            })
        else:
            report.append({
                'Query': query_name,
                'Rows': 0,
                'Columns': 0,
                'Status': '✗ Failed'
            })
    
    summary_df = pd.DataFrame(report)
    print(f"\n{summary_df.to_string(index=False)}")
    
    # Export summary
    summary_path = os.path.join(OUTPUT_DIR, '_query_summary.csv')
    summary_df.to_csv(summary_path, index=False)
    print(f"\n✓ Summary exported to: {summary_path}")
    
    print(f"\n{'='*80}")
    print(f"Total queries executed: {len(results)}")
    print(f"Successful: {sum(1 for df in results.values() if df is not None)}")
    print(f"Failed: {sum(1 for df in results.values() if df is None)}")
    print(f"{'='*80}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    
    print(f"\n{'='*80}")
    print("NASA EXOPLANET SQL QUERY EXECUTION")
    print(f"{'='*80}")
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    # Create output directory
    create_output_directory()
    
    # Prompt for password securely
    print(f"\nDatabase: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    print(f"User: {DB_CONFIG['user']}")
    password = getpass.getpass(f"\nEnter password for user '{DB_CONFIG['user']}': ")
    
    if not password:
        print("\n✗ Error: Password cannot be empty")
        return
    
    # Add password to config
    DB_CONFIG['password'] = password
    
    # Connect to database
    try:
        print(f"\nConnecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Database connection established")
    except psycopg2.Error as e:
        print(f"✗ Error connecting to database: {e}")
        return
    
    # Execute all queries
    results = {}
    
    for query_name, query_info in QUERIES.items():
        df = execute_query(conn, query_name, query_info)
        results[query_name] = df
        
        # Export to CSV
        if df is not None:
            export_to_csv(df, f"{query_name}.csv")
    
    # Close connection
    conn.close()
    print(f"\n✓ Database connection closed")
    
    # Create summary report
    create_summary_report(results)
    
    print(f"\n{'='*80}")
    print("✓ QUERY EXECUTION COMPLETED")
    print(f"{'='*80}")
    print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"All results exported to: {OUTPUT_DIR}/")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()