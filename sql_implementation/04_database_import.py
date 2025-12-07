"""
Database Import Script - NASA Exoplanet Data
============================================
Project: K-Means Clustering Analysis of NASA Exoplanets

This script imports cleaned exoplanet data into PostgreSQL database.

Requirements:
- PostgreSQL installed and running
- psycopg2 or psycopg2-binary package
- sqlalchemy package

Usage:
    python 04_database_import.py
"""

import pandas as pd
import psycopg2
from psycopg2 import sql, extras
from sqlalchemy import create_engine
import sys
import getpass
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'exoplanet_db',
    'user': 'postgres'
    # Password will be prompted at runtime
}

# Input files - we'll import Stage 2 (has most complete data)
INPUT_FILE = 'cleaned_exoplanets_stage2.csv'

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_connection(config):
    """
    Create a connection to PostgreSQL database.
    
    Returns:
    -------
    connection : psycopg2 connection object or None
    """
    try:
        print(f"Connecting to database: {config['database']}@{config['host']}:{config['port']}")
        conn = psycopg2.connect(**config)
        print("✓ Database connection established")
        return conn
    except psycopg2.Error as e:
        print(f"✗ Error connecting to database: {e}")
        return None


def load_cleaned_data(filepath):
    """
    Load cleaned exoplanet data from CSV.
    
    Parameters:
    ----------
    filepath : str
        Path to cleaned CSV file
    
    Returns:
    -------
    pd.DataFrame : Loaded data
    """
    try:
        print(f"\nLoading data from: {filepath}")
        df = pd.read_csv(filepath)
        print(f"✓ Loaded {len(df):,} planets")
        print(f"  Columns: {df.columns.tolist()}")
        return df
    except FileNotFoundError:
        print(f"✗ Error: File not found: {filepath}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error loading data: {e}")
        sys.exit(1)


def import_stars(conn, df):
    """
    Import unique star data into stars table.
    
    Parameters:
    ----------
    conn : psycopg2 connection
        Database connection
    df : pd.DataFrame
        Dataframe containing planet data with hostname and sy_dist
    
    Returns:
    -------
    dict : Mapping of hostname to star_id
    """
    print("\n" + "="*80)
    print("IMPORTING STARS")
    print("="*80)
    
    cursor = conn.cursor()
    
    # Get unique stars from the dataset
    stars_df = df[['hostname', 'sy_dist']].drop_duplicates('hostname').copy()
    stars_df = stars_df.dropna(subset=['hostname'])  # Remove any nulls
    
    print(f"Found {len(stars_df):,} unique host stars")
    
    # Create mapping dictionary
    star_id_map = {}
    
    # Insert stars
    insert_query = """
        INSERT INTO stars (hostname, sy_dist)
        VALUES (%s, %s)
        ON CONFLICT (hostname) DO UPDATE 
        SET sy_dist = EXCLUDED.sy_dist
        RETURNING star_id, hostname
    """
    
    inserted = 0
    for idx, row in stars_df.iterrows():
        try:
            cursor.execute(insert_query, (row['hostname'], row['sy_dist']))
            star_id, hostname = cursor.fetchone()
            star_id_map[hostname] = star_id
            inserted += 1
            
            if inserted % 100 == 0:
                print(f"  Processed {inserted:,} stars...", end='\r')
        except psycopg2.Error as e:
            print(f"\n⚠ Warning: Error inserting star {row['hostname']}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    print(f"\n✓ Imported {inserted:,} stars successfully")
    
    cursor.close()
    return star_id_map


def import_planets(conn, df, star_id_map):
    """
    Import planet data into planets table.
    
    Parameters:
    ----------
    conn : psycopg2 connection
        Database connection
    df : pd.DataFrame
        Dataframe containing planet data
    star_id_map : dict
        Mapping of hostname to star_id
    
    Returns:
    -------
    dict : Mapping of pl_name to planet_id
    """
    print("\n" + "="*80)
    print("IMPORTING PLANETS")
    print("="*80)
    
    cursor = conn.cursor()
    
    # Create planet_id mapping
    planet_id_map = {}
    
    # Insert query
    insert_query = """
        INSERT INTO planets (pl_name, star_id, pl_masse, pl_rade, pl_orbper, pl_eqt, density)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pl_name) DO UPDATE
        SET pl_masse = EXCLUDED.pl_masse,
            pl_rade = EXCLUDED.pl_rade,
            pl_orbper = EXCLUDED.pl_orbper,
            pl_eqt = EXCLUDED.pl_eqt,
            density = EXCLUDED.density
        RETURNING planet_id, pl_name
    """
    
    inserted = 0
    skipped = 0
    
    for idx, row in df.iterrows():
        # Get star_id from mapping
        if row['hostname'] not in star_id_map:
            print(f"\n⚠ Warning: Star {row['hostname']} not found in mapping, skipping planet {row['pl_name']}")
            skipped += 1
            continue
        
        star_id = star_id_map[row['hostname']]
        
        # Prepare values (handle NaN/None)
        pl_masse = None if pd.isna(row.get('pl_masse')) else float(row['pl_masse'])
        pl_rade = None if pd.isna(row.get('pl_rade')) else float(row['pl_rade'])
        pl_orbper = None if pd.isna(row.get('pl_orbper')) else float(row['pl_orbper'])
        pl_eqt = None if pd.isna(row.get('pl_eqt')) else float(row['pl_eqt'])
        density = None if pd.isna(row.get('density')) else float(row['density'])
        
        try:
            cursor.execute(insert_query, (
                row['pl_name'],
                star_id,
                pl_masse,
                pl_rade,
                pl_orbper,
                pl_eqt,
                density
            ))
            planet_id, pl_name = cursor.fetchone()
            planet_id_map[pl_name] = planet_id
            inserted += 1
            
            if inserted % 100 == 0:
                print(f"  Processed {inserted:,} planets...", end='\r')
                
        except psycopg2.Error as e:
            print(f"\n⚠ Warning: Error inserting planet {row['pl_name']}: {e}")
            conn.rollback()
            skipped += 1
            continue
    
    conn.commit()
    print(f"\n✓ Imported {inserted:,} planets successfully")
    if skipped > 0:
        print(f"⚠ Skipped {skipped:,} planets due to errors")
    
    cursor.close()
    return planet_id_map


def import_discoveries(conn, df, planet_id_map):
    """
    Import discovery data into discoveries table.
    
    Parameters:
    ----------
    conn : psycopg2 connection
        Database connection
    df : pd.DataFrame
        Dataframe containing discovery data
    planet_id_map : dict
        Mapping of pl_name to planet_id
    """
    print("\n" + "="*80)
    print("IMPORTING DISCOVERIES")
    print("="*80)
    
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO discoveries (planet_id, discoverymethod, disc_year)
        VALUES (%s, %s, %s)
        ON CONFLICT (planet_id) DO UPDATE
        SET discoverymethod = EXCLUDED.discoverymethod,
            disc_year = EXCLUDED.disc_year
        RETURNING discovery_id
    """
    
    inserted = 0
    skipped = 0
    
    for idx, row in df.iterrows():
        # Get planet_id from mapping
        if row['pl_name'] not in planet_id_map:
            skipped += 1
            continue
        
        planet_id = planet_id_map[row['pl_name']]
        
        # Prepare values
        discoverymethod = None if pd.isna(row.get('discoverymethod')) else str(row['discoverymethod'])
        disc_year = None if pd.isna(row.get('disc_year')) else int(row['disc_year'])
        
        try:
            cursor.execute(insert_query, (planet_id, discoverymethod, disc_year))
            inserted += 1
            
            if inserted % 100 == 0:
                print(f"  Processed {inserted:,} discoveries...", end='\r')
                
        except psycopg2.Error as e:
            print(f"\n⚠ Warning: Error inserting discovery for planet_id {planet_id}: {e}")
            conn.rollback()
            skipped += 1
            continue
    
    conn.commit()
    print(f"\n✓ Imported {inserted:,} discoveries successfully")
    if skipped > 0:
        print(f"⚠ Skipped {skipped:,} discoveries due to errors")
    
    cursor.close()


def verify_import(conn):
    """
    Verify the imported data with summary queries.
    
    Parameters:
    ----------
    conn : psycopg2 connection
        Database connection
    """
    print("\n" + "="*80)
    print("VERIFYING IMPORT")
    print("="*80)
    
    cursor = conn.cursor()
    
    # Count records in each table
    tables = ['stars', 'planets', 'discoveries']
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"\n{table.upper()} table: {count:,} records")
    
    # Check for orphaned records
    print("\n" + "-"*80)
    print("Checking data integrity:")
    
    # Planets without stars
    cursor.execute("""
        SELECT COUNT(*) FROM planets p
        LEFT JOIN stars s ON p.star_id = s.star_id
        WHERE s.star_id IS NULL
    """)
    orphaned_planets = cursor.fetchone()[0]
    if orphaned_planets > 0:
        print(f"⚠ Warning: {orphaned_planets} planets without valid star references")
    else:
        print("✓ All planets have valid star references")
    
    # Planets without discoveries
    cursor.execute("""
        SELECT COUNT(*) FROM planets p
        LEFT JOIN discoveries d ON p.planet_id = d.planet_id
        WHERE d.discovery_id IS NULL
    """)
    no_discovery = cursor.fetchone()[0]
    if no_discovery > 0:
        print(f"⚠ {no_discovery} planets without discovery records")
    else:
        print("✓ All planets have discovery records")
    
    # Sample query - top 5 planets by mass
    print("\n" + "-"*80)
    print("Sample Query - Top 5 Most Massive Planets:")
    cursor.execute("""
        SELECT p.pl_name, p.pl_masse, s.hostname, d.discoverymethod
        FROM planets p
        JOIN stars s ON p.star_id = s.star_id
        JOIN discoveries d ON p.planet_id = d.planet_id
        WHERE p.pl_masse IS NOT NULL
        ORDER BY p.pl_masse DESC
        LIMIT 5
    """)
    
    results = cursor.fetchall()
    for i, (name, mass, host, method) in enumerate(results, 1):
        print(f"  {i}. {name}: {mass:.2f} Earth masses (Host: {host}, Method: {method})")
    
    cursor.close()
    print("\n" + "="*80)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main import workflow."""
    
    print("\n" + "="*80)
    print("NASA EXOPLANET DATABASE IMPORT")
    print("="*80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Prompt for password securely
    print(f"\nDatabase: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    print(f"User: {DB_CONFIG['user']}")
    password = getpass.getpass(f"\nEnter password for user '{DB_CONFIG['user']}': ")
    
    if not password:
        print("\n✗ Error: Password cannot be empty")
        sys.exit(1)
    
    # Add password to config
    DB_CONFIG['password'] = password
    
    # Step 1: Load cleaned data
    df = load_cleaned_data(INPUT_FILE)
    
    # Step 2: Connect to database
    conn = create_connection(DB_CONFIG)
    if not conn:
        print("\n✗ Failed to connect to database. Exiting.")
        sys.exit(1)
    
    try:
        # Step 3: Import stars
        star_id_map = import_stars(conn, df)
        
        # Step 4: Import planets
        planet_id_map = import_planets(conn, df, star_id_map)
        
        # Step 5: Import discoveries
        import_discoveries(conn, df, planet_id_map)
        
        # Step 6: Verify import
        verify_import(conn)
        
        print("\n" + "="*80)
        print("✓ IMPORT COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
    except Exception as e:
        print(f"\n✗ Fatal Error: {e}")
        conn.rollback()
        
    finally:
        # Close connection
        if conn:
            conn.close()
            print("\n✓ Database connection closed")


if __name__ == "__main__":
    main()