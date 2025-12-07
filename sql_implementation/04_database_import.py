"""
IMPROVED Database Import Script - NASA Exoplanet Data
=====================================================
This version imports ALL stages and tracks which datasets each planet belongs to.
"""

import pandas as pd
import psycopg2
import sys
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
}

# All stage files
STAGE_FILES = {
    'stage1': 'cleaned_exoplanets_stage1.csv',
    'stage1c': 'cleaned_exoplanets_stage1c.csv', 
    'stage2': 'cleaned_exoplanets_stage2.csv',
    'stage2c': 'cleaned_exoplanets_stage2c.csv'
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def find_file(filename):
    """Find file in current directory or parent directory."""
    possible_paths = [filename, f'../{filename}', os.path.join('..', filename)]
    for path in possible_paths:
        if os.path.exists(path):
            return path
    return None


def load_all_stages():
    """Load all four stage datasets."""
    print("\n" + "="*80)
    print("LOADING ALL STAGE DATASETS")
    print("="*80)
    
    stages_data = {}
    
    for stage_name, filename in STAGE_FILES.items():
        filepath = find_file(filename)
        
        if not filepath:
            print(f"\n⚠ Warning: {filename} not found - skipping {stage_name}")
            continue
        
        try:
            df = pd.read_csv(filepath)
            stages_data[stage_name] = df
            print(f"\n✓ {stage_name}: {len(df):,} planets")
            print(f"  Columns: {list(df.columns)}")
        except Exception as e:
            print(f"\n✗ Error loading {stage_name}: {e}")
    
    if not stages_data:
        print("\n✗ No stage files found! Exiting.")
        sys.exit(1)
    
    return stages_data


def create_unified_dataset(stages_data):
    """
    Create a unified dataset with all planets and track stage membership.
    """
    print("\n" + "="*80)
    print("CREATING UNIFIED DATASET")
    print("="*80)
    
    # Start with Stage 1 as base (largest dataset)
    if 'stage1' in stages_data:
        df_unified = stages_data['stage1'].copy()
        df_unified['in_stage1'] = True
    else:
        # Fallback to any available stage
        first_stage = list(stages_data.keys())[0]
        df_unified = stages_data[first_stage].copy()
        df_unified['in_stage1'] = False
    
    # Initialize stage membership columns
    for stage in ['stage1c', 'stage2', 'stage2c']:
        df_unified[f'in_{stage}'] = False
    
    # Mark which stages each planet belongs to
    for stage_name, stage_df in stages_data.items():
        planet_names = set(stage_df['pl_name'])
        df_unified[f'in_{stage_name}'] = df_unified['pl_name'].isin(planet_names)
    
    # For planets ONLY in Stage 2 (not in Stage 1), add them
    if 'stage2' in stages_data:
        stage2_only = stages_data['stage2'][
            ~stages_data['stage2']['pl_name'].isin(df_unified['pl_name'])
        ].copy()
        
        if len(stage2_only) > 0:
            print(f"\n  Adding {len(stage2_only)} planets found only in Stage 2")
            stage2_only['in_stage1'] = False
            stage2_only['in_stage1c'] = False
            stage2_only['in_stage2'] = True
            stage2_only['in_stage2c'] = stage2_only['pl_name'].isin(
                stages_data.get('stage2c', pd.DataFrame())['pl_name']
            )
            df_unified = pd.concat([df_unified, stage2_only], ignore_index=True)
    
    print(f"\n✓ Unified dataset created: {len(df_unified):,} total unique planets")
    print(f"\n  Stage membership:")
    for stage in ['stage1', 'stage1c', 'stage2', 'stage2c']:
        count = df_unified[f'in_{stage}'].sum()
        pct = (count / len(df_unified) * 100)
        print(f"    • {stage}: {count:,} planets ({pct:.1f}%)")
    
    return df_unified


def import_stars(conn, df):
    """Import unique stars."""
    print("\n" + "="*80)
    print("IMPORTING STARS")
    print("="*80)
    
    cursor = conn.cursor()
    stars_df = df[['hostname', 'sy_dist']].drop_duplicates('hostname').dropna(subset=['hostname'])
    
    print(f"Found {len(stars_df):,} unique host stars")
    
    star_id_map = {}
    insert_query = """
        INSERT INTO stars (hostname, sy_dist)
        VALUES (%s, %s)
        ON CONFLICT (hostname) DO UPDATE 
        SET sy_dist = EXCLUDED.sy_dist
        RETURNING star_id, hostname
    """
    
    for idx, row in stars_df.iterrows():
        cursor.execute(insert_query, (row['hostname'], row['sy_dist']))
        star_id, hostname = cursor.fetchone()
        star_id_map[hostname] = star_id
    
    conn.commit()
    print(f"✓ Imported {len(star_id_map):,} stars")
    cursor.close()
    return star_id_map


def import_planets_unified(conn, df, star_id_map):
    """Import planets with stage membership tracking."""
    print("\n" + "="*80)
    print("IMPORTING PLANETS (WITH STAGE TRACKING)")
    print("="*80)
    
    cursor = conn.cursor()
    planet_id_map = {}
    
    insert_query = """
        INSERT INTO planets (
            pl_name, star_id, pl_masse, pl_rade, pl_orbper, pl_eqt, density,
            in_stage1, in_stage1c, in_stage2, in_stage2c
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pl_name) DO UPDATE
        SET pl_masse = EXCLUDED.pl_masse,
            pl_rade = EXCLUDED.pl_rade,
            pl_orbper = EXCLUDED.pl_orbper,
            pl_eqt = EXCLUDED.pl_eqt,
            density = EXCLUDED.density,
            in_stage1 = EXCLUDED.in_stage1,
            in_stage1c = EXCLUDED.in_stage1c,
            in_stage2 = EXCLUDED.in_stage2,
            in_stage2c = EXCLUDED.in_stage2c
        RETURNING planet_id, pl_name
    """
    
    inserted = 0
    for idx, row in df.iterrows():
        if row['hostname'] not in star_id_map:
            continue
        
        star_id = star_id_map[row['hostname']]
        
        # Handle NaN values
        pl_masse = None if pd.isna(row.get('pl_masse')) else float(row['pl_masse'])
        pl_rade = None if pd.isna(row.get('pl_rade')) else float(row['pl_rade'])
        pl_orbper = None if pd.isna(row.get('pl_orbper')) else float(row['pl_orbper'])
        pl_eqt = None if pd.isna(row.get('pl_eqt')) else float(row['pl_eqt'])
        density = None if pd.isna(row.get('density')) else float(row['density'])
        
        try:
            cursor.execute(insert_query, (
                row['pl_name'], star_id, pl_masse, pl_rade, pl_orbper, pl_eqt, density,
                bool(row['in_stage1']), bool(row['in_stage1c']),
                bool(row['in_stage2']), bool(row['in_stage2c'])
            ))
            planet_id, pl_name = cursor.fetchone()
            planet_id_map[pl_name] = planet_id
            inserted += 1
            
            if inserted % 100 == 0:
                print(f"  Processed {inserted:,} planets...", end='\r')
        except Exception as e:
            print(f"\n⚠ Error inserting {row['pl_name']}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    print(f"\n✓ Imported {inserted:,} planets with stage tracking")
    cursor.close()
    return planet_id_map


def import_discoveries(conn, df, planet_id_map):
    """Import discovery records."""
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
    """
    
    inserted = 0
    for idx, row in df.iterrows():
        if row['pl_name'] not in planet_id_map:
            continue
        
        planet_id = planet_id_map[row['pl_name']]
        discoverymethod = None if pd.isna(row.get('discoverymethod')) else str(row['discoverymethod'])
        disc_year = None if pd.isna(row.get('disc_year')) else int(row['disc_year'])
        
        try:
            cursor.execute(insert_query, (planet_id, discoverymethod, disc_year))
            inserted += 1
        except Exception as e:
            print(f"\n⚠ Error: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    print(f"✓ Imported {inserted:,} discovery records")
    cursor.close()


def verify_import(conn):
    """Verify import with stage-specific queries."""
    print("\n" + "="*80)
    print("VERIFYING IMPORT")
    print("="*80)
    
    cursor = conn.cursor()
    
    # Total counts
    for table in ['stars', 'planets', 'discoveries']:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"\n{table.upper()}: {count:,} records")
    
    # Stage-specific counts
    print("\n" + "-"*80)
    print("Planets by Stage:")
    for stage in ['stage1', 'stage1c', 'stage2', 'stage2c']:
        cursor.execute(f"SELECT COUNT(*) FROM planets WHERE in_{stage} = TRUE")
        count = cursor.fetchone()[0]
        print(f"  • {stage}: {count:,} planets")
    
    # Sample query
    print("\n" + "-"*80)
    print("Sample: Top 5 planets in Stage 2c with highest mass:")
    cursor.execute("""
        SELECT p.pl_name, p.pl_masse, p.density, s.hostname
        FROM planets p
        JOIN stars s ON p.star_id = s.star_id
        WHERE p.in_stage2c = TRUE AND p.pl_masse IS NOT NULL
        ORDER BY p.pl_masse DESC
        LIMIT 5
    """)
    
    for name, mass, density, host in cursor.fetchall():
        # FIX: Check if density is None before formatting
        density_str = f"{density:.2f}" if density is not None else "N/A"
        print(f"  • {name}: {mass:.2f} Earth masses, density {density_str} (host: {host})")
    
    cursor.close()
    print("\n" + "="*80)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("\n" + "="*80)
    print("NASA EXOPLANET DATABASE IMPORT (ALL STAGES)")
    print("="*80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load all stages
    stages_data = load_all_stages()
    
    # Create unified dataset
    df_unified = create_unified_dataset(stages_data)
    
    # Get password
    print("\n" + "="*80)
    print(f"Database: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    print(f"User: {DB_CONFIG['user']}")
    password = getpass.getpass(f"\nEnter password for user '{DB_CONFIG['user']}': ")
    
    if not password:
        print("\n✗ Error: Password cannot be empty")
        sys.exit(1)
    
    DB_CONFIG['password'] = password
    
    # Connect
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Connected to database")
        
        # Import data
        star_id_map = import_stars(conn, df_unified)
        planet_id_map = import_planets_unified(conn, df_unified, star_id_map)
        import_discoveries(conn, df_unified, planet_id_map)
        
        # Verify
        verify_import(conn)
        
        print("\n" + "="*80)
        print("✓ IMPORT COMPLETED SUCCESSFULLY")
        print("="*80)
        print("\nAll four stages are now tracked in the database!")
        print("Use queries like: SELECT * FROM planets WHERE in_stage1c = TRUE")
        print("="*80 + "\n")
        
        conn.close()
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()