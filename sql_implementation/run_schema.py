"""
Run Database Schema Creation
============================
This script executes the SQL schema file to create tables in PostgreSQL.
Password is requested securely at runtime.
"""

import psycopg2
import getpass
import sys
import os

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'exoplanet_db',
    'user': 'postgres'
}

SCHEMA_FILE = '03_database_schema.sql'

# Check multiple possible locations
POSSIBLE_LOCATIONS = [
    '03_database_schema.sql',  # Current directory
    'sql_implementation/03_database_schema.sql',  # Subdirectory
    os.path.join('sql_implementation', '03_database_schema.sql')  # OS-independent path
]

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("="*70)
    print("NASA EXOPLANET DATABASE - SCHEMA CREATION")
    print("="*70)
    
    # Find schema file
    schema_path = None
    for location in POSSIBLE_LOCATIONS:
        if os.path.exists(location):
            schema_path = location
            break
    
    if not schema_path:
        print(f"\n✗ Error: Schema file not found in any of these locations:")
        for loc in POSSIBLE_LOCATIONS:
            print(f"   • {loc}")
        print(f"\n   Current directory: {os.getcwd()}")
        sys.exit(1)
    
    print(f"\nDatabase Configuration:")
    print(f"  Host: {DB_CONFIG['host']}")
    print(f"  Port: {DB_CONFIG['port']}")
    print(f"  Database: {DB_CONFIG['database']}")
    print(f"  User: {DB_CONFIG['user']}")
    
    # Securely prompt for password
    password = getpass.getpass(f"\nEnter password for user '{DB_CONFIG['user']}': ")
    
    if not password:
        print("\n✗ Error: Password cannot be empty")
        sys.exit(1)
    
    # Add password to config
    DB_CONFIG['password'] = password
    
    print("\nConnecting to database...")
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Connected successfully")
        
        # Read SQL file
        print(f"\nReading schema file: {schema_path}")
        with open(schema_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        print(f"✓ Schema file loaded ({len(sql)} characters)")
        
        # Execute SQL
        print("\nExecuting schema creation...")
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        
        print("✓ Schema created successfully!")
        
        # Verify tables were created
        print("\nVerifying tables...")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
                AND table_name IN ('stars', 'planets', 'discoveries')
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        
        if len(tables) == 3:
            print("✓ All tables created:")
            for (table_name,) in tables:
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"  • {table_name}: {count} rows")
        else:
            print(f"⚠ Warning: Expected 3 tables, found {len(tables)}")
        
        # Close connection
        cursor.close()
        conn.close()
        
        print("\n" + "="*70)
        print("✓ SCHEMA CREATION COMPLETED SUCCESSFULLY")
        print("="*70)
        print("\nNext step: Run the data import script")
        print("  python 04_database_import.py")
        print("="*70 + "\n")
        
    except psycopg2.OperationalError as e:
        print(f"\n✗ Connection Error: {e}")
        print("\nPossible issues:")
        print("  • PostgreSQL is not running")
        print("  • Incorrect password")
        print("  • Database 'exoplanet_db' does not exist")
        print("\nTo create the database, run:")
        print('  createdb -U postgres exoplanet_db')
        print("  (or use SQL Shell and run: CREATE DATABASE exoplanet_db;)")
        sys.exit(1)
        
    except psycopg2.Error as e:
        print(f"\n✗ Database Error: {e}")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n✗ Unexpected Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()