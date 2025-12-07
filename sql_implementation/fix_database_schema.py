import psycopg2
import getpass

# Database Configuration
DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'exoplanet_db',
    'user': 'postgres'
}

def add_cluster_column():
    """Adds the missing cluster_id column to the planets table."""
    try:
        pwd = getpass.getpass(prompt='Enter PostgreSQL Database Password: ')
        conn = psycopg2.connect(**DB_PARAMS, password=pwd)
        conn.autocommit = True # Enable autocommit for DDL statements
        cursor = conn.cursor()
        
        print("Checking database schema...")
        
        # 1. Add the column
        try:
            print("Attempting to add 'cluster_id' column...")
            cursor.execute("ALTER TABLE planets ADD COLUMN cluster_id INTEGER;")
            print("✓ Success: Column 'cluster_id' added.")
        except psycopg2.errors.DuplicateColumn:
            print("ℹ Note: Column 'cluster_id' already exists.")

        # 2. Add an index for performance (Optional but good practice)
        try:
            print("Creating index for faster queries...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_planets_cluster ON planets(cluster_id);")
            print("✓ Success: Index created.")
        except Exception as e:
            print(f"Warning: Could not create index: {e}")

        conn.close()
        print("\nDatabase is now ready for K-Means results!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    add_cluster_column()