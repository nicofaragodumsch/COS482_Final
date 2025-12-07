# NASA Exoplanet Database Setup Instructions

## Entity-Relationship Diagram

```
┌─────────────────┐
│     STARS       │
├─────────────────┤
│ star_id (PK)    │
│ hostname        │
│ sy_dist         │
│ created_at      │
│ updated_at      │
└─────────┬───────┘
          │
          │ 1:M (one star, many planets)
          │
          ▼
┌─────────────────┐
│    PLANETS      │
├─────────────────┤
│ planet_id (PK)  │
│ pl_name         │
│ star_id (FK)    │◄──────┐
│ pl_masse        │       │
│ pl_rade         │       │
│ pl_orbper       │       │
│ pl_eqt          │       │
│ density         │       │
│ created_at      │       │
│ updated_at      │       │
└─────────┬───────┘       │
          │               │
          │ 1:1           │ References
          │ (one planet,  │
          │  one          │
          │  discovery)   │
          │               │
          ▼               │
┌─────────────────┐       │
│  DISCOVERIES    │       │
├─────────────────┤       │
│ discovery_id(PK)│       │
│ planet_id (FK)  │───────┘
│ discoverymethod │
│ disc_year       │
│ created_at      │
│ updated_at      │
└─────────────────┘
```

## Relationships

1. **STARS → PLANETS** (One-to-Many)
   - One star can host multiple planets
   - Foreign key: `planets.star_id` → `stars.star_id`
   - Cascade: DELETE and UPDATE

2. **PLANETS → DISCOVERIES** (One-to-One)
   - Each planet has exactly one discovery record
   - Foreign key: `discoveries.planet_id` → `planets.planet_id`
   - Unique constraint on `discoveries.planet_id`
   - Cascade: DELETE and UPDATE

## Setup Instructions

### Step 1: Install PostgreSQL

**Windows:**
```bash
# Download from: https://www.postgresql.org/download/windows/
# Or use chocolatey:
choco install postgresql
```

**Mac:**
```bash
brew install postgresql
brew services start postgresql
```

**Linux:**
```bash
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib
sudo systemctl start postgresql
```

### Step 2: Install Python Dependencies

```bash
pip install psycopg2-binary sqlalchemy pandas
```

### Step 3: Create Database

Open PostgreSQL command line (psql):

```bash
# Connect to PostgreSQL
psql -U postgres

# In psql:
CREATE DATABASE exoplanet_db;

# Connect to the new database
\c exoplanet_db

# Exit psql
\q
```

### Step 4: Create Schema

Run the schema creation script:

```bash
# Option 1: From command line
psql -U postgres -d exoplanet_db -f 03_database_schema.sql

# Option 2: From psql
\c exoplanet_db
\i 03_database_schema.sql
```

### Step 5: Configure Database Connection

Edit the `DB_CONFIG` in both Python scripts:

**In `04_database_import.py`:**
```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'exoplanet_db',
    'user': 'postgres',           # Your PostgreSQL username
    'password': 'your_password'   # Your PostgreSQL password
}
```

**In `06_execute_queries.py`:** (same configuration)

### Step 6: Import Data

```bash
python 04_database_import.py
```

Expected output:
- ✓ Connected to database
- ✓ Imported ~XXX stars
- ✓ Imported ~1,300 planets
- ✓ Imported ~1,300 discoveries
- ✓ Verification complete

### Step 7: Execute Queries

```bash
python 06_execute_queries.py
```

This will:
- Execute all 8 predefined queries
- Export results to `query_results/` directory
- Generate a summary report

### Step 8: Verify Installation

In psql:

```sql
-- Check tables exist
\dt

-- Count records
SELECT 'Stars' AS table, COUNT(*) FROM stars
UNION ALL
SELECT 'Planets', COUNT(*) FROM planets
UNION ALL
SELECT 'Discoveries', COUNT(*) FROM discoveries;

-- Test a join
SELECT p.pl_name, s.hostname, d.discoverymethod
FROM planets p
JOIN stars s ON p.star_id = s.star_id
JOIN discoveries d ON p.planet_id = d.planet_id
LIMIT 5;
```

## Troubleshooting

### Connection Error: "password authentication failed"

1. Check PostgreSQL is running:
   ```bash
   # Mac
   brew services list
   
   # Linux
   sudo systemctl status postgresql
   
   # Windows
   # Check Services app for "postgresql" service
   ```

2. Reset password:
   ```bash
   psql -U postgres
   ALTER USER postgres WITH PASSWORD 'new_password';
   ```

3. Check `pg_hba.conf` file (allows local connections)

### Import Error: "relation does not exist"

Run the schema creation script first:
```bash
psql -U postgres -d exoplanet_db -f 03_database_schema.sql
```

### Foreign Key Violations

This means data inconsistency. The import script handles this, but if you're inserting manually:
- Ensure stars exist before adding planets
- Ensure planets exist before adding discoveries

### Encoding Issues

If you see encoding errors:
```python
# Add to connection:
conn = psycopg2.connect(**DB_CONFIG, client_encoding='utf8')
```

## Files Created

After complete setup:

```
project/
├── 03_database_schema.sql       # Schema creation
├── 04_database_import.py        # Data import script
├── 05_sql_queries.sql           # Manual query reference
├── 06_execute_queries.py        # Automated query execution
└── query_results/               # Exported query results
    ├── nearest_planets.csv
    ├── planets_by_method.csv
    ├── discoveries_by_year.csv
    ├── recent_massive_planets.csv
    ├── most_massive_by_method.csv
    ├── multi_planet_systems.csv
    ├── planet_classification.csv
    ├── earth_like_by_method.csv
    └── _query_summary.csv
```

## Database Schema Features

### Constraints

1. **Primary Keys:** Auto-incrementing SERIAL columns
2. **Foreign Keys:** With CASCADE delete/update
3. **Unique Constraints:** Planet names, star names
4. **Check Constraints:** 
   - Mass, radius, period > 0
   - Temperature: 0-5000 K
   - Density: 0.01-100 (relative to Earth)
   - Discovery year: 1990-2030

### Indexes

Optimized for common queries:
- Star hostname lookups
- Planet name searches
- Discovery method filtering
- Discovery year ranges
- Mass/radius/period comparisons

### Views

1. **vw_planets_complete:** Full planet data with joins
2. **vw_discovery_stats:** Summary by discovery method

### Triggers

Auto-update `updated_at` timestamp on record modification

## Query Examples

All queries demonstrate different SQL concepts required for the assignment:

1. **JOIN:** Combining planets with stars
2. **AGGREGATE:** COUNT, AVG by discovery method
3. **GROUP BY:** Timeline analysis by year
4. **HAVING:** Stars with multiple planets
5. **SUBQUERY:** Most massive planet per method
6. **CASE:** Planet classification
7. **CTE:** Complex earth-like planet analysis
8. **WINDOW FUNCTIONS:** Ranking within star systems

## Next Steps

After database setup:
1. Add `cluster_id` column to planets table (after K-means)
2. Run clustering analysis
3. Update planets table with cluster assignments
4. Run cluster-specific queries (Query 11 in 05_sql_queries.sql)

---