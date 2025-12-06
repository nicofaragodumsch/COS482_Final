"""
NASA Exoplanet Archive Data Collection Script
Date: December 2025

This script demonstrates best practices for:
- API access and web scraping principles
- Error handling and data validation
- Documentation and code organization
- Data export for reproducibility

Purpose:
--------
Queries the NASA Exoplanet Archive TAP (Table Access Protocol) service
to retrieve confirmed exoplanet data for clustering analysis.

Requirements:
------------
- requests
- pandas
- numpy (optional, for data validation)

API Documentation:
-----------------
https://exoplanetarchive.ipac.caltech.edu/docs/TAP/usingTAP.html
"""

import requests
import pandas as pd
import time
from datetime import datetime
import os

# ============================================================================
# CONFIGURATION
# ============================================================================

# NASA Exoplanet Archive TAP service endpoint
TAP_URL = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"

# Fields to retrieve from the Planetary Systems table
# Note: Using the composite table which contains confirmed planets
FIELDS = [
    "pl_name",          # Planet Name
    "pl_masse",         # Planet Mass (Earth masses)
    "pl_rade",          # Planet Radius (Earth radii)
    "pl_orbper",        # Orbital Period (days)
    "pl_eqt",           # Equilibrium Temperature (K)
    "sy_dist",          # Distance from Earth (parsecs)
    "discoverymethod",  # Discovery Method
    "disc_year",        # Discovery Year
    "hostname"          # Host Star Name
]

# Output file name
OUTPUT_FILE = "raw_exoplanets.csv"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def build_query(fields, table="ps", where_clause=""):
    """
    Build an ADQL (Astronomical Data Query Language) query string.
    
    Parameters:
    ----------
    fields : list
        List of column names to retrieve
    table : str
        Table name (default: "ps" for Planetary Systems)
    where_clause : str
        Optional WHERE clause for filtering
    
    Returns:
    -------
    str : Complete ADQL query
    """
    field_string = ", ".join(fields)
    query = f"SELECT {field_string} FROM {table}"
    
    if where_clause:
        query += f" WHERE {where_clause}"
    
    return query


def query_nasa_tap(query, output_format="csv", max_retries=3):
    """
    Execute a query against the NASA Exoplanet Archive TAP service.
    
    Parameters:
    ----------
    query : str
        ADQL query string
    output_format : str
        Desired output format (csv, json, votable, etc.)
    max_retries : int
        Number of retry attempts if request fails
    
    Returns:
    -------
    requests.Response : Response object from the API
    
    Raises:
    ------
    Exception : If all retry attempts fail
    """
    params = {
        "query": query,
        "format": output_format
    }
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting API request (attempt {attempt + 1}/{max_retries})...")
            response = requests.get(TAP_URL, params=params, timeout=60)
            response.raise_for_status()  # Raise exception for bad status codes
            print(f"✓ Request successful! Received {len(response.content)} bytes")
            return response
            
        except requests.exceptions.Timeout:
            print(f"✗ Request timed out on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"  Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                raise Exception("Max retries exceeded: Request timed out")
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Request failed on attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"  Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                raise Exception(f"Max retries exceeded: {str(e)}")


def validate_dataframe(df, required_columns):
    """
    Perform basic validation on the retrieved dataframe.
    
    Parameters:
    ----------
    df : pd.DataFrame
        DataFrame to validate
    required_columns : list
        List of required column names
    
    Returns:
    -------
    dict : Validation report with statistics
    """
    report = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "missing_columns": [],
        "missing_value_counts": {},
        "data_types": {}
    }
    
    # Check for missing columns
    for col in required_columns:
        if col not in df.columns:
            report["missing_columns"].append(col)
    
    # Count missing values per column
    for col in df.columns:
        missing_count = df[col].isna().sum()
        if missing_count > 0:
            report["missing_value_counts"][col] = {
                "count": int(missing_count),
                "percentage": round(100 * missing_count / len(df), 2)
            }
        report["data_types"][col] = str(df[col].dtype)
    
    return report


def print_validation_report(report):
    """
    Print a formatted validation report.
    
    Parameters:
    ----------
    report : dict
        Validation report from validate_dataframe()
    """
    print("\n" + "="*70)
    print("DATA VALIDATION REPORT")
    print("="*70)
    print(f"Total Rows: {report['total_rows']:,}")
    print(f"Total Columns: {report['total_columns']}")
    
    if report["missing_columns"]:
        print(f"\n⚠ Missing Columns: {', '.join(report['missing_columns'])}")
    else:
        print("\n✓ All required columns present")
    
    if report["missing_value_counts"]:
        print("\nMissing Values by Column:")
        for col, info in report["missing_value_counts"].items():
            print(f"  • {col}: {info['count']:,} ({info['percentage']}%)")
    else:
        print("\n✓ No missing values detected")
    
    print("\nData Types:")
    for col, dtype in report["data_types"].items():
        print(f"  • {col}: {dtype}")
    print("="*70 + "\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function for data collection.
    """
    print("\n" + "="*70)
    print("NASA EXOPLANET ARCHIVE DATA COLLECTION")
    print("="*70)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target Fields: {', '.join(FIELDS)}")
    print("="*70 + "\n")
    
    # Step 1: Build the query
    print("Step 1: Building ADQL query...")
    # Query the Planetary Systems (ps) table for confirmed planets
    # The default_flag column helps identify the default parameter set
    query = build_query(FIELDS, table="ps")
    print(f"Query: {query}\n")
    
    # Step 2: Execute the query
    print("Step 2: Querying NASA Exoplanet Archive TAP service...")
    try:
        response = query_nasa_tap(query, output_format="csv")
    except Exception as e:
        print(f"\n✗ Fatal Error: {str(e)}")
        print("Data collection failed. Exiting.")
        return
    
    # Step 3: Load data into pandas DataFrame
    print("\nStep 3: Loading data into Pandas DataFrame...")
    try:
        # Read CSV from response content
        from io import StringIO
        df = pd.read_csv(StringIO(response.text))
        print(f"✓ Successfully loaded {len(df):,} exoplanets")
        print(f"✓ Retrieved {len(df.columns)} columns")
    except Exception as e:
        print(f"✗ Error loading data into DataFrame: {str(e)}")
        return
    
    # Step 4: Validate the data
    print("\nStep 4: Validating data quality...")
    validation_report = validate_dataframe(df, FIELDS)
    print_validation_report(validation_report)
    
    # Step 5: Display sample of data
    print("Sample of Retrieved Data (first 5 rows):")
    print(df.head())
    print(f"\nShape: {df.shape[0]} rows × {df.shape[1]} columns\n")
    
    # Step 6: Save to CSV
    print(f"Step 5: Saving raw data to '{OUTPUT_FILE}'...")
    try:
        df.to_csv(OUTPUT_FILE, index=False)
        file_size = os.path.getsize(OUTPUT_FILE)
        print(f"✓ Successfully saved to {OUTPUT_FILE}")
        print(f"  File size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
    except Exception as e:
        print(f"✗ Error saving file: {str(e)}")
        return
    
    # Step 7: Summary statistics
    print("\n" + "="*70)
    print("COLLECTION SUMMARY")
    print("="*70)
    print(f"Total exoplanets collected: {len(df):,}")
    print(f"Date range: {df['disc_year'].min():.0f} - {df['disc_year'].max():.0f}")
    print(f"Unique host stars: {df['hostname'].nunique():,}")
    print(f"Discovery methods represented: {df['discoverymethod'].nunique()}")
    print("\nTop 5 Discovery Methods:")
    print(df['discoverymethod'].value_counts().head())
    print("="*70)
    print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Data collection complete! ✓\n")


if __name__ == "__main__":
    main()