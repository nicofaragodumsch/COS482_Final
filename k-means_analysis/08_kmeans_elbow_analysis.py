import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import getpass
import os

# Database Connection Parameters
DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'exoplanet_db',
    'user': 'postgres'
}

def get_db_connection():
    """Prompts user for password and returns connection."""
    try:
        pwd = getpass.getpass(prompt='Enter PostgreSQL Database Password: ')
        conn = psycopg2.connect(**DB_PARAMS, password=pwd)
        return conn
    except Exception as e:
        print(f"Authentication failed: {e}")
        return None

def get_data_and_features(conn, stage_name):
    """
    Fetches data with different feature sets based on the stage.
    """
    print(f"Fetching data for {stage_name}...")
    
    # Map stage name to the database boolean flag
    stage_map = {
        "Stage 1": "in_stage1",
        "Stage 1c": "in_stage1c",
        "Stage 2": "in_stage2",
        "Stage 2c": "in_stage2c"
    }
    
    db_col = stage_map.get(stage_name)
    if not db_col:
        print(f"Error: Unknown stage {stage_name}")
        return pd.DataFrame(), []

    # --- FEATURE SELECTION LOGIC ---
    if "Stage 1" in stage_name:
        # Stage 1/1c: Focus on detection parameters (Radius, Period, Temp)
        # This allows us to keep planets that don't have Mass measurements yet.
        features = ['pl_rade', 'pl_orbper', 'pl_eqt']
        query_cols = "pl_name, pl_rade, pl_orbper, pl_eqt"
        
    else:
        # Stage 2/2c: Focus on physical properties (Mass, Density)
        # This restricts us to planets with complete physical profiles.
        features = ['pl_masse', 'pl_rade', 'pl_orbper', 'pl_eqt', 'density']
        query_cols = "pl_name, pl_masse, pl_rade, pl_orbper, pl_eqt, density"

    # Execute Query
    query = f"""
    SELECT {query_cols}
    FROM planets
    WHERE {db_col} = TRUE
    """
    
    df = pd.read_sql(query, conn)
    
    # Drop rows that miss ANY of the required features for this specific stage
    # (e.g. Stage 1 won't drop rows just because Mass is missing)
    df_clean = df.dropna(subset=features)
    
    dropped_count = len(df) - len(df_clean)
    if dropped_count > 0:
        print(f"  - Dropped {dropped_count} rows due to missing {features}")
        
    return df_clean, features

def preprocess_and_plot(df, features, stage_name):
    """Runs Elbow Method using the specific features for this stage."""
    if len(df) < 10:
        print(f"⚠️  Skipping {stage_name}: Not enough data points ({len(df)}).")
        return

    # 1. Log Transform
    # We apply log transform to all features except maybe density if it's already scaled?
    # Actually density spans orders of magnitude (0.1 g/cm3 to >100 g/cm3), so log is good.
    X = df[features].copy()
    
    for col in features:
        # Log10(x + epsilon)
        X[col] = np.log10(X[col] + 1e-6)

    # 2. Scale (StandardScaler)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 3. Calculate Inertia
    inertia = []
    k_range = range(1, 11)
    for k in k_range:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        kmeans.fit(X_scaled)
        inertia.append(kmeans.inertia_)

    # 4. Plot
    plt.figure(figsize=(8, 5))
    plt.plot(k_range, inertia, marker='o', linestyle='-', color='darkgreen', linewidth=2)
    plt.title(f'Elbow Method: {stage_name}\n(N={len(df)} | Features: {len(features)})')
    plt.xlabel('Number of Clusters (k)')
    plt.ylabel('Inertia (WCSS)')
    plt.xticks(k_range)
    plt.grid(True, alpha=0.3)
    
    # Save
    if not os.path.exists('query_results'):
        os.makedirs('query_results')
        
    safe_name = stage_name.lower().replace(' ', '_')
    plt.savefig(f'query_results/elbow_{safe_name}.png')
    print(f"✓ Saved plot: query_results/elbow_{safe_name}.png")
    plt.close()

if __name__ == "__main__":
    conn = get_db_connection()
    
    if conn:
        stages = ["Stage 1", "Stage 1c", "Stage 2", "Stage 2c"]
        
        for stage in stages:
            df, feature_list = get_data_and_features(conn, stage)
            if not df.empty:
                preprocess_and_plot(df, feature_list, stage)
        
        conn.close()
        print("\nAll elbow plots generated successfully.")