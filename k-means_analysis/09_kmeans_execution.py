import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from pandas.plotting import parallel_coordinates
import getpass
import os

# --- CONFIGURATION: HARDCODED K VALUES ---
# We use k=3 for Stage 1 (Broad analysis: Radius/Period)
# We use k=4 for Stage 2 (Deep analysis: Mass/Density reveals more groups)
K_VALUES = {
    "Stage 1": 3,
    "Stage 1c": 3,
    "Stage 2": 4,
    "Stage 2c": 4
}

DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'exoplanet_db',
    'user': 'postgres'
}

def get_db_connection():
    try:
        # Only ask for password once
        if 'PG_PASSWORD' in os.environ:
            pwd = os.environ['PG_PASSWORD']
        else:
            pwd = getpass.getpass(prompt='Enter PostgreSQL Database Password: ')
            os.environ['PG_PASSWORD'] = pwd # Cache it for session
            
        return psycopg2.connect(**DB_PARAMS, password=pwd)
    except Exception as e:
        print(f"Connection error: {e}")
        return None

def get_data(conn, stage_name):
    """Fetches data and handles feature selection per stage."""
    
    # Define features based on stage
    if "Stage 1" in stage_name:
        features = ['pl_rade', 'pl_orbper', 'pl_eqt']
        db_col = "in_stage1" if stage_name == "Stage 1" else "in_stage1c"
    else:
        features = ['pl_masse', 'pl_rade', 'pl_orbper', 'pl_eqt', 'density']
        db_col = "in_stage2" if stage_name == "Stage 2" else "in_stage2c"

    # Fetch data
    # Note: We select specific columns to ensure we have the raw data for plotting
    query = f"""
        SELECT planet_id, pl_name, {', '.join(features)} 
        FROM planets 
        WHERE {db_col} = TRUE
    """
    df = pd.read_sql(query, conn)
    
    # Drop rows that are missing ANY of the required features
    df_clean = df.dropna(subset=features).copy()
    
    return df_clean, features

def run_kmeans_and_save(df, features, k, stage_name, conn):
    """Fits K-Means, saves to DB, and creates plots."""
    print(f"   > Running K-Means (k={k}) on {len(df)} planets...")
    
    # 1. Preprocess (Log Transform + Scaling)
    X = df[features].copy()
    for col in features:
        # Log transform to handle astronomical scales
        X[col] = np.log10(X[col] + 1e-6)
        
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # 2. Fit Model
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X_scaled)
    
    # Attach labels to dataframe for plotting and saving
    df['cluster_id'] = labels
    
    # 3. Save to Database
    # Only save to DB if it's the "Final" stage (Stage 2c) or if you want to overwrite
    # For now, we update all, but usually, you want the 'best' stage to be the official one.
    if stage_name == "Stage 2c":
        print("   > Updating database 'cluster_id' column...")
        cursor = conn.cursor()
        # Batch update is safer/faster for 2000+ rows
        # We construct a list of tuples (cluster_id, planet_id)
        update_values = list(zip(df['cluster_id'].astype(int), df['planet_id'].astype(int)))
        
        cursor.executemany("""
            UPDATE planets 
            SET cluster_id = %s 
            WHERE planet_id = %s;
        """, update_values)
        conn.commit()
        print("   > Database updated successfully.")

    # 4. Visualization
    output_dir = 'query_results/clusters'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # A. Scatterplot (Mass vs Radius or Radius vs Period)
    plt.figure(figsize=(10, 6))
    
    # Dynamic axis selection
    if 'pl_masse' in features:
        x_col, y_col = 'pl_masse', 'pl_rade'
        x_lbl, y_lbl = 'Mass (Earth Masses)', 'Radius (Earth Radii)'
    else:
        x_col, y_col = 'pl_orbper', 'pl_rade'
        x_lbl, y_lbl = 'Orbital Period (Days)', 'Radius (Earth Radii)'
        
    sns.scatterplot(
        data=df, x=x_col, y=y_col, 
        hue='cluster_id', palette='viridis', 
        style='cluster_id', s=80, alpha=0.8
    )
    plt.xscale('log')
    plt.yscale('log')
    plt.title(f'K-Means Clusters: {stage_name} (k={k})')
    plt.xlabel(x_lbl)
    plt.ylabel(y_lbl)
    plt.grid(True, which="both", ls="--", alpha=0.2)
    plt.savefig(f'{output_dir}/scatter_{stage_name.lower().replace(" ", "_")}.png')
    plt.close()

    # B. Parallel Coordinates
    plt.figure(figsize=(12, 6))
    # Normalize data just for this plot (Z-score)
    df_norm = df[features + ['cluster_id']].copy()
    for col in features:
        df_norm[col] = np.log10(df_norm[col] + 1e-6)
        df_norm[col] = (df_norm[col] - df_norm[col].mean()) / df_norm[col].std()
    
    parallel_coordinates(df_norm, 'cluster_id', colormap='viridis', alpha=0.4)
    plt.title(f'Cluster Profiles: {stage_name}')
    plt.ylabel('Z-Score (Standardized Log Value)')
    plt.xticks(rotation=15)
    plt.savefig(f'{output_dir}/parallel_{stage_name.lower().replace(" ", "_")}.png')
    plt.close()
    
    print(f"   > Visualizations saved to {output_dir}/")

if __name__ == "__main__":
    conn = get_db_connection()
    if conn:
        print("="*60)
        print("STARTING K-MEANS CLUSTERING PIPELINE")
        print("="*60)
        
        # Loop through all stages defined in our config
        for stage, k in K_VALUES.items():
            print(f"\nProcessing {stage}...")
            
            # 1. Get Data
            df, feats = get_data(conn, stage)
            
            if len(df) > 10:
                # 2. Run Analysis
                run_kmeans_and_save(df, feats, k, stage, conn)
            else:
                print(f"⚠️  Skipping {stage}: Not enough data ({len(df)} rows).")
                print("   (Hint: Did you run the fixed import script to populate 'density'?)")
        
        conn.close()
        print("\n" + "="*60)
        print("PIPELINE COMPLETE")
        print("Check 'query_results/clusters/' for your images.")