import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import getpass
import sys
import os

# --- CONFIGURATION ---
STAGES = {
    "Stage 1":  {"k": 3, "col": "cluster_s1",  "db_flag": "in_stage1",  "feats": ['pl_rade', 'pl_orbper', 'pl_eqt']},
    "Stage 1c": {"k": 3, "col": "cluster_s1c", "db_flag": "in_stage1c", "feats": ['pl_rade', 'pl_orbper', 'pl_eqt']},
    "Stage 2":  {"k": 4, "col": "cluster_s2",  "db_flag": "in_stage2",  "feats": ['pl_masse', 'pl_rade', 'pl_orbper', 'pl_eqt', 'density']},
    "Stage 2c": {"k": 4, "col": "cluster_s2c", "db_flag": "in_stage2c", "feats": ['pl_masse', 'pl_rade', 'pl_orbper', 'pl_eqt', 'density']}
}

DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'exoplanet_db',
    'user': 'postgres'
}

def get_db_connection():
    try:
        if 'PG_PASSWORD' in os.environ:
            pwd = os.environ['PG_PASSWORD']
        else:
            pwd = getpass.getpass(prompt='Enter PostgreSQL Database Password: ')
            os.environ['PG_PASSWORD'] = pwd
        return psycopg2.connect(**DB_PARAMS, password=pwd)
    except Exception as e:
        print(f"Connection error: {e}")
        sys.exit(1)

def update_schema(conn):
    print("   > Checking database schema...")
    cursor = conn.cursor()
    for stage, config in STAGES.items():
        col = config['col']
        try:
            cursor.execute(f"ALTER TABLE planets ADD COLUMN IF NOT EXISTS {col} INTEGER;")
        except Exception:
            conn.rollback()
    conn.commit()

def run_clustering(conn, stage_name):
    config = STAGES[stage_name]
    print(f"\nProcessing {stage_name} (k={config['k']})...")
    
    # 1. Fetch & Clean
    features = config['feats']
    query = f"SELECT planet_id, pl_name, {', '.join(features)} FROM planets WHERE {config['db_flag']} = TRUE"
    df = pd.read_sql(query, conn).dropna(subset=features).copy()
    
    if len(df) < 10: return None

    # 2. Preprocess & Fit
    X = df[features].copy()
    for col in features: X[col] = np.log10(X[col] + 1e-6)
    
    scaler = StandardScaler()
    labels = KMeans(n_clusters=config['k'], random_state=42, n_init=10).fit_predict(scaler.fit_transform(X))
    df['cluster_label'] = labels
    
    # 3. Save
    cursor = conn.cursor()
    update_data = list(zip(df['cluster_label'].astype(int), df['planet_id'].astype(int)))
    cursor.executemany(f"UPDATE planets SET {config['col']} = %s WHERE planet_id = %s", update_data)
    conn.commit()
    
    return df

def get_stage_stats(conn, stage_name):
    config = STAGES[stage_name]
    col = config['col']
    
    query = f"""
        SELECT 
            {col} as cluster_id,
            COUNT(*) as count,
            AVG(pl_rade) as avg_rad,
            AVG(pl_orbper) as avg_period,
            AVG(pl_eqt) as avg_temp
        FROM planets
        WHERE {col} IS NOT NULL
        GROUP BY {col}
    """
    df = pd.read_sql(query, conn)
    if df.empty: return None
    
    # --- LOGIC: Sort by Radius to creating "Ranking" ---
    # Rank 1 = Smallest Cluster, Rank 4 = Largest Cluster
    df = df.sort_values('avg_rad').reset_index(drop=True)
    df['Size Rank'] = df.index + 1  
    
    # Create a nice label for the legend
    # e.g. "1 (Smallest)" vs "4 (Largest)"
    df['Cluster Group'] = df['Size Rank'].apply(lambda x: f"Cluster #{x} (by Radius)")
    
    df['Stage'] = stage_name
    return df

def generate_comparison_dashboard(all_stats_df):
    """Creates a dashboard that distinctly shows 3 vs 4 bars."""
    print("\nGenerating Corrected Dashboard...")
    
    sns.set_style("whitegrid")
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Multi-Stage Cluster Analysis: Distinct Cluster Comparison', fontsize=20, weight='bold', y=0.95)
    
    # Custom Palette: Blue -> Green -> Orange -> Red
    # Ensures consistent coloring across stages
    rank_palette = {
        "Cluster #1 (by Radius)": "#4575b4", # Deep Blue (Smallest)
        "Cluster #2 (by Radius)": "#91bfdb", # Light Blue
        "Cluster #3 (by Radius)": "#fee090", # Yellow/Orange
        "Cluster #4 (by Radius)": "#d73027"  # Red (Largest)
    }
    
    metrics = [
        ('count', 'Cluster Population (Count)'),
        ('avg_rad', 'Avg Radius (Earth Radii)'),
        ('avg_period', 'Avg Orbital Period (Days)'),
        ('avg_temp', 'Avg Temperature (Kelvin)')
    ]
    
    for idx, (col, title) in enumerate(metrics):
        ax = axes[idx // 2, idx % 2]
        
        # KEY CHANGE: This guarantees distinct bars for every cluster rank
        sns.barplot(
            data=all_stats_df, 
            x='Stage', 
            y=col, 
            hue='Cluster Group', 
            palette=rank_palette, 
            ax=ax,
            edgecolor='black',
            linewidth=1
        )
        
        ax.set_title(title, fontsize=14, weight='bold')
        ax.set_xlabel('')
        
        # Log scale for Radius/Period
        if col in ['avg_rad', 'avg_period']:
            ax.set_yscale('log')
            
        # Legend management
        if idx == 0:
            ax.legend(title='Cluster Rank (Small -> Large)', loc='upper right')
        else:
            ax.legend().remove()
            
    plt.tight_layout(rect=[0, 0.03, 1, 0.93])
    
    if not os.path.exists('query_results'): os.makedirs('query_results')
    plt.savefig('query_results/comparison_dashboard_v3.png', dpi=300)
    print("✓ Dashboard saved to: query_results/comparison_dashboard_v3.png")

if __name__ == "__main__":
    conn = get_db_connection()
    update_schema(conn)
    
    # 1. Run Clustering
    for stage in STAGES.keys():
        run_clustering(conn, stage)
        
    # 2. Collect Stats
    all_stats = []
    print("\nCollecting Statistics...")
    for stage in STAGES.keys():
        stats = get_stage_stats(conn, stage)
        if stats is not None:
            all_stats.append(stats)
            
    # 3. Generate Chart
    if all_stats:
        full_df = pd.concat(all_stats)
        generate_comparison_dashboard(full_df)
    else:
        print("No stats collected.")
        
    conn.close()