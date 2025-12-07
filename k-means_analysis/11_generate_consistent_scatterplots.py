import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import getpass
import os
import sys

# --- CONFIGURATION ---
STAGES = {
    "Stage 1":  {"col": "cluster_s1",  "feats": ['pl_rade', 'pl_orbper', 'pl_eqt']},
    "Stage 1c": {"col": "cluster_s1c", "feats": ['pl_rade', 'pl_orbper', 'pl_eqt']},
    "Stage 2":  {"col": "cluster_s2",  "feats": ['pl_masse', 'pl_rade', 'pl_orbper', 'pl_eqt', 'density']},
    "Stage 2c": {"col": "cluster_s2c", "feats": ['pl_masse', 'pl_rade', 'pl_orbper', 'pl_eqt', 'density']}
}

# --- FIX: EXPANDED PALETTE TO HANDLE K=3 AND K=4 ---
RANK_PALETTE = {
    "Cluster #1 (Smallest)": "#4575b4", # Deep Blue (Rocky)
    "Cluster #2":            "#91bfdb", # Light Blue (Super-Earth)
    "Cluster #3":            "#fee090", # Yellow/Orange (Neptunian)
    
    # CASE K=3: The 3rd cluster is the largest (Gas Giants), so we make it RED
    "Cluster #3 (Largest)":  "#d73027", 
    
    # CASE K=4: The 4th cluster is the largest (Gas Giants), so it is RED
    "Cluster #4 (Largest)":  "#d73027"  
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

def generate_labeled_scatter(conn, stage_name):
    print(f"\nGenerating consistent scatterplot for {stage_name}...")
    config = STAGES[stage_name]
    db_col = config['col']
    
    features = config['feats']
    query = f"""
        SELECT pl_name, {db_col} as cluster_id, {', '.join(features)} 
        FROM planets 
        WHERE {db_col} IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    
    if df.empty:
        print("  Skipping (No data found)")
        return

    # --- RANKING LOGIC ---
    cluster_stats = df.groupby('cluster_id')['pl_rade'].mean().sort_values().reset_index()
    cluster_stats['Rank'] = cluster_stats.index + 1
    
    def get_label(rank, total):
        if rank == 1: return "Cluster #1 (Smallest)"
        if rank == total: return f"Cluster #{total} (Largest)"
        return f"Cluster #{rank}"

    id_to_label = {}
    total_clusters = len(cluster_stats)
    for _, row in cluster_stats.iterrows():
        label = get_label(int(row['Rank']), total_clusters)
        id_to_label[row['cluster_id']] = label
        
    df['Cluster Label'] = df['cluster_id'].map(id_to_label)
    df = df.sort_values('Cluster Label')

    # --- PLOTTING ---
    plt.figure(figsize=(12, 8))
    sns.set_style("whitegrid")
    
    if 'pl_masse' in features:
        x_col, y_col = 'pl_masse', 'pl_rade'
        x_lbl, y_lbl = 'Mass (Earth Masses)', 'Radius (Earth Radii)'
        title_extra = "Mass vs. Radius"
    else:
        x_col, y_col = 'pl_orbper', 'pl_rade'
        x_lbl, y_lbl = 'Orbital Period (Days)', 'Radius (Earth Radii)'
        title_extra = "Period vs. Radius"

    try:
        sns.scatterplot(
            data=df, 
            x=x_col, 
            y=y_col, 
            hue='Cluster Label', 
            style='Cluster Label', 
            palette=RANK_PALETTE,
            s=100, 
            alpha=0.8,
            edgecolor='black'
        )
    except ValueError as e:
        print(f"  Visual Error: {e}")
        print("  (This usually means a label in the data doesn't match the palette keys)")
        return

    plt.xscale('log')
    plt.yscale('log')
    plt.title(f"{stage_name}: K-Means Analysis ({title_extra})", fontsize=16, weight='bold')
    plt.xlabel(x_lbl, fontsize=12)
    plt.ylabel(y_lbl, fontsize=12)
    plt.legend(title="Cluster Group (Sorted by Size)", fontsize=11, title_fontsize=12, loc='upper left')
    
    if not os.path.exists('query_results/scatter_consistent'):
        os.makedirs('query_results/scatter_consistent')
        
    filename = f"query_results/scatter_consistent/{stage_name.lower().replace(' ', '_')}_labeled.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: {filename}")
    plt.close()

if __name__ == "__main__":
    conn = get_db_connection()
    print("="*60)
    print("GENERATING CONSISTENTLY LABELED SCATTERPLOTS")
    print("="*60)
    for stage in STAGES.keys():
        generate_labeled_scatter(conn, stage)
    conn.close()
    print("\nDone. Check 'query_results/scatter_consistent/' for your slides.")