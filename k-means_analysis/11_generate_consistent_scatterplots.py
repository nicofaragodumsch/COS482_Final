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

# Consistent Color Palette
RANK_PALETTE = {
    "Cluster #1 (Smallest)": "#4575b4", # Deep Blue
    "Cluster #2":            "#91bfdb", # Light Blue
    "Cluster #3":            "#fee090", # Yellow/Orange
    "Cluster #3 (Largest)":  "#d73027", # Red (for k=3)
    "Cluster #4 (Largest)":  "#d73027"  # Red (for k=4)
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

def plot_scatter(df, x_col, y_col, x_lbl, y_lbl, title, filename):
    """Generic plotting function to avoid code duplication."""
    plt.figure(figsize=(12, 8))
    sns.set_style("whitegrid")

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
    except ValueError:
        return

    plt.xscale('log')
    plt.yscale('log')
    plt.title(title, fontsize=16, weight='bold')
    plt.xlabel(x_lbl, fontsize=12)
    plt.ylabel(y_lbl, fontsize=12)
    plt.legend(title="Cluster Group (Sorted by Size)", fontsize=11, loc='upper left')
    
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: {filename}")
    plt.close()

def generate_plots_for_stage(conn, stage_name):
    print(f"\nProcessing {stage_name}...")
    config = STAGES[stage_name]
    db_col = config['col']
    features = config['feats']
    
    # Fetch Data
    query = f"SELECT pl_name, {db_col} as cluster_id, {', '.join(features)} FROM planets WHERE {db_col} IS NOT NULL"
    df = pd.read_sql(query, conn)
    if df.empty: return

    # --- RANKING LOGIC (Same as before) ---
    cluster_stats = df.groupby('cluster_id')['pl_rade'].mean().sort_values().reset_index()
    cluster_stats['Rank'] = cluster_stats.index + 1
    
    def get_label(rank, total):
        if rank == 1: return "Cluster #1 (Smallest)"
        if rank == total: return f"Cluster #{total} (Largest)"
        return f"Cluster #{rank}"

    id_to_label = {}
    total_clusters = len(cluster_stats)
    for _, row in cluster_stats.iterrows():
        id_to_label[row['cluster_id']] = get_label(int(row['Rank']), total_clusters)
        
    df['Cluster Label'] = df['cluster_id'].map(id_to_label)
    df = df.sort_values('Cluster Label')

    # Ensure output directory exists
    out_dir = 'query_results/scatter_consistent'
    if not os.path.exists(out_dir): os.makedirs(out_dir)

    # --- PLOT 1: PERIOD vs RADIUS (Generated for ALL Stages) ---
    plot_scatter(
        df, 'pl_orbper', 'pl_rade', 
        'Orbital Period (Days)', 'Radius (Earth Radii)', 
        f"{stage_name}: Period vs. Radius",
        f"{out_dir}/{stage_name.lower().replace(' ', '_')}_period_radius.png"
    )

    # --- PLOT 2: MASS vs RADIUS (Only for Stages 2 & 2c) ---
    if 'pl_masse' in features:
        plot_scatter(
            df, 'pl_masse', 'pl_rade', 
            'Mass (Earth Masses)', 'Radius (Earth Radii)', 
            f"{stage_name}: Mass vs. Radius",
            f"{out_dir}/{stage_name.lower().replace(' ', '_')}_mass_radius.png"
        )

if __name__ == "__main__":
    conn = get_db_connection()
    print("="*60)
    print("GENERATING STANDARDIZED SCATTERPLOTS")
    print("="*60)
    for stage in STAGES.keys():
        generate_plots_for_stage(conn, stage)
    conn.close()