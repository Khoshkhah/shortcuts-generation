"""
Trace the true shortest path and show cell resolutions.
"""

import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import shortest_path
from pathlib import Path
import h3

PROJECT_ROOT = Path(__file__).parent.parent
DISTRICT = "Somerset"

# Target shortcut
TARGET_A = 5
TARGET_B = 933

def get_lca_and_cell(incoming_cell, outgoing_cell):
    """Compute LCA and its resolution."""
    if incoming_cell == 0 or outgoing_cell == 0:
        return 0, -1
    in_str = h3.int_to_str(incoming_cell)
    out_str = h3.int_to_str(outgoing_cell)
    min_res = min(h3.get_resolution(in_str), h3.get_resolution(out_str))
    for res in range(min_res, -1, -1):
        if h3.cell_to_parent(in_str, res) == h3.cell_to_parent(out_str, res):
            return h3.str_to_int(h3.cell_to_parent(in_str, res)), res
    return 0, -1

def main():
    print("=" * 70)
    print(f"ANALYZING SHORTEST PATH: {TARGET_A} → {TARGET_B}")
    print("=" * 70)
    
    # Load edges
    edges_df = pd.read_csv(PROJECT_ROOT / f"data/{DISTRICT}_driving_simplified_edges_with_h3.csv")
    edges_df['cost'] = edges_df['length'] / edges_df['maxspeed'].replace(0, np.inf)
    
    # Load graph
    graph_df = pd.read_csv(PROJECT_ROOT / f"data/{DISTRICT}_driving_edge_graph.csv")
    edge_costs = edges_df.set_index('id')['cost'].to_dict()
    graph_df['cost'] = graph_df['incoming_edge'].map(edge_costs)
    
    # Build sparse matrix
    all_edges = pd.concat([graph_df['incoming_edge'], graph_df['outgoing_edge']]).unique()
    n_edges = len(all_edges)
    edge_to_idx = {e: i for i, e in enumerate(all_edges)}
    idx_to_edge = {i: e for e, i in edge_to_idx.items()}
    
    src_indices = graph_df['incoming_edge'].map(edge_to_idx).values
    dst_indices = graph_df['outgoing_edge'].map(edge_to_idx).values
    costs = graph_df['cost'].values
    
    graph_matrix = csr_matrix((costs, (src_indices, dst_indices)), shape=(n_edges, n_edges))
    
    # Compute shortest paths
    print("\nComputing shortest paths...")
    dist_matrix, predecessors = shortest_path(
        csgraph=graph_matrix, 
        method='auto', 
        directed=True, 
        return_predecessors=True
    )
    
    # Get shortest path
    src_idx = edge_to_idx[TARGET_A]
    dst_idx = edge_to_idx[TARGET_B]
    
    # Reconstruct path (backwards from destination)
    path_indices = []
    current = dst_idx
    while current != src_idx:
        path_indices.append(current)
        pred = predecessors[src_idx, current]
        if pred == -9999:
            print("No path exists!")
            return
        current = pred
    path_indices.append(src_idx)
    path_indices.reverse()
    
    path_edges = [idx_to_edge[i] for i in path_indices]
    
    print(f"\n{'='*70}")
    print(f"SHORTEST PATH: {len(path_edges)} edges")
    print(f"Total cost: {dist_matrix[src_idx, dst_idx]:.4f}")
    print(f"{'='*70}\n")
    
    # Analyze each edge in path
    edge_info = edges_df.set_index('id')[['incoming_cell', 'outgoing_cell', 'lca_res', 'cost']].to_dict('index')
    
    print(f"{'Edge':<6} {'lca_res':<8} {'Cost':<10} {'Cumulative':<12} {'incoming→outgoing cells'}")
    print("-" * 80)
    
    cumulative = 0
    for edge_id in path_edges:
        info = edge_info.get(edge_id, {})
        lca_res = info.get('lca_res', 'N/A')
        cost = info.get('cost', 0)
        cumulative += cost
        in_cell = info.get('incoming_cell', 0)
        out_cell = info.get('outgoing_cell', 0)
        
        # Get cell at lca_res (abbreviated)
        if in_cell and out_cell:
            in_str = h3.int_to_str(in_cell)[-6:]
            out_str = h3.int_to_str(out_cell)[-6:]
            cell_str = f"...{in_str} → ...{out_str}"
        else:
            cell_str = "N/A"
            
        print(f"{edge_id:<6} {lca_res:<8} {cost:<10.4f} {cumulative:<12.4f} {cell_str}")
    
    # Show resolution pattern
    lca_res_sequence = [edge_info.get(e, {}).get('lca_res', -1) for e in path_edges]
    print(f"\n{'='*70}")
    print(f"lca_res SEQUENCE: {lca_res_sequence}")
    print(f"MIN lca_res in path: {min(lca_res_sequence)}")
    print(f"MAX lca_res in path: {max(lca_res_sequence)}")
    print(f"{'='*70}")
    
    # Compare with shortcut edges A and B
    a_info = edge_info.get(TARGET_A, {})
    b_info = edge_info.get(TARGET_B, {})
    print(f"\nEdge A (source): lca_res={a_info.get('lca_res')}")
    print(f"Edge B (target): lca_res={b_info.get('lca_res')}")
    print(f"Shortcut lca_res = max({a_info.get('lca_res')}, {b_info.get('lca_res')}) = {max(a_info.get('lca_res', 0), b_info.get('lca_res', 0))}")
    
    # Analysis
    min_path_res = min(lca_res_sequence)
    shortcut_lca = max(a_info.get('lca_res', 0), b_info.get('lca_res', 0))
    
    print(f"\n{'='*70}")
    print("ANALYSIS:")
    print(f"{'='*70}")
    if min_path_res < shortcut_lca:
        print(f"⚠️  The shortest path goes through edges with lca_res={min_path_res}")
        print(f"    But the shortcut can only be processed at R >= {shortcut_lca}")
        print(f"    This is why the algorithm can't find the optimal path!")
        print(f"\n    The optimal path requires processing at resolution {min_path_res}")
        print(f"    but the shortcut is 'confined' to resolution {shortcut_lca}+")
    else:
        print("✓ The shortest path stays within expected resolutions")


if __name__ == "__main__":
    main()
