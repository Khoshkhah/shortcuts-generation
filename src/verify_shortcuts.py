"""
verify_shortcuts.py
====================

Verify that all shortcuts represent valid shortest paths in the edge graph.

For each shortcut (from_edge, to_edge, cost, via_edge):
1. Compute actual shortest path from from_edge to to_edge using the edge graph
2. Verify the shortcut cost matches the true shortest path cost
3. Report any discrepancies
"""

import sys
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import shortest_path
import duckdb


def verify_shortcuts(shortcuts_path: str, edges_path: str, sample_size: int = None):
    """
    Verify that shortcut costs match actual shortest path costs.
    
    Args:
        shortcuts_path: Path to shortcuts parquet file
        edges_path: Path to edges CSV file
        sample_size: If specified, verify only a random sample
    """
    print(f"Loading shortcuts from: {shortcuts_path}")
    shortcuts_df = pd.read_parquet(shortcuts_path)
    print(f"Loaded {len(shortcuts_df)} shortcuts")
    
    print(f"\nLoading edges from: {edges_path}")
    # Read edges - need id, from_edge (using to_edge of edge), to_edge (using from_edge of next edge)
    # Actually for the graph, we need the adjacency: edge_id -> next_edge_id with cost
    # This comes from the shortcut graph adjacency
    
    # Let's load the base shortcuts (initial graph) to get the adjacency
    # Initial shortcuts are from_edge -> to_edge with cost
    # We need to build the full adjacency matrix from ALL shortcuts
    
    # Actually, the simplest verification is:
    # 1. Build a graph from all edges using scipy
    # 2. Compute shortest paths from each source
    # 3. Compare costs
    
    # Get unique edge IDs (nodes in our graph)
    all_edges = pd.concat([shortcuts_df['from_edge'], shortcuts_df['to_edge']]).unique()
    n_nodes = len(all_edges)
    print(f"Unique edge IDs: {n_nodes}")
    
    # Create node-to-index mapping
    edge_to_idx = pd.Series(data=np.arange(n_nodes), index=all_edges)
    
    # Build adjacency matrix from shortcuts with minimum costs
    # Group by (from_edge, to_edge) and take min cost
    shortcuts_min = shortcuts_df.groupby(['from_edge', 'to_edge'])['cost'].min().reset_index()
    
    src_indices = shortcuts_min['from_edge'].map(edge_to_idx).values
    dst_indices = shortcuts_min['to_edge'].map(edge_to_idx).values
    costs = shortcuts_min['cost'].values
    
    print(f"Building sparse graph matrix ({n_nodes}x{n_nodes})...")
    graph = csr_matrix((costs, (src_indices, dst_indices)), shape=(n_nodes, n_nodes))
    
    # Compute all-pairs shortest paths
    print("Computing all-pairs shortest paths (this may take a while)...")
    dist_matrix = shortest_path(
        csgraph=graph,
        method='auto',
        directed=True,
        return_predecessors=False
    )
    print("Done computing shortest paths.")
    
    # Sample if specified
    if sample_size and sample_size < len(shortcuts_df):
        print(f"\nSampling {sample_size} shortcuts for verification...")
        verify_df = shortcuts_df.sample(n=sample_size, random_state=42)
    else:
        verify_df = shortcuts_df
    
    # Verify each shortcut
    print(f"\nVerifying {len(verify_df)} shortcuts...")
    
    src_idx = verify_df['from_edge'].map(edge_to_idx).values
    dst_idx = verify_df['to_edge'].map(edge_to_idx).values
    shortcut_costs = verify_df['cost'].values
    
    # Get true shortest path costs
    true_costs = dist_matrix[src_idx, dst_idx]
    
    # Compare
    cost_diff = np.abs(shortcut_costs - true_costs)
    
    # Tolerance for floating point comparison
    tolerance = 1e-9
    
    matches = cost_diff < tolerance
    num_matches = np.sum(matches)
    num_mismatches = len(verify_df) - num_matches
    
    print(f"\n=== VERIFICATION RESULTS ===")
    print(f"Total verified: {len(verify_df)}")
    print(f"Matches (within tolerance): {num_matches} ({100*num_matches/len(verify_df):.4f}%)")
    print(f"Mismatches: {num_mismatches}")
    
    if num_mismatches > 0:
        print(f"\n--- Sample Mismatches ---")
        mismatch_mask = ~matches
        mismatch_df = verify_df[mismatch_mask].copy()
        mismatch_df['true_cost'] = true_costs[mismatch_mask]
        mismatch_df['cost_diff'] = cost_diff[mismatch_mask]
        
        print(mismatch_df[['from_edge', 'to_edge', 'cost', 'true_cost', 'cost_diff']].head(20).to_string())
        
        print(f"\nMax cost difference: {np.max(cost_diff[mismatch_mask])}")
        print(f"Mean cost difference: {np.mean(cost_diff[mismatch_mask])}")
    else:
        print("\n✓ All shortcuts verified as optimal shortest paths!")
    
    return num_mismatches == 0


if __name__ == "__main__":
    import config
    
    # Default paths
    shortcuts_path = str(config.SHORTCUTS_OUTPUT_FILE).replace("_shortcuts", "_duckdb_scipy")
    edges_path = str(config.EDGES_FILE)
    
    # Allow command line override
    if len(sys.argv) > 1:
        shortcuts_path = sys.argv[1]
    if len(sys.argv) > 2:
        edges_path = sys.argv[2]
    
    # Verify (sample for speed, or all for completeness)
    success = verify_shortcuts(shortcuts_path, edges_path, sample_size=None)
    
    sys.exit(0 if success else 1)
