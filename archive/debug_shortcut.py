"""
Debug script to trace a specific shortcut through both passes.


This script traces why shortcut (5 -> 933) has cost 7.95 instead of 5.65.
"""

import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import shortest_path
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

import sys
sys.path.insert(0, str(Path(__file__).parent))

from utilities import (
    initialize_spark,
    read_edges,
    initial_shortcuts_table,
    update_dummy_costs_for_edges,
    assign_cell_forward,
    assign_cell_backward,
    filter_active_shortcuts,
    merge_shortcuts,
    find_lca,
    find_resolution,
    get_parent_cell
)
import config

# Target shortcut to trace
TARGET_INCOMING = 5
TARGET_OUTGOING = 933

def get_edge_info(edges_df, edge_id):
    """Get detailed info about an edge."""
    row = edges_df.filter(F.col("id") == edge_id).first()
    if row:
        return {
            "id": row.id,
            "incoming_cell": row.incoming_cell,
            "outgoing_cell": row.outgoing_cell,
            "lca_res": row.lca_res
        }
    return None

def trace_shortcut(shortcuts_df, edges_df, phase, resolution):
    """Check if target shortcut exists and what its cost is."""
    target = shortcuts_df.filter(
        (F.col("incoming_edge") == TARGET_INCOMING) & 
        (F.col("outgoing_edge") == TARGET_OUTGOING)
    )
    
    if target.count() > 0:
        row = target.first()
        print(f"  [{phase} R={resolution:2d}] Shortcut {TARGET_INCOMING}→{TARGET_OUTGOING}: "
              f"cost={row.cost:.4f}, via_edge={row.via_edge}")
        return row.cost
    else:
        print(f"  [{phase} R={resolution:2d}] Shortcut {TARGET_INCOMING}→{TARGET_OUTGOING}: NOT FOUND")
        return None


def trace_cell_assignment(shortcuts_df, edges_df, current_res, is_backward=False):
    """Check cell assignment for target shortcut."""
    # Get edge info
    edge_A = get_edge_info(edges_df, TARGET_INCOMING)
    edge_B = get_edge_info(edges_df, TARGET_OUTGOING)
    
    if not edge_A or not edge_B:
        print(f"  Edge info not found!")
        return
    
    lca_res_A = edge_A["lca_res"]
    lca_res_B = edge_B["lca_res"]
    lca_res = max(lca_res_A, lca_res_B)
    
    # For forward pass
    if not is_backward:
        from utilities import _find_lca_impl, _get_parent_cell_impl
        
        # inner_cell = LCA(y_A, x_B)
        y_A = edge_A["outgoing_cell"]
        x_B = edge_B["incoming_cell"]
        inner_cell, inner_res = _find_lca_impl(y_A, x_B)
        
        is_valid = (lca_res <= current_res) and (inner_res >= current_res)
        current_cell = _get_parent_cell_impl(inner_cell, current_res) if is_valid else None
        
        print(f"  [FORWARD R={current_res:2d}] lca_res_A={lca_res_A}, lca_res_B={lca_res_B}, "
              f"lca_res={lca_res}, inner_res={inner_res}")
        print(f"                    Valid: {lca_res}<={current_res}<={inner_res} = {is_valid}, "
              f"Cell: {current_cell}")
    else:
        # Backward pass - two-cell approach
        from utilities import _find_lca_impl, _get_parent_cell_impl
        
        x_A = edge_A["incoming_cell"]
        y_A = edge_A["outgoing_cell"]
        x_B = edge_B["incoming_cell"]
        y_B = edge_B["outgoing_cell"]
        
        inner_cell, inner_res = _find_lca_impl(y_A, x_B)
        outer_cell, outer_res = _find_lca_impl(x_A, y_B)
        
        # Determine case
        if lca_res_A == current_res or lca_res_B == current_res:
            case = "Case 1-3 (Lat)"
            use_inner = True
            is_valid = (lca_res <= current_res) and (inner_res >= current_res)
            current_cell = _get_parent_cell_impl(inner_cell, current_res) if is_valid else None
        elif lca_res_A < current_res and lca_res_B < current_res:
            case = "Case 4 (Up→Down)"
            use_inner = False
            is_valid = (lca_res <= current_res) and (outer_res >= current_res)
            current_cell = _get_parent_cell_impl(outer_cell, current_res) if is_valid else None
        else:
            case = "No case matches"
            use_inner = None
            is_valid = False
            current_cell = None
        
        print(f"  [BACKWARD R={current_res:2d}] lca_res_A={lca_res_A}, lca_res_B={lca_res_B}, "
              f"lca_res={lca_res}")
        print(f"                     inner_res={inner_res}, outer_res={outer_res}")
        print(f"                     {case}, Valid: {is_valid}, Cell: {current_cell}")


def main():
    print("=" * 70)
    print(f"TRACING SHORTCUT {TARGET_INCOMING} → {TARGET_OUTGOING}")
    print("=" * 70)
    
    spark = initialize_spark()
    
    # Load data
    edges_df = read_edges(spark, str(config.EDGES_FILE)).cache()
    edges_cost_df = update_dummy_costs_for_edges(spark, str(config.EDGES_FILE), edges_df)
    shortcuts_df = initial_shortcuts_table(spark, str(config.GRAPH_FILE), edges_cost_df)
    
    # Get edge info
    edge_A = get_edge_info(edges_df, TARGET_INCOMING)
    edge_B = get_edge_info(edges_df, TARGET_OUTGOING)
    
    print(f"\nEdge A (incoming): {edge_A}")
    print(f"Edge B (outgoing): {edge_B}")
    print()
    
    # Check initial shortcuts
    print("\n--- INITIAL SHORTCUTS ---")
    trace_shortcut(shortcuts_df, edges_df, "INIT", -2)
    
    # Forward pass
    print("\n--- FORWARD PASS ---")
    for current_res in range(15, -2, -1):
        trace_cell_assignment(shortcuts_df, edges_df, current_res, is_backward=False)
        
        shortcuts_with_cell = assign_cell_forward(shortcuts_df, edges_df, current_res)
        active_shortcuts = filter_active_shortcuts(shortcuts_with_cell)
        
        # Check if target is in active shortcuts
        target_active = active_shortcuts.filter(
            (F.col("incoming_edge") == TARGET_INCOMING) & 
            (F.col("outgoing_edge") == TARGET_OUTGOING)
        ).count() > 0
        
        if target_active:
            print(f"  → Target IS active at R={current_res}")
        
        if active_shortcuts.count() == 0:
            continue
            
        # Compute shortest paths
        from generate_shortcuts import compute_shortest_paths_per_partition
        new_shortcuts = compute_shortest_paths_per_partition(active_shortcuts)
        
        # Check if target was generated
        target_new = new_shortcuts.filter(
            (F.col("incoming_edge") == TARGET_INCOMING) & 
            (F.col("outgoing_edge") == TARGET_OUTGOING)
        )
        if target_new.count() > 0:
            row = target_new.first()
            print(f"  → NEW shortcut cost: {row.cost:.4f}, via={row.via_edge}")
        
        # Merge
        shortcuts_df = merge_shortcuts(shortcuts_df, new_shortcuts)
        shortcuts_df = shortcuts_df.localCheckpoint()
        
        trace_shortcut(shortcuts_df, edges_df, "FWD", current_res)
    
    # Backward pass
    print("\n--- BACKWARD PASS ---")
    for current_res in range(-1, 16):
        trace_cell_assignment(shortcuts_df, edges_df, current_res, is_backward=True)
        
        shortcuts_with_cell = assign_cell_backward(shortcuts_df, edges_df, current_res)
        active_shortcuts = filter_active_shortcuts(shortcuts_with_cell)
        
        # Check if target is in active shortcuts
        target_active = active_shortcuts.filter(
            (F.col("incoming_edge") == TARGET_INCOMING) & 
            (F.col("outgoing_edge") == TARGET_OUTGOING)
        ).count() > 0
        
        if target_active:
            print(f"  → Target IS active at R={current_res}")
        
        if active_shortcuts.count() == 0:
            continue
            
        # Compute shortest paths
        from generate_shortcuts import compute_shortest_paths_per_partition
        new_shortcuts = compute_shortest_paths_per_partition(active_shortcuts)
        
        # Check if target was updated
        target_new = new_shortcuts.filter(
            (F.col("incoming_edge") == TARGET_INCOMING) & 
            (F.col("outgoing_edge") == TARGET_OUTGOING)
        )
        if target_new.count() > 0:
            row = target_new.first()
            print(f"  → UPDATED shortcut cost: {row.cost:.4f}, via={row.via_edge}")
        
        # Merge
        shortcuts_df = merge_shortcuts(shortcuts_df, new_shortcuts)
        shortcuts_df = shortcuts_df.localCheckpoint()
        
        trace_shortcut(shortcuts_df, edges_df, "BWD", current_res)
    
    # Final
    print("\n--- FINAL ---")
    final_cost = trace_shortcut(shortcuts_df, edges_df, "FINAL", 99)
    
    # Compute true shortest path
    print("\n--- TRUE SHORTEST PATH ---")
    edges_pd = edges_cost_df.toPandas()
    graph_pd = pd.read_csv(config.GRAPH_FILE)
    
    edge_costs = edges_pd.set_index('id')['cost'].to_dict()
    graph_pd['cost'] = graph_pd['incoming_edge'].map(edge_costs)
    
    all_edges = pd.concat([graph_pd['incoming_edge'], graph_pd['outgoing_edge']]).unique()
    n_edges = len(all_edges)
    edge_to_idx = {e: i for i, e in enumerate(all_edges)}
    
    src_indices = graph_pd['incoming_edge'].map(edge_to_idx).values
    dst_indices = graph_pd['outgoing_edge'].map(edge_to_idx).values
    costs = graph_pd['cost'].values
    
    graph_matrix = csr_matrix((costs, (src_indices, dst_indices)), shape=(n_edges, n_edges))
    dist_matrix, predecessors = shortest_path(csgraph=graph_matrix, method='auto', directed=True, return_predecessors=True)
    
    src_idx = edge_to_idx[TARGET_INCOMING]
    dst_idx = edge_to_idx[TARGET_OUTGOING]
    true_cost = dist_matrix[src_idx, dst_idx]
    
    print(f"True shortest path cost: {true_cost:.4f}")
    print(f"Algorithm cost: {final_cost:.4f if final_cost else 'N/A'}")
    print(f"Difference: {(final_cost - true_cost):.4f}" if final_cost else "N/A")
    
    # Reconstruct true path
    path = [dst_idx]
    current = dst_idx
    while current != src_idx and predecessors[src_idx, current] != -9999:
        current = predecessors[src_idx, current]
        path.append(current)
    path.reverse()
    
    print(f"\nTrue path (edge indices): {[all_edges[i] for i in path]}")
    
    spark.stop()


if __name__ == "__main__":
    main()
