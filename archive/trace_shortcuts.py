#!/usr/bin/env python3
"""
Trace the cost evolution of specific shortcuts through the algorithm.
This is a modified version of generate_shortcuts.py that logs costs at each step.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Target shortcuts to trace
TARGET = [(2, 71), (71, 78), (2, 78)]

def compute_shortest_paths_per_partition(pdf):
    """Compute shortest paths within a partition."""
    if pdf.empty:
        return pd.DataFrame(columns=["incoming_edge", "outgoing_edge", "via_edge", "cost"])
    
    all_edges = pd.concat([pdf["incoming_edge"], pdf["outgoing_edge"]]).unique()
    edge_to_idx = {e: i for i, e in enumerate(all_edges)}
    idx_to_edge = {i: e for e, i in edge_to_idx.items()}
    n = len(all_edges)
    
    src = pdf["incoming_edge"].map(edge_to_idx).values
    dst = pdf["outgoing_edge"].map(edge_to_idx).values
    costs = pdf["cost"].values
    
    matrix = csr_matrix((costs, (src, dst)), shape=(n, n))
    dist_matrix, predecessors = dijkstra(
        csgraph=matrix, directed=True, return_predecessors=True, limit=1e9
    )
    
    results = []
    incoming_indices = src
    outgoing_indices = dst
    
    unique_in = np.unique(incoming_indices)
    unique_out = np.unique(outgoing_indices)
    
    for i in unique_in:
        for j in unique_out:
            if i != j and dist_matrix[i, j] < 1e8:
                via_idx = predecessors[i, j]
                via_edge = idx_to_edge[via_idx] if via_idx >= 0 else idx_to_edge[j]
                results.append({
                    "incoming_edge": idx_to_edge[i],
                    "outgoing_edge": idx_to_edge[j],
                    "via_edge": via_edge,
                    "cost": dist_matrix[i, j]
                })
    
    return pd.DataFrame(results)


def trace_costs(shortcuts_df, phase, res):
    """Print costs of target shortcuts."""
    results = []
    for (a, b) in TARGET:
        match = shortcuts_df.filter(
            (F.col("incoming_edge") == a) & (F.col("outgoing_edge") == b)
        ).collect()
        if match:
            cost = match[0]["cost"]
            via = match[0]["via_edge"]
            results.append(f"{a}→{b}: cost={cost:.4f}, via={via}")
        else:
            results.append(f"{a}→{b}: -")
    return results


def main():
    # Create spark session
    spark = SparkSession.builder \
        .appName("ShortcutTrace") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Load data
    edges_pd = pd.read_csv("data/Somerset_driving_simplified_edges_with_h3.csv")
    graph_pd = pd.read_csv("data/Somerset_driving_edge_graph.csv")

    edges_pd["cost"] = edges_pd["length"] / edges_pd["maxspeed"].replace(0, np.inf)
    graph_pd["cost"] = graph_pd["incoming_edge"].map(edges_pd.set_index("id")["cost"].to_dict())

    edges_df = spark.createDataFrame(edges_pd)
    initial_shortcuts = spark.createDataFrame(graph_pd[["incoming_edge", "outgoing_edge", "cost"]])
    initial_shortcuts = initial_shortcuts.withColumn("via_edge", F.col("outgoing_edge"))

    from utilities import (
        assign_cell_forward, assign_cell_backward, filter_active_shortcuts,
        merge_shortcuts
    )

    print("="*70)
    print("COST TRACE: 2→71, 71→78, 2→78")
    print("="*70)

    # Initial state
    print("\n[INITIAL]")
    for r in trace_costs(initial_shortcuts, "INIT", 0):
        print(f"  {r}")

    # Forward pass
    print("\n" + "="*70)
    print("FORWARD PASS (15 → -1)")
    print("="*70)
    
    shortcuts_df = initial_shortcuts
    for res in range(15, -2, -1):
        assigned = assign_cell_forward(shortcuts_df, edges_df, res)
        active = filter_active_shortcuts(assigned)
        
        if active.count() > 0:
            # Compute shortest paths
            new_shortcuts = active.groupBy("current_cell").applyInPandas(
                compute_shortest_paths_per_partition,
                "incoming_edge long, outgoing_edge long, via_edge long, cost double"
            )
            
            # Merge
            shortcuts_df = merge_shortcuts(shortcuts_df, new_shortcuts)
        
        # Always show costs after each resolution
        print(f"\nR={res:2d}:", end="")
        costs = trace_costs(shortcuts_df, "FWD", res)
        for c in costs:
            parts = c.split(": ")
            shortcut = parts[0]
            info = parts[1] if len(parts) > 1 else "-"
            print(f"  {shortcut}: {info}", end="")
        print()

    # Backward pass  
    print("\n" + "="*70)
    print("BACKWARD PASS (-1 → 15)")
    print("="*70)

    for res in range(-1, 16):
        assigned = assign_cell_backward(shortcuts_df, edges_df, res)
        active = filter_active_shortcuts(assigned)
        
        if active.count() > 0:
            new_shortcuts = active.groupBy("current_cell").applyInPandas(
                compute_shortest_paths_per_partition,
                "incoming_edge long, outgoing_edge long, via_edge long, cost double"
            )
            
            shortcuts_df = merge_shortcuts(shortcuts_df, new_shortcuts)
        
        # Always show costs
        print(f"\nR={res:2d}:", end="")
        costs = trace_costs(shortcuts_df, "BWD", res)
        for c in costs:
            parts = c.split(": ")
            shortcut = parts[0]
            info = parts[1] if len(parts) > 1 else "-"
            print(f"  {shortcut}: {info}", end="")
        print()

    print("\n" + "="*70)
    print("FINAL STATE")
    print("="*70)
    for r in trace_costs(shortcuts_df, "FINAL", 0):
        print(f"  {r}")

    spark.stop()


if __name__ == "__main__":
    main()
