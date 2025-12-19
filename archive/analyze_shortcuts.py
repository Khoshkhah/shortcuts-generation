"""
Convert edge sequence to shortcuts and analyze each shortcut.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import h3

PROJECT_ROOT = Path(__file__).parent.parent
DISTRICT = "Somerset"

# The optimal path edges
PATH_EDGES = [5, 0, 928, 929, 933]

def find_lca_impl(cell1, cell2):
    """Find LCA of two H3 cells."""
    if cell1 == 0 or cell2 == 0:
        return 0, -1
    cell1_str = h3.int_to_str(cell1)
    cell2_str = h3.int_to_str(cell2)
    min_res = min(h3.get_resolution(cell1_str), h3.get_resolution(cell2_str))
    for res in range(min_res, -1, -1):
        if h3.cell_to_parent(cell1_str, res) == h3.cell_to_parent(cell2_str, res):
            return h3.str_to_int(h3.cell_to_parent(cell1_str, res)), res
    return 0, -1

def main():
    print("=" * 80)
    print(f"ANALYZING SHORTCUTS ALONG OPTIMAL PATH")
    print(f"Path edges: {PATH_EDGES}")
    print("=" * 80)
    
    # Load edges
    edges_df = pd.read_csv(PROJECT_ROOT / f"data/{DISTRICT}_driving_simplified_edges_with_h3.csv")
    edges_df['cost'] = edges_df['length'] / edges_df['maxspeed'].replace(0, np.inf)
    edge_info = edges_df.set_index('id')[['incoming_cell', 'outgoing_cell', 'lca_res', 'cost']].to_dict('index')
    
    # Load generated shortcuts
    shortcuts_df = pd.read_parquet(PROJECT_ROOT / f"output/{DISTRICT}_shortcuts")
    
    print("\n" + "=" * 80)
    print("EDGE INFO")
    print("=" * 80)
    print(f"{'Edge':<6} {'lca_res':<8} {'Cost':<10} {'x (incoming)':<20} {'y (outgoing)':<20}")
    print("-" * 80)
    for edge_id in PATH_EDGES:
        info = edge_info.get(edge_id, {})
        x = info.get('incoming_cell', 0)
        y = info.get('outgoing_cell', 0)
        print(f"{edge_id:<6} {info.get('lca_res', 'N/A'):<8} {info.get('cost', 0):<10.4f} "
              f"{x:<20} {y:<20}")
    
    # Convert to shortcuts (consecutive pairs)
    shortcuts_to_analyze = []
    for i in range(len(PATH_EDGES) - 1):
        inc = PATH_EDGES[i]
        out = PATH_EDGES[i + 1]
        shortcuts_to_analyze.append((inc, out))
    
    print("\n" + "=" * 80)
    print("SHORTCUTS ANALYSIS")
    print("=" * 80)
    
    for inc, out in shortcuts_to_analyze:
        print(f"\n{'='*60}")
        print(f"SHORTCUT: {inc} → {out}")
        print(f"{'='*60}")
        
        # Get edge info
        a_info = edge_info.get(inc, {})
        b_info = edge_info.get(out, {})
        
        x_A = a_info.get('incoming_cell', 0)
        y_A = a_info.get('outgoing_cell', 0)
        x_B = b_info.get('incoming_cell', 0)
        y_B = b_info.get('outgoing_cell', 0)
        
        lca_res_A = a_info.get('lca_res', -1)
        lca_res_B = b_info.get('lca_res', -1)
        lca_res = max(lca_res_A, lca_res_B)
        
        # Compute inner_cell and outer_cell
        inner_cell, inner_res = find_lca_impl(y_A, x_B)
        outer_cell, outer_res = find_lca_impl(x_A, y_B)
        
        print(f"\nEdge A ({inc}):")
        print(f"  x_A (incoming) = {x_A}")
        print(f"  y_A (outgoing) = {y_A}")
        print(f"  lca_res_A = {lca_res_A}")
        
        print(f"\nEdge B ({out}):")
        print(f"  x_B (incoming) = {x_B}")
        print(f"  y_B (outgoing) = {y_B}")
        print(f"  lca_res_B = {lca_res_B}")
        
        print(f"\nShortcut properties:")
        print(f"  lca_res = max({lca_res_A}, {lca_res_B}) = {lca_res}")
        print(f"  inner_cell = LCA(y_A, x_B) → res = {inner_res}")
        print(f"  outer_cell = LCA(x_A, y_B) → res = {outer_res}")
        
        # Check in generated shortcuts
        match = shortcuts_df[
            (shortcuts_df['incoming_edge'] == inc) & 
            (shortcuts_df['outgoing_edge'] == out)
        ]
        
        if len(match) > 0:
            row = match.iloc[0]
            print(f"\n✓ FOUND in generated shortcuts:")
            print(f"  cost = {row['cost']:.4f}")
            print(f"  via_edge = {row['via_edge']}")
            print(f"  inside = {row['inside']}")
            print(f"  cell = {row['cell']}")
        else:
            print(f"\n✗ NOT FOUND in generated shortcuts!")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY: Shortcut valid ranges")
    print("=" * 80)
    print(f"{'Shortcut':<12} {'lca_res':<8} {'inner_res':<10} {'outer_res':<10} {'Valid Range'}")
    print("-" * 60)
    
    for inc, out in shortcuts_to_analyze:
        a_info = edge_info.get(inc, {})
        b_info = edge_info.get(out, {})
        
        y_A = a_info.get('outgoing_cell', 0)
        x_B = b_info.get('incoming_cell', 0)
        x_A = a_info.get('incoming_cell', 0)
        y_B = b_info.get('outgoing_cell', 0)
        
        lca_res_A = a_info.get('lca_res', -1)
        lca_res_B = b_info.get('lca_res', -1)
        lca_res = max(lca_res_A, lca_res_B)
        
        _, inner_res = find_lca_impl(y_A, x_B)
        _, outer_res = find_lca_impl(x_A, y_B)
        
        valid_range = f"[{lca_res}, {inner_res}]"
        
        print(f"{inc}→{out:<8} {lca_res:<8} {inner_res:<10} {outer_res:<10} {valid_range}")


if __name__ == "__main__":
    main()
