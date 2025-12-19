
import logging
import time
import pandas as pd
import numpy as np
import duckdb
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import shortest_path

import config
import utilities_duckdb as utils

import logging_config_duckdb as log_conf

# Setup logging
logger = logging.getLogger(__name__)

def process_partition_scipy(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Process a single partition using Scipy's shortest_path.
    EXACTLY matches Spark version for identical via_edge output.
    """
    if len(pdf) == 0:
        return pd.DataFrame(columns=['from_edge', 'to_edge', 'via_edge', 'cost'])
    
    # Map nodes to indices
    nodes = pd.concat([pdf['from_edge'], pdf['to_edge']]).unique()
    n_nodes = len(nodes)
    node_to_idx = pd.Series(data=np.arange(n_nodes), index=nodes)
    
    # Deduplicate and keep minimum cost
    pdf_dedup = pdf.loc[
        pdf.groupby(['from_edge', 'to_edge'])['cost'].idxmin()
    ]
    
    src_indices = pdf_dedup['from_edge'].map(node_to_idx).values
    dst_indices = pdf_dedup['to_edge'].map(node_to_idx).values
    costs = pdf_dedup['cost'].values
    
    # Store via_edges for direct paths (SAME AS SPARK: +1 offset)
    via_values = pdf_dedup['via_edge'].values + 1
    via_matrix = csr_matrix((via_values, (src_indices, dst_indices)), shape=(n_nodes, n_nodes))
    
    # Build graph matrix
    graph = csr_matrix((costs, (src_indices, dst_indices)), shape=(n_nodes, n_nodes))
    
    # Compute shortest paths (SAME AS SPARK: method='auto')
    dist_matrix, predecessors = shortest_path(
        csgraph=graph,
        method='auto',
        directed=True,
        return_predecessors=True
    )
    
    # Convert back to DataFrame (SAME AS SPARK: chunked processing)
    results = []
    chunk_size = 2000
    
    for start_row in range(0, n_nodes, chunk_size):
        end_row = min(start_row + chunk_size, n_nodes)
        sub_dist = dist_matrix[start_row:end_row, :]
        sub_pred = predecessors[start_row:end_row, :]
        
        valid_mask = (sub_dist != np.inf)
        rows, cols = np.where(valid_mask)
        global_rows = rows + start_row
        
        # Filter self-loops
        non_loop_mask = (global_rows != cols)
        rows = rows[non_loop_mask]
        cols = cols[non_loop_mask]
        global_rows = global_rows[non_loop_mask]
        
        if len(rows) == 0:
            continue
        
        chunk_costs = sub_dist[rows, cols]
        chunk_preds = sub_pred[rows, cols]
        chunk_src = nodes[global_rows]
        chunk_dst = nodes[cols]
        
        # Determine via_edge (SAME AS SPARK: vectorized)
        original_vias = np.array(via_matrix[global_rows, cols]).flatten() - 1
        is_direct = (chunk_preds == global_rows)
        
        final_vias = np.where(
            is_direct,
            original_vias,
            nodes[chunk_preds]
        )
        
        chunk_df = pd.DataFrame({
            'from_edge': chunk_src,
            'to_edge': chunk_dst,
            'via_edge': final_vias,
            'cost': chunk_costs
        })
        results.append(chunk_df)
    
    if not results:
        return pd.DataFrame(columns=['from_edge', 'to_edge', 'via_edge', 'cost'])
    
    return pd.concat(results, ignore_index=True)



def run_scipy_algorithm(con: duckdb.DuckDBPyConnection):
    """
    Execute Scipy algorithm on 'shortcuts' table grouped by 'current_cell'.
    Updates 'shortcuts_next' with results.
    """
    logger.info("Fetching active shortcuts...")
    # Get all data into Pandas
    df = con.sql("SELECT * FROM shortcuts WHERE current_cell IS NOT NULL").df()
    
    if df.empty:
        logger.info("No active shortcuts to process.")
        con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts WHERE 1=0")
        return

    logger.info(f"Processing {len(df)} shortcuts across {df['current_cell'].nunique()} partitions")
    
    # Group by cell and apply processing
    results = []
    for cell, group in df.groupby('current_cell'):
        processed_group = process_partition_scipy(group)
        if not processed_group.empty:
            processed_group['current_cell'] = cell
            results.append(processed_group)
            
    if results:
        final_df = pd.concat(results)
        # Load back to DuckDB
        con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM final_df")
    else:
        con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts WHERE 1=0")

def main():
    log_conf.setup_logging("generate_shortcuts_duckdb_scipy")
    logger.info("Starting DuckDB + Scipy Shortcuts Generation")
    
    con = utils.initialize_duckdb()
    
    # 1. Load Data
    logger.info("Loading edges...")
    utils.read_edges(con, str(config.EDGES_FILE))
    utils.create_edges_cost_table(con, str(config.EDGES_FILE))
    
    logger.info("Creating initial shortcuts...")
    utils.initial_shortcuts_table(con, str(config.GRAPH_FILE))
    
    # 2. Forward Pass
    start_res = 15
    end_res = -1
    
    # We follow the same loop structure as hybrid
    # Forward Pass: 15 down to -1 (inclusive)
    for res in range(15, -2, -1):
        logger.info(f"\n--- Resolution {res} ---")
        utils.assign_cell_forward(con, res)
        # assign logic creates shortcuts_next (candidates) from shortcuts
        # But wait, logic is: shortcuts -> assign -> active shortcuts -> extend -> merge
        
        # Correct flow replication:
        # 1. Assign cells to current shortcuts -> result is shortcuts labeled with cell
        # 2. Filter for active
        # 3. Extend
        # 4. Merge
        
        # My utilities_duckdb.assign_cell_forward creates 'shortcuts_next' which contains VALID assignments.
        # But we need to use this for processing, THEN merge.
        
        # Let's fix the flow to match hybrid:
        
        # A. Assign Cells
        utils.assign_cell_forward(con, res) 
        # Output: shortcuts_next (table with current_cell set)
        
        # B. Run Algorithm on shortcuts_next
        # We rename shortcuts_next to 'shortcuts_active' for clarity?
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # C. Run Scipy on shortcuts_active
        # 'run_scipy_algorithm' expects 'shortcuts' table with current_cell? 
        # No, let's make it accept table name or just use shortcuts_active
        
        # Creating a temporary table for the function to consume
        con.execute("DROP TABLE IF EXISTS shortcuts_processing")
        con.execute("CREATE TABLE shortcuts_processing AS SELECT * FROM shortcuts_active WHERE current_cell IS NOT NULL")
        
        # D. Execute Scipy -> outputs into 'shortcuts_next' (recreated inside function from pandas)
        # Using a specialized version of query inside the function
        logger.info("Fetching active shortcuts for Scipy...")
        df = con.sql("SELECT * FROM shortcuts_processing").df()
        
        results = []
        if not df.empty:
            for cell, group in df.groupby('current_cell'):
                processed = process_partition_scipy(group)
                if not processed.empty:
                    processed['current_cell'] = cell
                    results.append(processed)
        
        if results:
            final_df = pd.concat(results)
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM final_df")
        else:
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts WHERE 1=0")
            
        # E. Merge
        logger.info(f"Merging {con.sql('SELECT COUNT(*) FROM shortcuts_next').fetchone()[0]} new shortcuts...")
        # Note: Algo produced shortcuts_next, so we are good.
        
        utils.merge_shortcuts(con)
        con.execute("DROP TABLE shortcuts_active")

    # ================================================================
    # PHASE 2: BACKWARD PASS (0 → 15)
    # ================================================================
    logger.info("\n=== PHASE 2: BACKWARD PASS (0 → 15) ===")
    
    for res in range(0, 16):
        logger.info(f"\n--- Backward Resolution {res} ---")
        
        # A. Assign Cells (backward uses same logic, or could use assign_cell_backward)
        utils.assign_cell_backward(con, res) 
        
        # B. Rename for processing
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # C. Prepare for Scipy
        con.execute("DROP TABLE IF EXISTS shortcuts_processing")
        con.execute("CREATE TABLE shortcuts_processing AS SELECT * FROM shortcuts_active WHERE current_cell IS NOT NULL")
        
        # D. Execute Scipy
        logger.info("Fetching active shortcuts for Scipy (backward)...")
        df = con.sql("SELECT * FROM shortcuts_processing").df()
        
        results = []
        if not df.empty:
            for cell, group in df.groupby('current_cell'):
                if cell is None:
                    continue
                processed = process_partition_scipy(group)
                if not processed.empty:
                    processed['current_cell'] = cell
                    results.append(processed)
        
        if results:
            final_df = pd.concat(results)
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM final_df")
        else:
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts WHERE 1=0")
            
        # E. Merge
        logger.info(f"Merging {con.sql('SELECT COUNT(*) FROM shortcuts_next').fetchone()[0]} new shortcuts...")
        utils.merge_shortcuts(con)
        con.execute("DROP TABLE shortcuts_active")

    # 3. Finalize
    logger.info("\nFinalizing output...")
    utils.add_final_info(con)
    
    output_path = str(config.SHORTCUTS_OUTPUT_FILE).replace("_shortcuts", "_duckdb_scipy")
    logger.info(f"Saving to {output_path}")
    utils.save_output(con, output_path)
    logger.info("Done.")

if __name__ == "__main__":
    main()
