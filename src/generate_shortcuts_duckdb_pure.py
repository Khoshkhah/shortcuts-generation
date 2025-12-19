
import logging
import duckdb
import config
import utilities_duckdb as utils

import logging_config_duckdb as log_conf

# Setup logging
# Note: Handlers are configured by setup_logging when called from main.
logger = logging.getLogger(__name__)

def compute_shortest_paths_pure_duckdb(con: duckdb.DuckDBPyConnection, max_iterations: int = 100):
    """
    Compute shortest paths using iterative SQL (Bellman-Ford-ish / Delta Stepping).
    Input: 'shortcuts_active' table (subset of shortcuts valid for current cell).
    Output: 'shortcuts_next' table with all-pairs shortest paths.
    """
    # 1. Initialize 'paths' with base shortcuts
    # We maintain (from_edge, to_edge) PK and minimize cost.
    con.execute("DROP TABLE IF EXISTS paths")
    con.execute("""
        CREATE TABLE paths AS 
        SELECT from_edge, to_edge, cost, via_edge, current_cell 
        FROM shortcuts_active
    """)
    
    # 2. Iterative expansion
    i = 0
    while i < max_iterations:
        # Create indices to speed up join
        con.execute("CREATE INDEX IF NOT EXISTS idx_paths_from ON paths(from_edge)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_paths_to ON paths(to_edge)")
        
        # Determine new candidates by extending current paths by ONE hop of ORIGINAL shortcuts
        # (Or geometric expansion path+path? Spark code expands path+path)
        
        # Spark Logic: new = current JOIN current
        # This is geometric expansion (1+1=2, 2+2=4...). Very fast convergence.
        
        stats_before = con.sql("SELECT COUNT(*), SUM(cost) FROM paths").fetchone()
        row_count_before = stats_before[0]
        cost_sum_before = stats_before[1] if stats_before[1] is not None else 0.0
        
        con.execute("""
            CREATE OR REPLACE TABLE new_paths AS
            SELECT 
                L.from_edge,
                R.to_edge,
                L.cost + R.cost AS cost,
                L.to_edge AS via_edge, -- Via edge is the intermediate node (L.to_edge = R.from_edge)
                L.current_cell
            FROM paths L
            JOIN paths R ON L.to_edge = R.from_edge AND L.current_cell = R.current_cell
            WHERE L.from_edge != R.to_edge -- No loops
        """)
        
        # Merge new paths into existing paths, keeping MIN cost
        # DuckDB doesn't support easy MERGE for this without unique constraint. 
        # Easier to Union and Group By.
        
        con.execute("""
            CREATE OR REPLACE TABLE combined_paths AS
            SELECT * FROM paths
            UNION ALL
            SELECT * FROM new_paths
        """)
        
        con.execute("""
            CREATE OR REPLACE TABLE paths_reduced AS
            SELECT 
                from_edge, 
                to_edge, 
                min(cost) as cost,
                first(via_edge) as via_edge,
                current_cell
            FROM combined_paths
            GROUP BY from_edge, to_edge, current_cell
        """)
        
        con.execute("DROP TABLE paths")
        con.execute("ALTER TABLE paths_reduced RENAME TO paths")
        
        # Convergence check: Count AND Checksum (Sum of costs)
        stats = con.sql("SELECT COUNT(*), SUM(cost) FROM paths").fetchone()
        row_count_after = stats[0]
        cost_sum_after = stats[1] if stats[1] is not None else 0.0 # Handle empty table case
        
        logger.info(f"Iteration {i}: Rows {row_count_before} -> {row_count_after}, CostSum {cost_sum_before:.4f} -> {cost_sum_after:.4f}")
        
        # Stop if STABLE (no new rows AND no cost improvement)
        # Note: Cost sum decreases as we find shorter paths.
        if row_count_after == row_count_before and abs(cost_sum_after - cost_sum_before) < 1e-6:
            logger.info("Converged.")
            break
            
        i += 1
        
    con.execute("DROP TABLE IF EXISTS shortcuts_next")
    con.execute("ALTER TABLE paths RENAME TO shortcuts_next")

def main():
    log_conf.setup_logging("generate_shortcuts_duckdb_pure")
    logger.info("Starting DuckDB Pure SQL Shortcuts Generation")
    
    con = utils.initialize_duckdb()
    
    # 1. Load Data
    logger.info("Loading edges...")
    utils.read_edges(con, str(config.EDGES_FILE))
    utils.create_edges_cost_table(con, str(config.EDGES_FILE))
    
    logger.info("Creating initial shortcuts...")
    utils.initial_shortcuts_table(con, str(config.GRAPH_FILE))
    
    # 2. Forward Pass (15 → -1)
    logger.info("\n=== PHASE 1: FORWARD PASS (15 → -1) ===")
    for res in range(15, -2, -1):  # 15, 14, ..., 0, -1
        logger.info(f"\n--- Resolution {res} ---")
        
        # A. Assign Cells
        utils.assign_cell_forward(con, res)
        con.execute("DROP TABLE IF EXISTS shortcuts_active")
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # B. Filter active shortcuts (current_cell IS NOT NULL)
        con.execute("DELETE FROM shortcuts_active WHERE current_cell IS NULL")
        
        # C. Run Algorithm
        active_count = con.sql("SELECT COUNT(*) FROM shortcuts_active").fetchone()[0]
        logger.info(f"Active shortcuts: {active_count}")
        
        if active_count > 0:
            compute_shortest_paths_pure_duckdb(con)
        else:
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
        
        # D. Merge
        logger.info(f"Merging {con.sql('SELECT COUNT(*) FROM shortcuts_next').fetchone()[0]} new shortcuts...")
        utils.merge_shortcuts(con)
        
        # Cleanup
        con.execute("DROP TABLE IF EXISTS shortcuts_active")

    # 3. Backward Pass (0 → 15)
    logger.info("\n=== PHASE 2: BACKWARD PASS (0 → 15) ===")
    for res in range(0, 16):  # 0, 1, ..., 15
        logger.info(f"\n--- Backward Resolution {res} ---")
        
        # A. Assign Cells (backward)
        utils.assign_cell_backward(con, res)
        con.execute("DROP TABLE IF EXISTS shortcuts_active")
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # B. Filter active shortcuts (current_cell IS NOT NULL)
        con.execute("DELETE FROM shortcuts_active WHERE current_cell IS NULL")
        
        # C. Run Algorithm
        active_count = con.sql("SELECT COUNT(*) FROM shortcuts_active").fetchone()[0]
        logger.info(f"Active shortcuts: {active_count}")
        
        if active_count > 0:
            compute_shortest_paths_pure_duckdb(con)
        else:
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
        
        # D. Merge
        logger.info(f"Merging {con.sql('SELECT COUNT(*) FROM shortcuts_next').fetchone()[0]} new shortcuts...")
        utils.merge_shortcuts(con)
        
        # Cleanup
        con.execute("DROP TABLE IF EXISTS shortcuts_active")

    # 4. Finalize
    logger.info("\nFinalizing output...")
    utils.add_final_info(con)
    
    output_path = str(config.SHORTCUTS_OUTPUT_FILE).replace("_shortcuts", "_duckdb_pure")
    logger.info(f"Saving to {output_path}")
    utils.save_output(con, output_path)
    logger.info("Done.")

if __name__ == "__main__":
    main()
