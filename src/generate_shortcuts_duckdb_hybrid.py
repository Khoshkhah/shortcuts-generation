
import logging
import pandas as pd
import duckdb
import config
import utilities_duckdb as utils

# Import algorithms from sibling modules
import generate_shortcuts_duckdb_scipy as algo_scipy
import generate_shortcuts_duckdb_pure as algo_pure

import logging_config_duckdb as log_conf

# Setup logging
logger = log_conf.setup_logging(__name__)

def main():
    logger.info("Starting DuckDB Hybrid Shortcuts Generation")
    
    con = utils.initialize_duckdb()
    
    # 1. Load Data
    logger.info("Loading edges...")
    utils.read_edges(con, str(config.EDGES_FILE))
    utils.create_edges_cost_table(con, str(config.EDGES_FILE))
    
    logger.info("Creating initial shortcuts...")
    utils.initial_shortcuts_table(con, str(config.GRAPH_FILE))
    
    # 2. Iteration Logic
    # Forward Pass: 15 -> 0
    # Backward Pass: 0 -> 15 (Simulated by logic, but actually we just continue processing)
    
    # In hybrid Spark: 
    # Forward resolutions: 15 down to 0
    # Backward resolutions: 0 up to 15 (if needed)
    
    # Algorithm selection: Same as Spark Hybrid defaults
    # Scipy: resolutions -1 to 11 (fine resolutions, faster)
    # Pure:  resolutions 12 to 15 (coarse resolutions, better for large partitions)
    scipy_resolutions = set(range(-1, 12))  # -1 to 11
    pure_resolutions = set(range(12, 16))   # 12 to 15
    logger.info(f"Scipy resolutions: {sorted(scipy_resolutions)}")
    logger.info(f"Pure resolutions: {sorted(pure_resolutions)}")
    
    logger.info("\n=== PHASE 1: FORWARD PASS (15 → -1) ===")
    for res in range(15, -2, -1):  # 15, 14, ..., 0, -1
        logger.info(f"\n--- Resolution {res} ---")
        
        # A. Assign Cells
        utils.assign_cell_forward(con, res)
        con.execute("DROP TABLE IF EXISTS shortcuts_active")
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # B. Filter active shortcuts (current_cell IS NOT NULL)
        con.execute("DELETE FROM shortcuts_active WHERE current_cell IS NULL")
        
        active_count = con.sql("SELECT COUNT(*) FROM shortcuts_active").fetchone()[0]
        logger.info(f"Active shortcuts: {active_count}")
        
        if active_count > 0:
            if res in scipy_resolutions:
                logger.info("Using Scipy Algorithm")
                # Prepare table expected by run_scipy_algorithm functionality
                # run_scipy_algorithm in my impl creates 'shortcuts_processing' from 'shortcuts' (active).
                # Wait, I implemented it to read 'shortcuts_processing' inside process_partition
                # But here I have 'shortcuts_active'.
                
                # Let's adapt:
                con.execute("DROP TABLE IF EXISTS shortcuts_processing")
                con.execute("CREATE TABLE shortcuts_processing AS SELECT * FROM shortcuts_active")
                
                # Execute logic manually or call function if it was designed to take args?
                # Ideally I should refactor algo_scipy to take connection and table name.
                # But I can just replicate the 'fetch and process' logic here reusing the function.
                
                logger.info("Fetching active shortcuts for Scipy...")
                df = con.sql("SELECT * FROM shortcuts_processing").df()
                
                results = []
                if not df.empty:
                    for cell, group in df.groupby('current_cell'):
                        processed = algo_scipy.process_partition_scipy(group)
                        if not processed.empty:
                            processed['current_cell'] = cell
                            results.append(processed)
                
                if results:
                    final_df = pd.concat(results)
                    con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM final_df")
                else:
                    con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
                    
            else:
                logger.info("Using Pure DuckDB Algorithm")
                algo_pure.compute_shortest_paths_pure_duckdb(con)
                # This outputs 'shortcuts_next'
        else:
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
            
        # C. Merge
        logger.info(f"Merging {con.sql('SELECT COUNT(*) FROM shortcuts_next').fetchone()[0]} new shortcuts...")
        utils.merge_shortcuts(con)
        con.execute("DROP TABLE IF EXISTS shortcuts_active")

    logger.info("\n=== PHASE 2: BACKWARD PASS (0 → 15) ===")
    for res in range(0, 16):  # 0, 1, ..., 15
        logger.info(f"\n--- Backward Resolution {res} ---")
        
        # A. Assign Cells (backward)
        utils.assign_cell_backward(con, res)
        
        con.execute("DROP TABLE IF EXISTS shortcuts_active")
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # B. Filter active shortcuts (current_cell IS NOT NULL)
        con.execute("DELETE FROM shortcuts_active WHERE current_cell IS NULL")
        
        active_count = con.sql("SELECT COUNT(*) FROM shortcuts_active").fetchone()[0]
        logger.info(f"Active shortcuts: {active_count}")
        
        if active_count > 0:
            if res in scipy_resolutions:
                logger.info("Using Scipy Algorithm")
                con.execute("DROP TABLE IF EXISTS shortcuts_processing")
                con.execute("CREATE TABLE shortcuts_processing AS SELECT * FROM shortcuts_active")
                
                df = con.sql("SELECT * FROM shortcuts_processing").df()
                results = []
                if not df.empty:
                    for cell, group in df.groupby('current_cell'):
                        processed = algo_scipy.process_partition_scipy(group)
                        if not processed.empty:
                            processed['current_cell'] = cell
                            results.append(processed)
                if results:
                    final_df = pd.concat(results)
                    con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM final_df")
                else:
                    con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
            else:
                logger.info("Using Pure DuckDB Algorithm")
                algo_pure.compute_shortest_paths_pure_duckdb(con)
        else:
             con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
             
        logger.info(f"Merging {con.sql('SELECT COUNT(*) FROM shortcuts_next').fetchone()[0]} new shortcuts...")
        utils.merge_shortcuts(con)
        con.execute("DROP TABLE IF EXISTS shortcuts_active")

    # 3. Finalize
    logger.info("\nFinalizing output...")
    utils.add_final_info(con)
    
    output_path = str(config.SHORTCUTS_OUTPUT_FILE).replace("_shortcuts", "_duckdb_hybrid")
    logger.info(f"Saving to {output_path}")
    utils.save_output(con, output_path)
    logger.info("Done.")

if __name__ == "__main__":
    main()
