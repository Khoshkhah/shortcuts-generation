"""
generate_shortcuts.py
======================

Main entry point for shortcuts generation using Scipy.

Algorithm:
1. Forward pass (resolution 15 → -1): Build LOCAL shortcuts within cells
2. Backward pass (resolution -1 → 15): Build GLOBAL shortcuts with two-cell approach
"""

import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import shortest_path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

from logging_config import get_logger, log_section, log_dict
from utilities import (
    initialize_spark,
    read_edges,
    initial_shortcuts_table,
    update_dummy_costs_for_edges,
    assign_cell_forward,
    assign_cell_backward,
    filter_active_shortcuts,
    merge_shortcuts,
    add_final_info
)
import config

logger = get_logger(__name__)


# ============================================================================
# SCIPY SHORTEST PATH COMPUTATION
# ============================================================================

def compute_shortest_paths_per_partition(
    df_shortcuts: DataFrame,
    partition_columns: list = ["current_cell"]
) -> DataFrame:
    """
    Compute all-pairs shortest paths using Scipy per partition.
    
    Uses Spark's applyInPandas to process each cell partition.
    """
    logger.info("Computing shortest paths using Scipy (partition-wise)")
    
    output_schema = StructType([
        StructField("from_edge", IntegerType(), False),
        StructField("to_edge", IntegerType(), False),
        StructField("via_edge", IntegerType(), False),
        StructField("cost", DoubleType(), False),
    ])
    
    def process_partition_scipy(pdf: pd.DataFrame) -> pd.DataFrame:
        """Process a single partition using Scipy's shortest_path."""
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
        
        # Store via_edges for direct paths (add 1 to distinguish from 0)
        via_values = pdf_dedup['via_edge'].values + 1
        via_matrix = csr_matrix((via_values, (src_indices, dst_indices)), shape=(n_nodes, n_nodes))
        
        # Build graph matrix
        graph = csr_matrix((costs, (src_indices, dst_indices)), shape=(n_nodes, n_nodes))
        
        # Compute shortest paths
        dist_matrix, predecessors = shortest_path(
            csgraph=graph,
            method='auto',
            directed=True,
            return_predecessors=True
        )
        
        # Convert back to DataFrame
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
            
            # Determine via_edge
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
    
    result = df_shortcuts.groupBy(partition_columns).applyInPandas(
        process_partition_scipy,
        schema=output_schema
    )
    
    logger.info("✓ Scipy computation completed")
    return result


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    log_section(logger, "SHORTCUTS GENERATION - SCIPY VERSION")
    
    config_info = {
        "edges_file": str(config.EDGES_FILE),
        "graph_file": str(config.GRAPH_FILE),
        "output_file": str(config.SHORTCUTS_OUTPUT_FILE),
        "district": config.DISTRICT_NAME
    }
    log_dict(logger, config_info, "Configuration")
    
    spark = None
    
    try:
        # Initialize
        logger.info("Initializing Spark session...")
        spark = initialize_spark()
        logger.info("✓ Spark session initialized")
        
        # Load data
        logger.info("Loading edge data...")
        edges_df = read_edges(spark, str(config.EDGES_FILE)).cache()
        edges_count = edges_df.count()
        logger.info(f"✓ Loaded {edges_count} edges")
        
        logger.info("Computing edge costs...")
        edges_cost_df = update_dummy_costs_for_edges(spark, str(config.EDGES_FILE), edges_df)
        logger.info("✓ Edge costs computed")
        
        logger.info("Creating initial shortcuts table...")
        shortcuts_df = initial_shortcuts_table(spark, str(config.GRAPH_FILE), edges_cost_df)
        shortcuts_count = shortcuts_df.count()
        logger.info(f"✓ Created {shortcuts_count} initial shortcuts")
        
        resolution_results = []
        
        # ================================================================
        # PHASE 1: FORWARD PASS (15 → -1)
        # ================================================================
        log_section(logger, "PHASE 1: FORWARD PASS (15 → -1)")
        
        for current_res in range(15, -2, -1):  # 15, 14, ..., 0, -1
            log_section(logger, f"Forward: Resolution {current_res}")
            
            # Assign cells
            logger.info(f"Assigning cells for resolution {current_res}...")
            shortcuts_with_cell = assign_cell_forward(shortcuts_df, edges_df, current_res)
            
            # Filter active shortcuts
            active_shortcuts = filter_active_shortcuts(shortcuts_with_cell)
            active_count = active_shortcuts.count()
            logger.info(f"✓ {active_count} active shortcuts at resolution {current_res}")
            
            if active_count == 0:
                logger.info("No active shortcuts, skipping...")
                continue
            
            # Cache for computation
            active_shortcuts = active_shortcuts.cache()
            
            # Compute shortest paths
            new_shortcuts = compute_shortest_paths_per_partition(active_shortcuts)
            new_count = new_shortcuts.count()
            logger.info(f"✓ Generated {new_count} shortcuts")
            
            resolution_results.append({
                "phase": "forward",
                "resolution": current_res,
                "active": active_count,
                "generated": new_count
            })
            
            # Merge back
            shortcuts_df = merge_shortcuts(shortcuts_df, new_shortcuts)
            shortcuts_df = shortcuts_df.localCheckpoint()
            
            active_shortcuts.unpersist()
        
        # ================================================================
        # PHASE 2: BACKWARD PASS (0 → 15)
        # ================================================================
        log_section(logger, "PHASE 2: BACKWARD PASS (0 → 15)")
        
        for current_res in range(0, 16):  # 0, 1, ..., 15
            log_section(logger, f"Backward: Resolution {current_res}")
            
            # Assign cells using two-cell approach
            logger.info(f"Assigning cells for resolution {current_res}...")
            shortcuts_with_cell = assign_cell_backward(shortcuts_df, edges_df, current_res)
            
            # Filter active shortcuts
            active_shortcuts = filter_active_shortcuts(shortcuts_with_cell)
            active_count = active_shortcuts.count()
            logger.info(f"✓ {active_count} active shortcuts at resolution {current_res}")
            
            if active_count == 0:
                logger.info("No active shortcuts, skipping...")
                continue
            
            active_shortcuts = active_shortcuts.cache()
            
            # Compute shortest paths (now GLOBALLY optimal)
            new_shortcuts = compute_shortest_paths_per_partition(active_shortcuts)
            new_count = new_shortcuts.count()
            logger.info(f"✓ Generated {new_count} shortcuts")
            
            resolution_results.append({
                "phase": "backward",
                "resolution": current_res,
                "active": active_count,
                "generated": new_count
            })
            
            # Merge back
            shortcuts_df = merge_shortcuts(shortcuts_df, new_shortcuts)
            shortcuts_df = shortcuts_df.localCheckpoint()
            
            active_shortcuts.unpersist()
        
        # ================================================================
        # SAVE OUTPUT
        # ================================================================
        log_section(logger, "SAVING OUTPUT")
        
        final_count = shortcuts_df.count()
        logger.info(f"Final shortcuts count: {final_count}")
        
        logger.info("Adding final info (cell, inside)...")
        shortcuts_df = add_final_info(shortcuts_df, edges_df)
        
        output_path = str(config.SHORTCUTS_OUTPUT_FILE)
        output_path = str(config.SHORTCUTS_OUTPUT_FILE).replace("_shortcuts", "_spark_scipy")

        logger.info(f"Saving to: {output_path}")
        shortcuts_df.write.mode("overwrite").parquet(output_path)
        logger.info("✓ Saved successfully!")
        
        # Summary
        log_section(logger, "SUMMARY")
        for result in resolution_results:
            logger.info(f"  {result['phase']:8} res={result['resolution']:2}: "
                       f"{result['active']} active → {result['generated']} generated")
        
        logger.info(f"\n✓ Total shortcuts: {final_count}")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
    
    finally:
        if spark:
            logger.info("Shutting down Spark...")
            spark.stop()
            logger.info("✓ Spark session closed")
    
    log_section(logger, "COMPLETED")


if __name__ == "__main__":
    main()
