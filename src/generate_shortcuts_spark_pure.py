"""
generate_shortcuts_spark.py
============================

Shortcuts generation using Pure Spark SQL operations.

Algorithm:
1. Forward pass (resolution 15 → -1): Build LOCAL shortcuts within cells
2. Backward pass (resolution 0 → 15): Build GLOBAL shortcuts with two-cell approach

Uses self-joins and window functions instead of Scipy for shortest path computation.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
# PURE SPARK SHORTEST PATH COMPUTATION
# ============================================================================

def has_converged(current_paths: DataFrame, next_paths: DataFrame) -> bool:
    """
    Check if the shortest path computation has converged.
    
    Convergence occurs when no new paths or path improvements are found.
    """
    join_keys = ["from_edge", "to_edge", "cost", "current_cell"]
    
    # Find rows in next_paths that don't exist in current_paths
    changes = next_paths.join(
        current_paths.select(join_keys),
        on=join_keys,
        how="left_anti"
    )
    
    # True if no changes found (converged)
    return changes.limit(1).count() == 0


def compute_shortest_paths_pure_spark(
    shortcuts_df: DataFrame,
    max_iterations: int = 10
) -> DataFrame:
    """
    Compute all-pairs shortest paths using pure Spark SQL operations.
    
    Uses self-joins and window functions to iteratively extend paths until
    convergence (no new paths and no cost improvements).
    
    Args:
        shortcuts_df: Input shortcuts DataFrame with current_cell column
        max_iterations: Maximum iterations before stopping
    
    Returns:
        DataFrame with computed shortest paths
    """
    
    logger.info(f"Starting pure Spark shortest path computation (max_iterations={max_iterations})")
    
    # Initialize
    current_paths = shortcuts_df.select(
        "from_edge", "to_edge", "cost", "via_edge", "current_cell"
    ).cache()
    
    # Get initial stats
    stats = current_paths.agg(
        F.count("*").alias("count"),
        F.sum("cost").alias("cost_sum")
    ).collect()[0]
    prev_count = stats["count"]
    prev_cost_sum = stats["cost_sum"] if stats["cost_sum"] is not None else 0.0
    
    logger.info(f"Initial: {prev_count} paths, CostSum: {prev_cost_sum:.4f}")
    
    for iteration in range(max_iterations):
        try:
            # --- PATH EXTENSION via self-join ---
            new_paths = current_paths.alias("L").join(
                current_paths.alias("R"),
                [
                    F.col("L.to_edge") == F.col("R.from_edge"),
                    F.col("L.current_cell") == F.col("R.current_cell")
                ],
                "inner"
            ).filter(
                (F.col("L.from_edge") != F.col("R.to_edge"))
            ).select(
                F.col("L.from_edge").alias("from_edge"),
                F.col("R.to_edge").alias("to_edge"),
                (F.col("L.cost") + F.col("R.cost")).alias("cost"),
                F.col("L.to_edge").alias("via_edge"),
                F.col("L.current_cell").alias("current_cell")
            ).cache()
            
            # --- COST MINIMIZATION using window function ---
            all_paths = current_paths.unionByName(new_paths)
            
            window_spec = Window.partitionBy(
                "from_edge",
                "to_edge",
                "current_cell"
            ).orderBy(
                F.col("cost").asc(),
                F.col("via_edge").asc()
            )
            
            next_paths = all_paths.withColumn(
                "rnk",
                F.row_number().over(window_spec)
            ).filter(
                F.col("rnk") == 1
            ).drop("rnk").localCheckpoint()
            
            # --- GET STATS FOR CONVERGENCE CHECK ---
            stats = next_paths.agg(
                F.count("*").alias("count"),
                F.sum("cost").alias("cost_sum")
            ).collect()[0]
            curr_count = stats["count"]
            curr_cost_sum = stats["cost_sum"] if stats["cost_sum"] is not None else 0.0
            
            cost_diff = prev_cost_sum - curr_cost_sum  # Positive = improvement
            
            logger.info(f"Iteration {iteration}: Rows {prev_count} -> {curr_count}, CostSum {prev_cost_sum:.4f} -> {curr_cost_sum:.4f} (diff: {cost_diff:.4f})")
            
            # Clean up
            current_paths.unpersist()
            new_paths.unpersist()
            
            # --- CONVERGENCE CHECK: Same count AND no cost improvement ---
            if curr_count == prev_count and abs(cost_diff) < 1e-6:
                logger.info("Converged.")
                current_paths = next_paths
                break
            
            current_paths = next_paths
            prev_count = curr_count
            prev_cost_sum = curr_cost_sum
            
        except Exception as e:
            logger.error(f"Error in iteration {iteration}: {str(e)}")
            raise
    
    logger.info(f"Shortest path computation completed after {iteration + 1} iterations")
    
    # Remove cell column and return result
    return current_paths.drop("current_cell")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main(max_iterations: int = 10):
    """Main execution function for pure Spark version."""
    log_section(logger, "SHORTCUTS GENERATION - PURE SPARK VERSION")
    
    config_info = {
        "edges_file": str(config.EDGES_FILE),
        "graph_file": str(config.GRAPH_FILE),
        "output_file": str(config.SHORTCUTS_OUTPUT_FILE),
        "district": config.DISTRICT_NAME,
        "max_iterations": max_iterations
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
            
            # Compute shortest paths using pure Spark
            new_shortcuts = compute_shortest_paths_pure_spark(
                active_shortcuts, 
                max_iterations=max_iterations
            )
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
            
            # Compute shortest paths using pure Spark
            new_shortcuts = compute_shortest_paths_pure_spark(
                active_shortcuts,
                max_iterations=max_iterations
            )
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
        final_df = add_final_info(shortcuts_df, edges_df)
        
        output_path = str(config.SHORTCUTS_OUTPUT_FILE).replace("_shortcuts", "_spark_pure")
        logger.info(f"Saving to: {output_path}")
        final_df.write.mode("overwrite").parquet(output_path)
        logger.info("✓ Saved successfully!")
        
        # ================================================================
        # SUMMARY
        # ================================================================
        log_section(logger, "SUMMARY")
        for r in resolution_results:
            logger.info(f"  {r['phase']:8s} res={r['resolution']:2d}: {r['active']} active → {r['generated']} generated")
        logger.info(f"\n✓ Total shortcuts: {final_count}")
        
    except Exception as e:
        logger.error(f"Error during execution: {str(e)}")
        raise
    
    finally:
        if spark:
            logger.info("Shutting down Spark...")
            spark.stop()
            logger.info("✓ Spark session closed")
    
    log_section(logger, "COMPLETED")


if __name__ == "__main__":
    main(max_iterations=100)
