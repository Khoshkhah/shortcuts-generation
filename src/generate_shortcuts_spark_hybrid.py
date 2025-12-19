"""
generate_shortcuts_hybrid.py
=============================

Hybrid shortcuts generation combining Scipy and Pure Spark algorithms.

Allows selecting which algorithm to use for each resolution level:
- Scipy: Faster for smaller partitions (fine resolutions)
- Pure Spark: Better for larger partitions (coarse resolutions)

Algorithm:
1. Forward pass (resolution 15 → -1): Build LOCAL shortcuts within cells
2. Backward pass (resolution 0 → 15): Build GLOBAL shortcuts with two-cell approach
"""

import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import shortest_path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
# SCIPY SHORTEST PATH COMPUTATION (from generate_shortcuts.py)
# ============================================================================

def compute_shortest_paths_scipy(
    df_shortcuts: DataFrame,
    partition_columns: list = ["current_cell"]
) -> DataFrame:
    """Compute all-pairs shortest paths using Scipy per partition."""
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
        
        # Store via_edges for direct paths
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
# PURE SPARK SHORTEST PATH COMPUTATION (from generate_shortcuts_spark.py)
# ============================================================================

def has_converged(current_paths: DataFrame, next_paths: DataFrame) -> bool:
    """Check if the shortest path computation has converged."""
    join_keys = ["from_edge", "to_edge", "cost", "current_cell"]
    
    changes = next_paths.join(
        current_paths.select(join_keys),
        on=join_keys,
        how="left_anti"
    )
    
    return changes.limit(1).count() == 0


def compute_shortest_paths_pure_spark(
    shortcuts_df: DataFrame,
    max_iterations: int = 10
) -> DataFrame:
    """Compute all-pairs shortest paths using pure Spark SQL operations."""
    
    logger.info(f"Computing shortest paths using pure Spark (max_iterations={max_iterations})")
    
    current_paths = shortcuts_df.select(
        "from_edge", "to_edge", "cost", "via_edge", "current_cell"
    ).cache()
    
    initial_count = current_paths.count()
    logger.info(f"Initial paths count: {initial_count}")
    
    for iteration in range(max_iterations):
        logger.info(f"--- Iteration {iteration} ---")
        
        try:
            # Path extension via self-join
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
            
            new_count = new_paths.count()
            logger.info(f"Found {new_count} new paths")
            
            if new_count == 0:
                logger.info("✓ No new paths found. Converged.")
                new_paths.unpersist()
                break
            
            # Cost minimization via window function
            all_paths = current_paths.unionByName(new_paths)
            
            window_spec = Window.partitionBy(
                "from_edge", "to_edge", "current_cell"
            ).orderBy(F.col("cost").asc(), F.col("via_edge").asc())
            
            next_paths = all_paths.withColumn(
                "rnk", F.row_number().over(window_spec)
            ).filter(F.col("rnk") == 1).drop("rnk").localCheckpoint()
            
            next_count = next_paths.count()
            logger.info(f"After cost minimization: {next_count} paths")
            
            current_paths.unpersist()
            new_paths.unpersist()
            
            if next_count == initial_count and has_converged(current_paths, next_paths):
                logger.info(f"✓ Iteration {iteration}: Converged!")
                current_paths = next_paths
                break
            
            current_paths = next_paths
            initial_count = next_count
            logger.info(f"✓ Iteration {iteration}: Completed")
            
        except Exception as e:
            logger.error(f"Error in iteration {iteration}: {str(e)}")
            raise
    
    logger.info("✓ Pure Spark computation completed")
    return current_paths.drop("current_cell")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main(
    scipy_resolutions: list = None,
    pure_spark_resolutions: list = None,
    max_iterations: int = 10
):
    """
    Main execution function for hybrid version.
    
    Args:
        scipy_resolutions: Resolutions to use Scipy for (default: 0-10)
        pure_spark_resolutions: Resolutions to use pure Spark for (default: 11-15)
        max_iterations: Max iterations for pure Spark algorithm
    """
    
    # Default strategy: Scipy for fine, pure Spark for coarse
    if scipy_resolutions is None:
        scipy_resolutions = list(range(-1, 12))  # -1 to 11
    if pure_spark_resolutions is None:
        pure_spark_resolutions = list(range(12, 16))  # 12 to 15
    
    log_section(logger, "SHORTCUTS GENERATION - HYBRID VERSION")
    
    config_info = {
        "edges_file": str(config.EDGES_FILE),
        "graph_file": str(config.GRAPH_FILE),
        "output_file": str(config.SHORTCUTS_OUTPUT_FILE),
        "district": config.DISTRICT_NAME,
        "scipy_resolutions": str(scipy_resolutions),
        "pure_spark_resolutions": str(pure_spark_resolutions),
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
        
        for current_res in range(15, -2, -1):
            log_section(logger, f"Forward: Resolution {current_res}")
            
            # Determine which algorithm to use
            use_scipy = current_res in scipy_resolutions
            algorithm = "Scipy" if use_scipy else "Pure Spark"
            logger.info(f"Using {algorithm} algorithm for resolution {current_res}")
            
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
            
            active_shortcuts = active_shortcuts.cache()
            
            # Compute shortest paths with selected algorithm
            if use_scipy:
                new_shortcuts = compute_shortest_paths_scipy(active_shortcuts)
            else:
                new_shortcuts = compute_shortest_paths_pure_spark(
                    active_shortcuts, max_iterations=max_iterations
                )
            
            new_count = new_shortcuts.count()
            logger.info(f"✓ Generated {new_count} shortcuts using {algorithm}")
            
            resolution_results.append({
                "phase": "forward",
                "resolution": current_res,
                "active": active_count,
                "generated": new_count,
                "algorithm": algorithm
            })
            
            # Merge back
            shortcuts_df = merge_shortcuts(shortcuts_df, new_shortcuts)
            shortcuts_df = shortcuts_df.localCheckpoint()
            
            active_shortcuts.unpersist()
        
        # ================================================================
        # PHASE 2: BACKWARD PASS (0 → 15)
        # ================================================================
        log_section(logger, "PHASE 2: BACKWARD PASS (0 → 15)")
        
        for current_res in range(0, 16):
            log_section(logger, f"Backward: Resolution {current_res}")
            
            # Determine which algorithm to use
            use_scipy = current_res in scipy_resolutions
            algorithm = "Scipy" if use_scipy else "Pure Spark"
            logger.info(f"Using {algorithm} algorithm for resolution {current_res}")
            
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
            
            # Compute shortest paths with selected algorithm
            if use_scipy:
                new_shortcuts = compute_shortest_paths_scipy(active_shortcuts)
            else:
                new_shortcuts = compute_shortest_paths_pure_spark(
                    active_shortcuts, max_iterations=max_iterations
                )
            
            new_count = new_shortcuts.count()
            logger.info(f"✓ Generated {new_count} shortcuts using {algorithm}")
            
            resolution_results.append({
                "phase": "backward",
                "resolution": current_res,
                "active": active_count,
                "generated": new_count,
                "algorithm": algorithm
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
        
        output_path = str(config.SHORTCUTS_OUTPUT_FILE).replace("_shortcuts", "_spark_hybrid")
        logger.info(f"Saving to: {output_path}")
        final_df.write.mode("overwrite").parquet(output_path)
        logger.info("✓ Saved successfully!")
        
        # ================================================================
        # SUMMARY
        # ================================================================
        log_section(logger, "SUMMARY")
        for r in resolution_results:
            logger.info(f"  {r['phase']:8s} res={r['resolution']:2d}: {r['active']} active → {r['generated']} generated ({r['algorithm']})")
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
    # Default: Scipy for -1 to 11, Pure Spark for 12-15
    main(max_iterations=100)
