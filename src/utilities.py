"""
utilities.py
============

Core utilities for Shortcuts Generation using PySpark and H3.

Key features:
- Simplified cell assignment with assign_cell_downward/upward
- Uses None for non-usable shortcuts (filter with isNotNull())
"""

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, DoubleType, IntegerType, LongType, ByteType
)
from pyspark.sql.window import Window
import h3


# ============================================================================
# 1. SPARK SESSION INITIALIZATION
# ============================================================================

def initialize_spark(app_name: str = "ShortcutsGeneration", driver_memory: str = "8g") -> SparkSession:
    """Initialize and configure a PySpark session."""
    src_dir = Path(__file__).resolve().parent

    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    existing_pythonpath = os.environ.get("PYTHONPATH", "")
    path_parts = [str(src_dir)]
    if existing_pythonpath:
        path_parts.append(existing_pythonpath)
    pythonpath_value = os.pathsep.join(path_parts)
    os.environ['PYTHONPATH'] = pythonpath_value
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executorEnv.PYTHONPATH", pythonpath_value)
        .getOrCreate()
    )

    for module_file in src_dir.glob("*.py"):
        spark.sparkContext.addPyFile(str(module_file))
    
    import config
    checkpoint_dir = str(config.CHECKPOINT_DIR)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    
    return spark


# ============================================================================
# 2. DATA LOADING
# ============================================================================

def read_edges(spark: SparkSession, file_path: str) -> DataFrame:
    """Load edge data from CSV file."""
    edges_df = spark.read.csv(file_path, header=True, inferSchema=True)
    return edges_df.select("id", "from_cell", "to_cell", "lca_res")


def initial_shortcuts_table(spark: SparkSession, file_path: str, edges_cost_df: DataFrame) -> DataFrame:
    """Create initial shortcuts table from edge graph."""
    shortcuts_df = spark.read.csv(file_path, header=True, inferSchema=True)
    # Graph file has from_edge, to_edge
    shortcuts_df = shortcuts_df.select("from_edge", "to_edge")
    shortcuts_df = shortcuts_df.withColumn("via_edge", F.col("to_edge"))
    
    shortcuts_df = shortcuts_df.join(
        edges_cost_df.select("id", "cost"),
        shortcuts_df.from_edge == edges_cost_df.id,
        "left"
    ).drop(edges_cost_df.id)
    
    return shortcuts_df


# ============================================================================
# 3. COST CALCULATION
# ============================================================================

@F.udf(returnType=DoubleType())
def dummy_cost(length: float, maxspeed: float) -> float:
    """Calculate edge cost: length / maxspeed."""
    if maxspeed <= 0:
        return float('inf')
    return float(length) / float(maxspeed)


def update_dummy_costs_for_edges(spark: SparkSession, file_path: str, edges_df: DataFrame) -> DataFrame:
    """Add cost column to edges DataFrame."""
    edges_df_cost = spark.read.csv(file_path, header=True, inferSchema=True).select("id", "length", "maxspeed")
    edges_df_cost = edges_df_cost.withColumn("cost", dummy_cost(F.col("length"), F.col("maxspeed")))
    
    edges_result = edges_df.drop("cost") if "cost" in edges_df.columns else edges_df
    edges_result = edges_result.join(edges_df_cost.select("id", "cost"), on="id", how="left")
    
    return edges_result


# ============================================================================
# 4. H3 UTILITIES
# ============================================================================

def _find_lca_impl(cell1: int, cell2: int) -> tuple:
    """Find the LCA of two H3 cells. Returns (lca_cell, lca_res)."""
    if cell1 == 0 or cell2 == 0:
        return 0, -1
    cell1_str = h3.int_to_str(cell1)
    cell2_str = h3.int_to_str(cell2)
    min_res = min(h3.get_resolution(cell1_str), h3.get_resolution(cell2_str))
    for res in range(min_res, -1, -1):
        if h3.cell_to_parent(cell1_str, res) == h3.cell_to_parent(cell2_str, res):
            return h3.str_to_int(h3.cell_to_parent(cell1_str, res)), res
    return 0, -1


def _get_parent_cell_impl(cell: int, target_res: int) -> int:
    """Get parent cell at target resolution."""
    if cell == 0 or target_res < 0:
        return 0
    cell_str = h3.int_to_str(cell)
    cell_res = h3.get_resolution(cell_str)
    if target_res > cell_res:
        return cell
    return h3.str_to_int(h3.cell_to_parent(cell_str, target_res))


@F.udf(LongType())
def find_lca(cell1: int, cell2: int) -> int:
    """UDF: Find LCA cell of two H3 cells."""
    lca, _ = _find_lca_impl(cell1, cell2)
    return lca


@F.udf(IntegerType())
def find_resolution(cell: int) -> int:
    """UDF: Get resolution of an H3 cell."""
    if cell == 0:
        return -1
    return h3.get_resolution(h3.int_to_str(cell))


@F.udf(LongType())
def get_parent_cell(cell: int, target_res: int) -> int:
    """UDF: Get parent cell at target resolution."""
    return _get_parent_cell_impl(cell, target_res)


# ============================================================================
# 5. CELL ASSIGNMENT (FORWARD AND BACKWARD PASSES)
# ============================================================================

def assign_cell_forward(shortcuts_df: DataFrame, edges_df: DataFrame, current_res: int) -> DataFrame:
    """
    Assign current_cell for FORWARD pass (resolution 15 → -1).
    
    Key insight: A shortcut that satisfies BOTH inner_res and outer_res conditions
    should be processed in BOTH cells to enable merging with different shortcuts.
    
    This function returns a union of:
    - Shortcuts assigned to inner_cell (when lca_res <= current_res <= inner_res)
    - Shortcuts assigned to outer_cell (when lca_res <= current_res <= outer_res)
    
    Args:
        shortcuts_df: Shortcuts DataFrame (incoming_edge, outgoing_edge, via_edge, cost)
        edges_df: Edges DataFrame with lca_res, incoming_cell, outgoing_cell
        current_res: Target H3 resolution level
    
    Returns:
        DataFrame with current_cell column added (may have duplicates for shortcuts in both cells)
    """
    # Drop existing temp columns if present
    for col in ["lca_res", "inner_cell", "outer_cell", "inner_res", "outer_res", "current_cell"]:
        if col in shortcuts_df.columns:
            shortcuts_df = shortcuts_df.drop(col)
    
    # Join incoming edge info: A = (A.from -> A.to)
    # CONVENTION: to_cell = A.to (where A ends), from_cell = A.from (where A starts)
    shortcuts_df = shortcuts_df.join(
        edges_df.select(
            F.col("id").alias("_in_id"),
            F.col("to_cell").alias("a_to"),     # A.to = where A ends
            F.col("from_cell").alias("a_from"),   # A.from = where A starts
            F.col("lca_res").alias("lca_res_A")
        ),
        shortcuts_df.from_edge == F.col("_in_id"),
        "left"
    ).drop("_in_id")
    
    # Join outgoing edge info: B = (B.from -> B.to)
    shortcuts_df = shortcuts_df.join(
        edges_df.select(
            F.col("id").alias("_out_id"),
            F.col("to_cell").alias("b_to"),     # B.to = where B ends
            F.col("from_cell").alias("b_from"),   # B.from = where B starts
            F.col("lca_res").alias("lca_res_B")
        ),
        shortcuts_df.to_edge == F.col("_out_id"),
        "left"
    ).drop("_out_id")
    
    # Compute lca_res = max(lca_res_A, lca_res_B)
    shortcuts_df = shortcuts_df.withColumn(
        "lca_res",
        F.greatest(F.col("lca_res_A"), F.col("lca_res_B"))
    )
    
    # Compute inner_cell = LCA(A.to, B.from) - junction point where A ends and B starts
    # For direct shortcuts (A.to == B.from), inner_res = 15
    shortcuts_df = shortcuts_df.withColumn(
        "inner_cell",
        find_lca(F.col("a_to"), F.col("b_from"))
    )
    
    # Compute outer_cell = LCA(A.from, B.to) - outer boundary from A's start to B's end
    shortcuts_df = shortcuts_df.withColumn(
        "outer_cell",
        find_lca(F.col("a_from"), F.col("b_to"))
    )
    
    # Compute resolutions
    shortcuts_df = shortcuts_df.withColumn(
        "inner_res",
        find_resolution(F.col("inner_cell"))
    )
    shortcuts_df = shortcuts_df.withColumn(
        "outer_res",
        find_resolution(F.col("outer_cell"))
    )
    
    # Part 1: Assign to INNER_CELL
    # Valid when: lca_res <= current_res <= inner_res
    inner_valid = (
        (F.col("lca_res") <= current_res) & 
        (F.col("inner_res") >= current_res)
    )
    
    inner_df = shortcuts_df.filter(inner_valid).withColumn(
        "current_cell",
        get_parent_cell(F.col("inner_cell"), F.lit(current_res))
    )
    
    # Part 2: Assign to OUTER_CELL
    # Valid when: lca_res <= current_res <= outer_res
    outer_valid = (
        (F.col("lca_res") <= current_res) & 
        (F.col("outer_res") >= current_res)
    )
    
    outer_df = shortcuts_df.filter(outer_valid).withColumn(
        "current_cell",
        get_parent_cell(F.col("outer_cell"), F.lit(current_res))
    )
    
    # Union both (shortcuts valid for both will appear twice with different cells)
    result = inner_df.unionByName(outer_df)
    
    # Drop temp columns
    result = result.drop("a_from", "a_to", "b_from", "b_to",
                         "lca_res_A", "lca_res_B", "lca_res",
                         "inner_cell", "outer_cell", "inner_res", "outer_res")
    
    return result


def assign_cell_backward(shortcuts_df: DataFrame, edges_df: DataFrame, current_res: int) -> DataFrame:
    """
    Assign current_cell for BACKWARD pass (resolution -1 → 15).
    
    Key insight: A shortcut that satisfies BOTH inner_res and outer_res conditions
    should be processed in BOTH cells (it's DOWN→UP in inner_cell but UP→DOWN in outer_cell).
    
    This function returns a union of:
    - Shortcuts assigned to inner_cell (Cases 1,2,3: at least one Lateral)
    - Shortcuts assigned to outer_cell (Case 4: Up→Down, or shortcuts valid for outer but not inner)
    
    Args:
        shortcuts_df: Shortcuts DataFrame
        edges_df: Edges DataFrame
        current_res: Target H3 resolution level
    
    Returns:
        DataFrame with current_cell column added (may have duplicates for shortcuts in both cells)
    """
    # Drop existing temp columns if present
    for col in ["lca_res", "inner_cell", "outer_cell", "inner_res", "outer_res", "current_cell"]:
        if col in shortcuts_df.columns:
            shortcuts_df = shortcuts_df.drop(col)
    
    # Join incoming edge info: A = (A.from -> A.to)
    # CONVENTION: to_cell = A.to (where A ends), from_cell = A.from (where A starts)
    shortcuts_df = shortcuts_df.join(
        edges_df.select(
            F.col("id").alias("_in_id"),
            F.col("to_cell").alias("a_to"),     # A.to = where A ends
            F.col("from_cell").alias("a_from"),   # A.from = where A starts
            F.col("lca_res").alias("lca_res_A")
        ),
        shortcuts_df.from_edge == F.col("_in_id"),
        "left"
    ).drop("_in_id")
    
    # Join outgoing edge info: B = (B.from -> B.to)
    shortcuts_df = shortcuts_df.join(
        edges_df.select(
            F.col("id").alias("_out_id"),
            F.col("to_cell").alias("b_to"),     # B.to = where B ends
            F.col("from_cell").alias("b_from"),   # B.from = where B starts
            F.col("lca_res").alias("lca_res_B")
        ),
        shortcuts_df.to_edge == F.col("_out_id"),
        "left"
    ).drop("_out_id")
    
    # Compute lca_res = max(lca_res_A, lca_res_B)
    shortcuts_df = shortcuts_df.withColumn(
        "lca_res",
        F.greatest(F.col("lca_res_A"), F.col("lca_res_B"))
    )
    
    # Compute inner_cell = LCA(A.to, B.from) - junction point where A ends and B starts
    # For direct shortcuts (A.to == B.from), inner_res = 15
    shortcuts_df = shortcuts_df.withColumn(
        "inner_cell",
        find_lca(F.col("a_to"), F.col("b_from"))
    )
    
    # Compute outer_cell = LCA(A.from, B.to) - outer boundary from A's start to B's end
    shortcuts_df = shortcuts_df.withColumn(
        "outer_cell",
        find_lca(F.col("a_from"), F.col("b_to"))
    )
    
    # Compute resolutions
    shortcuts_df = shortcuts_df.withColumn(
        "inner_res",
        find_resolution(F.col("inner_cell"))
    )
    shortcuts_df = shortcuts_df.withColumn(
        "outer_res",
        find_resolution(F.col("outer_cell"))
    )
    
    # Part 1: Assign to INNER_CELL
    # Valid when: lca_res <= current_res <= inner_res
    inner_valid = (
        (F.col("lca_res") <= current_res) & 
        (F.col("inner_res") >= current_res)
    )
    
    inner_df = shortcuts_df.filter(inner_valid).withColumn(
        "current_cell",
        get_parent_cell(F.col("inner_cell"), F.lit(current_res))
    )
    
    # Part 2: Assign to OUTER_CELL
    # Valid when: lca_res <= current_res <= outer_res
    # This handles Case 4 (Up→Down) AND shortcuts valid for outer that weren't handled by inner
    outer_valid = (
        (F.col("lca_res") <= current_res) & 
        (F.col("outer_res") >= current_res)
    )
    
    outer_df = shortcuts_df.filter(outer_valid).withColumn(
        "current_cell",
        get_parent_cell(F.col("outer_cell"), F.lit(current_res))
    )
    
    # Union both (shortcuts valid for both will appear twice with different cells)
    result = inner_df.unionByName(outer_df)
    
    # Drop temp columns
    result = result.drop("a_from", "a_to", "b_from", "b_to",
                         "lca_res_A", "lca_res_B", "lca_res",
                         "inner_cell", "outer_cell", "inner_res", "outer_res")
    
    return result


def filter_active_shortcuts(shortcuts_df: DataFrame) -> DataFrame:
    """
    Filter to only shortcuts with assigned cells (usable at current resolution).
    
    Returns:
        DataFrame with only rows where current_cell is not null
    """
    return shortcuts_df.filter(F.col("current_cell").isNotNull())


# ============================================================================
# 6. MERGING RESULTS
# ============================================================================

def merge_shortcuts(main_df: DataFrame, new_shortcuts: DataFrame) -> DataFrame:
    """
    Merge new shortcuts into main table, keeping minimum cost paths.
    
    Args:
        main_df: Main shortcuts DataFrame
        new_shortcuts: Newly computed shortcuts
    
    Returns:
        Updated DataFrame with best shortcuts
    """
    # Standardize columns
    main_df = main_df.select("from_edge", "to_edge", "cost", "via_edge")
    new_shortcuts = new_shortcuts.select("from_edge", "to_edge", "cost", "via_edge")
    
    combined = main_df.unionByName(new_shortcuts)
    
    # Keep minimum cost for each (source, target) pair
    window_spec = Window.partitionBy("from_edge", "to_edge").orderBy(F.col("cost").asc())
    
    result = combined.withColumn(
        "rank", F.row_number().over(window_spec)
    ).filter(
        F.col("rank") == 1
    ).drop("rank")
    
    return result


# ============================================================================
# 7. FINAL OUTPUT PREPARATION
# ============================================================================

def add_final_info(shortcuts_df: DataFrame, edges_df: DataFrame) -> DataFrame:
    """
    Add final columns for output: cell, inside.
    Also filters out shortcuts with inverted valid ranges.
    
    - cell: H3 cell where shortcut is used
    - inside: Direction indicator (+1=up, 0=lateral, -1=down)
    
    A shortcut is valid if: lca_res <= inner_res OR lca_res <= outer_res
    
    Args:
        shortcuts_df: Shortcuts DataFrame
        edges_df: Edges DataFrame
    
    Returns:
        DataFrame with cell and inside columns, filtered to valid shortcuts
    """
    # Join incoming edge info: A = (A.from -> A.to)
    # CONVENTION: to_cell = A.to (where A ends), from_cell = A.from (where A starts)
    shortcuts_df = shortcuts_df.join(
        edges_df.select(
            F.col("id").alias("_in_id"),
            F.col("to_cell").alias("a_to"),     # A.to = where A ends
            F.col("from_cell").alias("a_from"),   # A.from = where A starts
            F.col("lca_res").alias("lca_in")
        ),
        shortcuts_df.from_edge == F.col("_in_id"),
        "left"
    ).drop("_in_id")
    
    # Join outgoing edge info: B = (B.from -> B.to)
    shortcuts_df = shortcuts_df.join(
        edges_df.select(
            F.col("id").alias("_out_id"),
            F.col("to_cell").alias("b_to"),     # B.to = where B ends
            F.col("from_cell").alias("b_from"),   # B.from = where B starts
            F.col("lca_res").alias("lca_out")
        ),
        shortcuts_df.to_edge == F.col("_out_id"),
        "left"
    ).drop("_out_id")
    
    # Compute lca_res
    shortcuts_df = shortcuts_df.withColumn(
        "lca_res",
        F.greatest(F.col("lca_in"), F.col("lca_out"))
    )
    
    # Compute inner_cell = LCA(A.to, B.from) - junction point where A ends and B starts
    # For direct shortcuts (A.to == B.from), inner_res = 15
    shortcuts_df = shortcuts_df.withColumn(
        "inner_cell",
        find_lca(F.col("a_to"), F.col("b_from"))
    )
    shortcuts_df = shortcuts_df.withColumn(
        "inner_res",
        find_resolution(F.col("inner_cell"))
    )
    
    # Compute outer_cell = LCA(A.from, B.to) - outer boundary from A's start to B's end
    shortcuts_df = shortcuts_df.withColumn(
        "outer_cell",
        find_lca(F.col("a_from"), F.col("b_to"))
    )
    shortcuts_df = shortcuts_df.withColumn(
        "outer_res",
        find_resolution(F.col("outer_cell"))
    )
    
    # Validity filter: keep only shortcuts where lca_res <= inner_res OR lca_res <= outer_res
    # NOTE: This is a validity check only. With the current algorithm, all generated
    # shortcuts should pass this filter. If any are filtered, it indicates a bug.
    shortcuts_df = shortcuts_df.filter(
        (F.col("lca_res") <= F.col("inner_res")) | 
        (F.col("lca_res") <= F.col("outer_res"))
    )
    
    # Compute inside:
    # -2: outer-only shortcut (lca_res > inner_res) - cannot participate in inner cell merging
    # +1: upward (lca_in > lca_out)
    # 0: lateral (lca_in == lca_out)
    # -1: downward (lca_in < lca_out)
    shortcuts_df = shortcuts_df.withColumn(
        "inside",
        F.when(F.col("lca_res") > F.col("inner_res"), -2)  # outer-only
        .when(F.col("lca_in") == F.col("lca_out"), 0)
        .when(F.col("lca_in") < F.col("lca_out"), -1)
        .otherwise(1).cast(ByteType())
    )
    
    # Compute whole_res = min(lca_in, lca_out) - the coarsest level of either edge
    shortcuts_df = shortcuts_df.withColumn(
        "whole_res",
        F.least(F.col("lca_in"), F.col("lca_out"))
    )
    
    # Compute cell = parent(outer_cell, whole_res)
    shortcuts_df = shortcuts_df.withColumn(
        "cell",
        get_parent_cell(F.col("outer_cell"), F.col("whole_res"))
    )
    
    # Drop temp columns, keep final output
    shortcuts_df = shortcuts_df.drop(
        "a_from", "a_to", "b_from", "b_to", 
        "lca_in", "lca_out", "lca_res", "whole_res",
        "inner_cell", "inner_res", "outer_cell", "outer_res"
    )
    
    return shortcuts_df

