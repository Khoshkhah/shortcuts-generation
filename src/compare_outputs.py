
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description="Compare shortcut generation outputs.")
    parser.add_argument("--ref", required=True, help="Path to reference parquet directory/file")
    parser.add_argument("--new", required=True, help="Path to new parquet directory/file")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("CompareOutputs").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Paths
    ref_path = args.ref
    new_path = args.new
    
    print(f"Reference Path: {ref_path}")
    print(f"New Path:       {new_path}")
    
    # Load DataFrames
    try:
        ref_df = spark.read.parquet(ref_path)
        new_df = spark.read.parquet(new_path)
    except Exception as e:
        print(f"Error loading files: {e}")
        spark.stop()
        return

    # Count
    ref_count = ref_df.count()
    new_count = new_df.count()
    
    print(f"\n--- Counts ---")
    print(f"Reference Count: {ref_count}")
    print(f"New Count:       {new_count}")
    
    # Schema alignment
    # Reference likely uses incoming_edge/outgoing_edge
    print("\n--- Schema Check ---")
    # Create clean DataFrames with standardized schema BEFORE joining
    # This disconnects the lineage from conflicting metadata
    ref_clean = ref_df.select(
        F.col("incoming_edge" if "incoming_edge" in ref_df.columns else "from_edge").alias("from_edge"),
        F.col("outgoing_edge" if "outgoing_edge" in ref_df.columns else "to_edge").alias("to_edge"),
        F.col("cost"),
        F.col("via_edge"),
        F.col("inside"),
        F.col("cell")
    )
    
    new_clean = new_df.select(
        F.col("from_edge"),
        F.col("to_edge"),
        F.col("cost"),
        F.col("via_edge"),
        F.col("inside"),
        F.col("cell")
    )

    # Rename columns explicitly to avoid any ambiguity
    ref_renamed = ref_clean.withColumnRenamed("from_edge", "ref_from") \
                           .withColumnRenamed("to_edge", "ref_to") \
                           .withColumnRenamed("cost", "ref_cost") \
                           .withColumnRenamed("via_edge", "ref_via") \
                           .withColumnRenamed("inside", "ref_inside") \
                           .withColumnRenamed("cell", "ref_cell")
                           
    new_renamed = new_clean.withColumnRenamed("from_edge", "new_from") \
                           .withColumnRenamed("to_edge", "new_to") \
                           .withColumnRenamed("cost", "new_cost") \
                           .withColumnRenamed("via_edge", "new_via") \
                           .withColumnRenamed("inside", "new_inside") \
                           .withColumnRenamed("cell", "new_cell")
    
    # Compare matching rows
    # Join on mismatched names
    final_df = ref_renamed.join(
        new_renamed,
        (F.col("ref_from") == F.col("new_from")) & 
        (F.col("ref_to") == F.col("new_to")),
        "outer"
    )
    
    # Categorize
    matched = final_df.filter(F.col("ref_from").isNotNull() & F.col("new_from").isNotNull()) 
    # Note: ref_from isNotNull implies matched because of inner join logic, but let's stick to safe filter
    
    print(f"\n--- Comparison Results ---")
    print(f"Matched Shortcuts: {matched.count()}")

    # Detailed Column Checks
    print(f"\n--- Column-wise Exact Matches ---")
    
    # Via Edge
    via_mismatch = matched.filter(F.col("ref_via") != F.col("new_via"))
    print(f"Via Edge Mismatches: {via_mismatch.count()}")
    if via_mismatch.count() > 0:
        print("  Sample Via Mismatches:")
        via_mismatch.select("ref_from", "ref_to", "ref_via", "new_via").show(5)

    # Inside
    inside_mismatch = matched.filter(F.col("ref_inside") != F.col("new_inside"))
    print(f"Inside Mismatches:   {inside_mismatch.count()}")
    if inside_mismatch.count() > 0:
         print("  Sample Inside Mismatches:")
         inside_mismatch.select("ref_from", "ref_to", "ref_inside", "new_inside").show(5)

    # Cell
    cell_mismatch = matched.filter(F.col("ref_cell") != F.col("new_cell"))
    print(f"Cell Mismatches:     {cell_mismatch.count()}")
    
    # Compare costs for matched
    cost_diff = matched.withColumn("diff", F.abs(F.col("ref_cost") - F.col("new_cost")))
    diff_stats = cost_diff.select(F.mean("diff"), F.max("diff")).collect()[0]
    
    print(f"\n--- Cost Differences (Matched) ---")
    print(f"Mean Absolute Difference: {diff_stats[0]}")
    print(f"Max Absolute Difference:  {diff_stats[1]}")
    
    matches_exact = cost_diff.filter(F.col("diff") < 1e-6).count()
    print(f"Exact Matches (diff < 1e-6): {matches_exact}")
    
    print("\n--- Top 10 Discrepancies ---")
    cost_diff.filter(F.col("diff") > 1e-6) \
             .sort(F.col("diff").desc()) \
             .select("ref_from", "ref_to", "ref_cost", "new_cost", "diff") \
             .show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
