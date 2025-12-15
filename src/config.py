"""
Configuration parameters for Shortcuts Generation

This module centralizes all configurable parameters for the application.
"""

import os
from pathlib import Path

# ============================================================================
# SPARK CONFIGURATION
# ============================================================================

SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "8g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
SPARK_EXECUTOR_CORES = int(os.getenv("SPARK_EXECUTOR_CORES", "4"))
SPARK_APP_NAME = "ShortcutsGeneration"

# Project Root (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Checkpoint directory for Spark lineage management
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"

# ============================================================================
# FILE PATHS
# ============================================================================

DATA_DIR = PROJECT_ROOT / "data"

# District name - change this to process different datasets
DISTRICT_NAME = "Somerset"
#DISTRICT_NAME = "Burnaby"

# Edge data file - contains road network with H3 indices
EDGES_FILE = DATA_DIR / f"{DISTRICT_NAME}_driving_simplified_edges_with_h3.csv"

# Edge graph file - defines edge connectivity
GRAPH_FILE = DATA_DIR / f"{DISTRICT_NAME}_driving_edge_graph.csv"

# Output directory
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Final shortcuts output
SHORTCUTS_OUTPUT_FILE = OUTPUT_DIR / f"{DISTRICT_NAME}_shortcuts"

# ============================================================================
# COMPUTATION PARAMETERS
# ============================================================================

# H3 Resolution Range
MIN_H3_RESOLUTION = 0
MAX_H3_RESOLUTION = 15

# Process from high to low resolution (downward pass)
RESOLUTION_RANGE_DOWN = range(MAX_H3_RESOLUTION, MIN_H3_RESOLUTION - 2, -1)

# Process from low to high resolution (upward pass)
RESOLUTION_RANGE_UP = range(MIN_H3_RESOLUTION, MAX_H3_RESOLUTION + 1)

# ============================================================================
# LOGGING
# ============================================================================

LOG_LEVEL = "INFO"
VERBOSE = True
