# Shortcuts Generation

Hierarchical shortcut generation for road networks using H3, PySpark, DuckDB, and Scipy.

## Overview

This project generates precomputed all-pairs shortest path shortcuts for efficient road network routing. It implements 6 algorithm variants across two execution engines:

- **PySpark**: Distributed computation (better for very large networks)
- **DuckDB**: Single-node in-memory (faster for medium-sized networks)

## Performance Comparison (Somerset Dataset: 5,900 edges → 378,141 shortcuts)

| Engine     | Algorithm | Running Time | Notes |
|------------|-----------|--------------|-------|
| **DuckDB** | Hybrid    | ~2 min       | Fastest overall |
| **DuckDB** | Pure      | ~2.5 min     | SQL-only, no Python UDFs |
| **DuckDB** | Scipy     | ~2.5 min     | Uses scipy.shortest_path per partition |
| **Spark**  | Hybrid    | ~3 min       | Default for medium networks |
| **Spark**  | Pure      | ~4.5 min     | SQL-only via Spark DataFrames |
| **Spark**  | Scipy     | ~9.5 min     | Uses applyInPandas with scipy |

All implementations produce **100% verified optimal shortest paths**.

## Algorithm

Three-phase hierarchical approach:

1. **Forward Pass** (Resolution 15 → -1): Build shortcuts within progressively larger cells
2. **Global Pass** (Resolution -1): Handle network-spanning shortcuts
3. **Backward Pass** (Resolution 0 → 15): Propagate improvements across cell boundaries

See [docs/algorithm.md](docs/algorithm.md) for details.

## Project Structure

```
road-to-shortcut/
├── src/
│   ├── config.py                          # Configuration
│   ├── utilities.py                       # Core utilities (Spark)
│   ├── utilities_duckdb.py                # Core utilities (DuckDB)
│   ├── generate_shortcuts_spark_pure.py   # Spark Pure implementation
│   ├── generate_shortcuts_spark_scipy.py  # Spark Scipy implementation
│   ├── generate_shortcuts_spark_hybrid.py # Spark Hybrid implementation
│   ├── generate_shortcuts_duckdb_pure.py  # DuckDB Pure implementation
│   ├── generate_shortcuts_duckdb_scipy.py # DuckDB Scipy implementation
│   ├── generate_shortcuts_duckdb_hybrid.py# DuckDB Hybrid implementation
│   ├── compare_outputs.py                 # Compare two shortcut outputs
│   └── verify_shortcuts.py                # Verify shortcuts are optimal
├── docs/
│   ├── core_concepts.md                   # Edge, cell, shortcut definitions
│   ├── data_structures.md                 # DataFrame schemas
│   ├── algorithm.md                       # Algorithm overview
│   └── tree_decomposition.md              # Theory and H3 relationship
├── notebooks/
│   ├── analysis.ipynb                     # Shortcut analysis
│   └── validate_shortcuts.ipynb           # Validation notebook
├── data/                                  # Symlink to road network data
├── output/                                # Generated shortcuts
├── logs/                                  # Execution logs
└── requirements.txt
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Edit config.py to set DISTRICT_NAME and paths
vim src/config.py

# Run generation (choose one)
cd src
python generate_shortcuts_duckdb_hybrid.py  # Fastest
python generate_shortcuts_spark_pure.py     # Pure Spark
python generate_shortcuts_spark_scipy.py    # Spark + Scipy
```

## Verification

```bash
# Verify shortcuts are optimal shortest paths
python src/verify_shortcuts.py output/Somerset_duckdb_hybrid

# Compare two implementations
python src/compare_outputs.py --ref output/Somerset_spark --new output/Somerset_duckdb
```

## Key Concepts

- **current_cell = None**: Shortcut not usable at current resolution (filtered out)
- **current_cell = H3 cell**: Shortcut is active and partitioned by this cell
- **via_edge**: First hop in the path (used for path reconstruction)

See [docs/core_concepts.md](docs/core_concepts.md) for detailed definitions.

## Input Data

Required CSV files:
- `{district}_driving_simplified_edges_with_h3.csv`: Edge data with H3 cells
- `{district}_driving_edge_graph.csv`: Edge connectivity

## Output

Parquet file with columns:
- `from_edge`: Starting edge ID
- `to_edge`: Ending edge ID  
- `via_edge`: Intermediate edge for path reconstruction
- `cost`: Total travel cost
- `cell`: H3 cell where shortcut is used
- `inside`: Direction (+1=up, 0=lateral, -1=down)

## License

MIT
