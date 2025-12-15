# Shortcuts Generation

Hierarchical shortcut generation for road networks using PySpark, H3, and Scipy.

## Overview

This project generates precomputed shortcuts for efficient road network routing. It uses:

- **H3 Hexagonal Hierarchy**: Natural tree decomposition of geographic space
- **PySpark**: Distributed computation across H3 cell partitions  
- **Scipy**: Fast C-optimized shortest path algorithms (Dijkstra)

## Algorithm

Three-phase hierarchical approach:

1. **Downward Pass** (Resolution 15 → 0): Build shortcuts within progressively larger cells
2. **Global Pass** (Resolution -1): Handle network-spanning shortcuts
3. **Upward Pass** (Resolution 0 → 15): Propagate improvements to finer resolutions

See [docs/algorithm.md](docs/algorithm.md) for details.

## Project Structure

```
shortcuts-generation/
├── src/
│   ├── config.py              # Configuration
│   ├── logging_config.py      # Logging setup
│   ├── utilities.py           # Core utilities with cell assignment
│   └── generate_shortcuts.py  # Main entry point
├── docs/
│   ├── core_concepts.md       # Edge, cell, shortcut definitions
│   ├── data_structures.md     # DataFrame schemas
│   ├── algorithm.md           # Algorithm overview
│   └── tree_decomposition.md  # Theory and H3 relationship
├── data/                      # Symlink to road network data
├── output/                    # Generated shortcuts
├── logs/                      # Execution logs
└── requirements.txt
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Edit config.py to set DISTRICT_NAME
# Run generation
cd src
python generate_shortcuts.py
```

## Key Concepts

- **current_cell = None**: Shortcut not usable at current resolution (filtered out)
- **current_cell = H3 cell**: Shortcut is active and partitioned by this cell

See [docs/core_concepts.md](docs/core_concepts.md) for detailed definitions.

## Input Data

Required CSV files:
- `{district}_driving_simplified_edges_with_h3.csv`: Edge data with H3 cells
- `{district}_driving_edge_graph.csv`: Edge connectivity

## Output

Parquet file with columns:
- `incoming_edge`: Starting edge ID
- `outgoing_edge`: Ending edge ID  
- `via_edge`: Intermediate edge for path reconstruction
- `cost`: Total travel cost
- `cell`: H3 cell where shortcut is used
- `inside`: Direction (+1=up, 0=lateral, -1=down)
