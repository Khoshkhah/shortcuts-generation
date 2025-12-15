# Shortcuts Generation Documentation

This folder contains documentation for the shortcuts generation algorithm.

## Documents

| Document | Description |
|----------|-------------|
| [Core Concepts](core_concepts.md) | Definitions of edges, cells, shortcuts, LCA, and resolution |
| [Data Structures](data_structures.md) | DataFrame schemas and column descriptions |
| [Algorithm](algorithm.md) | Three-phase algorithm: downward, global, and upward passes |
| [Tree Decomposition](tree_decomposition.md) | Theory of tree decomposition and H3 as natural tree structure |

## Quick Reference

### Key Terms

- **Edge**: A road segment with incoming_cell (start) and outgoing_cell (end)
- **Shortcut**: Precomputed path from incoming_edge to outgoing_edge
- **LCA**: Lowest Common Ancestor - coarsest cell containing both endpoints
- **Resolution**: H3 hierarchy level (0=global, 15=finest)

### Special Values

| Value | Meaning |
|-------|---------|
| `cell = 0` | Invalid/global cell |
| `resolution = -1` | Global scope |
| `current_cell = None` | Shortcut not usable at current resolution |

### Cell Assignment

| Pass | Condition | Cell Value |
|------|-----------|------------|
| Downward | `lca_res <= R AND via_res >= R` | `parent(via_cell, R)` |
| Upward | `lca_res == R` | `via_cell` |
| Not usable | Otherwise | `None` |
