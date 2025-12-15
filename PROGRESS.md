# Documentation Progress

Last updated: 2025-12-13

## Status: Implementation Complete

Documentation and implementation updated with refined algorithm.

## Algorithm Summary

### Two-Phase Approach
1. **Forward Pass** (15 → -1): Build LOCAL shortcuts
2. **Backward Pass** (-1 → 15): Build GLOBAL shortcuts

### Cell Assignment

**Forward Pass:**
- Uses `inner_cell = LCA(y_A, x_B)` (junction point)
- Valid if: `lca_res <= R <= inner_res`
- Cell = `parent(inner_cell, R)`

**Backward Pass (Two-Cell Approach):**
- Case 1,2,3 (Lat→Lat, Lat→Up, Down→Lat): `inner_cell = LCA(y_A, x_B)`
- Case 4 (Up→Down): `outer_cell = LCA(x_A, y_B)`
- Select based on: `lca_res_A == R` or `lca_res_B == R`

## Key Definitions

| Term | Definition |
|------|------------|
| inner_cell | LCA(y_A, x_B) - junction point |
| outer_cell | LCA(x_A, y_B) - bypass point |
| lca_res | max(lca_res_A, lca_res_B) |

## Files Updated

- `src/utilities.py`: `assign_cell_forward`, `assign_cell_backward`
- `src/generate_shortcuts.py`: Two-phase algorithm
- `docs/algorithm.md`: Complete documentation
