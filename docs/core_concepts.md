# Core Concepts

This document defines the fundamental concepts used in the shortcuts generation algorithm.

## Table of Contents

1. [Edge Model](#edge-model)
2. [H3 Cells](#h3-cells)
3. [Shortcuts](#shortcuts)
4. [Resolution and LCA](#resolution-and-lca)
5. [Cell Assignment Logic](#cell-assignment-logic)

---

## Edge Model

An **edge** represents a road segment in the network. Each edge has two H3 cells:

```
Edge: A road segment from point P1 to point P2

        P1 ──────────────▶ P2
     (start)    edge     (end)
        │                  │
        ▼                  ▼
  outgoing_cell      incoming_cell
```

> [!IMPORTANT]
> **Cell Naming Convention**
> - `incoming_cell` = where edge **ENDS** (the cell you're entering)
> - `outgoing_cell` = where edge **STARTS** (the cell you're leaving)

### Definitions

| Term | Definition |
|------|------------|
| **incoming_cell** | The H3 cell where the edge **ENDS** |
| **outgoing_cell** | The H3 cell where the edge **STARTS** |
| **incoming_edge** | In a shortcut A→B, edge A is the incoming edge |
| **outgoing_edge** | In a shortcut A→B, edge B is the outgoing edge |

### Direct Connection Rule

For shortcut (A, B) to be **direct** (A connects directly to B):
- `A.incoming_cell == B.outgoing_cell` → Edge A ends where Edge B starts

### Example

```
Road segment from coordinates (49.25, -123.10) to (49.26, -123.11)

outgoing_cell = h3.latlng_to_cell(49.25, -123.10, resolution=15)  # edge.in (start)
incoming_cell = h3.latlng_to_cell(49.26, -123.11, resolution=15)  # edge.out (end)
```


---

## H3 Cells

H3 is a hierarchical hexagonal grid system. Each cell has a **resolution** from 0 (largest, ~1,100 km) to 15 (smallest, ~0.5 m).

### Resolution Scale

| Resolution | Average Hexagon Edge | Use Case |
|------------|---------------------|----------|
| 0 | ~1,107 km | Continental |
| 5 | ~8 km | Region |
| 10 | ~66 m | Block |
| 15 | ~0.5 m | Point-level |

### Special Values

| Value | Meaning |
|-------|---------|
| **Cell = 0** | Invalid or global cell (no specific H3 cell assigned) |
| **Resolution = -1** | Global/universal scope (spans entire network, not bound to any H3 cell) |

---

## Shortcuts

A **shortcut** represents a precomputed path between two edges. The edges do NOT need to be directly connected - a shortcut can span multiple intermediate edges.

```
Shortcut: A precomputed PATH from edge A to edge B

  ┌─────────┐                           ┌─────────┐
  │ Edge A  │ ─ ─ ─ ─ (path) ─ ─ ─ ─ ─▶ │ Edge B  │
  │(incoming│                           │(outgoing│
  │  edge)  │                           │  edge)  │
  └─────────┘                           └─────────┘
```

### Shortcut Columns

| Column | Type | Description |
|--------|------|-------------|
| `incoming_edge` | int | ID of the starting edge |
| `outgoing_edge` | int | ID of the ending edge |
| `via_edge` | int | ID of the intermediate edge (for path reconstruction) |
| `cost` | float | Total travel cost from incoming_edge to outgoing_edge |
| `cell` | long | H3 cell for query filtering = `parent(outer_cell, whole_res)` |
| `inside` | byte | Direction: +1=upward, 0=lateral, -1=downward, -2=outer-only |

### When Can Shortcut A→B Exist?

A shortcut from edge A to edge B can only exist if **both edges are in the same bag** at some resolution.

**Key Constraint:**

> LCA(A) must be an ancestor of LCA(B), **or** LCA(B) must be an ancestor of LCA(A)

If neither is an ancestor of the other, the edges never appear in the same bag, so no shortcut can exist!

---

## Resolution and LCA

### LCA (Lowest Common Ancestor)

The **LCA** of two cells is the coarsest (lowest resolution) cell that contains both cells.

```
Resolution 0:     ┌─────────────────────────────────┐
                  │              LCA                │
Resolution 5:     │   ┌───────────┬───────────┐    │
                  │   │  Parent A │  Parent B │    │
Resolution 10:    │   │ ┌───┐     │     ┌───┐ │    │
                  │   │ │ A │     │     │ B │ │    │
                  └───┴─┴───┴─────┴─────┴───┴─┴────┘
```

### Edge LCA Resolution

For an edge, `lca_res` = resolution of the LCA of its incoming and outgoing cells.

```python
edge_lca = find_lca(incoming_cell, outgoing_cell)
lca_res = h3.get_resolution(edge_lca)
```

**Meaning of lca_res:**
- **High lca_res (e.g., 15)**: Edge is contained within a small cell (short edge)
- **Low lca_res (e.g., 5)**: Edge spans a large area (long edge or crosses cell boundaries)
- **lca_res = -1**: Invalid edge or global scope

**Edge is in bags at resolutions `lca_res` through `15`** (and no higher/coarser)

### Shortcut LCA Resolution

For a shortcut A→B:

```
lca_res = max(lca_res_A, lca_res_B)
```

**Why?** This is the coarsest resolution where BOTH edges are present in the same bag:
- Edge A is in bags at resolutions `lca_res_A` through `15`
- Edge B is in bags at resolutions `lca_res_B` through `15`
- Both present together starting at `max(lca_res_A, lca_res_B)`

### Shortcut Inner Cell and Outer Cell

Using the naming convention: Edge A = (A.from → A.to), Edge B = (B.from → B.to)

```
inner_cell = LCA(A.to, B.from)  # junction: where A ends and B starts
outer_cell = LCA(A.from, B.to)  # outer boundary: from A's start to B's end

inner_res = resolution(inner_cell)
outer_res = resolution(outer_cell)
```

**Direct Shortcut Rule:** For a direct A→B connection (A.to == B.from):
- `inner_cell = A.to = B.from` (same cell)
- `inner_res = 15` (finest resolution)


### Shortcut Valid Range

A shortcut A→B is valid at resolutions from `lca_res` to `via_res`:

```
Resolution:  0   1   2   ...  lca_res  ...  via_res  ...  15
             │   │   │   │      │       │      │      │   │
VALID RANGE: ───────────────────╋═══════════════╋─────────────
                                ↑               ↑
                             coarsest        finest
                             (lca_res)      (via_res)

Constraint: lca_res <= via_res <= 15
```

---

## Cell Assignment Logic

### Overview

For each resolution level, we assign a **current_cell** to each shortcut:
- If the shortcut is **valid** at this resolution: `current_cell = actual H3 cell`
- If the shortcut is **not valid**: `current_cell = None` (null, marker to skip)

### Forward Pass (Resolution 15 → 0)

Processing from finest to coarsest. Computes **LOCAL** shortcuts (optimal within subtree only).

**When is a shortcut valid in the forward pass?**

```
Shortcut is valid at resolution R if:
  - lca_res <= R <= via_res  (shortcut exists at this resolution)
```

```
Resolution:  15  14  13  12  11  10  9   8   7   6   5   4   3   2   1   0
              │   │   │   │   │   │   │   │   │   │   │   │   │   │   │   │
via_res = 12  ───────────────────────▶│
                                      │
lca_res = 8   ────────────────────────┼───────────────────────────────────▶
                                      │
VALID RANGE:  ───────────────────────╋═══════════════╋────────────────────
                                     12              8
                                   finest         coarsest
```

**Cell assignment:**
```python
if lca_res <= current_res <= via_res:
    current_cell = get_parent_cell(via_cell, current_res)
else:
    current_cell = None
```

### Backward Pass (Resolution 0 → 15)

Processing from coarsest to finest. Computes **GLOBAL** shortcuts (optimal across entire network).

**When is a shortcut processed in the backward pass?**

```
Shortcut is processed at resolution R if:
  - lca_res == R  (shortcut is "born" at this resolution)
```

This selects shortcuts at their coarsest valid resolution - where they first become relevant.

**Cell assignment:**
```python
if lca_res == current_res:
    current_cell = via_cell
else:
    current_cell = None
```

### Filtering

After cell assignment, filtering is simple:

```python
def filter_active_shortcuts(shortcuts_df):
    return shortcuts_df.filter(F.col("current_cell").isNotNull())
```

---

## Visual Summary

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        SHORTCUT GENERATION FLOW                          │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  FORWARD PASS (15 → 0)  [LOCAL shortcuts]                                │
│  ─────────────────────                                                   │
│  For each resolution R (finest to coarsest):                             │
│    1. assign_cell_forward(shortcuts, edges, R)                           │
│       → current_cell = parent(via_cell, R) if lca_res <= R <= via_res    │
│       → current_cell = None otherwise                                    │
│    2. filter where current_cell IS NOT NULL                              │
│    3. compute LOCAL shortest paths per cell partition                    │
│    4. merge results back to main table                                   │
│                                                                          │
│  BACKWARD PASS (0 → 15)  [GLOBAL shortcuts]                              │
│  ──────────────────────                                                  │
│  For each resolution R (coarsest to finest):                             │
│    1. assign_cell_backward(shortcuts, edges, R)                          │
│       → current_cell = via_cell if lca_res == R                          │
│       → current_cell = None otherwise                                    │
│    2. filter where current_cell IS NOT NULL                              │
│    3. compute GLOBAL shortest paths per cell partition                   │
│    4. merge results back to main table                                   │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

