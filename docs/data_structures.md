# Data Structures

This document describes the data structures used in shortcuts generation.

## Edge DataFrame

The edges DataFrame contains road network segments with H3 spatial indexing.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | int | Unique edge identifier |
| `incoming_cell` | long | H3 cell ID at the **end** point (resolution 15) |
| `outgoing_cell` | long | H3 cell ID at the **start** point (resolution 15) |
| `lca_res` | int | Resolution of LCA between incoming and outgoing cells |
| `length` | float | Edge length in meters |
| `maxspeed` | float | Maximum speed in m/s |
| `cost` | float | Travel cost = length / maxspeed |

### Example Data

```
id    incoming_cell         outgoing_cell         lca_res  length   maxspeed  cost
───   ───────────────────   ───────────────────   ───────  ───────  ────────  ─────
1     621773568381886463    621773568381886464    15       50.0     13.89     3.6
2     621773568381886464    621773568382934015    14       120.0    13.89     8.6
3     621773568382934015    621773568118693887    11       500.0    27.78     18.0
```

---

## Shortcuts DataFrame (Working)

During computation, shortcuts have additional columns for spatial partitioning.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `incoming_edge` | int | ID of the starting edge |
| `outgoing_edge` | int | ID of the ending edge |
| `via_edge` | int | ID of intermediate edge (for path reconstruction) |
| `cost` | float | Total travel cost |
| `current_cell` | long | H3 cell for current resolution (0 if not usable) |

### Lifecycle

```
Initial:
  incoming_edge, outgoing_edge, via_edge, cost

After assign_cell:
  incoming_edge, outgoing_edge, via_edge, cost, current_cell

After filter:
  (same, but only rows where current_cell != 0)

After shortest path computation:
  incoming_edge, outgoing_edge, via_edge, cost (updated)
```

---

## Shortcuts DataFrame (Final)

The final output format for shortcuts.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `incoming_edge` | int | ID of the starting edge |
| `outgoing_edge` | int | ID of the ending edge |
| `via_edge` | int | ID of intermediate edge |
| `cost` | float | Total travel cost |
| `cell` | long | H3 cell for query filtering = `parent(outer_cell, whole_res)` |
| `inside` | byte | Direction indicator: +1=up, 0=lateral, -1=down, -2=outer-only |

### Cell Column

The `cell` column determines the H3 cell used for filtering during query time.

**Key term: `whole_res`**
- `whole_res = min(lca_res_A, lca_res_B)` — the **minimum** of the two edge LCA resolutions
- This represents the coarsest resolution at which the **entire shortcut** (both edges) is contained
- Unlike `lca_res = max(...)`, which is where the shortcut *first becomes active*, `whole_res` is where the shortcut is *fully contained*

**Formula:**
```
whole_res = min(lca_res_A, lca_res_B)
outer_cell = LCA(A.from, B.to)
cell = parent(outer_cell, whole_res)
```

This enables efficient filtering during query time using `parent_check(sc.cell, high.cell, high.res)`.

### Inside Column Values

| Value | Meaning | Description |
|-------|---------|-------------|
| +1 | Upward | Shortcut goes from inside → outside (lca_in > lca_out) |
| 0 | Lateral | Shortcut stays at same resolution level (lca_in == lca_out) |
| -1 | Downward | Shortcut goes from outside → inside (lca_in < lca_out) |
| -2 | Outer-only | Shortcut only valid for outer cell merges (lca_res > inner_res) |

---

## Edge Graph DataFrame

Defines connectivity between edges (which edges connect to which).

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `incoming_edge` | int | ID of the first edge |
| `outgoing_edge` | int | ID of the connected edge |

Two edges are connected if the outgoing point of `incoming_edge` equals the incoming point of `outgoing_edge`.

---

## Special Values Reference

| Value | Context | Meaning |
|-------|---------|---------|
| `cell = 0` | Any cell column | Invalid/global cell, no specific H3 location |
| `resolution = -1` | Resolution value | Global scope, spans entire network |
| `current_cell = None` | Working shortcuts | Shortcut not usable at current resolution |
| `cost = inf` | Cost column | Unreachable path |
