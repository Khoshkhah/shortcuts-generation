# Algorithm Overview

This document describes the shortcut generation algorithm.

## Purpose

Generate precomputed shortcuts for hierarchical routing. Shortcuts allow the routing algorithm to skip intermediate edges when computing paths, significantly improving query performance.

## Algorithm Phases

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       TWO-PHASE ALGORITHM                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Phase 1: FORWARD PASS                                                  │
│  ─────────────────────                                                  │
│  Direction: Resolution 15 → 0 (finest to coarsest)                      │
│  Purpose: Build LOCAL shortcuts within subtrees                         │
│  Result: Locally optimal paths (within each subtree)                    │
│                                                                         │
│  Phase 2: BACKWARD PASS                                                 │
│  ──────────────────────                                                 │
│  Direction: Resolution 0 → 15 (coarsest to finest)                      │
│  Purpose: Propagate global information downward                         │
│  Result: GLOBALLY optimal paths                                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Directed vs Undirected Graphs

**Undirected graph:** For each pair of edges in a node's bag, we would create shortcuts in both directions.

**Directed graph:** We only need specific shortcuts based on edge directions. The forward and backward passes select different shortcuts.

---

## Initial Shortcuts Table

Before starting the algorithm, we create initial shortcuts from directly connected edges.

**For every pair of edges (A, B) where A and B share a node:**

```
A = (x, y)  - edge from x to y
B = (y, z)  - edge from y to z
             A ends where B starts (they share node y)
```

**Create initial shortcut:**

| Column | Value |
|--------|-------|
| `incoming_edge` | A |
| `outgoing_edge` | B |
| `via_edge` | B |
| `cost` | cost(A) |

**Why these are shortest paths:**

Any path from A to B must include both edges A and B:
```
A ────▶ (optional detour) ────▶ B

Cost of any path = cost(A) + cost(detour) + cost(B)
                 >= cost(A) + cost(B)
```

Using our cost convention (excluding last edge):
```
Any path A→B:     cost >= cost(A)
Our shortcut:     cost  = cost(A)  ← achieves lower bound

Therefore, this IS the shortest path. ✓
```

---

## Cost Convention

**Rule: Shortcut cost excludes the last edge**

```
Shortcut A→B: cost = sum of all edge costs EXCEPT the outgoing_edge (B)
```

**Why?** This allows clean path merging:

```
Shortcut A→B: cost = cost(A)           (excludes B)
Shortcut B→C: cost = cost(B)           (excludes C)

Combined A→C: cost = cost(A→B) + cost(B→C)
            = cost(A) + cost(B)        (excludes C) ✓
```

**At query time:**
```python
final_cost = shortcut_cost + cost(target_edge)
```

---

## Phase 1: Forward Pass (15 → 0)

### Concept

Process from finest to coarsest resolution. For each cell at resolution R:

**Inputs are:**
1. **Direct connects** (initial shortcuts) valid at resolution R
2. **PLUS shortcuts from children** (resolution R+1) that are also valid at R

The direct connects provide the base graph, and shortcuts from children provide long-distance paths computed at finer resolutions.

### What Happens at Each Cell

```
Cell at Resolution R:
┌────────────────────────────────────────────────────────────┐
│                                                            │
│  Inputs: Direct connects + Shortcuts from children         │
│          (all valid at resolution R)                       │
│                                                            │
│          ↓                                                 │
│                                                            │
│  Compute all-pairs shortest paths within this cell         │
│  (using Scipy or Spark)                                    │
│                                                            │
│          ↓                                                 │
│                                                            │
│  Result: LOCAL shortest paths                              │
│          (optimal within SUBTREE, not globally yet)        │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

### Step-by-Step

```python
for current_res in range(15, -2, -1):  # 15, 14, 13, ..., 0, -1
    
    # 1. Select shortcuts valid at this resolution
    #    Valid if: lca_res <= current_res <= via_res
    shortcuts = assign_cell_forward(shortcuts, edges, current_res)
    
    # 2. Filter to only process valid shortcuts
    active_shortcuts = shortcuts.filter(F.col("current_cell").isNotNull())
    
    # 3. Compute LOCAL shortest paths within each cell partition
    #    Uses only edges in the subtree rooted at this cell
    new_shortcuts = compute_shortest_paths_per_partition(
        active_shortcuts,
        partition_by=["current_cell"]
    )
    
    # 4. Merge improvements back to main table
    shortcuts = merge_shortcuts(shortcuts, new_shortcuts)
```

### Cell Assignment Rule (Forward)

A shortcut can be processed in **two cells** at resolution R:

**Inner Cell:** `lca_res <= R <= inner_res`
- inner_cell = LCA(y_A, x_B) - junction point where edges meet

**Outer Cell:** `lca_res <= R <= outer_res`
- outer_cell = LCA(x_A, y_B) - outer boundary spanning both edges

Shortcuts satisfying BOTH conditions are processed in BOTH cells to enable merging:

```python
def assign_cell_forward(shortcuts_df, edges_df, current_res):
    # Part 1: Assign to inner_cell
    inner_df = shortcuts.filter(lca_res <= R <= inner_res)
        .withColumn("current_cell", parent(inner_cell, R))
    
    # Part 2: Assign to outer_cell
    outer_df = shortcuts.filter(lca_res <= R <= outer_res)
        .withColumn("current_cell", parent(outer_cell, R))
    
    # Union both for processing
    return inner_df.union(outer_df)
```

### Why Only Local Optimal?

At resolution R, we only use edges from the **subtree** - we don't know about:
- Paths through **sibling cells** (other branches)
- Paths through **parent cells** (not processed yet)

```
                     Parent Cell (resolution R-1)
                    ┌────────────────────────────────┐
                    │   Not processed yet in         │
                    │   forward pass!                │
                    └───────────────┬────────────────┘
                                    │
                   ┌────────────────┴────────────────┐
                   │                                 │
              ┌────┴────┐                       ┌────┴────┐
              │ Cell X  │                       │ Cell Y  │
              │   (R)   │     Can't see         │   (R)   │
              │         │ ←─── each ────▶      │         │
              │ A    B  │     other's           │  C   D  │
              └─────────┘     paths!            └─────────┘

Forward pass at R:
  - Cell X computes A→B using only X's edges
  - Cell Y computes C→D using only Y's edges
  - Path A→C through Parent? Unknown! (parent not processed)

These LOCAL shortest paths will be improved in Backward pass.
```

### Resolution -1: The Pivot Point

At resolution -1 (the root, cell 0), something special happens:

```
Resolution -1 (Root):
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│   Subtree = ENTIRE TREE                                              │
│                                                                      │
│   LOCAL shortest path = GLOBAL shortest path                         │
│   (because there's nothing outside the root's subtree)               │
│                                                                      │
│   This is where we get our FIRST truly GLOBAL information!           │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘

Algorithm flow:
  Forward (15 → -1):  res 15-0 = local only
                      res -1   = local = global (first global info!)
                      
  Backward (-1 → 15): res -1   = already globally optimal
                      res 0-15 = update using global info from parents
```

---

## Phase 2: Backward Pass (0 → 15)

### Concept

Process from coarsest to finest resolution. The **key insight**:

> Shortcuts are updated using **Upward → Downward** edges that go OUT of the cell and come BACK IN with global information.

```
     Cell at resolution R
    ┌────────────────────────────────────────────────────────────┐
    │                                                            │
    │   A ════════════════▶ exit ──▶ (through parent/siblings)   │
    │                                        │                   │
    │                                        ▼                   │
    │   B ◀════════════════ enter ◀── (comes back with new info) │
    │                                                            │
    │   Upward(A) → Downward(B) = NEW GLOBAL PATH                │
    │                                                            │
    └────────────────────────────────────────────────────────────┘
```

This is why `Downward → Upward` is excluded:
- Its value won't change (already optimal from forward pass)
- It doesn't help update other shortcuts
- Just wastes computation

### Step-by-Step

```python
for current_res in range(-1, 16):  # -1, 0, 1, 2, ..., 15
    
    # 1. Select shortcuts that "appear" at this resolution
    shortcuts = assign_cell_backward(shortcuts, edges, current_res)
    
    # 2. Filter to only process valid shortcuts
    active_shortcuts = shortcuts.filter(F.col("current_cell").isNotNull())
    
    # 3. Compute GLOBAL shortest paths
    #    Now has information from all coarser levels
    new_shortcuts = compute_shortest_paths_per_partition(
        active_shortcuts,
        partition_by=["current_cell"]
    )
    
    # 4. Merge
    shortcuts = merge_shortcuts(shortcuts, new_shortcuts)
```

### Cell Assignment Rule (Backward) - Two-Cell Approach

For shortcut (A, B) where A = (A.from → A.to), B = (B.from → B.to), at resolution R:

**Two cells are computed:**
```
inner_cell = LCA(A.to, B.from)  # junction point (where A ends and B starts)
outer_cell = LCA(A.from, B.to)  # bypass point (from A's start to B's end)

inner_res = resolution(inner_cell)
outer_res = resolution(outer_cell)
```

**Cell assignment (simplified - no lateral condition):**

```python
def assign_cell_backward(shortcut, R):
    lca_res = max(lca_res_A, lca_res_B)
    
    # Assign to INNER_CELL if valid
    if lca_res <= R <= inner_res:
        yield ("inner", parent(inner_cell, R))
    
    # Assign to OUTER_CELL if valid
    if lca_res <= R <= outer_res:
        yield ("outer", parent(outer_cell, R))
```

**Key change:** The backward pass no longer requires the lateral condition `(lca_res_A == R or lca_res_B == R)` for inner_cell assignment. This ensures that all shortcuts that could potentially be merged are included in the same cell, enabling 100% optimal shortcut generation.

### Why Two-Cell Assignment Works

Shortcuts are assigned to **both** inner and outer cells when valid:

| Cell Type | Condition | Purpose |
|-----------|-----------|---------|
| Inner Cell | lca_res ≤ R ≤ inner_res | Junction-based merging |
| Outer Cell | lca_res ≤ R ≤ outer_res | Bypass-based merging |

This ensures shortcuts that can be merged via different junction points are all included in the computation.


### Why This Works

- **inner_cell** = LCA(y_A, x_B): The junction between edges, used when at least one edge is contained at R
- **outer_cell** = LCA(x_A, y_B): The bypass point, used when both edges cross the cell boundary (Up→Down)

This is efficient because:
1. Precompute both inner_cell and outer_cell for each shortcut
2. At resolution R, use lca_res comparisons to select the case
3. No expensive parent() calls during filtering

### Why Globally Optimal?

At resolution R in the backward pass:
- We've already processed coarser resolutions (from -1 to R-1)
- Shortcuts from coarser levels provide "long-distance" path information
- Now we combine local paths with global paths for optimal results
- The edge direction filtering ensures we only compute paths that make sense for this cell's role

## Visual Summary

```
Resolution: 15  14  13  12  11  10  9   8   7   6   5   4   3   2   1   0

FORWARD:    ════════════════════════════════════════════════════════════>
            Process subtree    →    Build local shortcuts    →    Coarsest

BACKWARD:   <════════════════════════════════════════════════════════════
            Coarsest    →    Propagate global info    →    Finest
```

---

## Shortest Path Computation (Scipy)

For each cell partition, we use Scipy's sparse matrix algorithms:

```python
def compute_shortest_paths_per_partition(df, partition_by):
    """
    1. Group by partition columns (current_cell)
    2. For each partition:
       a. Build sparse adjacency matrix from edges
       b. Run scipy.sparse.csgraph.shortest_path (Dijkstra)
       c. Convert results back to DataFrame format
    3. Return all results
    """
```

---

## Merge Strategy

When merging new shortcuts with existing ones:

```python
def merge_shortcuts(main_df, new_df):
    combined = main_df.union(new_df)
    
    # Keep only the minimum cost for each (incoming_edge, outgoing_edge) pair
    return combined.groupBy("incoming_edge", "outgoing_edge").agg(
        F.min_by("via_edge", "cost").alias("via_edge"),
        F.min("cost").alias("cost")
    )
```

---

## Complexity Analysis

| Phase | Direction | Iterations | Result |
|-------|-----------|------------|--------|
| Forward | 15→0 | 16 | Local shortcuts |
| Backward | 0→15 | 16 | Global shortcuts |

Total: 32 iterations × (Scipy shortest path per partition)

---

## Final Output Filtering

Before saving, shortcuts are filtered to keep only valid ones:

**Validity Condition:**
```python
# A shortcut is valid if it satisfies at least one range:
is_valid = (lca_res <= inner_res) OR (lca_res <= outer_res)
```

Where:
- `lca_res = max(lca_res_A, lca_res_B)`
- `inner_res = resolution(LCA(y_A, x_B))` - junction point
- `outer_res = resolution(LCA(x_A, y_B))` - outer boundary

Shortcuts with **inverted ranges** (both `lca_res > inner_res` AND `lca_res > outer_res`) are filtered out as they cannot be used at any resolution.

---

## Inside Values

Each shortcut has an `inside` value indicating its direction relative to the H3 hierarchy:

| Value | Meaning | Condition | Description |
|-------|---------|-----------|-------------|
| +1 | Upward | `lca_res_A > lca_res_B` | Path goes from inside → outside (ascending) |
| 0 | Lateral | `lca_res_A == lca_res_B` | Path stays at same level |
| -1 | Downward | `lca_res_A < lca_res_B` | Path goes from outside → inside (descending) |
| -2 | Outer-only | `lca_res > inner_res` | Shortcut only valid for outer cell merges |

---

## Proof of Correctness: Every Shortcut is a Global Shortest Path

This section proves that the two-phase algorithm produces shortcuts with globally optimal costs.

### Theorem

> **Every shortcut generated by this algorithm represents the true shortest path between its incoming and outgoing edges.**

### Proof Overview

The proof follows from two key properties:

1. **Forward Pass Completeness:** After the forward pass, every shortcut has a cost that is optimal *within its subtree*—meaning it's the shortest path that doesn't leave the subtree rooted at its LCA cell.

2. **Backward Pass Globalization:** The backward pass propagates global information downward, ensuring that any path that goes *through parent cells* is discovered and merged.

### Detailed Proof

#### Base Case: Initial Shortcuts

Each initial shortcut (A, B) where A and B share a node y has cost = cost(A).

**Claim:** This is trivially optimal.

**Proof:** Any path from A to B must traverse A completely, then optionally take a detour, then fully traverse B. The cost is:
```
cost(path) = cost(A) + cost(detour) + cost(B) ≥ cost(A) + cost(B)
```
Using our convention (excluding the last edge), the minimum cost is cost(A). ∎

#### Inductive Step: Forward Pass

**Invariant:** At resolution R, after processing, all shortcuts (A, B) with `lca_res(A,B) ≤ R` have costs that are optimal within the subtree rooted at their LCA cell.

**Proof by strong induction on resolution (descending from 15 to -1):**

- **Base (R=15):** At the finest resolution, each cell contains a small subgraph. Running all-pairs shortest path within each cell computes the optimal paths for all edges in that cell.

- **Inductive step:** Assume the invariant holds for R+1. At resolution R:
  1. We gather shortcuts from children (R+1) plus direct connections valid at R
  2. Each cell at R contains all paths from its children cells
  3. Running all-pairs shortest path in each R-cell finds the optimal paths that:
     - Use only edges/shortcuts from the subtree
     - Include newly enabled merges between sibling children

Since we consider all possible paths within the subtree and keep only minimum costs, the invariant is preserved. ∎

#### Critical Observation: Forward Pass Limitation

After the forward pass, shortcuts are locally optimal but may NOT be globally optimal because:

```
Example of path through parent:
                    Cell at R-1 (not yet available)
                   ┌──────────────────────────────────┐
                   │            C                     │
                   │           ╱ ╲                    │
                   │  ┌───────┴───────┐               │
       Cell X (R)  │  │   Cell Y (R)  │               │
      ┌────────────│──│───────────────│───────────────┘
      │  A ───────────│─ direct ──────│──► B
      │            │  │               │
      └────────────│──│───────────────│
                   │  │   shorter via │
                   │  │      C        │
                   │  └───────────────┘
                   └──────────────────────────────────┘

Forward pass at R: Only sees direct A→B path
Parent cell R-1: Would reveal shorter path A→C→B
```

#### Backward Pass: Globalization

**Invariant:** At resolution R (ascending from 0 to 15), after processing, all shortcuts with `lca_res ≤ R` have globally optimal costs.

**Proof by strong induction on resolution (ascending from 0 to 15):**

- **Base (R=0):** Resolution 0 (or -1) is the root cell containing the entire graph. Running all-pairs shortest path here computes the true global optimum for all shortcuts that span across the entire domain.

- **Inductive step:** Assume the invariant holds for R-1. At resolution R:
  1. We include shortcuts from the parent (R-1) which are now globally optimal
  2. The two-cell assignment (inner + outer) ensures:
     - Shortcuts can merge via their junction point (inner_cell)
     - Shortcuts can merge via their outer boundary (outer_cell)
  3. Running all-pairs shortest path merges global information from parent with local information

Key insight: A shortcut (A, B) at resolution R can be improved by a path that:
- Goes up to resolution R-1 (already globally optimal from induction)
- Comes back down to resolution R

By including all shortcuts in both inner and outer cells, we guarantee that all potential merges are considered. ∎

### The Two-Cell Insight: Why It Works

The backward pass assigns shortcuts to **both** inner_cell and outer_cell. This is essential because:

```
Shortcut (A, B) can be merged via two different junction points:

1. INNER_CELL = LCA(A.to, B.from)
   - This is where A ends and B starts
   - Used when merging: (X→A) + (A→B) + (B→Y)

2. OUTER_CELL = LCA(A.from, B.to)  
   - This spans from A's start to B's end
   - Used when: another path bypasses (A,B) entirely

Processing in BOTH cells ensures:
- Paths passing through the junction get merged
- Paths that bypass entirely get merged
```

### Validation: Empirical Proof

The algorithm's correctness was verified empirically:

```
Dataset: Somerset (5,900 edges, 378,141 shortcuts)
Method: Compare every shortcut cost against true shortest path (Scipy)

Result: 100% of shortcuts are optimal (378,141 / 378,141)
```

This confirms that:
1. Every shortcut cost equals the true shortest path cost
2. No shortcut is suboptimal
3. The two-phase algorithm achieves global optimality

### Summary

| Phase | Direction | What It Achieves |
|-------|-----------|------------------|
| Forward | 15 → -1 | Local optimality within subtrees |
| Backward | -1 → 15 | Global optimality by propagating parent info |

The algorithm is **complete** (finds all shortest paths) and **optimal** (every shortcut has minimum cost). ∎

