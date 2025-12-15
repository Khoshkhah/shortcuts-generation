# Tree Decomposition and H3 Hierarchy

This document explains the theoretical foundation of hierarchical routing using tree decomposition concepts and how H3 cells provide a natural tree structure for road networks.

## Tree Decomposition Overview

### What is Tree Decomposition?

**Tree decomposition** is a graph theory technique that represents a graph as a tree of "bags" (subsets of vertices). It enables efficient computation of problems that would otherwise be computationally expensive.

```
Original Graph:                    Tree Decomposition:
                                   
    A───B───C                           ┌─────────┐
    │   │   │                           │ {A,B,D} │
    D───E───F                           └────┬────┘
    │   │   │                                │
    G───H───I                      ┌─────────┼─────────┐
                                   │         │         │
                              ┌────┴────┐ ┌──┴───┐ ┌───┴───┐
                              │ {D,E,G} │ │{B,E} │ │{B,C,F}│
                              └────┬────┘ └──┬───┘ └───────┘
                                   │         │
                              ┌────┴────┐ ┌──┴───┐
                              │ {G,H,E} │ │{E,F} │
                              └─────────┘ └──────┘
```

### Key Properties

1. **Coverage**: Every vertex appears in at least one bag
2. **Edge Coverage**: For every edge (u,v), there exists a bag containing both u and v
3. **Running Intersection**: For any vertex v, the bags containing v form a connected subtree

### Treewidth

The **treewidth** of a graph measures how "tree-like" it is:
- Trees have treewidth 1
- Road networks typically have low treewidth (~3-10)
- Complete graphs have high treewidth (n-1)

Low treewidth means the graph can be efficiently decomposed into a tree structure.

---

## H3 as Natural Tree Decomposition

### H3 Hexagonal Hierarchy

H3 provides a natural tree decomposition of geographic space:

```
Resolution 0 (Coarse):     ┌─────────────────────────────────────┐
                           │              Root Cell              │
                           └───────────────────┬─────────────────┘
                                               │
Resolution 1:              ┌───────────────────┼───────────────────┐
                           │                   │                   │
                      ┌────┴────┐         ┌────┴────┐         ┌────┴────┐
                      │ Cell A  │         │ Cell B  │         │ Cell C  │
                      └────┬────┘         └────┬────┘         └────┬────┘
                           │                   │                   │
Resolution 2:         ┌────┼────┐         ┌────┼────┐         ┌────┼────┐
                      │    │    │         │    │    │         │    │    │
                     ┌┴┐  ┌┴┐  ┌┴┐       ┌┴┐  ┌┴┐  ┌┴┐       ┌┴┐  ┌┴┐  ┌┴┐
                     │ │  │ │  │ │       │ │  │ │  │ │       │ │  │ │  │ │
                     └─┘  └─┘  └─┘       └─┘  └─┘  └─┘       └─┘  └─┘  └─┘
                           ⋮                   ⋮                   ⋮
Resolution 15 (Fine):  Individual road segment endpoints
```

Each H3 cell at resolution R contains ~7 child cells at resolution R+1.

### Road Network in H3 Tree

When we overlay a road network on the H3 grid:

```
Resolution 5 Cell:
┌─────────────────────────────────────────────────┐
│                                                 │
│    ═══════════════════════════════════════     │  
│         │              │              │         │
│    ═════╪══════════════╪══════════════╪═════   │  Road network
│         │              │              │         │
│    ═════╪══════════════╪══════════════╪═════   │
│         │              │              │         │
│    ═══════════════════════════════════════     │
│                                                 │
└─────────────────────────────────────────────────┘
         │              │              │
         ▼              ▼              ▼
    Resolution 6    Resolution 6    Resolution 6
    Child Cells     Child Cells     Child Cells
```

---

## Mapping to Tree Decomposition Concepts

### Bags = H3 Cells

Each H3 cell is a "bag" containing:
- Road segments (edges) that pass through the cell
- Intersections (nodes) within the cell

```
Tree Decomposition Bag          H3 Cell
────────────────────            ───────
{v1, v2, v3, v4}       ↔       Cell containing edges e1, e2, e3
```

### Tree Structure = H3 Hierarchy

The parent-child relationship of H3 cells forms the tree:

```
                    Bag at Resolution R
                    (Parent H3 Cell)
                           │
              ┌────────────┼────────────┐
              │            │            │
        Bag at R+1    Bag at R+1    Bag at R+1
        (Child Cell)  (Child Cell)  (Child Cell)
```

### Running Intersection Property

H3 naturally satisfies this:
- An edge appears in a parent cell if it spans multiple child cells
- An edge appears in child cells if it's contained within them
- The cells containing any edge form a connected path in the tree

```
Edge spanning cells:

Resolution R:     ┌─────────────────────┐
                  │    Edge crosses     │  ← Edge in parent bag
                  │    multiple         │
                  │    children         │
                  └──────────┬──────────┘
                             │
Resolution R+1:   ┌────┐  ┌──┴──┐  ┌────┐
                  │    │  │Edge │  │    │  ← Edge in specific child bag
                  │    │  │here │  │    │
                  └────┘  └─────┘  └────┘
```

---

## Why This Matters for Routing

### Separator Property

In tree decomposition, removing a bag "separates" the graph. In H3 terms:

```
To travel from Cell A to Cell C, you MUST pass through their common ancestor:

        ┌───────────────┐
        │ Common Parent │ ← Separator
        │   (LCA Cell)  │
        └───────┬───────┘
                │
       ┌────────┴────────┐
       │                 │
   ┌───┴───┐         ┌───┴───┐
   │Cell A │         │Cell C │
   │(Start)│         │ (End) │
   └───────┘         └───────┘
```

This is why **LCA (Lowest Common Ancestor)** is central to our algorithm.

### Divide and Conquer

Tree decomposition enables divide-and-conquer:

1. **Solve locally**: Compute shortest paths within each cell
2. **Combine upward**: Merge solutions at parent cells
3. **Propagate results**: Share improvements across the tree

This is exactly what our three-phase algorithm does:

| Algorithm Phase | Tree Decomposition Operation |
|-----------------|------------------------------|
| Downward Pass | Bottom-up: Solve children, combine at parent |
| Global Pass | Solve at root (entire graph) |
| Upward Pass | Top-down: Propagate improvements to children |

---

## Shortcuts in Tree Decomposition Context

### Shortcut = Contracted Path

A shortcut represents a path that has been "contracted" (simplified):

```
Original:       A ──→ B ──→ C ──→ D ──→ E
                
Shortcut:       A ═══════════════════════→ E
                       (via_edge = B)
```

### Cell Assignment = Bag Membership

When we assign a cell to a shortcut:

```
Shortcut A→E spans from Cell X to Cell Y

LCA of X and Y = Cell P (parent)

The shortcut "belongs to" Cell P because:
- You can't use it unless you pass through P
- It represents a path that crosses cell boundaries at level P
```

### Hierarchical Shortcuts

```
Resolution 0:  ┌─────────────────────────────────────────┐
               │  Long-distance shortcuts (highways)     │
               │  Connect distant parts of network       │
               └─────────────────────┬───────────────────┘
                                     │
Resolution 5:  ┌──────────┬──────────┼──────────┬──────────┐
               │ Regional │          │          │ Regional │
               │shortcuts │          │          │shortcuts │
               └──────────┘          │          └──────────┘
                                     │
Resolution 10: ┌───┬───┬───┬───┬───┬─┴─┬───┬───┬───┬───┬───┐
               │ Local shortcuts (neighborhood streets)   │
               └───────────────────────────────────────────┘
                                     │
Resolution 15: Individual edge connections
```

---

## Computational Complexity

### Without Tree Decomposition

All-pairs shortest path: **O(V³)** or **O(V² log V + VE)**

For large road networks (V = millions), this is infeasible.

### With H3 Tree Decomposition

| Step | Complexity |
|------|------------|
| Per cell (resolution R) | O(k² log k) where k = edges in cell |
| Cells per resolution | ~N / 7^R cells |
| Total per resolution | O(N × k × log k / 7^R) |
| All 16 resolutions | O(16 × N × k × log k) ≈ O(N log N) |

The hierarchical structure reduces complexity from **O(N³)** to approximately **O(N log N)**.

---

## Line Graph and Edge-Based Routing

### The Line Graph Perspective

Our algorithm computes shortest paths between **edges**, not vertices. This is because we're working with the **line graph** of the road network.

**Line Graph L(G):**
- Each edge in original graph G becomes a vertex in L(G)
- Two vertices in L(G) are connected if their corresponding edges share a vertex in G

```
Original Graph G:              Line Graph L(G):

    v1 ──e1── v2               e1 ────── e2
              │                 │         │
             e2                 │         │
              │                 │         │
    v3 ──e3── v4 ──e4── v5     e3 ────── e4
                               
Vertices: v1,v2,v3,v4,v5       Vertices: e1,e2,e3,e4
Edges: e1,e2,e3,e4             Edges: (e1,e2),(e2,e3),(e2,e4),(e3,e4)
```

### Building Tree Decomposition for Line Graph

To create a tree decomposition for the line graph L(G) using H3:

**Step 1: Place vertices at leaves**
- Each road network vertex v is assigned to an H3 cell at the finest resolution (leaf)

**Step 2: Place edges in bags**

#### The Rule (Abstract Definition)

For each edge (a, b) in the road network:
- Find the cell containing vertex a: `cell(a)`
- Find the cell containing vertex b: `cell(b)`
- Find the **unique path** in the tree from `cell(a)` to `cell(b)`
- Place edge (a,b) in the bags of **ALL nodes along this path**

```
Rule: Edge (a,b) ∈ Bag(N) for ALL nodes N on the unique path from cell(a) to cell(b)
```

```
Example: Edge (a,b) where a ∈ leaf cell A, b ∈ leaf cell C

Tree structure:
                         Root
                          │
                    ┌─────┴─────┐
                    │           │
                 Parent P    (other)
                    │
           ┌───────┴───────┐
           │               │
        Cell A          Cell C
        (has a)         (has b)

Unique path from A to C:  A → P → C

Edge (a,b) is placed in: Bag(A), Bag(P), Bag(C)
```

#### H3 Interpretation (Equivalent Definition)

In H3 terms, this is equivalent to saying:

> Edge (a,b) ∈ Bag(N) ⟺ N has a child C where one endpoint is **inside** C and the other is **outside**

Why are these equivalent? Because:
- If (a,b) is on the path from cell(a) to cell(b), then at node N:
  - Either a or b is in one child subtree
  - The other endpoint is in a different subtree (or outside N entirely)
  - This means the edge "crosses" a child boundary

```
At Parent P: Edge (a,b) crosses the boundary between children A and C

                    ┌───────────────┐
                    │    Parent P   │ ← Edge (a,b) is here because:
                    │               │   - Child A contains 'a' but not 'b'
                    └───────┬───────┘   - Child C contains 'b' but not 'a'
                            │
               ┌────────────┴────────────┐
               │                         │
          ┌────┴────┐               ┌────┴────┐
          │ Cell A  │               │ Cell C  │
          │ has 'a' │               │ has 'b' │
          │ not 'b' │               │ not 'a' │
          └─────────┘               └─────────┘
               ↑                         ↑
          Edge in A's               Edge in C's
          bag (a inside)            bag (b inside)
```

#### Edge Placement vs Resolution

```
Resolution R:     ┌─────────────────────────────────────┐
                  │            Parent Cell              │
                  │  Edge (a,b) crosses child boundary  │ ← Edge in this bag
                  └────────────────┬────────────────────┘
                                   │
Resolution R+1:        ┌───────────┴───────────┐
                       │                       │
                  ┌────┴────┐             ┌────┴────┐
                  │ Child X │             │ Child Y │
                  │  has a  │─────────────│  has b  │
                  │ (inside)│   edge      │ (inside)│
                  └─────────┘ crosses     └─────────┘
                              boundary
```

- At resolution R: Edge is in Parent's bag (crosses X/Y boundary)
- At resolution R+1: Edge is in both X's and Y's bags

### Why This Creates Valid Tree Decomposition

This construction satisfies all tree decomposition properties for the line graph:

1. **Coverage**: Every edge (vertex in L(G)) appears in at least one bag
   - Edge is placed in the cell containing its endpoints

2. **Edge Coverage**: For adjacent edges e1=(a,b) and e2=(b,c) in G:
   - Both share vertex b
   - Both are placed in the cell containing b
   - So there exists a bag containing both e1 and e2 ✓

3. **Running Intersection**: For any edge e:
   - e is placed in all bags along a **path** in the tree
   - Paths in trees are connected subtrees ✓

### The Goal: Shortest Paths Between Edges

In routing, we need to find the shortest path from one road segment (edge) to another:

```
Query: "Shortest path from edge e_src to edge e_dst"

This is equivalent to:
- Finding shortest path between two vertices in the line graph L(G)
- Our shortcuts precompute these paths hierarchically
```

**Shortcut Table = Precomputed All-Pairs Shortest Paths in L(G)**

| incoming_edge | outgoing_edge | cost | via_edge |
|---------------|---------------|------|----------|
| e_src | e_dst | 42.5 | e_intermediate |

### Connection to LCA

The **LCA resolution** of a shortcut tells us:
- At what level of the tree the shortcut "lives"
- The coarsest resolution where both edges are in the same bag

---

## Shortcut Existence and Cell Assignment

### When Can Shortcut A→B Exist?

A shortcut from edge A to edge B can only exist if **both edges appear in the same bag** at some resolution.

**Key Constraint**: For edges A and B to share a bag:

> **LCA(A) must be an ancestor of LCA(B), or LCA(B) must be an ancestor of LCA(A)**

If neither is an ancestor of the other, their paths in the tree never intersect, so they're never in the same bag!

```
Edge A's path: cell(a_in) ←→ LCA(A) ←→ cell(a_out)
Edge B's path: cell(b_in) ←→ LCA(B) ←→ cell(b_out)

For paths to intersect: one LCA must be ancestor of the other
```

### The Three Cases

**Case 1: LCA(A) is ancestor of LCA(B)** (A reaches higher/coarser resolution)

```
            LCA(A)          ← A is here (res = lca_res_A)
               │              A present, B NOT present
              ...           
               │
            LCA(B)          ← B reaches here (res = lca_res_B)
               │              BOTH A and B present (intersection starts)
              ...           ← Both present
               │
            leaves

Coarsest shared bag: LCA(B)
Shortcut lca_res = lca_res_B = max(lca_res_A, lca_res_B)
```

**Case 2: LCA(B) is ancestor of LCA(A)** (B reaches higher)
```
Symmetric to Case 1
Shortcut lca_res = lca_res_A = max(lca_res_A, lca_res_B)
```

**Case 3: LCA(A) == LCA(B)** (both reach same level)
```
         LCA(A)=LCA(B)      ← Both A and B are here
               │
              ...           ← Both present
               │
            leaves

Shortcut lca_res = lca_res_A = lca_res_B
```

### The Formula

**Shortcut's lca_res = max(lca_res_A, lca_res_B)**

This equals the resolution of the LOWER LCA (the one closer to leaves), which is the coarsest bag where both edges are present.

### Via Cell and Via Resolution

```
via_cell = LCA(a_out, b_in)  # where edge A exits and edge B enters
via_res = resolution(via_cell)
```

- `a_out` = outgoing cell of edge A
- `b_in` = incoming cell of edge B
- `via_cell` = the finest cell containing both edges (the junction point)

### Shortcut Valid Range

```
A shortcut is valid at resolutions from lca_res to via_res:

     lca_res <= R <= via_res
     ↑                ↑
  coarsest          finest
```

### Cell Assignment at Resolution R

For a shortcut A→B at resolution R:

1. **Is shortcut valid at R?** Yes if `lca_res <= R <= via_res`

2. **Which cell?** 
```python
current_cell = get_parent(via_cell, R)  # project via_cell to resolution R
```

```
At resolution R where lca_res <= R <= via_res:

   ┌─────────────────────────────────┐
   │   Cell at resolution R          │ ← current_cell = parent(via_cell, R)
   │   (contains both A and B)       │
   └─────────────────┬───────────────┘
                     │
            ┌────────┴────────┐
            │                 │
         LCA(A)            LCA(B)
            │                 │
           ...               ...
```

---

## Summary

| Concept | Tree Decomposition | H3 Implementation |
|---------|-------------------|-------------------|
| Bag | Subset of vertices | H3 cell |
| Tree edge | Bag containment | Parent-child cells |
| Separator | Bag removal cuts graph | LCA cell |
| Treewidth | Bag size - 1 | ~edges per cell |
| Bottom-up | Solve children first | Downward pass |
| Top-down | Propagate from root | Upward pass |
| Shortcut exists | Edges in same bag | LCA(A) ancestor of LCA(B) or vice versa |
| Shortcut lca_res | Coarsest shared bag | max(lca_res_A, lca_res_B) |
