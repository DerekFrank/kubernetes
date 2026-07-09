# Provisioning Solver — Benchmark Results

Measured on the `solver` package (`pkg/scheduler/unified/solver`), the pure
provisioning core (`Problem → []Solution`). 48-core machine. Run with:

```
go test ./pkg/scheduler/unified/solver/ -bench . -benchmem -run '^$'
```

These test the design's headline claims. Where a claim **can't** be honestly
benchmarked yet, that is stated rather than faked.

## What the numbers show

### Packing quality — greedy vs naive per-pod (the 375→28 claim)

| pods | greedy nodes | greedy pods/node | naive nodes |
|---|---|---|---|
| 100 | **4** | 25.0 | 100 |
| 500 | **19** | 26.3 | 500 |
| 1000 | **37** | 27.0 | 1000 |

**Result: the packing claim holds decisively.** Greedy consolidates 1000 pods onto
37 nodes (27 pods/node) where naive per-pod provisioning opens 1000. This is the
"few large nodes, not many small ones" property that makes batch provisioning worth
doing, reproduced on the new solver.

### Greedy latency — a real, measured problem

| pods | greedy ns/op | naive ns/op | greedy allocs/op |
|---|---|---|---|
| 100 | 8.2ms | 13.0ms | 51k |
| 500 | 143ms | 62ms | 881k |
| 1000 | **556ms** | 119ms | **3.3M** |

**Greedy is superlinear and, past ~500 pods, slower than naive.** Naive is ~linear
(it never re-probes existing claims); greedy pays O(pods × open-claims) probes, and
each probe clones + narrows the claim's requirement set and instance-type slice —
3.3M allocations at 1000 pods. This is the honest cost of first-fit-onto-every-open-
claim with immutable-probe semantics.

This does **not** refute the design — it localizes the cost to the greedy solver's
probe/rollback implementation, which is optimizable (candidate reuse instead of
clone-per-probe; indexing open claims by compatibility instead of scanning all).
But as written today, greedy's packing win comes at a latency cost that grows with
batch size, and that is the first thing to fix before any "faster" claim against a
production system is credible.

### Catalog-width scaling — linear in distinct offering shapes

| distinct shapes | ns/op (200 pods) |
|---|---|
| 12 | 18ms |
| 48 | 44ms |
| 192 | 143ms |
| 384 | 277ms |

**Result: roughly linear in catalog width** (~4× shapes → ~4× time). The per-pod
compatibility scan over offerings is the cost; it does not blow up super-linearly.
Real catalogs (hundreds of instance-type × zone shapes) are tractable, though the
constant rides on top of the greedy-latency issue above.

### Portfolio (greedy + ILP) — no packing gain here, 3–5× latency

| pods | greedy nodes / ns | portfolio nodes / ns |
|---|---|---|
| 6 | 4 / 0.18ms | 4 / 0.57ms |
| 8 | 5 / 0.27ms | 5 / 1.6ms |
| 10 | 6 / 0.40ms | 6 / 2.2ms |
| 12 | 7 / 0.55ms | 7 / 2.4ms |

**Result: on these batches the portfolio adds 3–5× latency for zero node-count
gain** — greedy already found the minimum-node packing, so the ILP had nothing to
improve. This is the honest portfolio finding: a portfolio only pays off when the
solvers genuinely diverge, and for well-behaved batches greedy is already optimal.
The ILP earns its place only on adversarial mispacking inputs (the unit test
`TestILP_MatchesOrBeatsGreedy` constructs one and confirms ILP ≤ greedy there). The
"fan out to all solvers" default is therefore not free; routing (only run ILP on
small hard splits) is the optimization the numbers argue for.

## Claims that are NOT yet benchmarkable (stated, not faked)

- **Cluster-size independence (vs kube-scheduler).** The design's headline structural
  claim — provisioning cost is O(pods, offerings), independent of cluster size,
  because bind-first peeled off everything that binds. This is *structurally true*
  of the solver (it has no node parameter — it literally cannot scan existing
  nodes), but a *comparison curve* against kube-scheduler's O(pods × nodes) Filter
  scan requires wiring the solver against the real framework harness in
  `schedule/comparison_bench_test.go` (which today benches the older inline
  `Schedule()`, not this pipeline). Structural claim: solid. Comparison plot: TODO.

- **Parallelism via independent splits (vs kube-scheduler's serialized cycle).** NOT
  benchmarkable yet: `Split` is a pass-through stub (one split), and `Pipeline.Run`
  iterates splits serially with no goroutines. There is nothing to fan out. Gated on
  Split partitioning + a concurrent orchestrator landing. Until then, any
  "parallel" number would measure one split or brand-new code.

- **Karpenter comparison.** NOT runnable in this repo: Karpenter is deliberately not
  vendored (the `capacity` types are local mirrors precisely to avoid it). A real
  comparison needs Karpenter's `scheduling.Solve` run on identical fixtures in a
  separate module. Hand-rolling a "Karpenter-like" greedy and labeling it Karpenter
  would be measuring our own code — not done.

## Takeaways

1. **Packing works** — the core value (dense consolidation vs per-pod) is real and measured.
2. **Greedy latency is the bug to fix** — superlinear, alloc-heavy, slower than naive past 500 pods. Clone-per-probe is the culprit; it's an implementation problem, not a design one.
3. **The portfolio isn't free** — greedy is already optimal on easy batches; ILP pays off only on hard small splits, arguing for routing over fan-out-to-all.
4. **Catalog width is linear** — wide catalogs are tractable.
5. **The two biggest headline claims (cluster-size independence, parallelism) are structural today, not yet plotted** — honestly gated on cross-package wiring and Split, respectively.
