# Dual-Perspective Review — Provisioning Solver POC

Two independent expert reviews of the solver package + design docs, from a
**Karpenter-compliance** angle (does it faithfully model Karpenter's Solve) and a
**kube-scheduler-expert** angle (does it follow upstream guidance). They converge on
the same two core issues from opposite directions — which is the signal to trust.

## The two findings both reviewers independently flagged (fix these first)

### 1. `In`-only narrowing silently drops `NotIn`/`Exists`/`Gt`/`Lt` → emits infeasible claims that *win* Select
- **Where:** `capacity.Requirement.Intersect` (types.go) always rebuilds as `In`; `podHardRequirements` (virtualnode.go) and `podRequirements` (solver.go) filter `if op == In` and drop everything else. Duplicated in two files that will drift.
- **Why it's severe, not cosmetic:** a pod with `capacity-type NotIn [spot]` ("on-demand only") has the term *discarded* — the claim stays wide, `CheapestPrice` picks spot, and the emitted NodeClaim violates a hard requirement. Because Select is an argmin on cost and violating a constraint is *cheaper* (tighter packing), the infeasible candidate doesn't just appear — it **dominates and wins**.
- **Both angles:** Karpenter reviewer (F1, HIGH) — unsound narrowing contradicts the "solvers emit only feasible solutions" invariant. kube-scheduler reviewer (F8 + F1) — real NodeAffinity honors NotIn/Exists/Gt/Lt/MatchFields; dropping them is a correctness gap the moment it leaves POC scope.
- **Fix:** implement full operator semantics in `Requirement.Intersect`, de-dup the two extraction copies, AND make Select re-check feasibility by default (see #3 below) so a bad narrowing can't win.

### 2. The per-`(pod, claim)` Narrower interface cannot express topology spread / inter-pod affinity
- **Where:** `Narrower.Narrow(pod, claim) *Status` (virtualnode.go) is per-pod, per-claim, stateless. `DefaultNarrowers()` is TaintToleration + NodeAffinity + NodeResourcesFit — all node-local.
- **Why it's a fundamental gap:** topology spread and pod (anti-)affinity are *relational* — skew is a function of where all the OTHER pods went. kube-scheduler computes this in PreFilter into CycleState and Filter reads it. A Narrower that only sees one `(pod, claim)` pair structurally cannot compute skew. The docs invite "register a topology Narrower" as simple extensibility, but that Narrower is unwritable against this interface.
- **Both angles:** Karpenter reviewer (F6, HIGH) — topology/affinity is Karpenter Solve core, absent here, and `minimalPodInfo` returns nil for all affinity accessors so even the plumbing to read it is stubbed. kube-scheduler reviewer (F2, HIGH) — needs a PreFilter analog + claim-set-aware narrowing; the "Narrow mirrors Filter one-to-one" claim holds only for node-local Filters.
- **Fix:** reframe cross-pod constraints as needing a Problem-level pre-compute (PreFilter analog) + a claim-set-aware surface, distinct from the per-(pod,claim) Narrower. Stop listing "topology Narrower" as a drop-in registration in the doc.

## Karpenter-specific findings

- **F2 (offering set preserved) — MODELED CORRECTLY.** `finalize` keeps the whole surviving `InstanceTypes` slice; the offering axis is emitted as a set ("any of these"), matching Karpenter's flexible-NodeClaim contract. Strongest-modeled part.
- **F3 (cost) — SIMPLIFIED BUT SOUND.** EffectivePrice + CheapestPrice is a reasonable provisioning-time comparator; the fixed MemoryOverhead tax is a thoughtful, documented crudeness. Consolidation/drift/disruption is out of scope, honestly stated.
- **F4 (weighted NodePool fallback) — DOC-ONLY, ZERO CODE. NOT honestly signposted.** The doc's most elaborate section (superposition-of-superpositions, ranked CapacitySources, `Problem{Sources}`) has NO implementation: the real `Problem` struct has only Pods/Offerings/Narrowers, no Sources/rank/NodePool. A doc reader would believe ranked fallback exists. **Biggest doc-vs-code divergence.** Either implement it or clearly mark it unimplemented.
- **F5 (in-flight packing) — MODELED; real-node awareness removed by bind-first, honestly disclosed** (but note bind-first is greedy about existing capacity and forecloses a cross-optimization Karpenter's single loop allows; the "consolidation reclaims it" defense is plausible but a real behavioral difference).
- **F7 (greedy largest-first) — FAIR MODEL.** Caveat: sort key is CPU-only (memory-dominated batches mis-order); ILP optimizes node-count-then-cost while Karpenter is cost-driven.

## kube-scheduler-specific findings

- **F1 (no-Validate + cost argmin) — DANGEROUS DIVERGENCE / a trap.** Fusing feasibility into a cost-minimizing solver inverts the deliberate Filter(feasibility)/Score(ranking) separation. Feasibility should be a default Select-time re-check (cheap — Narrowers are pure), not opt-in, ESPECIALLY since the pitch is a competitive third-party solver surface.
- **F5 (competitive N-solver fan-out) — FOREIGN TO KUBE-SCHEDULER BUT DEFENSIBLE.** kube-scheduler Score plugins *combine* (weighted sum); they don't compete-and-discard. Stop calling it a Filter/Score analog; it's query-planner-style cost-based plan selection, which the purity of Problem→[]Solution legitimately enables. Reframe accordingly.
- **F3 (Status codes) — BORROWED BUT INERT.** Unschedulable vs UnschedulableAndUnresolvable are chosen correctly but never consumed (no PostFilter); the distinction is dead code. Either drop it or actually use it (all-offerings-Unresolvable = genuinely unprovisionable).
- **F6 (purity) — FAITHFUL, STRONGEST ALIGNMENT.** No stateful-Filter trap (the equivalence-cache lesson); immutable base + rollback-on-probe + ILP clone-per-branch are all in the snapshot spirit. Caveat: purity is *inherited from bind-first* peeling off the stateful part — same problem relocated, not eliminated.
- **F7 (registration) — NOT-YET-A-REGISTRY.** DefaultNarrowers is a hardcoded slice, no config enable/disable, no Handle/PreFilterExtensions/QueueingHints. Fine for POC; "remove = don't register" is true in Go but not yet operationally config-driven.
- **Got right:** overhead-as-single-seam, cost-as-data (avoids [0,100] normalization), keeping percentageOfNodesToScore + assume/bind on the stock bind path, benchmark honesty.

## Net verdict (both reviewers agree)

Strong on the **shape of a NodeClaim and how you pack onto it** (flexible offering set, in-flight packing, greedy, purity) and on the kube-scheduler lessons that matter (stateless, snapshot-spirit, cost-as-data). Weak on **which constraints actually prune the claim**: the `In`-only narrowing emits infeasible-but-winning claims (#1), and the per-(pod,claim) Narrower can't express the cross-pod constraints (#2) that both Karpenter Solve and kube-scheduler PreFilter treat as core. Two doc sections (weighted fallback, topology) are doc-only with zero code and read as if realized. Reconcile #1 and #2, and honestly mark the doc-only sections, and the design sits squarely inside both systems' philosophies rather than beside them.
