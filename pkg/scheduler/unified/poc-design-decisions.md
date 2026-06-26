# POC Design Decisions

**Last updated:** 2026-06-26
**Scope:** the unified scheduling POC in this `pkg/scheduler/unified/` package. A fuller as-built status doc (`poc-status.md`) and the original plan (`poc-plan.md`) are maintained outside this repo; this doc is self-contained on the decisions.

This doc records the **major design questions** the unified-scheduling POC has taken a stance on (D1–D16), with the rationale and status of each, plus the **open questions** (O1–O6) we have *not* yet decided. It is the reader's-digest entry point for the design.

Each decision entry: **Stance** what we decided, **Why** the reasoning, **Status** built / designed / decided-not-built / changed-our-mind. The "Open Design Questions" section at the end holds the live forks — things still genuinely undecided, not stances.

---

## The two meta-stances

Everything below reduces to two commitments:

1. **Marginal cost of displacement is the universal currency.** Bind, provision, preempt, and consolidate are not different mechanisms — they are one `argmin` over what each decision costs, *including the cost of re-placing whatever it displaces*. This makes the scheduler recursive (a displacing action's cost is defined by re-solving the placement of what it displaced) and is what unifies the scheduler and the autoscaler. No current system models it because the scheduler and autoscaler each see only half — naming it is what Gluon exists to do. The decisions D1–D3 below are this principle applied to provisioning, preemption, and the candidate ordering.
2. **`Schedule()` is a pure, batch-scoped decision function; everything stateful belongs to the caller.** Execution, bind-failure handling, backoff, batch composition, and commit are the caller's. This is what lets one engine serve provisioning, consolidation, and admission (Kueue) without change.

---

## Economic model (the spine)

### D1. What justifies provisioning a new node?
- **Stance:** Never cost alone. Binding to capacity you intend to *keep* is free (marginal cost ~0), so provisioning is justified only by **(a)** an unsatisfiable-elsewhere scheduling term, or **(b)** consolidation pairing (launched while strictly more expensive capacity is decommissioned).
- **Why:** Provisioning is *always* strictly more expensive than reusing already-paid capacity, so "provision because it's cheaper" is never rational. The early "cheap spot beats existing on-demand" intuition compared total prices and was economically backwards.
- **Status:** Decided; reflected in default `Schedule()` behavior (bind-first).

### D2. How do bind / provision / pack-onto-in-flight compete?
- **Stance:** One marginal-cost axis, `argmin`, ties broken by tier (`existing < in-flight < dummy`).
- **Why:** Karpenter hard-codes the `existing → in-flight → new` ordering; here it *falls out* of marginal cost (existing ≈ 0 ≤ in-flight delta ≤ a whole new node) rather than being a gate. Real nodes win ties (no launch latency, no stockout risk).
- **Status:** Built (three-tier candidate model in `Schedule()`).

### D3. Is preemption a special phase?
- **Stance:** No — preemption is **deferred provisioning**: a candidate scored at `marginalCost(re-place victim) + disruption`.
- **Why:** The evicted victim rebounds and must itself be placed (usually a new node), so preemption ≈ provisioning + disruption. Provisioning Pareto-dominates it unless the victim is disposable or provisioning is impossible (quota/stockout).
- **Status:** Designed, not built.

*(D4 was "what is the unifying principle?" — merged into meta-stance #1 above, since it's the principle D1–D3 share, not a separate fork. D-numbers D5+ are left unchanged to keep existing references stable.)*

---

## Scoring & preferences

### D5. How do soft preferences enter scoring? *(changed our minds, now rebuilt)*
- **Stance:** **Multiplicative cost-adjustment** ("newer gen is 20% better" → `cost × 0.8`); **tipping = make it a hard constraint.** *Not* the additive `PreferenceValue` benefit term we first built.
- **Why:** The additive model needs an absolute `$/hr` valuation no user can set, and it lets a soft preference burn a whole new node. Multiplicative-on-cost is unit-free (everything is dollars), composes order-free, and — because `$0 × k = $0` — *cannot* tip provision-over-free-bind, which is the correct behavior. Preference value is a **workload** property, not a capacity one. If a preference is worth launching a node, it isn't soft → use `requiredDuringScheduling`.
- **Status:** Built. The additive `PreferenceValue` term and `preferenceScorer`/`concreteNodeBenefit` are deleted; `priceScorer` now applies `price × (1 − weight×PreferenceDiscount)` for every preference the option's set *guarantees* (capped <1 so it never reaches $0). Tier-0 existing nodes score `effCost 0`, so a soft preference structurally cannot tip provisioning. Tests rewritten to assert the new model (`SoftPreferenceDoesNotTipProvisioning`, `PreferenceRanksOfferingsWhenProvisioning`, etc.). Still future work: source the discount from a workload-authored CEL expression (today it's a single `Options.PreferenceDiscount` × the affinity weight). Cost-adjustments must reference **pinnable labels** (arch/gen/family), since a continuous CEL isn't invertible to a label constraint for narrowing.

### D6. Where does "honor the preference" live?
- **Stance:** The constraint is an **output** of scoring, not an input. The winning option carries its own narrowing; we pin it at commit. Preferences never filter the candidate set.
- **Why:** Because nothing is filtered, the candidate set never empties — which **eliminates Karpenter's promote-to-hard + relax retry loop** for soft terms. "Score arm, launch arm" with all sibling arm types retained (multi-type pin).
- **Status:** Built (commit-time pinning of the winning option).

### D7. Is flexibility a guard or a score?
- **Stance:** A **peer scorer** on the same cost axis (fallback-pool diversity, diminishing returns), competing in the same `argmin` as price and preference — not an out-of-band floor.
- **Why:** Over-constraining a NodeClaim (collapsing to one spot pool) has a real cost; making it a score lets it trade off against preference and price inside one comparison rather than as a veto.
- **Status:** Built (`flexibilityScorer`), though its principled grounding (cost = expected extra nodes from lost packing room) is deferred (see O4).

### D8. Reuse kube-scheduler's `[0,100]` normalization?
- **Stance:** No.
- **Why:** Bounded per-cycle rescaling destroys the absolute magnitude and the stable zero our marginal-cost model depends on; kube-scheduler's weights compose *same-unit* signals, whereas our problem is *cross-unit* conversion. (The D5 multiplicative reframe then dissolves most of our remaining normalization problem by keeping everything in dollars.)
- **Status:** Decided. Open: how foreign `[0,100]` plugins map onto the `$/hr` axis, and where strict (lexicographic) ordering lives — both in Remaining Considerations.

---

## Bin-packing

### D9. Inline vs. post-hoc fix for greedy mispacking?
- **Stance:** **Inline lookahead**, not a post-hoc split pass (Karpenter PR #3008). Credit each candidate by the fillable headroom that remaining batch demand can use, priced per-unit.
- **Why:** Per-pod marginal cost is myopic — it bills one pod the whole instance-tier jump, so tight fresh nodes beat growing in-flight ones, opening many small nodes. The credit removes that billing artifact so packing happens at decision time, with no split machinery, no displaced-pod estimator, no "can't price these pods" gap. We tested this against an *unmerged* Karpenter proposal and it works: 375 → 28 nodes.
- **Status:** Built (`PackingWeight`, default off). `PackingWeight=1.0` is principled (headroom at par), not tuned — verified by sweep (2.0 packs worse).
- *(Note: the "is the scheduler intrinsically quadratic at scale" question is a measured finding, not a design decision — the "50s" was a loose-packing symptom, not an algorithmic limit, fixed by the lookahead. A finding, not a fork, so it's not listed here.)*

---

## Cluster state

### D10. How is cluster state represented and shared?
- **Stance:** **Immutable `Snapshot` base + per-solve copy-on-write `view`.** The base is never mutated and is shared by pointer across concurrent solves; per-solve deltas (tentative placements, masks, resource totals) live in the view.
- **Why:** Lets a live provision loop and an always-running consolidation sim share one base with no locking — the precondition for the RCU model. The race detector caught the original code mutating the shared base (`AddPodInfo`); resource accounting was moved into the view to fix it.
- **Status:** Built and `-race`-proven (16 concurrent solves). The RCU publish/swap + incremental writer-side updates on top are **not** built.

### D11. How does node removal (consolidation) work?
- **Stance:** Propagate the removal through derived indexes **exactly as a real deletion would** — not a lazy "skip on read" mask. `Deschedule(nodes)` is a thin wrapper: mask the nodes (their pods become the displaced set) → `Schedule` the displaced pods.
- **Why:** A node's pods feed cluster-wide aggregates (topology counts, etc.). Hiding the node on read while leaving its pods in the counts makes the scheduler spread against phantoms. Removal must decrement every aggregate the pods fed.
- **Status:** Built (`withoutNodes`, `Deschedule`; index-consistency-after-removal tested).

### D12. How is topology spread modeled?
- **Stance:** **Requirement injection, not a provisional index.** Compute the valid domains up front, inject as an `In` requirement that flows through normal narrowing, and record the count only when a node's domain collapses to one value.
- **Why:** A PotentialNode's domain is unresolved, and constraining it later would retroactively move counts. The tempting fix (a provisional index that counts an unresolved pod against all possible domains, then "resolves" it) is complex and unnecessary — injecting the requirement makes narrowing and topology *one* operation. Followed Karpenter's model. (The domain *universe* is load-bearing: skew must be measured against all possible domains, else everything piles into the first one used.)
- **Status:** Built. Anti-affinity (which *does* need conservative all-possible-domains tracking) explicitly deferred.

---

## Architecture & boundaries

### D13. Is `Schedule()` pure, or does it commit?
- **Stance:** **Pure function** of (snapshot, pods) → decisions. It never executes, never writes state.
- **Why:** Purity is what lets consolidation, Kueue, and the binder+provisioner all call the *same* function — they differ only in what they do with the returned decisions. Consolidation and Kueue never execute binds at all.
- **Status:** Built.

### D14. Who owns batch composition and bind-failure handling?
- **Stance:** The **caller**, not the solver. Failure memory = the caller's per-pod backoff queue (mirroring kube-scheduler's `handleBindingCycleError`). **Never** node-marking.
- **Why:** A bind failure excludes the pod from the *next* batch via backoff; re-batching the pending set naturally drops the failed pod and retries the rest against real state. No dependency-DAG tracking, no whole-batch livelock, no solver state. Transient vs. persistent need no distinction (both just back off). Node-marking was rejected — node health isn't the scheduler's to assert, and a pod-caused failure misread as node-failure would cordon the fleet.
- **Status:** Resolved; demonstrated by the caller harness (transient-recovers, persistent-isolated, fixed-capacity-on-relaunch).

### D15. What is the commit boundary for binds vs. provisioning?
- **Stance:** Binds can stay **per-pod-committed and re-decidable** via re-batching; an **emitted NodeClaim is the one real external commit** that the next batch must fold in as *fixed in-flight capacity* (not reopen).
- **Why:** Backoff cleanly handles pod retries but cannot un-launch a node. So the binds-vs-provisioning asymmetry is real: binds are cheap to redo, provisioning commits are not.
- **Status:** Resolved (design); the "fold emitted NodeClaims as fixed capacity" behavior is demonstrated by the harness.

### D16. One pipeline, or one library with two callers?
- **Stance (directional):** **(B) one library / two callers** as the migration path; **(A) one async-commit pipeline** as the destination.
- **Why:** The fast bind path (~ms) and slow provision path (~min, external) have different latency models. (B) — kube-scheduler calls the core with `offerings=∅` (pure binding), Karpenter with the full catalog (provisioning) — is lower-risk and is already the shape `Schedule()` has. (A) collapses them once async-commit is proven.
- **Status:** Directional; current `Schedule()` is the (B) shape.

---

## Open Design Questions

These are genuine forks we have **not** taken a stance on yet (distinct from the decisions above, which are settled).

### O1. Is expand-then-score the right scoring mechanism? *(leaning: replace)*
The expand-then-score lattice is built and correct, but its cost scales with **catalog width** — 500 instance types is ~100–1000× the no-offerings cost, because every pod expands an option lattice over the full surviving type set, and real catalogs are wide. The binding hot path is at parity with kube-scheduler (architecture sound); the *scoring inner loop* is the gating perf concern. Directions: per-pod catalog pruning, representative-offering scoring, or a non-lattice formulation. The D5 multiplicative-cost-adjustment may itself be the replacement (no honor/don't-honor lattice to expand). This is the clearest "we have evidence to reconsider" item.

### O2. How do foreign / non-priced scoring signals join the cost axis?
The cost axis is `$/hr`. Open: (a) how a third-party Score plugin authored against kube-scheduler's `[0,100]` scale maps onto it; (b) whether the axis should be raw `$/hr` or an abstract comparable so non-priced environments (node groups, on-prem, fixed fleets with no per-offering price) are first-class; (c) a documented total order with explicit tie-breaks so the `argmin` is reproducible. (D8 settled that we *don't* adopt `[0,100]` normalization; this is the unresolved remainder.)

### O3. Where does strict (lexicographic) ordering live?
The scorers are a commutative bag, which structurally cannot encode a hard ordering like "reserved → spot → on-demand, never violated." Strict-ordering policies are real autoscaler requirements and need a home that isn't the scorer bag (a pre-scoring partition, or a lexicographic comparator above the cost `argmin`). Unresolved.

### O4. Soft-preference "honor at a marginal cost" on in-flight nodes
D5 settled the common case (multiplicative discount; soft preferences rank offerings, never tip provisioning). The unbuilt refinement: a soft preference *should* be able to tighten an **in-flight node being provisioned anyway** when the marginal packing cost is low. Pricing that cost (expected extra-node cost when remaining batch demand no longer fits) unifies flexibility + packing-lookahead, but needs a dedicated signal we deferred as too much machinery for the POC.

### O5. Recursive displacement cost (preemption / consolidation depth)
The cost-of-displacement principle (meta-stance #1) is recursive in theory, but the POC computes no recursion — preemption isn't built, and `Deschedule` re-places displaced pods one level deep. Production needs a *bounded* recursion (one level, or a cheap re-placement estimate) and a decision on how deep.

### O6. RCU / incremental writer for cluster state
The immutable base + COW view (D10) is the precondition; unbuilt is the atomic-pointer publish/swap and incremental, structure-shared writer-side updates so watch events produce new base versions cheaply (O(change), not a full rebuild per call). This is the "live, always-up-to-date cluster" piece.
