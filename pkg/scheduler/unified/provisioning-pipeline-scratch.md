# Provisioning Pipeline

The provisioning half is structured as a pipeline, distinct from the inline `solve()` follow-up.

## Why a pipeline

Bind-first (D17) defines what the provisioning problem *is*. Because binding to retained capacity is handled structurally and first, the provisioning step does not reason about existing nodes at all — it answers one narrow question: **"given this set of pods that could not bind, what new capacity should we create?"** That is a pure function of `(pods, offerings)`. Two structural advantages fall out, and they are why the provisioning solve is a pipeline:

1. **It can completely ignore existing nodes.** Karpenter's `Solve` cannot — it interleaves existing-node fit, in-flight NodeClaims, and new capacity in one loop, coupled to cluster state. Ours doesn't have to: bind-first already skimmed off everything that fits existing capacity. So each provisioning solve is `(pods, offerings) → NodeClaims` with **no snapshot, no cluster state, no shared mutable base**. That purity is what makes the rest possible.

2. **It can be parallelized with goroutines.** kube-scheduler cannot — its scheduling cycle is serialized (one pod at a time) and mutates a shared cache/snapshot, so parallelism would race. A pure provisioning solve over an independent group of pods has no shared state to race on, so it fans out across cores. This runs as a **separate goroutine** off the bind hot path (the async-commit half of D16).

The pipeline below structures that pure, parallel provisioning solve.

## The pipeline

Runs asynchronously from binding. Pods that bind-first could not place flow in; NodeClaim decisions flow out.

```
   pending (unschedulable) pods
              │
        ┌─────▼─────┐
        │  Batch()  │   collect pods over a window into one provisioning batch
        └─────┬─────┘
        ┌─────▼─────┐
        │  Split()  │   partition the batch into INDEPENDENT sub-problems
        └─────┬─────┘
              │  split_1        split_2        split_3   ...   (parallel from here)
        ┌─────▼──────────────────▼──────────────▼─────┐
        │  Solve()   Problem → Solution[]              │  parallel FAN-OUT: N independent
        │            (greedy, best-fit, ILP, …)         │  solvers per split, each emits candidates
        └─────┬─────────────────────────────────────────┘
        ┌─────▼─────┐
        │PostSolve()│   (Problem, Solution[]) → Solution[]   sequential PIPELINE:
        │  (opt.)   │   warm-start / ensemble-propose / refine / prune
        └─────┬─────┘
        ┌─────▼─────┐
        │ Select()  │   Solution[] → Solution   per split: reduce to the winner
        └─────┬─────┘
              │
        NodeClaims (per split, unioned) → caller provisions
```

Phase contracts (the shapes that make it parallel and pure):

- **Batch()** `stream → []Pod`. Turns the pending stream into a set. The batch is the unit of the solve (packing lookahead is defined over the set).
- **Split()** `[]Pod → [][]Pod`. Partition into independent sub-problems that can be solved concurrently and never share a resulting NodeClaim.
- **Solve()** `Problem → Solution[]`, pure, **fan-out**. Each solver is a self-contained provisioning heuristic reading only the immutable `Problem` (no cluster state) — so N of them run concurrently over one shared `Problem` with zero races. `emptySolution` is the identity incumbent (see PostSolve).
- **PostSolve()** `(Problem, Solution[]) → Solution[]`, pure, **pipeline** (optional). Reads the candidate set, so it is *dependent* — it runs after the fan-out, in order. Covers warm-start (seed a solver with prior candidates), ensemble-propose (read the set, append a new candidate), refine (map), and prune (filter). All three verbs are the same `Solution[] → Solution[]` shape.
- **Select()** `Solution[] → Solution`, per split. The reduce — pick the best candidate on a common comparison metric. Never *adds* a candidate (that's PostSolve); only *chooses*.

`Solution` ≈ `{NodeClaims, placements, unplaced, cost}` for one split.

### One interface, not two: `Refine(Problem, Solution[]) → Solution[]`

Solve and PostSolve are the **same interface** at different arities of the incumbent:

| Kind | Reads | = `Refine(Problem, Solution[])` with… | Composition |
|---|---|---|---|
| **Solver** (greedy, ILP) | Problem only | `Refine(Problem, ∅)` — empty incumbent | **parallel fan-out** |
| **Warm-started / ensemble / refine / prune** | Problem + candidates | `Refine(Problem, prior)` | **sequential pipeline** |
| **Select** | candidates only | `Solution[] → Solution` | terminal reduce |

**The type encodes the concurrency.** A plugin that reads Problem-only depends on nothing shared → it fans out in parallel. The instant a plugin's input includes `Solution[]`, it depends on prior candidates → it serializes behind whoever produced them. You don't annotate "parallel: true"; the signature carries it. So "Solve is a fan-out, not a pipeline" and "PostSolve is a pipeline" are both *consequences of the input type*, not separate design choices — and Solve is just the empty-incumbent base case of the one `Refine` interface.

### No Validate() phase — feasibility is a solver invariant

There is no `Validate(Solution) → bool` gate before Select. Two kinds of validity exist, and neither warrants a phase:

- **Internal validity** (does the solution satisfy pod + offering constraints?) is **time-invariant and fused into Solve** — a solver constructs only-feasible solutions, exactly as Karpenter's `Solve` does, exactly as a Filter's contract is "passes only feasible nodes." A solver that emits an infeasible solution is *buggy*, not *policy*. And solvers **nominate binding** — trust is fully extended to their output, so re-checking it downstream is incoherent. Internal validity is therefore a **solver contract**, not a pipeline stage. (This also avoids redundant work: the solver already enforced fit/topology/narrowing *during* construction.)
  - *Sharp edge to remember:* the candidate set is OR-ed and Select is an argmin on cost, and violating a constraint is usually **cheaper** (tighter packing). So a constraint-violating candidate doesn't slip through — it **wins**. Omitting Validate therefore places *higher-stakes* trust in solvers than kube-scheduler places in Filters (whose violations merely *add* a bad option; ours *dominate*). Fine for trusted/default solvers; an **opt-in** feasibility gate is the escape hatch for untrusted third-party solvers — present by wiring, absent by default (like Select being identity at N=1).
- **External validity** (staleness — did the world move while we batched?) **cannot be a pipeline phase**: any mid-pipeline check is itself stale by commit time (TOCTOU). Staleness is a **commit-boundary** concern, reconciled by assume-then-arbitrate, not a gate. For *provisioning* it is mostly benign — a stale decision provisions a now-redundant node (consolidation reclaims it) or hits a vanished offering (launch retries); both self-heal, unlike stale *binding*. The one sharp case — capacity appeared during the window, so the pod should now *bind* not provision — is handled by D18: the pod is *nominated* to the NodeClaim, and when real capacity appears it re-enters scheduling, bind-first re-runs against fresh state, and it binds (the redundant claim is GC'd). The requeue mechanism heals it; no new phase is needed. The real lever staleness imposes is the **Batch window length** (longer = denser packing but more staleness churn), not a Validate stage.

## Mapping to a known shape

This is MapReduce / a query planner — a well-understood decomposition:

| Pipeline | MapReduce analog |
|---|---|
| Batch | collect input |
| Split | partition by key |
| Solve (portfolio) | map (parallel; multiple mappers = ensemble) |
| PostSolve | combine / refine (sequential, reads the mapped set) |
| Select | reduce (best per key) |

## Per-phase design

### Batch()
Provisioning is intrinsically a set operation — the packing lookahead's headroom credit is *defined* over `remainingBatchDemand` and is unconstructable pod-at-a-time (per-pod provisioning produced the measured 375-node pathology). Batching is the floor, not an optimization. It also decouples provisioning latency from the per-pod bind hot path (binding stays synchronous; provisioning batches async). Karpenter does this too (batching window).

The batch boundary is a latency knob: a bigger batch packs denser but delays provisioning, and is a larger staleness unit (more of the world changes underneath a long window). The window policy is a tradeoff the design owns. Batch is the queue-drain boundary that turns a stream into the pipeline's input.

### Split()
This is where both advantages get their teeth. Independent splits (a) solve in parallel — the goroutine win — and (b) keep each solve small, directly attacking the catalog-width/packing cost (O1). Splitting on genuine incompatibility is **exact, not an approximation**: a pod requiring `arm64` and one requiring `amd64` can *never* share a NodeClaim, so solving them together is pure wasted cross-product. Splitting them loses nothing and parallelizes for free.

**Compatibility is not transitive, so it is not a clean partition.** Pod A (`zone in {a,b}`) is compatible with B (`zone a`) and C (`zone b`), but B and C are not compatible with each other. There is no clean equivalence class. The imperfect options are: over-split (isolate A → lose the packing it could have shared) or duplicate A across splits and reconcile at Select (complexity + double-provision risk). Split therefore keys on **hard incompatibility only** (arch, OS, zone-*if-pinned*, taints, DRA class) — the fault lines nothing can cross — and keeps everything within a class together for packing.

**Two further constraints on the split key:**
- **Never split on soft preferences** — that would scatter poddable-together pods and destroy packing density (the whole 375→28 win).
- **Spread/anti-affinity are "keep-together" forces that oppose splitting.** Pods that spread against each other, or repel each other, are *coupled* — Split keeps a coupled group in one split or it breaks the constraint. So topology makes splits coarser, reducing parallelism exactly where cross-pod constraints exist.

Split's parallelism is therefore **data-dependent**. A heterogeneous batch (many arches/zones/taints) shatters into many parallel splits; a homogeneous batch (all the same shape) is one split and gets zero parallelism. The phase is correct either way; its headline benefit materializes for diverse workloads.

### Solve()
This is where "ignore existing nodes" pays off maximally: a Solve plugin is a pure `(pods, offerings) → Solution` with no snapshot, no handle, no shared state — the cleanest possible plugin contract, trivially parallel and trivially unit-testable. Fan-out-to-all + Select is a **portfolio**: run greedy-largest-first, best-fit-decreasing, cheapest-single-type, and (for small splits) an exact ILP, then keep the best. Different heuristics win in different regimes; a portfolio is robust to any one's failure mode. Wall-clock is `max`, not `sum`, because they run concurrently.

The portfolio is a set of *complementary* heuristics: running *every* solver on *every* split is `N_solvers ×` the compute, worthwhile only when the solvers genuinely diverge (a portfolio of near-identical greedies pays N× for nothing). Split() may also **route** (tiny zone-locked split → ILP; huge split → greedy) instead of fanning out to all — routing trades some robustness for saved compute. Fan-out-all is the default; routing is the optimization.

### PostSolve()
The `(Problem, Solution[]) → Solution[]` signature makes warm-start expressible directly (a solver reading prior candidates as a bound), and the same signature covers *everything* that consumes the candidate set: **ensemble-propose** (read the set, append a smarter candidate — the "solver that wants to see the others' options while making its own" case; it is an append, **not** a Select plugin, because Select chooses and never adds), **refine** (map — flexibility widening, spot retargeting, consolidation-merge à la PR #3008), and **prune** (filter). All three are the same shape; PostSolve is "the stages downstream of the fan-out," unified.

PostSolve is optional and ships as a no-op. With one trusted default solver there is nothing to warm-start from, no ensemble, and the solver already emits well-shaped NodeClaims — so PostSolve is empty and Select is identity. It exists in the *contract* so warm-start/ensemble are additive later with no contract break.

**Two costs the signature carries:**
- **It can grow the set** (append) and **order matters** (an appended candidate is visible to the next PostSolve) — so PostSolve is an *ordered pipeline*, and plugin order is part of the contract, like Filter order. Not a commutative bag.
- **Feasibility-preservation is not guaranteed by the phase** — a `(Problem, Solution[])` stage can synthesize arbitrary candidates. `Solution.unplaced` + Select absorb this, so "does not strand a pod" is an *optional declared property* of a given plugin rather than a phase contract.

### Select()
One decision point, one argmin, per split. Trivially parallel across splits. When splits are non-overlapping and PostSolve normalized the candidates, Select is a simple min over a common metric.

Select inherits whatever Split and PostSolve leave unresolved. If Split allows overlap (duplicated pods), Select must **reconcile/dedup** so a pod isn't provisioned twice — a global step that breaks the clean per-split parallelism. The comparison metric is the comparability question of O2 (raw `$/hr` vs an abstract comparable; non-priced environments like fixed fleets have no per-offering price). When PostSolve normalizes, Select is trivial; otherwise Select carries the comparability burden.

## Overall shape

The spine is Batch→Split→Solve→PostSolve→Select, and the two advantages are what carry it. Ignoring existing nodes makes Solve a pure function — an architectural unlock neither Karpenter's `Solve` (coupled to cluster state) nor kube-scheduler (coupled to snapshot+cache) has. Parallelism via independent splits is the throughput unlock kube-scheduler's serialized cycle structurally cannot have. Both are *consequences of bind-first* — provisioning is simple enough to be pure and parallel because binding is peeled off first.

**Per phase:**
- **Batch, Solve, Select** — load-bearing, always present.
- **Split** — powerful and conditional. The key is defined precisely (hard-incompatibility classes; never soft preferences; spread/anti-affinity force coarser splits) and its parallelism is data-dependent. This phase is where the design's throughput win lives.
- **PostSolve** — the `(Problem, Solution[])` signature carries warm-start / ensemble / refine / prune (all the same shape). Optional; ships as a no-op. It is *the same interface as Solve* (empty-incumbent = solver) — the dependent-input wiring of the one `Refine` interface, not a separate phase.
- **No Validate** — feasibility is a solver invariant (solvers nominate binding; trust already extended); staleness is a commit-boundary concern (D18), not a phase. Optional feasibility gate only for untrusted solvers.

**Plugin surfaces: Narrow (conjunctive, the constraints — the primary surface), Solve (competitive, the packing strategies), PostSolve (dependent). No Assign plugin (mechanism, not policy), no Cost plugin (offering data).** Two design points that carry the most weight:
1. **Split correctness under non-transitive compatibility** — hard-incompat keys are exact; anything softer risks lost packing or double-provisioning, trading packing quality for the parallelism win.
2. **Solver-decides-soft-Narrow** — the solver applies a soft Narrower by pricing the tighter-claim result against offerings, yielding the D5 "soft prefs don't tip provisioning" property in the greedy loop.

## How it composes with what's decided

Orthogonal to the waterfall and bind-first — different levels of the stack:
- **Bind-first (D17)** decides bind-vs-follow-up. It feeds this pipeline its input (the unschedulable remainder) and is the reason Solve can ignore existing nodes.
- **The waterfall** orders the *follow-up actions* (preempt vs provision). This pipeline is the internal structure of the **provision rung** — it does not touch preemption.
- So the full picture: `bind-first → waterfall(preempt | provision) → [provision rung = Batch→Split→Solve→PostSolve→Select, async]`.

## Inside Solve: two plugin surfaces — Solve (competitive) and Narrow (conjunctive)

There is no "Assign" plugin. A packing *algorithm* is **mechanism, not policy**: no workload author ever asks for "best-fit-decreasing." It has no audience, so it is not a plugin surface. This maps exactly onto kube-scheduler: **the scheduling cycle is fixed; Filter and Score are the plugins.** Our analog:

| kube-scheduler | ours |
|---|---|
| the scheduling cycle (fixed loop) | the **solve loop** inside a Solver (fixed *per solver*) |
| **Filter** (plugin) | **Narrow** (plugin) |
| **Score** (plugin) | — folded away; see "cost is offering data" |

So the plugin surfaces are:

- **Narrow** — the constraint plugins: topology, node-affinity, taints, resource-fit, soft preferences. **Conjunctive** — all registered Narrowers apply, intersected (like Filter, AND-ed). This is the ecosystem-extensibility surface, and the one that matters most: you extend the scheduler by adding constraints, and you *remove* a constraint (e.g. topology) by unregistering its Narrower — which just yields wider claims (the "delete the plugin" property, preserved).
- **Solve** — the solver plugins (greedy, best-fit, …). **Competitive** — fan out, Select picks the best. **Every solver calls the registered Narrowers.** A solver is "a packing strategy that consults the constraints"; it is *not* where constraints live.
- **PostSolve** — the dependent-stage plugins (refine / ensemble-propose / prune), reading the candidate set.

Narrow and Solve relate as **library and consumer**: add a Narrower → every solver honors it; add a Solver → it gets all constraints for free. The two surfaces compose without touching each other.

### Interfaces

```go
// Narrow — the constraint plugin surface (conjunctive, Filter-analog).
type Narrower interface {
    Name() string
    // Narrow prunes the claim's option set by this constraint. Hard constraints
    // prune unconditionally (empty set → the pod can't join → Unschedulable). Soft
    // constraints prune ONLY when the solver elects to honor them (see below).
    Narrow(pod *v1.Pod, claim *PotentialNode) *fwk.Status
    Hard() bool   // hard: solver must apply. soft: solver may apply (priced against offerings).
}

// Solve — the solver plugin surface (competitive, fan-out). Each solver calls the
// registered Narrowers. Pure: reads only Problem (no cluster state — bind-first payoff).
type Solver interface {
    Name() string
    Solve(Problem) []Solution
}

type Problem struct {
    Pods      []*v1.Pod
    Sources   []CapacitySource   // provider-neutral, ranked (fallback); each carries offerings + reqs
    Narrowers []Narrower         // the constraint plugins every solver consults
}
```

`Solve` returning `[]Solution` keeps the competitive/fan-out shape; a single-solver deployment returns one and Select is identity.

### Soft preferences ARE soft Narrowers — there is no Score surface

A soft preference is only mechanically real when it **changes the claim** — i.e. when it narrows (pins `arch=arm`). A soft pref that doesn't narrow is a no-op. So there is no separate "score" a soft pref contributes; it is a **Narrower the solver may or may not apply**:

- **Hard Narrower** — solver *must* apply (required affinity, taint, resource fit). Non-negotiable pruning.
- **Soft Narrower** — solver *may* apply, deciding by what the narrowing costs: honoring it = a tighter claim = fewer/pricier surviving offerings; declining = wider/cheaper/more resilient. The solver weighs "pin arm (satisfy pref)" vs "stay flexible (cheaper)" **by pricing the result against the offerings** (see next). Declining is always available and always cheaper-or-equal, which is why a soft pref can never tip provisioning (the D5 property falls out for free).

So "narrow-on-score" is just: a soft Narrower the solver applies when the offering-priced tradeoff favors it. No Score plugin, no scoring term anywhere.

### Cost is offering DATA, not a plugin or a primitive

Price is intrinsic to the offering. **Performance-value ("newer gen is 20% better") is also offering data** — it is a property of the *hardware's worth*, not a workload preference, so it is an adjustment on the offering's effective price (gen-5 at $1.00 rated 20% better = $0.80 effective). This deletes the D5 `PreferenceDiscount` knob: value-of-capacity belongs on the capacity, not in a workload-authored exchange rate.

Consequences:
- A `Solution`'s cost is **derived** — aggregate of the effective prices of the offerings it used. Not a pluggable `Cost()`.
- **Select** reads effective offering price off the solution to compare candidates. No Cost plugin to register.
- **O2 (non-priced fleets) mostly dissolves** — a priceless offering is just one with an empty/uniform price field; Select falls back to a price-free metric (node-count / utilization), both derivable from offerings + solution. No separate cost model to be "missing."

### NewNodeClaim: the shared constructor (⊤)

The one non-plugin shared function: `NewNodeClaim(sources)` builds the *widest* valid claim (all admissible offerings across the ranked sources, overhead-correct allocatable). It owns the **fixed per-node overhead model** (`capacity.MemoryOverheadBytes`) — the single seam where the crude 1Gi model graduates to the real one, improving every solver at once. A solver that hand-rolled construction and dropped the overhead would emit *infeasible* (overcommitted) claims, so this is **correctness**, not just consistency. "Open a claim" and "add a pod" are separate ops: the constructor is the pure ⊤, and seeding a pod is a distinct operation.

## Fallback ordering: a NodeClaim is a superposition of superpositions

**Requirement, not an option.** Weighted/ranked fallback ("prefer capacity source A; use B only where A can't satisfy") is a hard requirement — real autoscaler implementations depend on it, and the design must support it. Karpenter carries the ordering as **NodePool** weight; other systems carry it differently.

**The carrier is provider-neutral.** The solver core names no provider concept — it sees an ordered set of neutral **capacity sources**, each `{ offerings, requirements/taints/labels, rank }`. Each provider compiles its concept in (NodePool weight → a source rank, etc.). `NewNodeClaim(sources)` builds over neutral sources, never `[]NodePool`.

### It lives in the CLAIM's structure, not a pipeline phase (granularity)

Fallback is resolved **per-NodeClaim**: within one batch, claim 1 may land on source A while claim 3 lands on B because A couldn't hold claim 3's shape. Source resolution happens *inside the assignment loop, at claim granularity* — below any pipeline phase, which all operate on pod-sets. So fallback **cannot be a clean pipeline phase**:
- **Not Split** — Split is disjoint partition by *incompatibility*; sources are *ranked alternatives*, not incompatible. Splitting by source forbids a claim from considering "A-or-B."
- **Not batch-level fan-out** — solving the whole batch against only-A fails pods that need B. Source is per-claim, unliftable to a batch-level fan-out.

The structure that carries it is the **NodeClaim itself: a superposition of superpositions** — outer axis = which source (ranked), inner axis = which offering (cost). This is a richer claim *type*, lives inside Solve where per-claim resolution belongs, and is *not* a phase.

### Eager is just "collapse early" — a special case, not a forced inheritance

Because the claim is a superposition, the eager/late question is only *when the outer (source) axis collapses*:
- **Eager** — collapse the source axis at construction (pick a source up front, superpose only offerings; re-solve on failure = a retry loop). This is Karpenter's model.
- **Late** — carry `{A, B}` in the claim and collapse the source axis at commit, same "record on collapse" we already use for zones. **No retry** — the claim narrows by rank-then-cost as packing proceeds (the soft-preference-relaxation-elimination win again).

**Eager is late with an early collapse**, so the superposition-of-superpositions structure *subsumes* eager. Late is the natural mode and the prize (no retry loop); eager falls out as a collapse policy. The design is not tied to Karpenter's eager retry.

### The two axes collapse to different DEPTHS

The axes are not symmetric at collapse — this is the core of the emitted-claim contract:

- **outer (source/nodepool): collapses to ONE.** Take the **highest-weight source that still has surviving valid offerings**, and create the claim against that single source. Full collapse. Weight is **lexicographic (O3), above cost and flexibility** — a rank-1 source with even *one* valid offering beats a rank-2 source with many. Cost/flexibility are only tiebreaks *within* a rank, never across sources.
- **inner (offering): does NOT collapse.** Emit the **set** of surviving instance types — "any of these will satisfy this claim" — and let the **cloud provider choose** at fulfillment. This is the flexibility feature, not laziness: retaining the offering set is what absorbs spot stockout / capacity variance (it is exactly what `FlexibilityValue` rewards, and what a Karpenter NodeClaim *is* — a requirement set the provider resolves, not a resolved instance).

So the terminal claim is `(one source, {surviving offerings})`, **not** `(one source, one offering)`.

`Narrow` prunes *both* axes during packing (a pod requirement can eliminate offerings *or* eliminate a whole source when none of its offerings survive); source rank enters only at the final collapse. **Cost prices the surviving *set*** (e.g. cheapest-available across it) — used to compare candidate claims in Select and as a within-rank tiebreak — but pricing the set is not collapsing it. Flexibility operates within the chosen source's offering set only; it can never override source rank.

## Open questions

- **Eager vs late collapse of the source axis** — whether late collapse (carry all ranks, collapse at commit, no retry) holds under heterogeneous per-source requirements. Eager (collapse at construction) is a *special case* of the same structure, not a separate mechanism.
- **Neutral `CapacitySource` shape** — the minimal `{offerings, requirements, rank}` that each provider's fallback concept compiles into, and how per-source heterogeneous taints/labels/reqs intersect under one claim's nested superposition.
- **Rank vs Cost ordering** — O3: source rank is lexicographic, above the cost argmin, and Cost never compares across sources. Whether the collapse is cleanly two-phase (rank then cost).
- **Split key** — the exact set of hard-incompatibility dimensions; how spread/anti-affinity coupling constrains it; whether to allow overlap + reconcile or forbid it.
- **Batch window** — time vs count vs queue-drain, and the latency/packing/staleness tradeoff.
- **Solve routing vs fan-out-all** — whether to always run every solver, or route splits to suitable solvers.
- **Select metric** — Select reads effective offering price off the solution (cost is offering data, not a plugin) and falls back to node-count/utilization for non-priced fleets; whether this is the whole comparator.
- **Cross-split packing loss** — whether a global re-pack across splits is ever wanted, or intra-split packing is enough.
- **Soft-Narrower policy** — the exact solver policy for *when* to honor a soft Narrower (price threshold vs. flexibility tradeoff). Soft prefs are soft Narrowers the solver optionally applies (no Score surface); performance-value ("new gen 20% better") is offering data (no `PreferenceDiscount` knob).

## Wiring is the deliverable (not the plugins)

What gets written into kube-scheduler is **(1) the wiring and (2) a default solver** — not a rich plugin set. The plugin interface (`Refine`, `Select`) is table stakes; the wiring is the opinionated, permanent, hard-to-reverse part every plugin's contract depends on, so that's where the design care goes.

- **The fan-out *shape* is committed; one solver ships.** The contract is always "Solve produces a set → Select reduces it"; one solver is registered, so the set is a singleton and Select is identity — a plain call, no goroutines. Registering a 2nd solver later turns on fan-out with **zero contract change**. Fan-out is unretrofittable cheaply (a plugin can't grant itself a portfolio — only the orchestrator can invoke N solvers), so the *shape* is in from day one; the *parallelism* and *portfolio* are additive behind it.
- **Concurrency is the orchestrator's, exclusively.** Plugins stay pure and synchronous `Refine`; the orchestrator decides whether to run them on goroutines. A plugin that spawns its own goroutines breaks the raceless-`Problem` invariant that makes fan-out safe.
- **One topology is pinned; there is no DAG engine.** `(Problem, Solution[]) → Solution[]` *permits* arbitrary dataflow; the wiring fixes exactly one shape — parallel fan-out → (optional ordered PostSolve) → Select — not a general plugin-graph interpreter.
- **The default solver is the inline `solve()`:** greedy largest-first + packing lookahead + `NarrowForPod` + topology injection (33 tests green), repackaged behind `Refine(Problem, ∅)`. The default is repackaging, not new work.
