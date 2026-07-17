# Preferred Architecture: in-flight NodeClaims are cycle-native; the fire boundary is the seam

This is the decided shape for provisioning in the unified scheduler. It supersedes
the open "PostFilter vs native" question in `provisioning-location-decision.md`: the
answer is **both, split by which kind of not-yet-real capacity you mean**, and the
organizing principle is the **fire boundary** (the moment a NodeClaim is handed to
the cloud provider), which is a one-way gate.

## The two kinds of not-yet-real capacity

- **In-flight NodeClaims** — committed capacity accumulating pods, narrowing toward
  concrete. **First-class in the scheduling cycle.** Filter, Score, and the topology
  index reason about them exactly like real nodes. This is forced by correctness (see
  "Why in-flight must be cycle-native").
- **The dummy** — the speculative "should we open a brand-new node at all?" question.
  **Lives only in the PostFilter plugin**, which runs only when a pod fails to bind.
  The bind hot path never pays catalog-width scoring.

The cycle's new vocabulary is therefore *one* concept — "committed capacity that
isn't a real node yet" — and the expensive generative part (minting new claims from
the full offering catalog) is quarantined in PostFilter.

## The PostFilter plugin is deliberately tiny

The PostFilter plugin is **another run of Filter, explicitly in Narrow mode, against
a fresh dummy.** It is not new scheduling logic — it is the existing Filter plugins
pointed at a superposition (the dummy) instead of a concrete node, narrowing it by
the unschedulable pod's constraints. If a viable narrowing survives, a new in-flight
claim is born (or the pod joins an existing in-flight claim); the claim then lives in
the cycle from that point on. This makes "Narrow is Filter for superpositions" an
implementation fact, not a thesis.

Default follow-up order is unchanged and back-compat: **bind → preempt → provision →
wait** (the waterfall). Provisioning triggering only when no feasible bind exists is
exactly PostFilter's native trigger (Filter failed everywhere).

## Why in-flight must be cycle-native (the hard requirement)

Correctness of cross-pod constraints under provisioning. Empty cluster, maxSkew=1,
zones {a,b,c}, 3 spread pods each needing a node: if the topology index counts only
*launched* nodes, each pod's provisioning decision reads 0/0/0 and picks zone-a →
3/0/0, maxSkew violated. Because provisioning latency (minutes–hours) always exceeds
scheduling latency (ms), deferring — "let the pod re-queue until its node is real" —
is blind exactly when it matters: decision N+1 happens long before decision N's node
exists. So the topology index (and the bind path) **must** count committed-but-
unlaunched claims. This is the anti–split-brain requirement, and it is what makes
in-flight claims cycle-native rather than a PostFilter bolt-on. It is a correctness
floor for any scheduler that honors `topologySpreadConstraints` / pod anti-affinity —
i.e. all of them.

## The fire boundary is a one-way gate

A NodeClaim is a **mutable superposition until it fires** (is handed to the cloud
provider), and an **immutable commitment after**. You cannot un-narrow a fired claim:
if you told the provider "launch a 32xl," you cannot mid-request say "actually a
16xl." That physical fact splits every membership-change question cleanly:

### Pre-fire (nothing purchased): mutable and soft
- A claim is a superposition; its topology domain is **provisional / soft**.
- A pod joining → narrow the claim (may pin its domain).
- A pod leaving (see "Opportunistic rebind") → **UnNarrow: recompute the claim from
  its remaining members.** Not a per-pod decrement — recompute the requirement set as
  the intersection of the remaining members' requirements. Re-widening is free
  (nothing committed), and a re-solve that would strand a staying member simply
  doesn't release the axis that member pinned (recompute-from-members is correct by
  construction — the survivors are exactly what the claim would be if built from
  them). If members → ∅, the claim dissolves.
- Topology reservation is provisional: released/recomputed on membership change. A
  transient inconsistency here is **self-healing** (the next re-solve fixes it;
  nothing is on real hardware), so strict atomicity is desirable but not a
  correctness floor.

### Post-fire (purchased): immutable and frozen
- The claim is now effectively a real node that hasn't registered yet. Its domain is
  **committed** — immovable.
- A pod leaving → it just leaves **slack** on an oversized node. **The scheduler does
  not re-solve, does not shrink, does not cancel.** Underutilization is
  consolidation's job on its own loop (with its own budgets/PDBs), later. Not P0, not
  the scheduler's call.
- Topology reservation is frozen: the skew contribution belongs to the **node**, not
  the pod. So a departing pod's contribution is **not** decremented post-fire — the
  node is coming in zone-a whether or not this pod rides it.

**Consequence:** the dangerous case — "a re-solve re-narrows the claim to a different
domain and strands a staying pod" — exists only pre-fire, where it is harmless
(nothing committed, re-solve is correct by construction), and is **structurally
impossible post-fire** (no re-solve happens). The launch-you-can't-take-back is
exactly what makes the post-fire side trivial.

## When to fire: demand-driven debounce, not a fixed batch window

Fire a claim **T seconds after the last membership change**, bounded by a **T-max**
ceiling so a claim that keeps accreting pods can't starve launch indefinitely. The
timer resets on any membership *change* (add or leave), not just adds.

This answers the batching question demand-driven instead of time-windowed: the "batch"
is *whatever accumulated on a claim before it went quiet*, per-claim, with no global
barrier — each claim fires on its own schedule. This is not what Karpenter does
(fixed provisioning window); it is what the kube-scheduler should do, because it fits
a per-pod cycle with no global batch step.

This also dissolves the packing-lookahead problem rather than solving it: because the
claim stays a superposition until fire, node *size* is committed only at fire, by
which point every pod that was going to join has joined. There is no premature
per-pod size commitment, so the myopic mispacking (the 375-node pathology) can't
occur. The remaining lever is the ordinary **join-vs-mint** decision (does pod N grow
an existing in-flight claim or start a new one?) — the same bind-to-existing-vs-need-
new call the cycle already makes for real nodes, not new machinery.

## The payoff: the scheduler maintains correctness across the launch, so kubelet just runs the pods

Because the cycle counts in-flight claims and keeps topology accurate the entire time
a claim is launching, **the scheduler has already made and kept a correct placement
before the node exists.** So when the node registers, its pods do not need to go back
through scheduling to be re-validated — the placement was correct all along. Pods
nominated to a claim are **pre-bound to the claim's determined node name at fire**
(a NodeClaim has its node name at fire time), so when the node comes up, kubelet picks
up its assigned pods through the completely normal path. No re-scheduling round-trip;
the latency of provisioning is paid once (the launch), not twice (launch + re-schedule).

Costs to own:
- Pods are **bound to a node that doesn't exist yet** for the launch window (a
  `Bound`-to-pending-capacity state). If the launch fails (stockout, quota), those
  pods need an **unbind/re-queue** path. Tractable, but it is the reviewable edge.
- kubelet admission re-checks **node-local** constraints (resources, taints) when the
  node registers, so those have a backstop. It has **no** cluster-wide view, so it
  **cannot** catch cross-pod violations (spread, anti-affinity). Those have no
  backstop — which is why the in-flight topology accounting (above) must be correct
  while the claim is pre-fire, and frozen-correct once fired.

## Opportunistic rebind (the requeue valve)

A pod nominated to a **pre-fire** claim can become bindable to a real node before its
claim fires (an existing node frees up; another claim launches with slack). This is
handled by the existing QueueingHint machinery: the hint requeues the pod, bind-first
binds it to the real node (this is **binding, not preemption** — capacity appeared,
nothing was evicted), and the pod leaves its claim → the claim **UnNarrows**
(recompute-from-members, per pre-fire rules above). The topology move is the same
decrement/increment a preemption already performs, generalized so a pre-fire claim's
provisional domain is a legal source.

This requeue valve is the re-validation step the direct-bind payoff otherwise removes
— it is event-driven rather than unconditional, and it operates **only on pre-fire
claims** (a fired claim's pods are committed; opportunistic rebind off a fired claim
is not offered, since the purchase stands).

## `UnNarrow()` — the one new primitive this requires

Pre-fire membership departure needs `UnNarrow`: recompute a claim's requirements +
surviving instance types from its remaining members. Because narrowing is monotonic
intersection, the correct un-narrow is **recompute from the member set** (not reverse
one pod's contribution — narrowing is lossy and doesn't record per-pod attribution).
It is O(members × constraints), claim-sized, and only ever runs pre-fire. It is not
heavy; it is the inverse of "narrow a pod in," computed by rebuild rather than by
subtraction.

## Summary of what changes in the scheduling cycle

1. **Topology index counts in-flight (pre-fire, provisional; post-fire, frozen)
   claims** — forced by cross-pod correctness under provisioning.
2. **In-flight claims are first-class capacity** the cycle Filters/Scores/packs onto —
   twin of (1).
3. **Nomination + opportunistic rebind** — a pod committed to a claim, re-bindable to a
   real node pre-fire via QueueingHints; the departure UnNarrows the claim.
4. **Pre-bind to the claim's future node name at fire** — so kubelet runs the pods on
   registration with no re-schedule; plus an unbind-on-launch-failure path.
5. **Per-claim demand-driven debounce fire** (T-quiet with a T-max ceiling), resetting
   on membership change.
6. **`UnNarrow()`** — recompute-from-members, pre-fire only.
7. **The cycle is runnable against a simulated snapshot** — so consolidation /
   Deschedule replays the same decision logic (bind/preempt/provision) against a
   node-removed snapshot.

The PostFilter plugin stays tiny (Filter-in-Narrow-mode against a fresh dummy). The
dummy is the only thing that lives outside the cycle. Everything else about
provisioning is a native cycle capability, because correctness requires it and the
fire boundary keeps the post-commitment side trivial.
