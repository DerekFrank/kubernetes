# Reconciliation notes & open decisions

This file records the decisions taken while reconciling the POC to
`preferred-architecture.md` and relocating it out of the `unified/` folder. Items
marked **DECISION** were chosen unilaterally where the right path was ambiguous —
revisit and correct if wrong.

## Relocation

- **DECISION:** moved `pkg/scheduler/unified/` → `pkg/scheduler/provisioning/`.
  Rationale: the preferred architecture makes provisioning a first-class cycle
  capability rather than a "unified" side experiment, and `provisioning` names what
  the package does. Import paths rewritten; git history preserved via `git mv`.
- The four sub-packages (`capacity`, `virtualnode`, `schedule`, `solver`) kept their
  names and internal boundaries — only the parent path changed.
- **Alternative not taken:** merging the engine directly into `schedule_one.go` /
  registering a live `PostFilter` plugin in `framework/plugins/registry.go`. That
  would force rewriting the pure-engine test suite (12 files) around a live
  `framework.Handle`, violating "keep the tests functionally intact." The engine
  stays a pure library; live wiring is the follow-up below.

## Implemented (additive — fits the pure engine)

These are the primitives `preferred-architecture.md` names that the POC lacked. They
are additive (new symbols + new tests), so the existing suite is untouched.

1. **Fire boundary** (`virtualnode/fire.go`): a `PotentialNode` is a mutable
   superposition until `Fire()`, immutable after. `Narrow`/`UnNarrow` are rejected
   post-fire (the "one-way gate"); occupancy tracking (`AddPod`/`RemovePod`) still
   works because a fired claim behaves like a real node accreting/losing pods
   without re-solving.
2. **`UnNarrow()`** (`virtualnode/fire.go`): the one new primitive — recompute a
   claim's requirements + surviving instance types from its remaining members
   (rebuild from the birth catalog, re-narrow each surviving member), NOT a per-pod
   decrement. Pre-fire only. O(members × constraints).
3. **Demand-driven debounce fire** (`virtualnode/fire.go`, `FireTimer`): pure
   `ShouldFire(now, quiet, max)` — fire T-quiet after the last membership change,
   bounded by a T-max ceiling. Kept fully explicit (all methods take `now`) so it
   stays pure/testable, matching the engine's no-I/O ethos.

## The dummy / PostFilter seam

`preferred-architecture.md` wants the speculative "open a brand-new node?" dummy to
live **only** in a PostFilter plugin that runs when a pod fails to bind. The POC
already realizes the *trigger* correctly: `solve()` mints the dummy only in the
unschedulable-remainder branch, i.e. after bind-first fails everywhere — which is
exactly PostFilter's native trigger. What was missing was the *framing*: the
dummy-minting is now factored into a clearly-named seam (`mintDummy`) with a doc
comment mapping it to the PostFilter plugin body, so in-flight claims stay
cycle-native and the dummy is the one thing quarantined outside the cycle.

- **Not done:** extracting `mintDummy` into an actual registered
  `framework.PostFilterPlugin`. See "Live wiring" — it needs the live cycle.

## Live wiring — NOT built (needs the live scheduler cycle, not a pure batch)

`preferred-architecture.md` §"payoff", §"opportunistic rebind", and summary items
3–5 describe behavior that only exists in the live scheduling cycle. The pure
batch engine cannot host them without becoming the live scheduler; they are the
reviewable follow-up (tracked as O7 in `poc-design-decisions.md`):

- **Nominate-to-NodeClaim** (D18/O7): overload `pod.Status.NominatedNodeName` (POC)
  / dedicated `NominatedNodeClaimName` (KEP) so a pod commits to an in-flight claim
  and self-promotes to a real bind on node registration.
- **Opportunistic rebind** (the requeue valve): a pre-fire claim's pod becoming
  bindable to a real node via QueueingHints → bind + `UnNarrow` the claim. The
  `UnNarrow` primitive it depends on now exists; the QueueingHint wiring does not.
- **Pre-bind at fire + unbind-on-launch-failure**: bind nominated pods to the
  claim's future node name at fire; unbind/re-queue on stockout/quota failure.

## Cross-pod correctness status (from REVIEW_FINDINGS.md)

- Full node-selector operator semantics (NotIn/Exists/Gt/Lt) already ported into
  `capacity.Requirement` — the "In-only silently drops NotIn" finding is resolved.
- Topology spread is cycle-native via `Topology` + `TopologySpreadNarrower`
  (PreFilter-analog state). Inter-pod anti-affinity remains explicitly deferred.
