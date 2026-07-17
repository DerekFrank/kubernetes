# Decision: Where does provisioning live — PostFilter, or native in the scheduling cycle?

> **Resolved — see `preferred-architecture.md`.** The answer is *both, split by kind*:
> in-flight NodeClaims are cycle-native (correctness forces it); the dummy (mint-a-new-
> node) lives in a tiny PostFilter plugin that is Filter-in-Narrow-mode. This doc's
> analysis (the cycle changes are common to both invocation sites) stands and is the
> reasoning behind that resolution. Read it for *why the location is a small residual*;
> read `preferred-architecture.md` for the decided shape.

## The question, stated correctly

Not "big cycle rewrite (native) vs. clean bolt-on (PostFilter)." That framing is false and this
doc's main job is to kill it. Bind-first already fixed *when* provisioning fires: only when no
existing node is feasible — which is **exactly PostFilter's trigger** (Filter failed on every
node). So the two options are not "peer tier in the argmin" vs. "escape hatch." They are:

- **PostFilter:** provisioning is invoked via the PostFilter plugin extension point.
- **Native:** provisioning is invoked inline in the cycle at the same "no feasible bind" point.

Same trigger, same inputs, same outputs. The real question is **invocation site + packaging**,
because — as the walk-through below shows — the *scheduling-cycle changes are the same in both.*

## The scheduling-cycle changes required in BOTH options

Every one of these is forced by making provisioning share one consistent world with binding
(the anti–split-brain requirement). None of them is avoided by choosing PostFilter.

### 1. The topology index must count in-flight NodeClaims

Forced by the empty-cluster spread example: 3 spread pods, maxSkew=1, zones {a,b,c}. If the
topology index only counts *launched* nodes, each pod provisions into zone-a (index reads 0/0/0
three times) → 3/0/0, maxSkew violated. To spread correctly, deciding claim N's domain must see
claims 1..N-1's reserved domains. **So the PreFilter-computed topology state includes not-yet-
launched claims, and the bind path reads it.**

- **Both options:** identical. The index is shared cycle state written by provisioning and read
  by binding. PostFilter writes it just as inline code would — scheduling cycles serialize on the
  shared cache, so a PostFilter write is visible to the next pod's Filter.
- **Not a differentiator.**

### 2. In-flight NodeClaims are first-class capacity the cycle reasons about

Twin of #1 on the read side. When pod P is being placed, an unlaunched claim C is:
- a **topology entity** (its reserved domain counts, per #1), and
- a **provisioning target** (P can pack onto C — "nominate P to C" — instead of minting a new
  claim; Karpenter's in-flight NodeClaims).

The bind path must not treat an existing zone-a node as free while ignoring an in-flight zone-a
claim, or it re-creates the 3/0/0 skew on the bind side.

- **Both options:** the cycle must hold the in-flight claim set as accessible state and reason
  about it. Native holds it as cycle-resident mutable state; PostFilter holds it across batches.
  With the "fire on debounce" stage, native's held-and-mutated claims and PostFilter's accumulated
  batch are the same mechanism (incremental vs. all-at-once).
- **Not a differentiator** in *what* changes; a minor one in *where the set lives* (see Differences).

### 3. Nomination + the "pod became bindable" transition

A pod committed to new capacity is **nominated to its NodeClaim** (overload `NominatedNodeName`
now; `NominatedNodeClaimName` in the KEP). Two transitions the cycle must handle:

- **Pod nominated to claim C, but a real node becomes feasible first** → the pod re-enters the
  cycle, bind-first binds it to the real node, and its nomination clears. This is **binding, not
  preemption** (nothing is evicted — capacity *appeared*). The topology accounting is the *same
  decrement/increment a preemption already does*: −1 on C's reserved domain, +1 on the real
  node's domain. The only generalization is that a NodeClaim's reserved domain is a legal source
  in that move, alongside real nodes.
- **Claim C loses a member** (because that pod bound elsewhere) → C is **re-solved from its
  remaining members** (narrowing is lossy — you cannot subtract one pod's contribution; you
  recompute). Pre-fire, C may re-widen. The scheduler **never cancels** C — an emitted claim is a
  downstream/consolidation concern, not the scheduler's to un-launch.

- **Both options:** identical. Re-scheduling a pod and re-solving a claim from its member set are
  needed regardless of where provisioning is invoked. The topology decrement is preemption's
  existing machinery with a broader set of countable sources.
- **Not a differentiator.**

### 4. Binding pods to a launched NodeClaim's node

When C's node registers, the pods nominated to C bind to it. If a pod became bindable to some
*other* node before C launched (#3), it already left. A pod may leave a **fired-but-not-yet-
registered** claim too — required for latency: if nodes take hours to come up, a pod that can bind
*now* must not wait. So a nomination is **soft and overridable up until the pod is actually
bound**, independent of the claim's fired/registered state.

- **Both options:** identical. The claim→node→bind path and the soft-nomination-override rule are
  the same. Node-registration is an ordinary node-add event that requeues the nominated pods
  through the normal cycle.
- **Not a differentiator.**

### 5. A batching / debounce window

Per-pod provisioning is the 375-node pathology; the packing lookahead is defined over the
remaining batch, so provisioning needs the batch. Native gets this via "hold the claim, fire it
after 10s unmutated"; PostFilter gets it via "accumulate the unschedulable set, solve it." These
are the same window.

- **Both options:** identical mechanism, different spelling.
- **Not a differentiator.**

### 6. The cycle must be runnable against a simulated snapshot (for consolidation)

Consolidation / Deschedule = replay the scheduling decision (bind / provision / possibly preempt)
for a node's pods against a snapshot with that node removed. It is **the cycle run against a
mutated snapshot**, not a separate provisioning solver. So the cycle's decision logic must be
runnable against an arbitrary snapshot, not only live state.

- **Both options:** must satisfy this. Native: "run the cycle against a snapshot" includes
  provisioning automatically. PostFilter: the simulated cycle must also dispatch the PostFilter
  provisioning plugin against the snapshot. Both work; **native is marginally more natural** because
  the cycle-over-snapshot primitive already contains provisioning, whereas PostFilter requires the
  simulated run to invoke the plugin phase too.
- **Slight lean to native.**

## Verdict on the gut-check

**Confirmed: you muck with the cycle internals either way.** Five of the six changes (topology
index, in-flight capacity, nomination/rebind, launched-claim binding, batching) are *identical*
in both options, and they are the substantial ones. The sixth (snapshot-runnable cycle) slightly
favors native. The "PostFilter = smaller blast radius" story survives only as **packaging**, not
capability — see below.

## Where the two genuinely differ

1. **Invocation contract / upstream blast radius.** PostFilter is an established extension point
   with a defined contract; provisioning-as-PostFilter-plugin presents to sig-scheduling as
   "we enriched PreFilter's topology state and added a PostFilter plugin." Native presents as
   "we changed ScheduleOne's control flow." Given changes 1–5 already touch PreFilter/topology/
   queue/binding regardless, the *incremental* scrutiny of the invocation site is smaller than it
   looks — but it is not zero. **Favors PostFilter for acceptance, not for correctness.**

2. **Pluggability of the provisioning algorithm.** Largely a wash: the swappable part (solvers,
   Narrowers, cloud-provider capacity) lives behind the solver/Narrower interfaces regardless of
   invocation site. PostFilter additionally makes *whether/how provisioning triggers* pluggable.
   Minor edge to PostFilter.

3. **Preemption co-location + the waterfall.** Preemption already lives in PostFilter. Putting
   provisioning there co-locates the two follow-up actions the waterfall orders (preempt vs.
   provision) in one place, one mechanism. Native either splits them (provision inline, preempt in
   PostFilter) or moves preemption into the cycle too (more cycle change). **Favors PostFilter.**

4. **One-primitive unification.** If bind, preempt, provision, and consolidation are all
   "ScheduleOne over some state" (live for scheduling, simulated for consolidation), native makes
   the cycle the single universal primitive that every higher operation reuses. PostFilter
   fragments provisioning into a plugin the primitive must invoke. **Favors native for
   architectural honesty** — provisioning *is* a scheduling decision sharing all cycle state, and
   the design reflects that.

## Decision

**Native is the more honest end-state; PostFilter is the migration-friendly packaging of the same
cycle changes.** This is D16's "(A) destination / (B) migration path" — but the analysis above
changes the *rationale*: the old reason to start with PostFilter was "it's a lighter touch." That
reason is largely false. Changes 1–5 are common and are where the work is; the location of the
provisioning invocation is a small residual.

Given the goal is the *right* answer:

- The right architecture is **native** — provisioning shares the cycle's topology index, in-flight
  capacity, nomination, and snapshot-runnability, and the cycle-over-snapshot primitive unifies
  scheduling + consolidation + provisioning. Modeling provisioning as anything less than a native
  cycle capability creates a coordination seam (the plugin the primitive must invoke) for no
  capability gain.
- **PostFilter remains the correct place to *start*** for upstream acceptance, because the
  invocation contract is reviewable in isolation and preemption is already there — and, critically,
  starting there costs nothing structural, since changes 1–5 (the real work) are built the same way
  regardless and carry straight over. The Schedule/Deschedule interface is identical either way.

So: build changes 1–6 (they are common and unavoidable), invoke provisioning from PostFilter first
(reviewable, co-located with preemption), and collapse the invocation inline once the cycle changes
are upstream and proven. The location is a late, cheap, reversible decision; the cycle changes are
the early, expensive, common one — which is exactly why the gut-check is right that we're deep in
the internals no matter what.
