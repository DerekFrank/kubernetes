// Package schedule implements the provisioning scheduling engine: a pure function
// that evaluates binding, provisioning, and preemption for a batch of pods and
// returns a plan (the caller executes it).
//
// The engine is bind-first (preferred-architecture.md, D17): a pod with a feasible
// bind to retained capacity binds immediately via the stock, unmodified
// kube-scheduler Filter plugins — PotentialNodes never reach the framework. Only the
// unschedulable remainder enters the provisioning follow-up, where in-flight
// NodeClaims are first-class cycle candidates (Filtered/scored/packed onto like real
// nodes) and the speculative "open a brand-new node?" dummy is minted only when no
// existing node and no in-flight claim can host the pod — which is exactly
// PostFilter's native trigger (see mintDummy). The dummy is the one thing that lives
// outside the cycle; everything else about provisioning is a native cycle capability.
package schedule

import (
	"context"
	"fmt"
	"math"
	"sort"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/provisioning/capacity"
	"k8s.io/kubernetes/pkg/scheduler/provisioning/virtualnode"
)

// Schedule evaluates a batch of pods against existing nodes and potential capacity,
// returning bindings, NodeClaims, and preemptions in one pass. It is a thin entry
// point: it builds the per-solve view over the (immutable) cluster snapshot and
// delegates to solve(). See Deschedule for the consolidation entry point — both
// share solve().
func Schedule(
	ctx context.Context,
	f framework.Framework,
	input Input,
	opts Options,
) (*Result, error) {
	// Cluster view this solve runs against: a copy-on-write overlay over the
	// immutable base snapshot. Tentative placements are recorded into the overlay
	// (never the base) so later pods in the batch spread against earlier ones and
	// the base stays shareable.
	view := input.view
	if view == nil {
		view = newView(input.base())
	}
	return solve(ctx, f, view, input.Pods, input.Offerings, opts)
}

// solve is the shared engine behind Schedule and Deschedule: it places a batch of
// pods against a cluster view (existing nodes + potential capacity), greedily and
// largest-first, scoring bind / pack-onto-in-flight / provision on one marginal-cost
// axis. It is a pure function — it records tentative placements into the view and
// returns a plan, but performs no I/O and commits nothing. The caller executes.
func solve(
	ctx context.Context,
	f framework.Framework,
	snap *view,
	inputPods []*v1.Pod,
	offerings []*capacity.InstanceType,
	opts Options,
) (*Result, error) {
	result := &Result{
		Errors: make(map[*v1.Pod]error),
	}

	// Sort pods largest-first for greedy packing
	pods := make([]*v1.Pod, len(inputPods))
	copy(pods, inputPods)
	sort.Slice(pods, func(i, j int) bool {
		return podResourceScore(pods[i]) > podResourceScore(pods[j])
	})

	// in-flight new nodes already committed to in this batch (Karpenter's
	// "in-flight NodeClaims"). The unconstrained dummy is held separately and
	// only joins this slice once a pod commits to it.
	var potentialNodes []*virtualnode.PotentialNode
	// domains chosen for each in-flight/dummy PotentialNode (hostname → topologyKey → value),
	// so placements onto potential nodes count toward the right topology domain.
	pnDomains := map[string]map[string]string{}

	// Suffix sums of batch CPU demand: remainingCPUAfter[i] = total milliCPU of
	// pods[i+1:]. Used by the packing lookahead to value headroom that pods we
	// still have to place can fill.
	remainingCPUAfter := make([]float64, len(pods))
	var acc float64
	for i := len(pods) - 1; i >= 0; i-- {
		remainingCPUAfter[i] = acc
		acc += float64(podCPUMillis(pods[i]))
	}

	for podIdx, pod := range pods {
		state := framework.NewCycleState()

		// PreFilter — populates CycleState with pod-specific cached data
		_, preFilterStatus, _ := f.RunPreFilterPlugins(ctx, state, pod)
		if preFilterStatus != nil && !preFilterStatus.IsSuccess() && !preFilterStatus.IsSkip() {
			result.Errors[pod] = fmt.Errorf("prefilter: %s", preFilterStatus.Message())
			continue
		}

		// tier orders the three candidate kinds for tie-breaking when marginal
		// costs are equal: a retained real node beats an in-flight new node
		// (no launch latency, no stockout risk), which beats opening a brand-new
		// node from the unconstrained dummy.
		const (
			tierExisting = 0
			tierInFlight = 1
			tierDummy    = 2
		)
		type candidate struct {
			nodeInfo fwk.NodeInfo
			pNode    *virtualnode.PotentialNode // nil for real nodes
			tier     int
			isDummy  bool
			effCost  float64 // effective cost of committing this pod here (lower wins)
			won      option  // the winning narrowing to apply at commit (potential nodes)
		}
		// Topology spread as requirements, computed up front (Karpenter-style):
		// the valid-domain set per spread key, injected into each candidate's
		// requirements so off-skew domains are priced out of the superposition by
		// the same narrowing as everything else. nil if the pod has no spread.
		topoUniverse := topologyUniverse(pod, offerings, snap)
		topoReqs := topologyDomainReqs(pod, snap, topoUniverse)

		// --- Tier 0: existing real nodes (marginal cost ~0 — already paid) ---
		var existingFeasible []candidate
		for _, ni := range snap.Nodes() {
			status := f.RunFilterPlugins(ctx, state, pod, ni)
			if !status.IsSuccess() {
				continue
			}
			// Resource fit against the VIEW's accounting (base pods + tentative
			// placements this batch), not the NodeInfo's mutable counters — the
			// base NodeInfo is immutable and shared, so consumption lives in the
			// view. (The plugin's own resource check above only sees base usage.)
			if !resourceFitsConcrete(pod, ni, snap) {
				continue
			}
			// Topology spread: a concrete node has a fixed domain, so check it
			// directly against current counts. An existing node that would violate
			// maxSkew is NOT a feasible bind, so it never short-circuits below —
			// bind-first still honors spread because infeasible-by-topology nodes
			// are excluded here.
			if !nodeSatisfiesTopology(pod, ni.Node(), snap, topoUniverse) {
				continue
			}
			existingFeasible = append(existingFeasible, candidate{
				nodeInfo: ni,
				tier:     tierExisting,
				effCost:  0, // already-paid capacity
			})
		}

		// --- D17: bind-first. A feasible bind to retained capacity is ~$0 and, by
		// D1, always beats provisioning (which is strictly >$0) — so if ANY existing
		// node is feasible, bind immediately and do NOT build or score the
		// provisioning tiers. This hoists the cost-model theorem out of the inner
		// loop: catalog-width scoring cost (O1) is paid only for the unschedulable
		// remainder, and the bind hot path never touches the superposition
		// machinery. Soft preferences are a multiplicative discount on cost (D5), and
		// a discount on $0 is $0, so they can neither improve nor tip a free bind —
		// there is nothing for provisioning to win here. (Among multiple feasible
		// binds the choice is a pure-binding decision; the POC takes the first in
		// snapshot order — all are marginal-cost 0 — where a production path would
		// score them via the stock framework Score plugins. This is the "keep stock
		// kube-scheduler wholesale" half of D17.)
		if len(existingFeasible) > 0 {
			winner := existingFeasible[0]
			result.Bindings = append(result.Bindings, Binding{
				Pod:      pod,
				NodeName: winner.nodeInfo.Node().Name,
				Score:    0,
			})
			// Record into the view only — never mutate the shared, immutable base
			// NodeInfo. Resource fit and topology for later pods read from the view.
			snap.AddPod(pod, winner.nodeInfo.Node().Name, winner.nodeInfo.Node().Labels)
			continue
		}

		// --- Unschedulable remainder: provision / preempt follow-up ---
		// Only reached when no existing node is feasible. The provisioning-only
		// candidate set and the expand-then-score machinery below are built here,
		// not per pod, so a bindable pod pays none of it.
		var feasible []candidate

		// Per-pod scoring stack: one expander per soft preference (generates the
		// honor/don't-honor lattice), then pure scorers over each leaf option.
		expanders := buildExpanders(pod, opts)
		scorers := buildScorers(pod, opts)

		// --- Tier 1: in-flight PotentialNodes (effective = delta of adding the pod) ---
		// NarrowForPod applies the pod's hard constraints to the superposition
		// directly (no framework — a PotentialNode is not a concrete NodeInfo).
		// Snapshot state so we can roll back if narrowing ultimately fails.
		for _, pn := range potentialNodes {
			baseline := totalScore(option{reqs: pn.Requirements, types: pn.InstanceTypes}, scorers)
			savedTypes := pn.InstanceTypes
			savedReqs := pn.Requirements

			status := pn.NarrowForPod(pod)
			if status.IsSuccess() {
				seed, ok := seedWithTopology(pn.Requirements, pn.InstanceTypes, topoReqs)
				if !ok {
					continue // no surviving offering in a valid spread domain
				}
				best, score := bestOption(seed, expanders, scorers)
				usedCPU := potentialNodeUsedCPU(pn) + float64(podCPUMillis(pod))
				credit := lookaheadCredit(best, usedCPU, remainingCPUAfter[podIdx], opts.PackingWeight)
				feasible = append(feasible, candidate{
					nodeInfo: pn,
					pNode:    pn,
					tier:     tierInFlight,
					effCost:  score - baseline - credit, // ~0 if it fits slack with no new narrowing
					won:      best,
				})
			}
			// Roll back narrowing regardless: we only commit the winner's narrowing
			// after selection, so probing must not mutate the candidate set.
			pn.InstanceTypes = savedTypes
			pn.Requirements = savedReqs
		}

		// --- Tier 2: the unconstrained dummy (effective = a whole new node) ---
		// The dummy is the speculative "should we open a brand-new node at all?"
		// question. Per the preferred architecture it is the ONE thing that lives
		// outside the cycle: it is minted here only because we have reached the
		// unschedulable remainder (no existing node and — after tier 1 above — no
		// in-flight claim can host the pod), which is exactly kube-scheduler's
		// PostFilter trigger (Filter failed everywhere). In the production shape,
		// mintDummy + the scorers below become the body of a tiny PostFilter plugin;
		// in-flight claims (tier 1) stay cycle-native and never pay this. The dummy is
		// built fresh per pod from the full offering set, then narrowed by the pod's
		// hard constraints; its baseline is 0 because it is not yet part of the plan.
		dummy := mintDummy(pod, offerings)
		if dummy != nil {
			status := dummy.NarrowForPod(pod)
			if status.IsSuccess() {
				seed, ok := seedWithTopology(dummy.Requirements, dummy.InstanceTypes, topoReqs)
				if ok {
					best, score := bestOption(seed, expanders, scorers)
					credit := lookaheadCredit(best, float64(podCPUMillis(pod)), remainingCPUAfter[podIdx], opts.PackingWeight)
					feasible = append(feasible, candidate{
						nodeInfo: dummy,
						pNode:    dummy,
						tier:     tierDummy,
						isDummy:  true,
						effCost:  score - credit,
						won:      best,
					})
				}
			}
		}

		// --- Provisioning rung: pick the cheapest capacity (in-flight vs dummy). ---
		// This is a WITHIN-rung cardinal argmin — cost is the right currency here
		// (which node is cheaper is a real $/hr question). It selects the best
		// provisioning option but does NOT yet commit; the waterfall decides whether
		// this rung fires at all, relative to preemption.
		var provisionWinner *candidate
		if len(feasible) > 0 {
			bestIdx := 0
			for i := 1; i < len(feasible); i++ {
				if betterCandidate(feasible[i].effCost, feasible[i].tier,
					feasible[bestIdx].effCost, feasible[bestIdx].tier) {
					bestIdx = i
				}
			}
			provisionWinner = &feasible[bestIdx]
		}

		// --- Preemption rung: find a feasible eviction (least disruptive). ---
		// No cost — the waterfall orders preempt vs provision, it does not price them.
		preemptOpt, preemptOK := findPreemptionOption(snap, pod)

		// --- Waterfall: try rungs in configured order; first feasible one fires. ---
		// bind-first already handled retained capacity above; this orders the two
		// follow-up actions (preempt / provision) by fixed preference, not cost. Only
		// rungs present in the waterfall can fire — a preempt-only list never
		// provisions, and vice versa.
		committed := false
		doProvision := false
		for _, rung := range waterfallOrder(opts) {
			if rung == RungPreempt && preemptOK {
				result.Preemptions = append(result.Preemptions, Preemption{
					Pod:      pod,
					NodeName: preemptOpt.nodeName,
					Victims:  preemptOpt.victims,
				})
				// Victims become displaced demand the CALLER re-batches (they re-enter
				// scheduling like any deleted pod); solve() does not recursively commit
				// them. Mirrors kube-scheduler: preemption evicts + the preemptor binds
				// on a later pass.
				for _, vp := range preemptOpt.victims {
					snap.evictFromNode(vp, preemptOpt.nodeName)
				}
				snap.AddPod(pod, preemptOpt.nodeName, nil)
				committed = true
				break
			}
			if rung == RungProvision && provisionWinner != nil {
				doProvision = true
				break
			}
		}
		if committed {
			continue
		}
		if !doProvision {
			result.Errors[pod] = fmt.Errorf("no feasible node or instance type for pod %s/%s", pod.Namespace, pod.Name)
			continue
		}
		winner := *provisionWinner

		// --- Commit provisioning --- (a PotentialNode: in-flight or dummy).

		// Re-apply the narrowing we rolled back during probing (the dummy was never
		// rolled back, but re-narrowing is idempotent — narrowing is monotonic).
		_ = winner.pNode.NarrowForPod(pod)
		// Apply the winning option: pin the superposition to the preference
		// constraints that won, so the emitted NodeClaim honors what we scored.
		winner.pNode.Requirements = winner.won.reqs
		winner.pNode.InstanceTypes = winner.won.types

		// Topology spread: collapse to a single domain per spread key and
		// record it — the "count only on collapse" step. The valid-domain
		// requirement was already injected during scoring (topoReqs), so this
		// only resolves which specific domain among the valid ones.
		domains := pnDomains[winner.pNode.Hostname()]
		if domains == nil {
			domains = map[string]string{}
		}
		recordTopology(pod, winner.pNode, snap, domains)
		pnDomains[winner.pNode.Hostname()] = domains

		pi, _ := framework.NewPodInfo(pod)
		winner.pNode.AddPodInfo(pi)
		snap.AddPod(pod, winner.pNode.Hostname(), domains)
		if winner.isDummy {
			// The dummy collapsed into a committed in-flight node. Promote it
			// so subsequent pods can pack onto it, and mint a fresh dummy next
			// iteration (created at the top of the loop).
			potentialNodes = append(potentialNodes, winner.pNode)
		}
	}

	// --- Finalize PotentialNodes → NodeClaimResults ---
	for _, pn := range potentialNodes {
		pods := pn.GetPods()
		if len(pods) == 0 {
			continue
		}

		totalRequests := make(v1.ResourceList)
		var podList []*v1.Pod
		for _, pi := range pods {
			podList = append(podList, pi.GetPod())
			for _, c := range pi.GetPod().Spec.Containers {
				for name, qty := range c.Resources.Requests {
					existing := totalRequests[name]
					existing.Add(qty)
					totalRequests[name] = existing
				}
			}
		}

		// Copy requirements (excluding hostname)
		reqs := capacity.NewRequirements()
		for key, req := range pn.Requirements {
			if key == v1.LabelHostname {
				continue
			}
			reqs[key] = req.Copy()
		}

		result.NodeClaims = append(result.NodeClaims, NodeClaimResult{
			Name:                    pn.Hostname(),
			Requirements:            reqs,
			ResourceRequests:        totalRequests,
			Pods:                    podList,
			CheapestPrice:           pn.CheapestPrice(),
			CompatibleInstanceTypes: pn.InstanceTypes,
		})
	}

	return result, nil
}

// preferredTerm is a soft node-affinity preference: a label key, the values that
// satisfy it, and the weight the pod attached.
type preferredTerm struct {
	key    string
	values map[string]struct{}
	weight int32
}

// podPreferredRequirements extracts soft node-affinity preferences
// (preferredDuringSchedulingIgnoredDuringExecution) as weighted In-terms.
// Only In is handled for the POC.
func podPreferredRequirements(pod *v1.Pod) []preferredTerm {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return nil
	}
	var terms []preferredTerm
	for _, pref := range pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
		for _, expr := range pref.Preference.MatchExpressions {
			if expr.Operator != v1.NodeSelectorOpIn {
				continue
			}
			vals := make(map[string]struct{}, len(expr.Values))
			for _, v := range expr.Values {
				vals[v] = struct{}{}
			}
			terms = append(terms, preferredTerm{key: expr.Key, values: vals, weight: pref.Weight})
		}
	}
	return terms
}

// buildExpanders returns one soft-preference expander per preferred term. Chaining
// them generates the honor/don't-honor lattice over a superposition. With no
// preferences (or PreferenceDiscount 0) the result is empty and bestOption just
// scores the seed.
func buildExpanders(pod *v1.Pod, opts Options) []expander {
	if opts.PreferenceDiscount == 0 {
		return nil
	}
	prefs := podPreferredRequirements(pod)
	exps := make([]expander, 0, len(prefs))
	for _, p := range prefs {
		exps = append(exps, softPrefExpander{term: p})
	}
	return exps
}

// buildScorers returns the pure (option -> cost) scorers. Order-free: they only
// read an option's resulting offering set. Soft preferences are folded into the
// price scorer as a multiplicative discount (D5), not a separate term — so they
// adjust the cost axis rather than needing a $/hr exchange rate.
func buildScorers(pod *v1.Pod, opts Options) []scorer {
	return []scorer{
		priceScorer{prefs: podPreferredRequirements(pod), discount: opts.PreferenceDiscount},
		flexibilityScorer{value: opts.FlexibilityValue},
	}
}

// Deschedule simulates removing the named nodes: it reschedules the pods that
// were on them against the cluster as it would be WITHOUT those nodes, and
// returns the resulting plan. It is a thin wrapper over Schedule — the caller
// inspects the Result (all rebound? new NodeClaims? errors?) and applies its own
// policy (cost threshold, disruption budget) to decide whether to act.
//
// The removed nodes are excluded from the snapshot AND their pods are dropped
// from the derived topology index, so the reschedule sees a coherent world — not
// a lazy "skip these nodes on read" view that would still count their pods.
func Deschedule(
	ctx context.Context,
	f framework.Framework,
	input Input,
	nodeNames []string,
	opts Options,
) (*Result, error) {
	reduced, displaced := newView(input.base()).withoutNodes(nodeNames...)
	return solve(ctx, f, reduced, displaced, input.Offerings, opts)
}

// resourceFitsConcrete reports whether pod's CPU/memory requests fit on a node
// given the view's current usage (base pods + tentative placements). Allocatable
// is read from the immutable NodeInfo; usage comes from the view, so no shared
// state is mutated and concurrent solves over one base don't interfere.
func resourceFitsConcrete(pod *v1.Pod, ni fwk.NodeInfo, snap *view) bool {
	alloc := ni.GetAllocatable()
	usedCPU, usedMem := snap.requestedOn(ni.Node().Name)
	var needCPU, needMem int64
	for _, c := range pod.Spec.Containers {
		if cpu, ok := c.Resources.Requests[v1.ResourceCPU]; ok {
			needCPU += cpu.MilliValue()
		}
		if mem, ok := c.Resources.Requests[v1.ResourceMemory]; ok {
			needMem += mem.Value()
		}
	}
	if needCPU > alloc.GetMilliCPU()-usedCPU {
		return false
	}
	if needMem > alloc.GetMemory()-usedMem {
		return false
	}
	return true
}

// seedWithTopology builds the scoring seed option for a superposition, narrowing
// it by the up-front topology valid-domain requirements (if any). Returns false
// if no offering survives in a valid spread domain (the candidate is infeasible
// for this pod's spread). With no spread constraints, topoReqs is nil and the
// seed is the unmodified superposition.
func seedWithTopology(reqs capacity.Requirements, types []*capacity.InstanceType, topoReqs capacity.Requirements) (option, bool) {
	if len(topoReqs) == 0 {
		return option{reqs: reqs, types: types}, true
	}
	narrowed := capacity.NewRequirements()
	narrowed.Add(reqs)
	narrowed.Add(topoReqs)
	var keep []*capacity.InstanceType
	for _, it := range types {
		if narrowed.Compatible(it.Requirements) {
			keep = append(keep, it)
		}
	}
	if len(keep) == 0 {
		return option{}, false
	}
	return option{reqs: narrowed, types: keep}, true
}

// possibleDomains returns the domain values a potential node could still resolve
// to for a topology key, from its surviving instance types' requirements.
func possibleDomains(pn *virtualnode.PotentialNode, key string) []string {
	if req := pn.Requirements.Get(key); req != nil {
		return req.Values().UnsortedList()
	}
	seen := map[string]struct{}{}
	for _, it := range pn.InstanceTypes {
		if req := it.Requirements.Get(key); req != nil {
			for _, v := range req.Values().UnsortedList() {
				seen[v] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	return out
}

// podCPUMillis returns a pod's total CPU request in millicores.
func podCPUMillis(pod *v1.Pod) int64 {
	var m int64
	for _, c := range pod.Spec.Containers {
		if cpu, ok := c.Resources.Requests[v1.ResourceCPU]; ok {
			m += cpu.MilliValue()
		}
	}
	return m
}

// potentialNodeUsedCPU returns the total CPU (millicores) of pods already placed
// on an in-flight PotentialNode.
func potentialNodeUsedCPU(pn *virtualnode.PotentialNode) float64 {
	var m float64
	for _, pi := range pn.GetPods() {
		m += float64(podCPUMillis(pi.GetPod()))
	}
	return m
}

// lookaheadCredit resolves the instance type an option would launch and credits
// its fillable headroom against remaining batch demand (see packingCredit).
func lookaheadCredit(opt option, usedCPU, remainingCPU, weight float64) float64 {
	chosen, price, ok := cheapestTypeAndPrice(opt.reqs, opt.types)
	if !ok {
		return 0
	}
	return packingCredit(chosen, price, usedCPU, remainingCPU, weight)
}

// waterfallOrder returns the follow-up rung order for a solve. Defaults to
// today's behavior — preempt before provision — when unset, which is
// back-compat-safe; callers flip to {RungProvision, RungPreempt} to opt into
// provisioning-first. Bind is not a rung (it is always tried first, structurally).
func waterfallOrder(opts Options) []Rung {
	if len(opts.Waterfall) == 0 {
		return []Rung{RungPreempt, RungProvision}
	}
	return opts.Waterfall
}

// betterCandidate reports whether candidate (m1, t1) should beat the current best
// (m0, t0). Lower effective cost wins; within a tie (costs within epsilon), the
// lower tier wins (existing < in-flight < dummy).
func betterCandidate(m1 float64, t1 int, m0 float64, t0 int) bool {
	const epsilon = 1e-9
	if math.Abs(m1-m0) > epsilon {
		return m1 < m0
	}
	return t1 < t0
}

// mintDummy builds a fresh dummy PotentialNode from the offerings compatible with a
// pod — the speculative "open a brand-new node?" superposition. It is the seam the
// preferred architecture quarantines in a PostFilter plugin: it is called only for
// the unschedulable remainder (bind failed everywhere and no in-flight claim fits),
// which is PostFilter's native trigger, and it is the only thing that lives outside
// the cycle. Returns nil if no offering can host the pod at all.
func mintDummy(pod *v1.Pod, offerings []*capacity.InstanceType) *virtualnode.PotentialNode {
	podReqs := podSchedulingRequirements(pod)
	podRequests := podTotalRequests(pod)

	var compatible []*capacity.InstanceType
	for _, it := range offerings {
		if !podReqs.Compatible(it.Requirements) {
			continue
		}
		if !virtualnode.ResourcesFit(it.Allocatable(), podRequests) {
			continue
		}
		if !hasAvailableOffering(it, podReqs) {
			continue
		}
		compatible = append(compatible, it)
	}

	if len(compatible) == 0 {
		return nil
	}

	// Seed base requirements with the union of all compatible types' label domains.
	// This tells the PotentialNode what topology domains it COULD resolve to.
	baseReqs := unionRequirements(compatible)
	return virtualnode.New(baseReqs, compatible, nil)
}

// unionRequirements builds requirements where each key's values are the union across all types.
func unionRequirements(types []*capacity.InstanceType) capacity.Requirements {
	union := capacity.NewRequirements()
	for _, it := range types {
		for key, req := range it.Requirements {
			if existing, ok := union[key]; ok {
				union[key] = existing.Union(req)
			} else {
				union[key] = req.Copy()
			}
		}
	}
	return union
}

func podResourceScore(pod *v1.Pod) int64 {
	var cpuMillis, memBytes int64
	for _, c := range pod.Spec.Containers {
		if cpu, ok := c.Resources.Requests[v1.ResourceCPU]; ok {
			cpuMillis += cpu.MilliValue()
		}
		if mem, ok := c.Resources.Requests[v1.ResourceMemory]; ok {
			memBytes += mem.Value()
		}
	}
	return cpuMillis + memBytes/(1024*1024*1024)*1000
}

func podSchedulingRequirements(pod *v1.Pod) capacity.Requirements {
	// Single extraction implementation (honors all operators via the ported
	// capacity.Requirement), shared with the solver package.
	return virtualnode.PodHardRequirements(pod)
}

func podTotalRequests(pod *v1.Pod) v1.ResourceList {
	total := make(v1.ResourceList)
	for _, c := range pod.Spec.Containers {
		for name, qty := range c.Resources.Requests {
			existing := total[name]
			existing.Add(qty)
			total[name] = existing
		}
	}
	return total
}

func hasAvailableOffering(it *capacity.InstanceType, reqs capacity.Requirements) bool {
	for _, o := range it.Offerings {
		if o.Available && reqs.Compatible(o.Requirements) {
			return true
		}
	}
	return false
}
