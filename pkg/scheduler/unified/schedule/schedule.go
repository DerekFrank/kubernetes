// Package schedule implements the unified scheduling function that evaluates
// binding, provisioning, and preemption as alternatives in one pass.
//
// PotentialNodes flow through the SAME Filter/Score pipeline as concrete nodes.
// Modified plugins detect PotentialNodes and narrow the superposition as a side
// effect of Filter, making "potential new node" a first-class scheduling concept.
package schedule

import (
	"context"
	"fmt"
	"math"
	"sort"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
	"k8s.io/kubernetes/pkg/scheduler/unified/virtualnode"
)

// Schedule evaluates a batch of pods against existing nodes and potential capacity,
// returning bindings, NodeClaims, and preemptions in one pass.
func Schedule(
	ctx context.Context,
	f framework.Framework,
	input Input,
	opts Options,
) (*Result, error) {
	result := &Result{
		Errors: make(map[*v1.Pod]error),
	}

	// Sort pods largest-first for greedy packing
	pods := make([]*v1.Pod, len(input.Pods))
	copy(pods, input.Pods)
	sort.Slice(pods, func(i, j int) bool {
		return podResourceScore(pods[i]) > podResourceScore(pods[j])
	})

	// Cluster view this solve runs against: a copy-on-write overlay over the
	// immutable base snapshot. Tentative placements are recorded into the overlay
	// (never the base) so later pods in the batch spread against earlier ones and
	// the base stays shareable.
	snap := input.view
	if snap == nil {
		snap = newView(input.base())
	}

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
		var feasible []candidate

		// Per-pod scoring stack: one expander per soft preference (generates the
		// honor/don't-honor lattice), then pure scorers over each leaf option.
		expanders := buildExpanders(pod, opts)
		scorers := buildScorers(pod, opts)

		// Topology spread as requirements, computed up front (Karpenter-style):
		// the valid-domain set per spread key, injected into each candidate's
		// requirements so off-skew domains are priced out of the superposition by
		// the same narrowing as everything else. nil if the pod has no spread.
		topoUniverse := topologyUniverse(pod, input.Offerings, snap)
		topoReqs := topologyDomainReqs(pod, snap, topoUniverse)

		// --- Tier 0: existing real nodes (marginal cost ~0 — already paid) ---
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
			// directly against current counts.
			if !nodeSatisfiesTopology(pod, ni.Node(), snap, topoUniverse) {
				continue
			}
			feasible = append(feasible, candidate{
				nodeInfo: ni,
				tier:     tierExisting,
				// Already-paid capacity: marginal cost 0. Soft preferences are a
				// multiplicative discount on cost (D5), and a discount on $0 is $0 —
				// so a soft preference never makes a free bind more or less
				// attractive, and never tips provisioning over it. (A pod that
				// *requires* an attribute the node lacks was already filtered out.)
				effCost: 0,
			})
		}

		// --- Tier 1: in-flight PotentialNodes (effective = delta of adding the pod) ---
		// Plugins narrow the superposition as a side effect of Filter.
		// Snapshot state so we can roll back if Filter ultimately fails.
		for _, pn := range potentialNodes {
			baseline := totalScore(option{reqs: pn.Requirements, types: pn.InstanceTypes}, scorers)
			savedTypes := pn.InstanceTypes
			savedReqs := pn.Requirements

			status := f.RunFilterPlugins(ctx, state, pod, pn)
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
		// We always offer the option of launching a brand-new node, regardless of
		// whether existing/in-flight candidates are feasible. The dummy is built
		// fresh per pod from the full offering set, then narrowed by Filter; its
		// baseline is 0 because it is not yet part of the plan.
		dummy := createPotentialNode(pod, input.Offerings)
		if dummy != nil {
			status := f.RunFilterPlugins(ctx, state, pod, dummy)
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

		if len(feasible) == 0 {
			result.Errors[pod] = fmt.Errorf("no feasible node or instance type for pod %s/%s", pod.Namespace, pod.Name)
			continue
		}

		// --- Select: lowest effective cost wins; ties broken by tier ---
		bestIdx := 0
		for i := 1; i < len(feasible); i++ {
			if betterCandidate(feasible[i].effCost, feasible[i].tier,
				feasible[bestIdx].effCost, feasible[bestIdx].tier) {
				bestIdx = i
			}
		}
		winner := feasible[bestIdx]

		// --- Commit ---
		if winner.pNode != nil {
			// Re-run Filter on the winner to re-apply the narrowing we rolled back
			// during probing (the dummy was never rolled back, but re-running is
			// idempotent — narrowing is monotonic).
			_ = f.RunFilterPlugins(ctx, state, pod, winner.pNode)
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
		} else {
			result.Bindings = append(result.Bindings, Binding{
				Pod:      pod,
				NodeName: winner.nodeInfo.Node().Name,
				Score:    -int64(winner.effCost),
			})
			// Record into the view only — never mutate the shared, immutable base
			// NodeInfo. Resource fit and topology for later pods read from the view.
			snap.AddPod(pod, winner.nodeInfo.Node().Name, winner.nodeInfo.Node().Labels)
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
	return Schedule(ctx, f, Input{
		Pods:      displaced,
		Offerings: input.Offerings,
		view:      reduced,
	}, opts)
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
		return req.Values.UnsortedList()
	}
	seen := map[string]struct{}{}
	for _, it := range pn.InstanceTypes {
		if req := it.Requirements.Get(key); req != nil {
			for _, v := range req.Values.UnsortedList() {
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

// createPotentialNode builds a PotentialNode from offerings compatible with a pod.
func createPotentialNode(pod *v1.Pod, offerings []*capacity.InstanceType) *virtualnode.PotentialNode {
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
				// Union: merge value sets
				merged := existing.Values.Union(req.Values)
				union[key] = &capacity.Requirement{
					Key:      key,
					Operator: req.Operator,
					Values:   merged,
				}
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
	reqs := capacity.NewRequirements()
	for key, value := range pod.Spec.NodeSelector {
		reqs[key] = capacity.NewRequirement(key, v1.NodeSelectorOpIn, value)
	}
	if pod.Spec.Affinity != nil && pod.Spec.Affinity.NodeAffinity != nil {
		if required := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution; required != nil {
			for _, term := range required.NodeSelectorTerms {
				for _, expr := range term.MatchExpressions {
					if expr.Operator == v1.NodeSelectorOpIn {
						reqs[expr.Key] = capacity.NewRequirement(expr.Key, expr.Operator, expr.Values...)
					}
				}
			}
		}
	}
	return reqs
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
