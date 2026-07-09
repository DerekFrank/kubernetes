// Package solver is the pure provisioning core described in
// provisioning-pipeline-scratch.md: given the pods that could not bind and a set
// of offerings, decide what new capacity to create. It has NO cluster state, NO
// snapshot, NO framework handle — a Solver is a pure function Problem → []Solution,
// which is what makes the fan-out portfolio raceless and parallelizable.
//
// There are two plugin surfaces here:
//   - Narrower (from the virtualnode package): the conjunctive constraint surface
//     (taints, requirements, ... ) — solvers CONSULT it, they do not contain it.
//   - Solver: the competitive packing-strategy surface (greedy, ILP) — the framework
//     fans out to all registered solvers and Select picks the best.
//
// Cost is not a plugin: price (incl. performance-value) is offering data, read off
// the offering via EffectivePrice. Select compares Solutions on derived cost.
package solver

import (
	v1 "k8s.io/api/core/v1"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
	"k8s.io/kubernetes/pkg/scheduler/unified/virtualnode"
)

// Problem is the solver-neutral description of a provisioning sub-problem (one
// Split). It is immutable — every solver reads the same Problem concurrently.
type Problem struct {
	// Pods is the set to provision capacity for (the unschedulable remainder).
	Pods []*v1.Pod
	// Offerings is the catalog new capacity can be built from.
	Offerings []*capacity.InstanceType
	// Narrowers are the constraint plugins every solver consults. Empty means the
	// built-in defaults (taints + requirements) via virtualnode.DefaultNarrowers.
	Narrowers []virtualnode.Narrower
}

func (p Problem) narrowers() []virtualnode.Narrower {
	if len(p.Narrowers) == 0 {
		return virtualnode.DefaultNarrowers()
	}
	return p.Narrowers
}

// NodeClaim is the Kubernetes-shaped output for one piece of new capacity: the
// accumulated requirements + the still-compatible instance types ("any of these
// will do" — the offering axis is emitted as a set, not collapsed), for the cloud
// provider to fulfill. Mirrors Karpenter's v1.NodeClaim; a production version emits
// the real CRD.
type NodeClaim struct {
	Name          string
	Requirements  capacity.Requirements
	InstanceTypes []*capacity.InstanceType
	Requests      v1.ResourceList
	CheapestPrice float64
}

// PodBinding is the Kubernetes-shaped assignment of a pod to the capacity that will
// host it — here, the NodeClaim it will land on once provisioned (bound for real by
// the caller after the node registers, per the nominate-to-NodeClaim model, D18).
type PodBinding struct {
	Pod           *v1.Pod
	NodeClaimName string
}

// Solution is one candidate answer for a Problem, in Kubernetes terms: the
// NodeClaims to create, the PodBindings mapping each placed pod to its claim, and
// the pods that could not be placed. Cost is derived (sum of each claim's cheapest
// effective price), not supplied by the solver, so Select compares candidates on
// one ruler no solver can game.
type Solution struct {
	NodeClaims []NodeClaim
	Bindings   []PodBinding
	Unplaced   []*v1.Pod
}

// Cost is the derived comparison metric: total cheapest-effective-price across the
// NodeClaims. Lower is better. Unplaced pods are penalized heavily so a solution
// that strands pods never beats one that places them.
func (s Solution) Cost() float64 {
	var total float64
	for _, c := range s.NodeClaims {
		total += c.CheapestPrice
	}
	total += float64(len(s.Unplaced)) * unplacedPenalty
	return total
}

// NodeCount is the price-free comparison metric (for non-priced fleets / O2): fewer
// nodes is better, ties broken by fewer unplaced.
func (s Solution) NodeCount() int { return len(s.NodeClaims) + len(s.Unplaced)*1000 }

const unplacedPenalty = 1e9

// finalize converts internal working claims (PotentialNode superpositions with
// their assigned pods) into the Kubernetes-shaped Solution. This is where the
// working representation becomes NodeClaims[] + PodBindings[].
func finalize(claims []*virtualnode.PotentialNode, unplaced []*v1.Pod) Solution {
	sol := Solution{Unplaced: unplaced}
	for _, c := range claims {
		pods := c.GetPods()
		if len(pods) == 0 {
			continue
		}
		requests := make(v1.ResourceList)
		for _, pi := range pods {
			pod := pi.GetPod()
			sol.Bindings = append(sol.Bindings, PodBinding{Pod: pod, NodeClaimName: c.Hostname()})
			for _, ctr := range pod.Spec.Containers {
				for name, qty := range ctr.Resources.Requests {
					existing := requests[name]
					existing.Add(qty)
					requests[name] = existing
				}
			}
		}
		reqs := capacity.NewRequirements()
		for key, req := range c.Requirements {
			if key == v1.LabelHostname {
				continue
			}
			reqs[key] = req.Copy()
		}
		sol.NodeClaims = append(sol.NodeClaims, NodeClaim{
			Name:          c.Hostname(),
			Requirements:  reqs,
			InstanceTypes: c.InstanceTypes,
			Requests:      requests,
			CheapestPrice: c.CheapestPrice(),
		})
	}
	return sol
}

// Solver is the competitive packing-strategy plugin surface. Solve is pure: it
// reads only the Problem and returns candidate Solutions (usually one; an
// anytime/frontier solver may return several). Every solver consults the Problem's
// Narrowers and builds claims via NewNodeClaim.
type Solver interface {
	Name() string
	Solve(Problem) []Solution
}

// NewNodeClaim constructs the widest valid claim (⊤) over the offerings compatible
// with pod: all such instance types, overhead-correct allocatable (owned by
// capacity.InstanceType.Allocatable). This is the shared constructor every solver
// uses; it does NOT seed the pod (open-claim and add-pod are separate ops).
// Returns nil if no offering can host the pod at all.
func NewNodeClaim(pod *v1.Pod, offerings []*capacity.InstanceType) *virtualnode.PotentialNode {
	reqs := podRequirements(pod)
	requests := podRequests(pod)

	var compatible []*capacity.InstanceType
	for _, it := range offerings {
		if !reqs.Compatible(it.Requirements) {
			continue
		}
		if !virtualnode.ResourcesFit(it.Allocatable(), requests) {
			continue
		}
		compatible = append(compatible, it)
	}
	if len(compatible) == 0 {
		return nil
	}
	return virtualnode.New(unionRequirements(compatible), compatible, nil)
}

// tryAdd attempts to place pod on claim by applying the hard Narrowers, rolling
// back on failure so a rejected probe doesn't mutate the claim. Returns true if the
// pod was placed (claim narrowed + pod recorded).
func tryAdd(claim *virtualnode.PotentialNode, pod *v1.Pod, narrowers []virtualnode.Narrower) bool {
	savedTypes := claim.InstanceTypes
	savedReqs := claim.Requirements
	if status := claim.NarrowForPod(pod, narrowers...); !status.IsSuccess() {
		claim.InstanceTypes = savedTypes
		claim.Requirements = savedReqs
		return false
	}
	claim.AddPod(pod)
	return true
}

// --- pod helpers (local; the solver package is standalone) ---

func podRequirements(pod *v1.Pod) capacity.Requirements {
	reqs := capacity.NewRequirements()
	for key, value := range pod.Spec.NodeSelector {
		reqs[key] = capacity.NewRequirement(key, v1.NodeSelectorOpIn, value)
	}
	if pod.Spec.Affinity != nil && pod.Spec.Affinity.NodeAffinity != nil {
		if req := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution; req != nil {
			for _, term := range req.NodeSelectorTerms {
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

func podRequests(pod *v1.Pod) v1.ResourceList {
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

func podCPUMillis(pod *v1.Pod) int64 {
	var m int64
	for _, c := range pod.Spec.Containers {
		if cpu, ok := c.Resources.Requests[v1.ResourceCPU]; ok {
			m += cpu.MilliValue()
		}
	}
	return m
}

func unionRequirements(types []*capacity.InstanceType) capacity.Requirements {
	union := capacity.NewRequirements()
	for _, it := range types {
		for key, req := range it.Requirements {
			if existing, ok := union[key]; ok {
				union[key] = &capacity.Requirement{Key: key, Operator: req.Operator, Values: existing.Values.Union(req.Values)}
			} else {
				union[key] = req.Copy()
			}
		}
	}
	return union
}
