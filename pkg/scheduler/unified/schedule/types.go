package schedule

import (
	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

// ClusterState represents the current state of the cluster for scheduling decisions.
type ClusterState struct {
	Nodes []fwk.NodeInfo

	// snap, if set, is a prebuilt immutable base used in preference to Nodes —
	// for callers that build/reuse a base snapshot directly.
	snap *Snapshot
}

// Binding represents the decision to place a pod on an existing node.
type Binding struct {
	Pod      *v1.Pod
	NodeName string
	Score    int64
}

// Preemption represents the decision to evict pods to make room for a higher-priority pod.
type Preemption struct {
	Pod      *v1.Pod
	NodeName string
	Victims  []*v1.Pod
	Score    int64
}

// NodeClaimResult represents a new node to provision, expressed as accumulated
// requirements and resource requests. Equivalent to a Karpenter NodeClaim.
type NodeClaimResult struct {
	// Name is the unique identifier for this potential node.
	Name string

	// Requirements are the accumulated constraints from all placed pods.
	// The capacity provider selects a specific instance type satisfying these.
	Requirements capacity.Requirements

	// ResourceRequests is the sum of all placed pods' resource requests.
	ResourceRequests v1.ResourceList

	// Pods assigned to this NodeClaim.
	Pods []*v1.Pod

	// CheapestPrice is the lowest price among compatible offerings.
	CheapestPrice float64

	// CompatibleInstanceTypes remaining after all narrowing.
	CompatibleInstanceTypes []*capacity.InstanceType
}

// Result is the output of the unified scheduling function.
type Result struct {
	Bindings    []Binding
	NodeClaims  []NodeClaimResult
	Preemptions []Preemption
	Errors      map[*v1.Pod]error
}

// Rung is one action-class in the scheduling waterfall (see Options.Waterfall).
// The follow-up tries rungs in order and takes the first that is feasible for the
// pod — a fixed, legible order, NOT a cost comparison. We concede preempt and
// provision cost the same node in the contested case; ordering them is an ordinal
// preference (which the room can audit) rather than a cardinal $/hr tradeoff (which
// no user can supply). Bind is not a rung — a feasible bind to retained capacity
// dominates everything on every axis (free, instant, harmless), so it is always
// tried first, structurally, before the waterfall runs.
type Rung int

const (
	// RungPreempt: evict strictly-lower-priority victims to fit the pod on existing
	// capacity. Priority-gated; a production version also honors PDB/preemptionPolicy.
	RungPreempt Rung = iota
	// RungProvision: launch new capacity (in-flight NodeClaim or a fresh node).
	RungProvision
)

// Options configures scheduling behavior.
type Options struct {
	// Waterfall is the ordered list of action-classes the follow-up tries for a pod
	// that cannot bind to retained capacity. The first feasible rung wins; there is
	// no cost comparison across rungs. Defaults (nil) to today's behavior,
	// {RungPreempt, RungProvision} — preempt-before-provision — which is
	// back-compat-safe. Flip to {RungProvision, RungPreempt} for provision-first
	// (the "provisioning dominates preemption" preference), which is the opt-in the
	// design argues most workloads should choose. An empty rung list still binds but
	// never provisions or preempts (pure bind-or-fail). Preemption also only fires
	// if the pod's priority permits evicting something (already opt-in via
	// PriorityClass), so a preempt rung is inert for a pod with nothing to preempt.
	Waterfall []Rung

	// PreferenceDiscount is the multiplicative price discount per unit of
	// guaranteed soft-preference weight (D5). An option whose offering set
	// GUARANTEES a preferred term has its price scaled by (1 − weight×discount),
	// capped below 1 so it can never reach zero. "Newer gen is 20% better" is a
	// discount of 0.2 on a weight-1 term → effective cost ×0.8.
	//
	// This is multiplicative *on the cost axis*, not an additive benefit: it ranks
	// purchasable offerings (cheaper-effective wins) but cannot make any candidate
	// beat a free bind (a discount on $0 is $0), so a soft preference never tips
	// provisioning of a new node. If a preference must launch a node, express it as
	// a hard `requiredDuringScheduling` constraint instead. Set to 0 to ignore soft
	// preferences entirely (Karpenter's default provisioning behavior).
	PreferenceDiscount float64

	// FlexibilityValue is the $/hr value placed on an option retaining diverse
	// fallback offerings (failure-independent zone × capacity-type pools).
	// Narrowing a NodeClaim reduces pools, so a higher FlexibilityValue makes
	// over-constraining (e.g. pinning to a single spot pool) score worse —
	// expressing the cost-certainty-vs-stockout-resilience tradeoff as a peer
	// score term. Set to 0 to ignore flexibility.
	FlexibilityValue float64

	// PackingWeight controls a lookahead credit that counteracts greedy
	// mis-packing. Per-pod marginal cost is myopic: a tightly-sized fresh node
	// looks cheaper than growing an in-flight node a tier, because the pod is
	// billed the whole discrete instance-size jump while the headroom that jump
	// buys — which later pods fill for free — is ignored. PackingWeight credits a
	// candidate's cost by the value of the headroom it leaves that the REMAINING
	// batch demand can actually fill, so growing a node to absorb pods you still
	// have to place beats opening many tight nodes. 0 disables (pure marginal
	// cost); 1.0 values fillable headroom at its full per-unit price.
	PackingWeight float64
}

// Input bundles the arguments to Schedule().
type Input struct {
	Pods         []*v1.Pod
	ClusterState *ClusterState
	Offerings    []*capacity.InstanceType

	// view, if set, is the prebuilt copy-on-write view the solve runs against.
	// Set by Deschedule (which masks the removed nodes over a shared base). When
	// nil, Schedule builds a fresh view over base().
	view *view
}

// base returns the immutable base Snapshot for this input, building one from the
// flat node slice if the caller didn't provide a prebuilt one. This keeps the
// existing {ClusterState: &ClusterState{Nodes: ...}} call sites working while
// routing all node/topology reads through the Snapshot/view.
func (in Input) base() *Snapshot {
	if in.ClusterState != nil && in.ClusterState.snap != nil {
		return in.ClusterState.snap
	}
	var nodes []fwk.NodeInfo
	if in.ClusterState != nil {
		nodes = in.ClusterState.Nodes
	}
	return NewSnapshot(nodes)
}
