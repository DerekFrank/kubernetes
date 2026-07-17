package virtualnode

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler/provisioning/capacity"
)

// Topology is the cross-pod state a topology-spread Narrower needs but a single
// (pod, claim) pair cannot hold: per topology key, the count of matching placed
// pods per domain. It is the provisioning analog of what kube-scheduler computes in
// PodTopologySpread.PreFilter and stores in CycleState. It lives on the Problem
// (one per solve), is seeded from the existing world, and is updated when a pod is
// actually committed to a claim — never during probing.
//
// The domain universe (all domains a key could use, from offerings + existing
// nodes) is load-bearing: skew must be measured against every possible domain, not
// just those already used, or the first domain used looks "within skew" as the only
// seen value and every pod piles into it.
type Topology struct {
	// counts[topologyKey][domain] = matching placed pods in that domain.
	counts map[string]map[string]int
	// universe[topologyKey] = every domain the key could resolve to.
	universe map[string]sets.Set[string]
}

// NewTopology builds topology state with the given per-key domain universe.
func NewTopology(universe map[string]sets.Set[string]) *Topology {
	return &Topology{counts: map[string]map[string]int{}, universe: universe}
}

// Record increments the matching-pod count for (key, domain). Used to seed the
// existing world and to record a commit.
func (t *Topology) Record(key, domain string) {
	if t.counts[key] == nil {
		t.counts[key] = map[string]int{}
	}
	t.counts[key][domain]++
}

// validDomains returns the domains where placing one more matching pod keeps skew
// within maxSkew, measured over the full universe (zero-count domains included).
func (t *Topology) validDomains(key string, maxSkew int32) sets.Set[string] {
	universe := t.universe[key]
	counts := t.counts[key]
	min := -1
	for d := range universe {
		c := counts[d]
		if min == -1 || c < min {
			min = c
		}
	}
	if min == -1 {
		return sets.New[string]() // no known domains for this key
	}
	valid := sets.New[string]()
	for d := range universe {
		// placing here makes the domain (counts[d]+1); valid if that stays within
		// maxSkew of the current minimum: counts[d]+1-min <= maxSkew.
		if int32(counts[d]+1-min) <= maxSkew {
			valid.Insert(d)
		}
	}
	return valid
}

// leastLoaded returns the least-loaded domain among candidates (ties → any),
// used to pick the domain a committing pod records against.
func (t *Topology) leastLoaded(key string, candidates sets.Set[string]) string {
	counts := t.counts[key]
	best, bestN := "", -1
	for d := range candidates {
		if bestN == -1 || counts[d] < bestN {
			best, bestN = d, counts[d]
		}
	}
	return best
}

// Committer is implemented by Narrowers that maintain cross-pod state which must be
// updated only when a pod is actually placed (not during a rolled-back probe). The
// solver calls OnCommit after a successful, kept placement. Node-local Narrowers
// (taints, affinity, resources) do not implement it.
type Committer interface {
	OnCommit(pod *v1.Pod, claim *PotentialNode)
}

// TopologySpreadNarrower is the Narrow analog of the PodTopologySpread Filter. It
// reads the shared Topology counts to compute which domains keep maxSkew satisfied,
// narrows the claim's topology-key requirement to those domains, and on commit pins
// the claim to a single chosen domain and records it. Because it needs cross-pod
// state, it is constructed with a *Topology (from Problem) — unlike the node-local
// Narrowers, which need only (pod, claim).
type TopologySpreadNarrower struct {
	topo *Topology
}

// NewTopologySpreadNarrower builds the narrower over shared topology state.
func NewTopologySpreadNarrower(topo *Topology) *TopologySpreadNarrower {
	return &TopologySpreadNarrower{topo: topo}
}

func (*TopologySpreadNarrower) Name() string { return "PodTopologySpread" }
func (*TopologySpreadNarrower) Hard() bool   { return true }

func (n *TopologySpreadNarrower) Narrow(pod *v1.Pod, claim *PotentialNode) *fwk.Status {
	for _, c := range pod.Spec.TopologySpreadConstraints {
		if c.WhenUnsatisfiable != v1.DoNotSchedule {
			continue // only hard spread narrows; soft is a ranking concern
		}
		valid := n.topo.validDomains(c.TopologyKey, c.MaxSkew)
		if valid.Len() == 0 {
			return fwk.NewStatus(fwk.Unschedulable,
				fmt.Sprintf("no domain satisfies topology spread for key %s", c.TopologyKey))
		}
		req := capacity.NewRequirement(c.TopologyKey, v1.NodeSelectorOpIn, valid.UnsortedList()...)
		if err := claim.Narrow(capacity.Requirements{c.TopologyKey: req}, nil); err != nil {
			return fwk.NewStatus(fwk.Unschedulable,
				fmt.Sprintf("topology spread narrowed claim to empty for key %s", c.TopologyKey))
		}
	}
	return nil
}

// OnCommit pins the claim to the least-loaded valid domain per spread key and
// records it, so the next pod's Narrow sees the updated skew. This is the
// "record on collapse" step — after it, the claim's domain for the key is single.
func (n *TopologySpreadNarrower) OnCommit(pod *v1.Pod, claim *PotentialNode) {
	for _, c := range pod.Spec.TopologySpreadConstraints {
		if c.WhenUnsatisfiable != v1.DoNotSchedule {
			continue
		}
		// Surviving domains on the claim for this key ∩ still-valid domains.
		surviving := claimDomains(claim, c.TopologyKey, n.topo.universe[c.TopologyKey])
		valid := n.topo.validDomains(c.TopologyKey, c.MaxSkew)
		candidates := surviving.Intersection(valid)
		if candidates.Len() == 0 {
			candidates = surviving // fall back to whatever the claim can still be
		}
		domain := n.topo.leastLoaded(c.TopologyKey, candidates)
		if domain == "" {
			continue
		}
		// Pin the claim to the chosen domain and record it.
		_ = claim.Narrow(capacity.Requirements{
			c.TopologyKey: capacity.NewRequirement(c.TopologyKey, v1.NodeSelectorOpIn, domain),
		}, nil)
		n.topo.Record(c.TopologyKey, domain)
	}
}

// claimDomains returns the domains a claim could still resolve to for a key: its
// requirement's values if constrained, else the whole universe.
func claimDomains(claim *PotentialNode, key string, universe sets.Set[string]) sets.Set[string] {
	if req := claim.Requirements.Get(key); req != nil && req.Operator() == v1.NodeSelectorOpIn {
		return req.Values()
	}
	return universe
}

// PodMatchesSpread reports whether a pod counts toward a spread constraint's
// selector (used to seed existing pods). A nil selector matches nothing (matching
// kube-scheduler, which requires an explicit selector).
func PodMatchesSpread(pod *v1.Pod, selector *metav1.LabelSelector) bool {
	if selector == nil {
		return false
	}
	for k, v := range selector.MatchLabels {
		if pod.Labels[k] != v {
			return false
		}
	}
	return true
}
