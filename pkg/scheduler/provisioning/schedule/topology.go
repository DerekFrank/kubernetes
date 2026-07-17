package schedule

import (
	v1 "k8s.io/api/core/v1"

	"k8s.io/kubernetes/pkg/scheduler/provisioning/capacity"
	"k8s.io/kubernetes/pkg/scheduler/provisioning/virtualnode"
)

// Topology, the Karpenter way: a pod's spread constraints are turned into
// requirements UP FRONT (before candidate evaluation) and injected into the
// requirement set, so they flow through the same narrowing/scoring as every other
// constraint. We never hold provisional per-domain counts for unresolved nodes;
// a count is recorded only once a node's domain collapses to a single value
// (recordTopology, at commit). This sidesteps the "constraining a NodeClaim
// retroactively moves topology counts" problem: the domain requirement is added
// before placement, so narrowing and topology are one operation, not two that
// must be kept in sync.

// topologyDomainReqs computes, for each of the pod's spread keys, the set of
// domains that respect maxSkew given the view's current counts — expressed as an
// In requirement. Injected into candidate requirements so off-skew domains are
// filtered (concrete nodes) or priced out of the superposition (potential nodes).
// Returns nil if the pod has no spread constraints.
//
// `universe` is the full set of domain values per topology key the cluster could
// use (from offerings + existing nodes). It is essential: skew is measured
// against ALL domains, so a zero-count domain that no pod has touched yet must
// pull the minimum to 0 — otherwise the first-used domain looks "within skew" as
// the only seen value and every pod piles into it.
func topologyDomainReqs(pod *v1.Pod, snap *view, universe map[string][]string) capacity.Requirements {
	if len(pod.Spec.TopologySpreadConstraints) == 0 {
		return nil
	}
	reqs := capacity.NewRequirements()
	for _, tsc := range pod.Spec.TopologySpreadConstraints {
		counts := snap.domainCounts(tsc.TopologyKey, tsc.LabelSelector)
		domains := universe[tsc.TopologyKey]

		// Minimum over the FULL universe: any domain absent from counts is 0.
		minCount := int32(0)
		if len(domains) > 0 {
			first := true
			for _, d := range domains {
				c := counts[d]
				if first || c < minCount {
					minCount, first = c, false
				}
			}
		} else {
			minCount = minDomainCount(counts)
		}

		// Valid domains: those (across the universe) where adding this pod keeps
		// (count+1)-min <= maxSkew.
		var valid []string
		for _, d := range domains {
			if (counts[d]+1)-minCount <= tsc.MaxSkew {
				valid = append(valid, d)
			}
		}
		if len(valid) > 0 {
			reqs[tsc.TopologyKey] = capacity.NewRequirement(tsc.TopologyKey, v1.NodeSelectorOpIn, valid...)
		}
	}
	if len(reqs) == 0 {
		return nil
	}
	return reqs
}

// topologyUniverse gathers the set of domain values per topology key the cluster
// could place pods in: the zones/domains offered by instance types plus those of
// existing nodes. Computed once per solve.
func topologyUniverse(pod *v1.Pod, offerings []*capacity.InstanceType, snap *view) map[string][]string {
	keys := map[string]struct{}{}
	for _, tsc := range pod.Spec.TopologySpreadConstraints {
		keys[tsc.TopologyKey] = struct{}{}
	}
	if len(keys) == 0 {
		return nil
	}
	seen := map[string]map[string]struct{}{}
	add := func(key, val string) {
		if _, want := keys[key]; !want || val == "" {
			return
		}
		if seen[key] == nil {
			seen[key] = map[string]struct{}{}
		}
		seen[key][val] = struct{}{}
	}
	for _, it := range offerings {
		for key := range keys {
			if req := it.Requirements.Get(key); req != nil {
				for _, v := range req.Values().UnsortedList() {
					add(key, v)
				}
			}
		}
	}
	for _, ni := range snap.Nodes() {
		if ni.Node() == nil {
			continue
		}
		for key := range keys {
			add(key, ni.Node().Labels[key])
		}
	}
	out := make(map[string][]string, len(seen))
	for key, vals := range seen {
		for v := range vals {
			out[key] = append(out[key], v)
		}
	}
	return out
}

// nodeSatisfiesTopology reports whether a concrete node's domains keep every
// spread constraint within maxSkew (counts from the view, minimum over the full
// universe). Concrete nodes have a fixed domain, so this is a direct check.
func nodeSatisfiesTopology(pod *v1.Pod, node *v1.Node, snap *view, universe map[string][]string) bool {
	for _, tsc := range pod.Spec.TopologySpreadConstraints {
		domain, ok := node.Labels[tsc.TopologyKey]
		if !ok {
			return false
		}
		counts := snap.domainCounts(tsc.TopologyKey, tsc.LabelSelector)
		if (counts[domain]+1)-minOverUniverse(counts, universe[tsc.TopologyKey]) > tsc.MaxSkew {
			return false
		}
	}
	return true
}

// minOverUniverse returns the minimum count across the full domain universe
// (absent domains count 0); falls back to min over seen counts if universe empty.
func minOverUniverse(counts map[string]int32, domains []string) int32 {
	if len(domains) == 0 {
		return minDomainCount(counts)
	}
	min := int32(0)
	first := true
	for _, d := range domains {
		c := counts[d]
		if first || c < min {
			min, first = c, false
		}
	}
	return min
}

// recordTopology collapses a winning potential node to a single domain per spread
// key — the least-loaded reachable valid domain — and records it in `domains` (for
// snap.AddPod), pinning the superposition's requirement to it. This is the "record
// only on collapse" step: a potential node is one node, so it must commit to one
// domain, and that is when its placement counts. Idempotent across pods on the
// same node via `domains`.
func recordTopology(pod *v1.Pod, pn *virtualnode.PotentialNode, snap *view, domains map[string]string) {
	for _, tsc := range pod.Spec.TopologySpreadConstraints {
		if _, already := domains[tsc.TopologyKey]; already {
			continue
		}
		counts := snap.domainCounts(tsc.TopologyKey, tsc.LabelSelector)
		var chosen string
		var chosenCount int32
		first := true
		for _, d := range possibleDomains(pn, tsc.TopologyKey) {
			c := counts[d]
			if first || c < chosenCount {
				chosen, chosenCount, first = d, c, false
			}
		}
		if chosen != "" {
			domains[tsc.TopologyKey] = chosen
			pn.Requirements[tsc.TopologyKey] = capacity.NewRequirement(
				tsc.TopologyKey, v1.NodeSelectorOpIn, chosen)
		}
	}
}

func minDomainCount(counts map[string]int32) int32 {
	min := int32(0)
	first := true
	for _, c := range counts {
		if first || c < min {
			min, first = c, false
		}
	}
	return min
}
