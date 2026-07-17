package solver

import (
	"sort"

	v1 "k8s.io/api/core/v1"

	"k8s.io/kubernetes/pkg/scheduler/provisioning/virtualnode"
)

// ILP is a branch-and-bound solver that searches for the minimum-node-count
// assignment (ties broken by cost), rather than committing greedily. It is the
// "exact" comparison point for the portfolio: on small splits it provably matches
// or beats greedy's packing, which is the test of whether greedy leaves nodes on
// the table. It is exponential in the worst case, so it is bounded (MaxPods /
// MaxNodes) and falls back to greedy when the split is too large — an anytime
// solver that never returns a worse answer than greedy.
//
// This is not a general MILP engine; it is a bounded exact bin-packer over the same
// Narrowers/offerings every solver uses. The point is the portfolio contract (fan
// out greedy + ILP, Select the better), not the optimizer's sophistication.
type ILP struct {
	MaxPods  int // above this many pods, defer to greedy (default 12)
	MaxNodes int // cap on nodes the search will open (default = MaxPods)
}

func (ILP) Name() string { return "ilp" }

func (s ILP) Solve(p Problem) []Solution {
	maxPods := s.MaxPods
	if maxPods == 0 {
		maxPods = 12
	}
	if len(p.Pods) > maxPods {
		// Too large for exact search — defer to greedy so we never block or return junk.
		return Greedy{}.Solve(p)
	}
	if p.Topology != nil {
		// Topology spread maintains shared cross-pod state updated on commit. The
		// branch-and-bound search probes placements it later abandons, which would
		// corrupt that state — so defer to greedy (single-pass, commit == kept) when
		// spread is in play. (A topology-aware ILP would need per-branch state clones.)
		return Greedy{}.Solve(p)
	}
	narrowers := p.narrowers()

	// Search largest-first: better bounds earlier (big pods constrain most).
	pods := append([]*v1.Pod(nil), p.Pods...)
	sort.SliceStable(pods, func(i, j int) bool {
		return podCPUMillis(pods[i]) > podCPUMillis(pods[j])
	})

	maxNodes := s.MaxNodes
	if maxNodes == 0 {
		maxNodes = len(pods)
	}

	best := &searchState{bestNodes: maxNodes + 1}
	// The greedy solution is both a fallback and an initial upper bound for pruning.
	greedy := Greedy{}.Solve(p)[0]
	if len(greedy.Unplaced) == 0 {
		best.bestNodes = len(greedy.NodeClaims)
		best.best = &greedy
	}

	var claims []*virtualnode.PotentialNode
	s.branch(pods, 0, claims, narrowers, p, best)

	if best.best == nil {
		// Nothing placed everything within bounds — return greedy (may have unplaced).
		return []Solution{greedy}
	}
	return []Solution{*best.best}
}

type searchState struct {
	bestNodes int
	best      *Solution
}

// branch assigns pods[i:] onto the current set of open claims (or new ones),
// pruning any partial assignment that already uses >= the best full solution's node
// count. On reaching the last pod it finalizes and keeps the solution if it beats
// the incumbent on (node count, then cost).
func (s ILP) branch(pods []*v1.Pod, i int, claims []*virtualnode.PotentialNode, narrowers []virtualnode.Narrower, p Problem, best *searchState) {
	if len(claims) >= best.bestNodes {
		return // prune: already no better than the incumbent
	}
	if i == len(pods) {
		sol := finalize(cloneClaims(claims), nil)
		if len(sol.NodeClaims) < best.bestNodes ||
			(len(sol.NodeClaims) == best.bestNodes && best.best != nil && sol.Cost() < best.best.Cost()) {
			best.bestNodes = len(sol.NodeClaims)
			sol2 := sol
			best.best = &sol2
		}
		return
	}

	pod := pods[i]
	// Option 1: place on each existing claim that accepts it (clone so siblings in
	// the search tree are independent — no shared mutation).
	for idx := range claims {
		trial := cloneClaims(claims)
		if tryAdd(trial[idx], pod, narrowers) {
			s.branch(pods, i+1, trial, narrowers, p, best)
		}
	}
	// Option 2: open a new claim for it.
	claim := NewNodeClaim(pod, p.Offerings)
	if claim != nil && tryAdd(claim, pod, narrowers) {
		trial := append(cloneClaims(claims), claim)
		s.branch(pods, i+1, trial, narrowers, p, best)
	}
	// If neither placed the pod, this branch strands it — we simply don't recurse,
	// so it won't produce a full solution; the greedy fallback covers unplaceable pods.
}

// cloneClaims deep-copies the claim set so a branch can mutate its own copies
// without disturbing sibling branches (the search explores alternatives in parallel
// conceptually; each path needs isolated claim state).
func cloneClaims(claims []*virtualnode.PotentialNode) []*virtualnode.PotentialNode {
	out := make([]*virtualnode.PotentialNode, len(claims))
	for i, c := range claims {
		out[i] = c.Clone()
	}
	return out
}
