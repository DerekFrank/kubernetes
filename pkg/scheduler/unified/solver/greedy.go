package solver

import (
	"sort"

	v1 "k8s.io/api/core/v1"

	"k8s.io/kubernetes/pkg/scheduler/unified/virtualnode"
)

// Greedy is the default solver: sort pods largest-first, then for each pod try to
// pack it onto an already-open claim (first that accepts it after narrowing);
// failing that, open a new claim. It commits each pod immediately and never
// backtracks — good-enough packing, no global optimum.
//
// This is a straightforward first-fit, NOT a port of Karpenter's scheduler. It
// probes EVERY open claim per pod, and each probe allocates (Narrow rebuilds the
// requirement set and re-filters the instance-type slice, rolled back on failure),
// so cost is O(pods × open-claims) with per-probe allocation — superlinear as the
// open-claim set grows with the batch. A production greedy bounds the probe frontier
// (first-fit-decreasing over a capacity-indexed subset) and avoids per-probe
// allocation; see BENCHMARK_RESULTS.md.
type Greedy struct{}

func (Greedy) Name() string { return "greedy" }

func (Greedy) Solve(p Problem) []Solution {
	narrowers := p.narrowers()

	pods := append([]*v1.Pod(nil), p.Pods...)
	sort.SliceStable(pods, func(i, j int) bool {
		return podCPUMillis(pods[i]) > podCPUMillis(pods[j])
	})

	var claims []*virtualnode.PotentialNode
	var unplaced []*v1.Pod
	for _, pod := range pods {
		placed := false
		// Try existing open claims first (pack onto in-flight capacity).
		for _, claim := range claims {
			if tryAdd(claim, pod, narrowers) {
				placed = true
				break
			}
		}
		if placed {
			continue
		}
		// Open a fresh claim for this pod.
		claim := NewNodeClaim(pod, p.Offerings)
		if claim == nil || !tryAdd(claim, pod, narrowers) {
			unplaced = append(unplaced, pod)
			continue
		}
		claims = append(claims, claim)
	}
	return []Solution{finalize(claims, unplaced)}
}
