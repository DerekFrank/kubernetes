package solver

import (
	"sort"

	v1 "k8s.io/api/core/v1"

	"k8s.io/kubernetes/pkg/scheduler/unified/virtualnode"
)

// Greedy is the default solver: sort pods largest-first, then for each pod try to
// pack it onto an already-open claim (first that accepts it after narrowing);
// failing that, open a new claim. This is O(pods × claims) and is what Karpenter's
// scheduler and the current inline solve() both do in spirit. It commits each pod
// immediately and never backtracks — fast, good-enough packing, no global optimum.
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
