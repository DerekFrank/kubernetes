package schedule

import (
	"sort"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

// preemptionOption is a feasible preemption: evicting `victims` from `nodeName`
// makes `pod` fit there. There is deliberately NO cost field — the waterfall
// (Options.Waterfall) decides preempt-vs-provision by fixed rung order, not by
// pricing one against the other. We concede that preempt and provision cost the
// same node in the contested case; the difference is latency + availability, which
// no cardinal $/hr number can honestly capture (see the design doc). So the
// scheduler does not try. Within the preemption rung we still pick the LEAST
// disruptive feasible option (fewest victims, lowest priority) — an ordinal choice
// that needs no exchange rate.
type preemptionOption struct {
	nodeName string
	victims  []*v1.Pod
}

// podPriority returns a pod's priority (0 if unset), used to decide who may
// preempt whom.
func podPriority(pod *v1.Pod) int32 {
	if pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	return 0
}

// findPreemptionOption finds a feasible way to fit `pod` on some existing node by
// evicting strictly-lower-priority pods, choosing the least-disruptive option
// (fewest victims; ties broken by lowest total victim priority). Returns
// (option, true) if any node can be made to fit. It does not price the option —
// the caller places it in the waterfall relative to provisioning.
//
// Victim eligibility is priority-gated (only strictly-lower-priority pods), which
// is how Kubernetes already scopes preemption; a production version would also
// honor PodDisruptionBudgets and preemptionPolicy (see design doc — the victim's
// availability guards, orthogonal to the preempt-vs-provision ordering).
func findPreemptionOption(snap *view, pod *v1.Pod) (preemptionOption, bool) {
	best := preemptionOption{}
	found := false
	bestScore := struct {
		count int
		prio  int64
	}{}

	prio := podPriority(pod)
	for _, ni := range snap.Nodes() {
		node := ni.Node()
		if node == nil {
			continue
		}

		// Candidate victims: pods on this node with strictly lower priority than
		// the preemptor, sorted lowest-priority-first so we evict the least
		// important pods needed to make room (mirrors kube-scheduler's ordering).
		var victims []fwk.PodInfo
		for _, pi := range ni.GetPods() {
			if podPriority(pi.GetPod()) < prio {
				victims = append(victims, pi)
			}
		}
		if len(victims) == 0 {
			continue
		}
		sort.Slice(victims, func(i, j int) bool {
			return podPriority(victims[i].GetPod()) < podPriority(victims[j].GetPod())
		})

		// Evict victims lowest-priority-first until `pod` fits on this node (by the
		// view's resource accounting). Collect exactly the victims required.
		evicted := preemptionView(snap)
		var chosen []*v1.Pod
		var prioSum int64
		for _, pi := range victims {
			if resourceFitsConcrete(pod, ni, evicted) {
				break
			}
			vp := pi.GetPod()
			evicted.evictFromNode(vp, node.Name)
			chosen = append(chosen, vp)
			prioSum += int64(podPriority(vp))
		}
		if !resourceFitsConcrete(pod, ni, evicted) {
			continue // even evicting all lower-pri pods doesn't make room
		}

		// Least-disruptive wins: fewest victims, then lowest total victim priority.
		if !found || len(chosen) < bestScore.count ||
			(len(chosen) == bestScore.count && prioSum < bestScore.prio) {
			best = preemptionOption{nodeName: node.Name, victims: chosen}
			bestScore.count, bestScore.prio = len(chosen), prioSum
			found = true
		}
	}
	return best, found
}

// preemptionView clones the current view so victim eviction (probing whether a
// pod fits after eviction) doesn't mutate the caller's working state.
func preemptionView(v *view) *view {
	nv := &view{
		base:             v.base,
		added:            append([]placement(nil), v.added...),
		removed:          cloneSet(v.removed),
		masked:           cloneSet(v.masked),
		addedRequested:   map[string]resourceTotal{},
		removedRequested: map[string]resourceTotal{},
	}
	for k, t := range v.addedRequested {
		nv.addedRequested[k] = t
	}
	for k, t := range v.removedRequested {
		nv.removedRequested[k] = t
	}
	return nv
}
