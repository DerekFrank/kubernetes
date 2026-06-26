package schedule

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

// Snapshot is an IMMUTABLE cluster base: the concrete nodes and the pods placed
// on them. It carries no per-solve working state and is never mutated after
// construction, so it can be shared by pointer across concurrent solves (a live
// provision loop, a long-running consolidation sim) and, eventually, held by a
// Cluster behind an atomic pointer and swapped on watch events (RCU).
//
// Derived state (topology domain counts) is computed over the base plus a solve's
// overlay — see view. The base never holds it mutably, which is what keeps the
// base safe to share.
type Snapshot struct {
	nodes      []fwk.NodeInfo
	placements []placement
	// requested is the per-node total resource request of base placements,
	// precomputed once (the base is immutable). Read-only, shared across views.
	requested map[string]resourceTotal
}

// resourceTotal is a maintained per-node sum of pod requests, so resource-fit
// checks are O(1) instead of re-scanning all placements.
type resourceTotal struct {
	cpuMillis int64
	memBytes  int64
}

func podRequestTotal(pod *v1.Pod) resourceTotal {
	var t resourceTotal
	for _, c := range pod.Spec.Containers {
		if cpu, ok := c.Resources.Requests[v1.ResourceCPU]; ok {
			t.cpuMillis += cpu.MilliValue()
		}
		if mem, ok := c.Resources.Requests[v1.ResourceMemory]; ok {
			t.memBytes += mem.Value()
		}
	}
	return t
}

// placement is one scheduled pod together with the labels of the node it sits on
// (its topology domains) and that node's name.
type placement struct {
	pod      *v1.Pod
	nodeName string
	domains  map[string]string // node labels: topologyKey → domain value
}

// NewSnapshot builds an immutable base from concrete nodes, seeding placements
// from the pods already on each node. This is the O(nodes) construction cost;
// once built the base is reused read-only.
func NewSnapshot(nodes []fwk.NodeInfo) *Snapshot {
	s := &Snapshot{
		nodes:     append([]fwk.NodeInfo(nil), nodes...),
		requested: map[string]resourceTotal{},
	}
	for _, ni := range nodes {
		node := ni.Node()
		if node == nil {
			continue
		}
		for _, pi := range ni.GetPods() {
			pod := pi.GetPod()
			s.placements = append(s.placements, placement{
				pod:      pod,
				nodeName: node.Name,
				domains:  node.Labels,
			})
			t := s.requested[node.Name]
			pt := podRequestTotal(pod)
			t.cpuMillis += pt.cpuMillis
			t.memBytes += pt.memBytes
			s.requested[node.Name] = t
		}
	}
	return s
}

// view is per-solve working state layered over an immutable base Snapshot via
// copy-on-write. It records only this solve's deltas — tentative placements
// (additive), removed pods, and masked nodes (subtractive, e.g. a Deschedule
// simulation) — so nothing per-call copies the whole cluster, and the shared base
// is never touched. Reads compose base + overlay. Discarded when the solve ends.
type view struct {
	base    *Snapshot
	added   []placement         // tentative placements made during the solve
	removed map[string]struct{} // pod keys (ns/name) masked out of the base
	masked  map[string]struct{} // node names removed from the view
	// addedRequested is the per-node total of tentative placements, maintained
	// incrementally in AddPod so resource-fit checks stay O(1).
	addedRequested map[string]resourceTotal
}

func newView(base *Snapshot) *view {
	return &view{base: base, addedRequested: map[string]resourceTotal{}}
}

func podKey(namespace, name string) string { return namespace + "/" + name }

// Nodes returns the concrete nodes visible to the solve (base minus masked).
func (v *view) Nodes() []fwk.NodeInfo {
	if len(v.masked) == 0 {
		return v.base.nodes
	}
	out := make([]fwk.NodeInfo, 0, len(v.base.nodes))
	for _, ni := range v.base.nodes {
		if ni.Node() == nil {
			continue
		}
		if _, drop := v.masked[ni.Node().Name]; drop {
			continue
		}
		out = append(out, ni)
	}
	return out
}

// AddPod records a tentative placement in the overlay (base untouched) and
// maintains the per-node requested total.
func (v *view) AddPod(pod *v1.Pod, nodeName string, domains map[string]string) {
	v.added = append(v.added, placement{pod: pod, nodeName: nodeName, domains: domains})
	if v.addedRequested == nil {
		v.addedRequested = map[string]resourceTotal{}
	}
	t := v.addedRequested[nodeName]
	pt := podRequestTotal(pod)
	t.cpuMillis += pt.cpuMillis
	t.memBytes += pt.memBytes
	v.addedRequested[nodeName] = t
}

// RemovePod masks a pod out of the view by key (base untouched). Affects both
// base placements and overlay-added ones.
func (v *view) RemovePod(namespace, name string) {
	if v.removed == nil {
		v.removed = map[string]struct{}{}
	}
	v.removed[podKey(namespace, name)] = struct{}{}
}

// withoutNodes returns a new view over the SAME base with the named nodes masked
// and their pods removed from the derived state, plus the pods that were on those
// nodes (the displaced set). O(masked + their pods), not O(cluster): the base is
// shared, only the small mask grows. This is what Deschedule reschedules against.
func (v *view) withoutNodes(names ...string) (*view, []*v1.Pod) {
	nv := &view{
		base:           v.base,
		added:          v.added,
		removed:        cloneSet(v.removed),
		masked:         cloneSet(v.masked),
		addedRequested: v.addedRequested,
	}
	if nv.masked == nil {
		nv.masked = map[string]struct{}{}
	}
	for _, n := range names {
		nv.masked[n] = struct{}{}
	}

	var displaced []*v1.Pod
	for _, p := range v.allPlacements() {
		for _, n := range names {
			if p.nodeName == n {
				displaced = append(displaced, p.pod)
				break
			}
		}
	}
	return nv, displaced
}

// allPlacements returns base + overlay placements that are still present (not
// removed, not on a masked node).
func (v *view) allPlacements() []placement {
	out := make([]placement, 0, len(v.base.placements)+len(v.added))
	for _, p := range v.base.placements {
		if v.isPresent(p) {
			out = append(out, p)
		}
	}
	for _, p := range v.added {
		if v.isPresent(p) {
			out = append(out, p)
		}
	}
	return out
}

func (v *view) isPresent(p placement) bool {
	if _, gone := v.masked[p.nodeName]; gone {
		return false
	}
	if _, gone := v.removed[podKey(p.pod.Namespace, p.pod.Name)]; gone {
		return false
	}
	return true
}

// domainCounts returns, for a topology key, the count of present pods matching
// `selector` in each domain value — composed over base + overlay.
func (v *view) domainCounts(topologyKey string, selector *metav1.LabelSelector) map[string]int32 {
	counts := map[string]int32{}
	count := func(p placement) {
		domain, ok := p.domains[topologyKey]
		if !ok || !matchesSelector(p.pod, selector) {
			return
		}
		counts[domain]++
	}
	for _, p := range v.base.placements {
		if v.isPresent(p) {
			count(p)
		}
	}
	for _, p := range v.added {
		if v.isPresent(p) {
			count(p)
		}
	}
	return counts
}

// requestedOn returns the total CPU (millis) and memory (bytes) requested on a
// node: the base's precomputed per-node total plus this view's tentative-placement
// delta. O(1) — both totals are maintained, never re-scanned. Resource accounting
// lives in the view/base totals, so the immutable base NodeInfo is never mutated.
//
// This is correct for the hot path (concrete candidate nodes, which are never
// masked when queried and against which pods are only added, not removed in-solve).
func (v *view) requestedOn(nodeName string) (cpuMillis, memBytes int64) {
	bt := v.base.requested[nodeName]
	at := v.addedRequested[nodeName]
	return bt.cpuMillis + at.cpuMillis, bt.memBytes + at.memBytes
}

func cloneSet(s map[string]struct{}) map[string]struct{} {
	if s == nil {
		return nil
	}
	out := make(map[string]struct{}, len(s))
	for k := range s {
		out[k] = struct{}{}
	}
	return out
}

// matchesSelector reports whether a pod matches a topology spread label selector.
// A nil selector matches nothing; empty MatchLabels matches all.
func matchesSelector(pod *v1.Pod, selector *metav1.LabelSelector) bool {
	if selector == nil {
		return false
	}
	for k, val := range selector.MatchLabels {
		if pod.Labels[k] != val {
			return false
		}
	}
	return true
}
