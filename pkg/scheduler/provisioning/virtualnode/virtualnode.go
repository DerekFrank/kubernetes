// Package virtualnode implements PotentialNode — a NodeInfo variant that represents
// potential capacity as a superposition of compatible instance types.
//
// PotentialNode does NOT flow through the concrete Filter/Score plugin pipeline. The
// bind path filters concrete nodes with stock, unmodified kube-scheduler plugins; the
// provisioning path narrows this superposition directly via NarrowForPod, off the
// framework entirely (a PotentialNode is not a concrete NodeInfo, and the narrowing
// that matters — constraint intersection over the instance-type catalog — was never
// kube-scheduler plugin logic). This is the D19 split: no plugin fork, no
// divergence-from-upstream debt. A PotentialNode is a mutable superposition until it
// fires and an immutable commitment after (see fire.go).
package virtualnode

import (
	"fmt"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/provisioning/capacity"
)

var nodeCounter int64

// IsPotentialNode lets code holding a fwk.NodeInfo recover the underlying
// PotentialNode (potential, not-yet-provisioned capacity) via a type assertion.
// Under the D19 split the concrete Filter/Score plugins never see a PotentialNode —
// they only ever run against real nodes — so this is used by the provisioning path
// and callers inspecting a mixed node set, NOT by "NodeClaim-aware" forked plugins
// (there are none).
type IsPotentialNode interface {
	GetPotentialNode() *PotentialNode
}

// PotentialNode implements fwk.NodeInfo as a superposition of compatible instance types.
// It narrows as pods are added and plugins filter incompatible types.
type PotentialNode struct {
	// Requirements is the accumulated constraint set.
	Requirements capacity.Requirements

	// InstanceTypes still compatible after narrowing.
	InstanceTypes []*capacity.InstanceType

	// Taints from the NodePool spec.
	Taints []v1.Taint

	pods           []fwk.PodInfo
	requested      v1.ResourceList // running sum of placed pods' requests (maintained in AddPodInfo)
	hostname       string
	generation     int64
	representative *v1.Node // lazily computed

	// fired records whether this claim has crossed the fire boundary (handed to the
	// cloud provider). Pre-fire the claim is a mutable superposition; post-fire it is
	// an immutable commitment — Narrow/UnNarrow are rejected. See fire.go.
	fired bool
	// birthReqs / birthTypes are the widest (⊤) requirement set and instance-type
	// catalog the claim was constructed from, captured at New(). UnNarrow recomputes
	// the claim from its surviving members by rebuilding from this birth ⊤ and
	// re-narrowing each remaining member — narrowing is lossy (monotone intersection),
	// so the only correct un-narrow is recompute-from-members, not per-pod subtraction.
	birthReqs  capacity.Requirements
	birthTypes []*capacity.InstanceType
}

// New creates a PotentialNode from base requirements and compatible instance types.
func New(baseReqs capacity.Requirements, instanceTypes []*capacity.InstanceType, taints []v1.Taint) *PotentialNode {
	id := atomic.AddInt64(&nodeCounter, 1)
	hostname := fmt.Sprintf("potential-%04d", id)

	reqs := capacity.NewRequirements()
	reqs.Add(baseReqs)
	reqs[v1.LabelHostname] = capacity.NewRequirement(v1.LabelHostname, v1.NodeSelectorOpIn, hostname)

	// Capture the birth ⊤ (widest requirements + full catalog) so UnNarrow can
	// recompute the claim from its surviving members. birthReqs is a deep copy so
	// later narrowing of pn.Requirements can't mutate it; birthTypes shares the
	// (immutable) instance-type pointers.
	birthReqs := capacity.NewRequirements()
	birthReqs.Add(reqs)

	return &PotentialNode{
		Requirements:  reqs,
		InstanceTypes: instanceTypes,
		Taints:        taints,
		hostname:      hostname,
		birthReqs:     birthReqs,
		birthTypes:    append([]*capacity.InstanceType(nil), instanceTypes...),
	}
}

// --- IsPotentialNode ---

func (pn *PotentialNode) GetPotentialNode() *PotentialNode { return pn }

// --- fwk.NodeInfo interface ---

func (pn *PotentialNode) Node() *v1.Node {
	if pn.representative == nil {
		pn.representative = pn.buildRepresentative()
	}
	return pn.representative
}

func (pn *PotentialNode) GetPods() []fwk.PodInfo                            { return pn.pods }
func (pn *PotentialNode) GetPodsWithAffinity() []fwk.PodInfo                { return nil }
func (pn *PotentialNode) GetPodsWithRequiredAntiAffinity() []fwk.PodInfo    { return nil }
func (pn *PotentialNode) GetUsedPorts() fwk.HostPortInfo                    { return make(fwk.HostPortInfo) }
func (pn *PotentialNode) GetImageStates() map[string]*fwk.ImageStateSummary { return nil }
func (pn *PotentialNode) GetPVCRefCounts() map[string]int                   { return nil }
func (pn *PotentialNode) GetGeneration() int64                              { return pn.generation }
func (pn *PotentialNode) GetNodeDeclaredFeatures() ndf.FeatureSet           { return ndf.FeatureSet{} }
func (pn *PotentialNode) String() string                                    { return fmt.Sprintf("PotentialNode(%s)", pn.hostname) }

func (pn *PotentialNode) GetNodeAllocatableDRAClaimState() map[types.NamespacedName]*fwk.NodeAllocatableDRAClaimState {
	return nil
}

func (pn *PotentialNode) GetRequested() fwk.Resource        { return &zeroResource{} }
func (pn *PotentialNode) GetNonZeroRequested() fwk.Resource { return &zeroResource{} }

// GetAllocatable returns the MAXIMUM allocatable across compatible types.
// PotentialNode-aware plugins ignore this and check per-type directly.
// Non-aware plugins see the most generous capacity (optimistic fallback).
func (pn *PotentialNode) GetAllocatable() fwk.Resource {
	return &maxAllocatable{types: pn.InstanceTypes}
}

func (pn *PotentialNode) Snapshot() fwk.NodeInfo { return pn }

// Clone returns a deep-enough copy for independent narrowing: the requirements map
// and instance-type/pod slices are copied so a caller can narrow the clone without
// mutating the original. Instance types and pods themselves are shared by pointer
// (they are immutable within a solve). Used by search solvers (ILP) that explore
// sibling assignments from a common prefix without cross-contamination.
func (pn *PotentialNode) Clone() *PotentialNode {
	reqs := capacity.NewRequirements()
	for k, r := range pn.Requirements {
		reqs[k] = r.Copy()
	}
	types := append([]*capacity.InstanceType(nil), pn.InstanceTypes...)
	pods := append([]fwk.PodInfo(nil), pn.pods...)
	requested := v1.ResourceList{}
	for name, qty := range pn.requested {
		requested[name] = qty.DeepCopy()
	}
	return &PotentialNode{
		Requirements:  reqs,
		InstanceTypes: types,
		Taints:        pn.Taints,
		pods:          pods,
		requested:     requested,
		hostname:      pn.hostname,
		generation:    pn.generation,
		fired:         pn.fired,
		birthReqs:     pn.birthReqs,
		birthTypes:    pn.birthTypes,
	}
}

func (pn *PotentialNode) AddPodInfo(podInfo fwk.PodInfo) {
	pn.pods = append(pn.pods, podInfo)
	if pn.requested == nil {
		pn.requested = v1.ResourceList{}
	}
	for _, c := range podInfo.GetPod().Spec.Containers {
		for name, qty := range c.Resources.Requests {
			existing := pn.requested[name]
			existing.Add(qty)
			pn.requested[name] = existing
		}
	}
	pn.generation++
	pn.representative = nil
}

// AddPod records a pod on the claim via a minimal PodInfo wrapper. It lets callers
// that don't build framework PodInfos (e.g. the pure solver package) place a pod
// without depending on the scheduler framework.
func (pn *PotentialNode) AddPod(pod *v1.Pod) {
	pn.AddPodInfo(minimalPodInfo{pod: pod})
}

// minimalPodInfo is a bare fwk.PodInfo wrapping a pod. The provisioning solver only
// ever reads GetPod(); the precomputed-affinity accessors return empty because
// affinity narrowing is handled by Narrowers, not read off PodInfo here.
type minimalPodInfo struct{ pod *v1.Pod }

func (m minimalPodInfo) GetPod() *v1.Pod                                  { return m.pod }
func (m minimalPodInfo) GetRequiredAffinityTerms() []fwk.AffinityTerm     { return nil }
func (m minimalPodInfo) GetRequiredAntiAffinityTerms() []fwk.AffinityTerm { return nil }
func (m minimalPodInfo) GetPreferredAffinityTerms() []fwk.WeightedAffinityTerm {
	return nil
}
func (m minimalPodInfo) GetPreferredAntiAffinityTerms() []fwk.WeightedAffinityTerm {
	return nil
}
func (m minimalPodInfo) CalculateResource() fwk.PodResource { return fwk.PodResource{} }

func (pn *PotentialNode) RemovePod(logger klog.Logger, pod *v1.Pod) error {
	for i, p := range pn.pods {
		if p.GetPod().UID == pod.UID {
			pn.pods = append(pn.pods[:i], pn.pods[i+1:]...)
			for _, c := range pod.Spec.Containers {
				for name, qty := range c.Resources.Requests {
					if existing, ok := pn.requested[name]; ok {
						existing.Sub(qty)
						pn.requested[name] = existing
					}
				}
			}
			pn.generation++
			pn.representative = nil
			return nil
		}
	}
	return fmt.Errorf("pod %s not found on %s", pod.Name, pn.hostname)
}

func (pn *PotentialNode) SetNode(node *v1.Node) {}

// --- PotentialNode-specific methods ---

// Hostname returns the stable placeholder hostname.
func (pn *PotentialNode) Hostname() string { return pn.hostname }

// Narrower is the constraint plugin surface for provisioning (the Filter-analog).
// Each Narrower prunes a PotentialNode's option set by one constraint. Narrowers
// are CONJUNCTIVE — the solver applies all registered hard Narrowers, intersected —
// which is why removing a constraint (e.g. topology) is just "don't register its
// Narrower": the claim stays wider, nothing else changes. This is the ecosystem
// extensibility surface; solvers consult it, they do not contain the constraints.
type Narrower interface {
	Name() string
	// Narrow prunes claim by this constraint for pod. A non-success Status means
	// the pod cannot join the claim under this constraint (e.g. the option set went
	// empty, or a taint is untolerated).
	Narrow(pod *v1.Pod, claim *PotentialNode) *fwk.Status
	// Hard reports whether the solver MUST apply this Narrower (true) or MAY apply
	// it, deciding by what the narrowing costs against the offerings (false — a soft
	// preference). Soft Narrowers are how "narrow-on-score" works with no Score term.
	Hard() bool
}

// DefaultNarrowers is the built-in hard-constraint set every solver applies. There
// is ONE Narrower per corresponding kube-scheduler Filter plugin — the Narrow
// surface is the provisioning-side analog of Filter, so it mirrors those plugins
// one-to-one (same names, same semantics) rather than fusing them:
//
//	kube-scheduler Filter   →   Narrower
//	  TaintToleration       →   TaintTolerationNarrower
//	  NodeAffinity          →   NodeAffinityNarrower
//	  NodeResourcesFit      →   NodeResourcesFitNarrower
//
// Registering more (e.g. a topology Narrower) extends the scheduler; omitting one
// relaxes that constraint fleet-wide (the "delete the Filter plugin" property).
func DefaultNarrowers() []Narrower {
	return []Narrower{
		TaintTolerationNarrower{},
		NodeAffinityNarrower{},
		NodeResourcesFitNarrower{},
	}
}

// NarrowForPod applies the given Narrowers to the superposition in order (hard ones
// unconditionally; soft ones are left to the solver, so this only applies hard).
// It is the provisioning-side analog of running the concrete Filter plugins, but it
// operates on the superposition directly (no framework — a PotentialNode is not a
// concrete NodeInfo). With no Narrowers passed, it applies DefaultNarrowers.
//
// Returns a non-success Status if any hard Narrower rejects the pod.
func (pn *PotentialNode) NarrowForPod(pod *v1.Pod, narrowers ...Narrower) *fwk.Status {
	if len(narrowers) == 0 {
		narrowers = DefaultNarrowers()
	}
	for _, n := range narrowers {
		if !n.Hard() {
			continue // soft Narrowers are the solver's option, not applied here
		}
		if status := n.Narrow(pod, pn); !status.IsSuccess() {
			return status
		}
	}
	return nil
}

// TaintTolerationNarrower is the Narrow analog of the TaintToleration Filter: it
// rejects a pod that doesn't tolerate the claim's (NodePool) NoSchedule/NoExecute
// taints. The superposition shares one taint set (all a source's offerings carry
// the same NodePool taints), so this rejects the whole claim rather than narrowing.
type TaintTolerationNarrower struct{}

func (TaintTolerationNarrower) Name() string { return "TaintToleration" }
func (TaintTolerationNarrower) Hard() bool   { return true }
func (TaintTolerationNarrower) Narrow(pod *v1.Pod, claim *PotentialNode) *fwk.Status {
	return toleratesTaints(pod.Spec.Tolerations, claim.Taints)
}

// NodeAffinityNarrower is the Narrow analog of the NodeAffinity Filter: it narrows
// the option set by the pod's hard label requirements (nodeSelector + required
// node-affinity In-terms). Where the Filter accepts/rejects one fixed node, the
// Narrower intersects the requirement across the superposition, dropping instance
// types whose labels can't satisfy it.
type NodeAffinityNarrower struct{}

func (NodeAffinityNarrower) Name() string { return "NodeAffinity" }
func (NodeAffinityNarrower) Hard() bool   { return true }
func (NodeAffinityNarrower) Narrow(pod *v1.Pod, claim *PotentialNode) *fwk.Status {
	if err := claim.Narrow(podHardRequirements(pod), nil); err != nil {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, err.Error())
	}
	return nil
}

// NodeResourcesFitNarrower is the Narrow analog of the NodeResourcesFit Filter: it
// drops instance types that can't hold the claim's cumulative resource requests
// (this pod plus those already placed). The Filter checks one node's allocatable;
// the Narrower checks each surviving instance type's allocatable.
type NodeResourcesFitNarrower struct{}

func (NodeResourcesFitNarrower) Name() string { return "NodeResourcesFit" }
func (NodeResourcesFitNarrower) Hard() bool   { return true }
func (NodeResourcesFitNarrower) Narrow(pod *v1.Pod, claim *PotentialNode) *fwk.Status {
	if err := claim.Narrow(nil, podResourceRequests(pod)); err != nil {
		return fwk.NewStatus(fwk.Unschedulable, err.Error())
	}
	return nil
}

// toleratesTaints reports success only if every NoSchedule/NoExecute taint is
// tolerated. Mirrors the taint check the concrete TaintToleration plugin applies.
func toleratesTaints(tolerations []v1.Toleration, taints []v1.Taint) *fwk.Status {
	for i := range taints {
		taint := taints[i]
		if taint.Effect != v1.TaintEffectNoSchedule && taint.Effect != v1.TaintEffectNoExecute {
			continue
		}
		tolerated := false
		for _, tol := range tolerations {
			if tol.ToleratesTaint(klog.TODO(), &taint, false) {
				tolerated = true
				break
			}
		}
		if !tolerated {
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
				fmt.Sprintf("untolerated taint %s=%s:%s", taint.Key, taint.Value, taint.Effect))
		}
	}
	return nil
}

// podHardRequirements extracts a pod's hard label constraints (nodeSelector plus
// requiredDuringScheduling node-affinity In-terms) as capacity.Requirements. Only
// requiredDuringScheduling node-affinity In-terms. All operators (In / NotIn /
// Exists / DoesNotExist / Gt / Lt) are honored now that capacity.Requirement
// represents them — a pod's `arch NotIn [arm64]` narrows the superposition instead
// of being silently dropped. Multiple terms on the same key intersect.
// PodHardRequirements is the exported entry point for extracting a pod's hard label
// constraints as capacity.Requirements (see podHardRequirements). Callers outside
// this package (the solver) use it so there is one extraction implementation.
func PodHardRequirements(pod *v1.Pod) capacity.Requirements {
	return podHardRequirements(pod)
}

func podHardRequirements(pod *v1.Pod) capacity.Requirements {
	reqs := capacity.NewRequirements()
	for key, value := range pod.Spec.NodeSelector {
		addRequirement(reqs, capacity.NewRequirement(key, v1.NodeSelectorOpIn, value))
	}
	if pod.Spec.Affinity != nil && pod.Spec.Affinity.NodeAffinity != nil {
		if required := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution; required != nil {
			// Only the first NodeSelectorTerm is modeled (terms are OR-ed; a single
			// intersecting term is the common case and matches the POC scope).
			for _, term := range firstTerm(required.NodeSelectorTerms) {
				for _, expr := range term.MatchExpressions {
					addRequirement(reqs, capacity.NewRequirement(expr.Key, expr.Operator, expr.Values...))
				}
			}
		}
	}
	return reqs
}

// addRequirement intersects req into reqs under its key (multiple terms on one key
// compose by intersection, matching Karpenter's Requirements.Add).
func addRequirement(reqs capacity.Requirements, req *capacity.Requirement) {
	if existing, ok := reqs[req.Key]; ok {
		reqs[req.Key] = existing.Intersect(req)
	} else {
		reqs[req.Key] = req
	}
}

func firstTerm(terms []v1.NodeSelectorTerm) []v1.NodeSelectorTerm {
	if len(terms) == 0 {
		return nil
	}
	return terms[:1]
}

// podResourceRequests sums a pod's container resource requests.
func podResourceRequests(pod *v1.Pod) v1.ResourceList {
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

// Narrow intersects requirements and filters instance types. It is rejected after
// the claim has fired: a fired claim is an immutable commitment (its size/domain is
// purchased and cannot be un-said), so narrowing it is a programming error, not a
// no-op. See fire.go for the fire boundary.
func (pn *PotentialNode) Narrow(podReqs capacity.Requirements, podRequests v1.ResourceList) error {
	if pn.fired {
		return fmt.Errorf("cannot narrow %s: claim has fired (immutable post-fire)", pn.hostname)
	}
	narrowed := capacity.NewRequirements()
	narrowed.Add(pn.Requirements)
	narrowed.Add(podReqs)

	// Check the intersection is non-empty for all keys
	for _, req := range narrowed {
		if req.Len() == 0 {
			return fmt.Errorf("requirements narrowed to empty set on %s", pn.hostname)
		}
	}

	cumulative := pn.CumulativeRequestsWith(podRequests)
	filtered := FilterByRequirementsAndResources(pn.InstanceTypes, narrowed, cumulative)
	if len(filtered) == 0 {
		return fmt.Errorf("no instance types remain after narrowing on %s", pn.hostname)
	}

	pn.Requirements = narrowed
	pn.InstanceTypes = filtered
	pn.generation++
	pn.representative = nil
	return nil
}

// CouldFit is an O(types) capacity-only pre-check: could `additional` CPU/memory
// fit on top of what's already placed, given the claim's MOST-GENEROUS surviving
// instance type? It is a cheap necessary condition (not sufficient — it ignores
// label narrowing and other resources), used to skip the expensive allocating
// Narrow probe for claims that obviously can't accept the pod. Returns true when
// unsure (no CPU/mem request, or no surviving types), so it never wrongly rejects.
func (pn *PotentialNode) CouldFit(additional v1.ResourceList) bool {
	if len(pn.InstanceTypes) == 0 {
		return true // let Narrow decide
	}
	alloc := pn.GetAllocatable()
	if cpu, ok := additional[v1.ResourceCPU]; ok {
		used := pn.requested.Cpu().MilliValue()
		if used+cpu.MilliValue() > alloc.GetMilliCPU() {
			return false
		}
	}
	if mem, ok := additional[v1.ResourceMemory]; ok {
		used := pn.requested.Memory().Value()
		if used+mem.Value() > alloc.GetMemory() {
			return false
		}
	}
	return true
}

// CumulativeRequestsWith returns sum of placed pods' requests + additional. The
// placed-pods sum is maintained incrementally in AddPodInfo (pn.requested), so this
// is O(resources), not O(pods) — a per-probe rescan of every placed pod is what
// made greedy packing superlinear in batch size.
func (pn *PotentialNode) CumulativeRequestsWith(additional v1.ResourceList) v1.ResourceList {
	total := make(v1.ResourceList, len(pn.requested)+len(additional))
	for name, qty := range pn.requested {
		total[name] = qty.DeepCopy()
	}
	for name, qty := range additional {
		existing := total[name]
		existing.Add(qty)
		total[name] = existing
	}
	return total
}

// CheapestPrice returns the lowest EFFECTIVE offering price among compatible,
// available offerings. Effective price folds in performance-value (offering data),
// so this is the number the cost axis compares — no separate cost/score term.
func (pn *PotentialNode) CheapestPrice() float64 {
	cheapest := float64(0)
	first := true
	for _, it := range pn.InstanceTypes {
		for _, o := range it.Offerings {
			if !o.Available {
				continue
			}
			if !pn.Requirements.Compatible(o.Requirements) {
				continue
			}
			if ep := o.EffectivePrice(); first || ep < cheapest {
				cheapest = ep
				first = false
			}
		}
	}
	return cheapest
}

// CheapestInstanceType returns the cheapest compatible instance type.
func (pn *PotentialNode) CheapestInstanceType() *capacity.InstanceType {
	var best *capacity.InstanceType
	bestPrice := float64(0)
	first := true
	for _, it := range pn.InstanceTypes {
		for _, o := range it.Offerings {
			if !o.Available || !pn.Requirements.Compatible(o.Requirements) {
				continue
			}
			if ep := o.EffectivePrice(); first || ep < bestPrice {
				best = it
				bestPrice = ep
				first = false
			}
			break
		}
	}
	return best
}

// --- internal ---

func (pn *PotentialNode) buildRepresentative() *v1.Node {
	best := pn.CheapestInstanceType()
	labels := make(map[string]string)

	if best != nil {
		for key, req := range best.Requirements {
			if req.Len() == 1 {
				labels[key] = req.Any()
			}
		}
	}
	for key, req := range pn.Requirements {
		if req.Len() == 1 {
			labels[key] = req.Any()
		}
	}
	labels[v1.LabelHostname] = pn.hostname

	var allocatable v1.ResourceList
	if best != nil {
		allocatable = best.Allocatable()
	}

	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   pn.hostname,
			Labels: labels,
		},
		Spec: v1.NodeSpec{
			Taints: pn.Taints,
		},
		Status: v1.NodeStatus{
			Allocatable: allocatable,
			Conditions: []v1.NodeCondition{
				{Type: v1.NodeReady, Status: v1.ConditionTrue},
			},
		},
	}
}

// FilterByRequirementsAndResources keeps instance types that satisfy requirements
// and have enough allocatable for the given resource requests.
func FilterByRequirementsAndResources(
	types []*capacity.InstanceType,
	reqs capacity.Requirements,
	requests v1.ResourceList,
) []*capacity.InstanceType {
	var result []*capacity.InstanceType
	for _, it := range types {
		if !reqs.Compatible(it.Requirements) {
			continue
		}
		if !ResourcesFit(it.Allocatable(), requests) {
			continue
		}
		result = append(result, it)
	}
	return result
}

// ResourcesFit checks whether allocatable >= requests for all resources.
func ResourcesFit(allocatable, requests v1.ResourceList) bool {
	for name, requested := range requests {
		available, exists := allocatable[name]
		if !exists {
			if requested.Cmp(resource.Quantity{}) > 0 {
				return false
			}
			continue
		}
		if available.Cmp(requested) < 0 {
			return false
		}
	}
	return true
}

// --- Resource interface helpers ---

type zeroResource struct{}

func (z *zeroResource) GetMilliCPU() int64                            { return 0 }
func (z *zeroResource) GetMemory() int64                              { return 0 }
func (z *zeroResource) GetEphemeralStorage() int64                    { return 0 }
func (z *zeroResource) GetAllowedPodNumber() int                      { return 110 }
func (z *zeroResource) GetScalarResources() map[v1.ResourceName]int64 { return nil }
func (z *zeroResource) SetMaxResource(rl v1.ResourceList)             {}

type maxAllocatable struct {
	types []*capacity.InstanceType
}

func (m *maxAllocatable) GetMilliCPU() int64 {
	var max int64
	for _, it := range m.types {
		if cpu, ok := it.Allocatable()[v1.ResourceCPU]; ok {
			if v := cpu.MilliValue(); v > max {
				max = v
			}
		}
	}
	return max
}

func (m *maxAllocatable) GetMemory() int64 {
	var max int64
	for _, it := range m.types {
		if mem, ok := it.Allocatable()[v1.ResourceMemory]; ok {
			if v := mem.Value(); v > max {
				max = v
			}
		}
	}
	return max
}

func (m *maxAllocatable) GetEphemeralStorage() int64 {
	var max int64
	for _, it := range m.types {
		if es, ok := it.Allocatable()[v1.ResourceEphemeralStorage]; ok {
			if v := es.Value(); v > max {
				max = v
			}
		}
	}
	return max
}

func (m *maxAllocatable) GetAllowedPodNumber() int {
	max := 0
	for _, it := range m.types {
		if pods, ok := it.Allocatable()[v1.ResourcePods]; ok {
			if v := int(pods.Value()); v > max {
				max = v
			}
		}
	}
	if max == 0 {
		return 110
	}
	return max
}

func (m *maxAllocatable) GetScalarResources() map[v1.ResourceName]int64 {
	result := make(map[v1.ResourceName]int64)
	for _, it := range m.types {
		for name, qty := range it.Allocatable() {
			switch name {
			case v1.ResourceCPU, v1.ResourceMemory, v1.ResourceEphemeralStorage, v1.ResourcePods:
				continue
			default:
				if v := qty.Value(); v > result[name] {
					result[name] = v
				}
			}
		}
	}
	return result
}

func (m *maxAllocatable) SetMaxResource(rl v1.ResourceList) {}
