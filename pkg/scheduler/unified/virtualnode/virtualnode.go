// Package virtualnode implements PotentialNode — a NodeInfo variant that represents
// potential capacity as a superposition of compatible instance types.
//
// PotentialNode flows through the same Filter/Score pipeline as concrete nodes.
// Plugins are modified to detect it via the IsPotentialNode interface and operate
// on the superposition (narrowing compatible instance types) rather than checking
// fixed resources/labels.
package virtualnode

import (
	"fmt"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

var nodeCounter int64

// IsPotentialNode is the interface that NodeClaim-aware plugins use to detect
// whether a NodeInfo represents potential (not-yet-provisioned) capacity.
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
	hostname       string
	generation     int64
	representative *v1.Node // lazily computed
}

// New creates a PotentialNode from base requirements and compatible instance types.
func New(baseReqs capacity.Requirements, instanceTypes []*capacity.InstanceType, taints []v1.Taint) *PotentialNode {
	id := atomic.AddInt64(&nodeCounter, 1)
	hostname := fmt.Sprintf("potential-%04d", id)

	reqs := capacity.NewRequirements()
	reqs.Add(baseReqs)
	reqs[v1.LabelHostname] = capacity.NewRequirement(v1.LabelHostname, v1.NodeSelectorOpIn, hostname)

	return &PotentialNode{
		Requirements:  reqs,
		InstanceTypes: instanceTypes,
		Taints:        taints,
		hostname:      hostname,
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

func (pn *PotentialNode) GetPods() []fwk.PodInfo                    { return pn.pods }
func (pn *PotentialNode) GetPodsWithAffinity() []fwk.PodInfo        { return nil }
func (pn *PotentialNode) GetPodsWithRequiredAntiAffinity() []fwk.PodInfo { return nil }
func (pn *PotentialNode) GetUsedPorts() fwk.HostPortInfo            { return make(fwk.HostPortInfo) }
func (pn *PotentialNode) GetImageStates() map[string]*fwk.ImageStateSummary { return nil }
func (pn *PotentialNode) GetPVCRefCounts() map[string]int           { return nil }
func (pn *PotentialNode) GetGeneration() int64                      { return pn.generation }
func (pn *PotentialNode) GetNodeDeclaredFeatures() ndf.FeatureSet   { return ndf.FeatureSet{} }
func (pn *PotentialNode) String() string                            { return fmt.Sprintf("PotentialNode(%s)", pn.hostname) }

func (pn *PotentialNode) GetNodeAllocatableDRAClaimState() map[types.NamespacedName]*fwk.NodeAllocatableDRAClaimState {
	return nil
}

func (pn *PotentialNode) GetRequested() fwk.Resource    { return &zeroResource{} }
func (pn *PotentialNode) GetNonZeroRequested() fwk.Resource { return &zeroResource{} }

// GetAllocatable returns the MAXIMUM allocatable across compatible types.
// PotentialNode-aware plugins ignore this and check per-type directly.
// Non-aware plugins see the most generous capacity (optimistic fallback).
func (pn *PotentialNode) GetAllocatable() fwk.Resource {
	return &maxAllocatable{types: pn.InstanceTypes}
}

func (pn *PotentialNode) Snapshot() fwk.NodeInfo { return pn }

func (pn *PotentialNode) AddPodInfo(podInfo fwk.PodInfo) {
	pn.pods = append(pn.pods, podInfo)
	pn.generation++
	pn.representative = nil
}

func (pn *PotentialNode) RemovePod(logger klog.Logger, pod *v1.Pod) error {
	for i, p := range pn.pods {
		if p.GetPod().UID == pod.UID {
			pn.pods = append(pn.pods[:i], pn.pods[i+1:]...)
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

// Narrow intersects requirements and filters instance types. Called by
// PotentialNode-aware plugins as a side effect of Filter.
func (pn *PotentialNode) Narrow(podReqs capacity.Requirements, podRequests v1.ResourceList) error {
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

// CumulativeRequestsWith returns sum of placed pods' requests + additional.
func (pn *PotentialNode) CumulativeRequestsWith(additional v1.ResourceList) v1.ResourceList {
	total := make(v1.ResourceList)
	for _, pi := range pn.pods {
		for _, c := range pi.GetPod().Spec.Containers {
			for name, qty := range c.Resources.Requests {
				existing := total[name]
				existing.Add(qty)
				total[name] = existing
			}
		}
	}
	for name, qty := range additional {
		existing := total[name]
		existing.Add(qty)
		total[name] = existing
	}
	return total
}

// CheapestPrice returns the lowest offering price among compatible, available offerings.
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
			if first || o.Price < cheapest {
				cheapest = o.Price
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
			if first || o.Price < bestPrice {
				best = it
				bestPrice = o.Price
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
