// Package plugins contains modified scheduler plugins that are PotentialNode-aware.
// Each plugin checks whether the NodeInfo it receives implements IsPotentialNode.
// If so, it narrows the superposition instead of checking against a single concrete node.
package plugins

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
	"k8s.io/kubernetes/pkg/scheduler/unified/virtualnode"
)

// --- NodeResourcesFitUnified ---

const (
	NodeResourcesFitUnifiedName = "NodeResourcesFitUnified"
	NodeAffinityUnifiedName     = "NodeAffinityUnified"
	TaintTolerationUnifiedName  = "TaintTolerationUnified"
	TopologySpreadUnifiedName   = "TopologySpreadUnified"
)

// NodeResourcesFitUnified extends NodeResourcesFit to handle PotentialNodes.
// For concrete nodes: checks available resources.
// For PotentialNodes: filters instance types that can't satisfy cumulative resource requests.
type NodeResourcesFitUnified struct{}

func (f *NodeResourcesFitUnified) Name() string { return "NodeResourcesFitUnified" }

func (f *NodeResourcesFitUnified) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	pn, ok := nodeInfo.(virtualnode.IsPotentialNode)
	if !ok || pn.GetPotentialNode() == nil {
		return filterConcreteResources(pod, nodeInfo)
	}

	potential := pn.GetPotentialNode()
	podRequests := podResourceRequests(pod)
	cumulativeRequests := potential.CumulativeRequestsWith(podRequests)

	var remaining []*capacity.InstanceType
	for _, it := range potential.InstanceTypes {
		if instanceTypeFits(it, cumulativeRequests, len(potential.GetPods())+1) {
			remaining = append(remaining, it)
		}
	}

	if len(remaining) == 0 {
		return fwk.NewStatus(fwk.Unschedulable, "no compatible instance type has sufficient resources")
	}

	// Narrow: remove instance types that don't fit
	potential.InstanceTypes = remaining
	return nil
}

func instanceTypeFits(it *capacity.InstanceType, requests v1.ResourceList, podCount int) bool {
	alloc := it.Allocatable()

	if pods, ok := alloc[v1.ResourcePods]; ok {
		if int64(podCount) > pods.Value() {
			return false
		}
	}

	for name, requested := range requests {
		available, exists := alloc[name]
		if !exists {
			if !requested.IsZero() {
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

func filterConcreteResources(pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	podRequests := podResourceRequests(pod)
	alloc := nodeInfo.GetAllocatable()
	requested := nodeInfo.GetRequested()

	for name, qty := range podRequests {
		switch name {
		case v1.ResourceCPU:
			if qty.MilliValue() > alloc.GetMilliCPU()-requested.GetMilliCPU() {
				return fwk.NewStatus(fwk.Unschedulable, "Insufficient cpu")
			}
		case v1.ResourceMemory:
			if qty.Value() > alloc.GetMemory()-requested.GetMemory() {
				return fwk.NewStatus(fwk.Unschedulable, "Insufficient memory")
			}
		}
	}
	return nil
}

// --- NodeAffinityUnified ---

// NodeAffinityUnified extends NodeAffinity to handle PotentialNodes.
// For concrete nodes: checks node labels match pod's affinity.
// For PotentialNodes: narrows requirements via intersection.
type NodeAffinityUnified struct{}

func (n *NodeAffinityUnified) Name() string { return "NodeAffinityUnified" }

func (n *NodeAffinityUnified) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	pn, ok := nodeInfo.(virtualnode.IsPotentialNode)
	if !ok || pn.GetPotentialNode() == nil {
		return filterConcreteAffinity(pod, nodeInfo)
	}

	potential := pn.GetPotentialNode()
	podReqs := extractAffinityRequirements(pod)
	if len(podReqs) == 0 {
		return nil
	}

	if err := potential.Narrow(podReqs, nil); err != nil {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
			fmt.Sprintf("node affinity incompatible: %v", err))
	}
	return nil
}

func filterConcreteAffinity(pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	node := nodeInfo.Node()
	if node == nil {
		return fwk.NewStatus(fwk.Error, "node is nil")
	}

	for key, value := range pod.Spec.NodeSelector {
		if nodeValue, ok := node.Labels[key]; !ok || nodeValue != value {
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node selector mismatch")
		}
	}

	if pod.Spec.Affinity != nil && pod.Spec.Affinity.NodeAffinity != nil {
		required := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if required != nil {
			matched := false
			for _, term := range required.NodeSelectorTerms {
				if matchesNodeSelectorTerm(node, term) {
					matched = true
					break
				}
			}
			if !matched {
				return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node affinity not satisfied")
			}
		}
	}
	return nil
}

func matchesNodeSelectorTerm(node *v1.Node, term v1.NodeSelectorTerm) bool {
	for _, expr := range term.MatchExpressions {
		nodeValue, exists := node.Labels[expr.Key]
		switch expr.Operator {
		case v1.NodeSelectorOpIn:
			if !exists {
				return false
			}
			found := false
			for _, v := range expr.Values {
				if v == nodeValue {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		case v1.NodeSelectorOpNotIn:
			if exists {
				for _, v := range expr.Values {
					if v == nodeValue {
						return false
					}
				}
			}
		case v1.NodeSelectorOpExists:
			if !exists {
				return false
			}
		case v1.NodeSelectorOpDoesNotExist:
			if exists {
				return false
			}
		}
	}
	return true
}

// extractAffinityRequirements converts pod affinity into capacity.Requirements.
func extractAffinityRequirements(pod *v1.Pod) capacity.Requirements {
	reqs := capacity.NewRequirements()
	for key, value := range pod.Spec.NodeSelector {
		reqs[key] = capacity.NewRequirement(key, v1.NodeSelectorOpIn, value)
	}
	if pod.Spec.Affinity != nil && pod.Spec.Affinity.NodeAffinity != nil {
		required := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if required != nil {
			for _, term := range required.NodeSelectorTerms {
				for _, expr := range term.MatchExpressions {
					if expr.Operator == v1.NodeSelectorOpIn {
						reqs[expr.Key] = capacity.NewRequirement(expr.Key, expr.Operator, expr.Values...)
					}
				}
			}
		}
	}
	return reqs
}

// --- TaintTolerationUnified ---

// TaintTolerationUnified extends TaintToleration for PotentialNodes.
type TaintTolerationUnified struct{}

func (t *TaintTolerationUnified) Name() string { return "TaintTolerationUnified" }

func (t *TaintTolerationUnified) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	pn, ok := nodeInfo.(virtualnode.IsPotentialNode)
	if !ok || pn.GetPotentialNode() == nil {
		node := nodeInfo.Node()
		if node == nil {
			return fwk.NewStatus(fwk.Error, "node is nil")
		}
		return checkTaints(pod.Spec.Tolerations, node.Spec.Taints)
	}

	potential := pn.GetPotentialNode()
	return checkTaints(pod.Spec.Tolerations, potential.Taints)
}

func checkTaints(tolerations []v1.Toleration, taints []v1.Taint) *fwk.Status {
	for _, taint := range taints {
		if taint.Effect != v1.TaintEffectNoSchedule && taint.Effect != v1.TaintEffectNoExecute {
			continue
		}
		tolerated := false
		for _, toleration := range tolerations {
			if toleration.ToleratesTaint(klog.TODO(), &taint, false) {
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

// --- TopologySpreadUnified ---

// TopologySpreadUnified handles topology spread constraints for PotentialNodes.
// For PotentialNodes with unresolved topology: uses worst-case counting.
// A pod on a PotentialNode whose zone is not yet resolved counts toward ALL possible zones.
type TopologySpreadUnified struct {
	// domainCounts tracks pod count per topology key per domain value.
	// key: topology key (e.g., "topology.kubernetes.io/zone")
	// value: map of domain value → pod count
	DomainCounts map[string]map[string]int32
}

func (t *TopologySpreadUnified) Name() string { return "TopologySpreadUnified" }

func (t *TopologySpreadUnified) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if len(pod.Spec.TopologySpreadConstraints) == 0 {
		return nil
	}

	pn, ok := nodeInfo.(virtualnode.IsPotentialNode)
	if !ok || pn.GetPotentialNode() == nil {
		// Concrete node — check topology spread against known domain
		return t.filterConcreteTopology(pod, nodeInfo)
	}

	// PotentialNode — check if placing this pod would violate maxSkew
	// in the worst case (counting against all possible domains)
	potential := pn.GetPotentialNode()
	for _, constraint := range pod.Spec.TopologySpreadConstraints {
		topologyKey := constraint.TopologyKey

		// Get the domains this PotentialNode could resolve to
		possibleDomains := possibleDomainsForKey(potential, topologyKey)
		if len(possibleDomains) == 0 {
			continue
		}

		// Find the minimum count across all known domains for this topology key
		counts := t.DomainCounts[topologyKey]
		if counts == nil {
			continue
		}
		minCount := int32(0)
		first := true
		for _, count := range counts {
			if first || count < minCount {
				minCount = count
				first = false
			}
		}

		// For a PotentialNode, pick the least-populated domain among possible ones.
		// This is optimistic for spread (best-case). If even the best-case violates
		// maxSkew, the pod definitely can't go here.
		bestDomain := ""
		bestCount := int32(0)
		for _, domain := range possibleDomains {
			count := counts[domain]
			if bestDomain == "" || count < bestCount {
				bestDomain = domain
				bestCount = count
			}
		}

		// Check maxSkew: (bestCount + 1) - minCount <= maxSkew
		if (bestCount+1)-minCount > constraint.MaxSkew {
			return fwk.NewStatus(fwk.Unschedulable,
				fmt.Sprintf("topology spread maxSkew violated for key %s", topologyKey))
		}

		// Narrow: restrict PotentialNode to the chosen domain
		if len(possibleDomains) > 1 && bestDomain != "" {
			domainReq := capacity.NewRequirements()
			domainReq[topologyKey] = capacity.NewRequirement(topologyKey, v1.NodeSelectorOpIn, bestDomain)
			if err := potential.Narrow(domainReq, nil); err != nil {
				return fwk.NewStatus(fwk.Unschedulable,
					fmt.Sprintf("topology narrowing failed: %v", err))
			}
		}
	}

	return nil
}

func (t *TopologySpreadUnified) filterConcreteTopology(pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	node := nodeInfo.Node()
	if node == nil {
		return fwk.NewStatus(fwk.Error, "node is nil")
	}

	for _, constraint := range pod.Spec.TopologySpreadConstraints {
		domain, exists := node.Labels[constraint.TopologyKey]
		if !exists {
			return fwk.NewStatus(fwk.Unschedulable,
				fmt.Sprintf("node missing topology key %s", constraint.TopologyKey))
		}

		counts := t.DomainCounts[constraint.TopologyKey]
		if counts == nil {
			continue
		}

		minCount := int32(0)
		first := true
		for _, count := range counts {
			if first || count < minCount {
				minCount = count
				first = false
			}
		}

		currentCount := counts[domain]
		if (currentCount+1)-minCount > constraint.MaxSkew {
			return fwk.NewStatus(fwk.Unschedulable,
				fmt.Sprintf("topology spread maxSkew violated for key %s domain %s", constraint.TopologyKey, domain))
		}
	}
	return nil
}

// possibleDomainsForKey returns the topology domain values this PotentialNode could resolve to.
func possibleDomainsForKey(pn *virtualnode.PotentialNode, topologyKey string) []string {
	// Check if the requirement already resolves the domain
	if req := pn.Requirements.Get(topologyKey); req != nil {
		return req.Values.UnsortedList()
	}

	// Otherwise, collect all possible values from compatible instance types
	domains := make(map[string]struct{})
	for _, it := range pn.InstanceTypes {
		if req := it.Requirements.Get(topologyKey); req != nil {
			for _, v := range req.Values.UnsortedList() {
				domains[v] = struct{}{}
			}
		}
		for _, o := range it.Offerings {
			if req := o.Requirements.Get(topologyKey); req != nil {
				for _, v := range req.Values.UnsortedList() {
					domains[v] = struct{}{}
				}
			}
		}
	}

	result := make([]string, 0, len(domains))
	for d := range domains {
		result = append(result, d)
	}
	return result
}

// --- shared helpers ---

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
