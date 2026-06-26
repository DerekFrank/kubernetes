package schedule

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

// filterPlugin is the minimal Filter interface our unified plugins satisfy.
// The framework stub (newMarginalCostFramework, schedule_marginalcost_test.go)
// runs a slice of these against each candidate.
type filterPlugin interface {
	Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status
}

// --- Pod / offering / node builders ---

func makePod(name string, cpu, memMi int64) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "main",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(cpu*1000, resource.DecimalSI),
						v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dMi", memMi)),
					},
				},
			}},
		},
	}
}

func makeInstanceType(name string, cpu, memGi int64, zone, capacityType string, price float64) *capacity.InstanceType {
	reqs := capacity.NewRequirements()
	reqs[v1.LabelInstanceTypeStable] = capacity.NewRequirement(v1.LabelInstanceTypeStable, v1.NodeSelectorOpIn, name)
	if zone != "" {
		reqs[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, zone)
	}
	if capacityType != "" {
		reqs["karpenter.sh/capacity-type"] = capacity.NewRequirement("karpenter.sh/capacity-type", v1.NodeSelectorOpIn, capacityType)
	}

	offerings := []*capacity.Offering{{
		Requirements: func() capacity.Requirements {
			r := capacity.NewRequirements()
			if zone != "" {
				r[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, zone)
			}
			if capacityType != "" {
				r["karpenter.sh/capacity-type"] = capacity.NewRequirement("karpenter.sh/capacity-type", v1.NodeSelectorOpIn, capacityType)
			}
			return r
		}(),
		Price:     price,
		Available: true,
	}}

	return &capacity.InstanceType{
		Name:         name,
		Requirements: reqs,
		Offerings:    offerings,
		Capacity: v1.ResourceList{
			v1.ResourceCPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
			v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", memGi)),
			v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
		},
	}
}

func makeMultiZoneInstanceType(name string, cpu, memGi int64, zones []string, capacityType string, price float64) *capacity.InstanceType {
	reqs := capacity.NewRequirements()
	reqs[v1.LabelInstanceTypeStable] = capacity.NewRequirement(v1.LabelInstanceTypeStable, v1.NodeSelectorOpIn, name)
	reqs[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, zones...)
	if capacityType != "" {
		reqs["karpenter.sh/capacity-type"] = capacity.NewRequirement("karpenter.sh/capacity-type", v1.NodeSelectorOpIn, capacityType)
	}

	var offerings []*capacity.Offering
	for _, zone := range zones {
		r := capacity.NewRequirements()
		r[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, zone)
		if capacityType != "" {
			r["karpenter.sh/capacity-type"] = capacity.NewRequirement("karpenter.sh/capacity-type", v1.NodeSelectorOpIn, capacityType)
		}
		offerings = append(offerings, &capacity.Offering{
			Requirements: r,
			Price:        price,
			Available:    true,
		})
	}

	return &capacity.InstanceType{
		Name:         name,
		Requirements: reqs,
		Offerings:    offerings,
		Capacity: v1.ResourceList{
			v1.ResourceCPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
			v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", memGi)),
			v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
		},
	}
}

// --- Integration tests (drive Schedule() directly) ---

// TestBatchPacking: 10 pods each requesting 4 CPU. Only m5.4xlarge (16 CPU) can
// hold a 4-CPU pod; m5.large (2 CPU) cannot fit even one. Greedy packing should
// fill ~3 large instances (4+4+2 pods), not open a node per pod.
func TestBatchPacking(t *testing.T) {
	var pods []*v1.Pod
	for i := 0; i < 10; i++ {
		pods = append(pods, makePod(fmt.Sprintf("pod-%d", i), 4, 1024))
	}

	offerings := []*capacity.InstanceType{
		makeMultiZoneInstanceType("m5.large", 2, 8, []string{"us-west-2a"}, "on-demand", 0.096),
		makeMultiZoneInstanceType("m5.4xlarge", 16, 64, []string{"us-west-2a"}, "on-demand", 0.32),
	}

	input := Input{
		Pods:         pods,
		ClusterState: &ClusterState{Nodes: nil},
		Offerings:    offerings,
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	if len(result.NodeClaims) == 0 {
		t.Fatal("expected NodeClaims, got none")
	}
	if len(result.NodeClaims) > 4 {
		t.Fatalf("expected ~3 NodeClaims (batch packing), got %d", len(result.NodeClaims))
	}

	totalPods := 0
	for _, nc := range result.NodeClaims {
		totalPods += len(nc.Pods)
		t.Logf("  NodeClaim %s: %d pods, cheapest $%.3f", nc.Name, len(nc.Pods), nc.CheapestPrice)
	}
	if totalPods != 10 {
		t.Fatalf("expected 10 pods placed, got %d", totalPods)
	}
	t.Logf("PASS: %d pods packed into %d NodeClaims", totalPods, len(result.NodeClaims))
}

// TestMixedDecisions: an 8-CPU existing node fits 2 of 5 pods (marginal cost 0);
// the rest provision. One call returns both bindings AND NodeClaims.
func TestMixedDecisions(t *testing.T) {
	existingNode := makeConcreteNodeInfo("existing-1", 8, 32, map[string]string{
		v1.LabelTopologyZone:       "us-west-2a",
		v1.LabelInstanceTypeStable: "m5.2xlarge",
	})

	pods := []*v1.Pod{
		makePod("fits-1", 4, 4096),
		makePod("fits-2", 4, 4096),
		makePod("needs-new-1", 4, 4096),
		makePod("needs-new-2", 4, 4096),
		makePod("needs-new-3", 4, 4096),
	}

	offerings := []*capacity.InstanceType{
		makeMultiZoneInstanceType("m5.2xlarge", 8, 32, []string{"us-west-2a"}, "spot", 0.15),
	}

	input := Input{
		Pods:         pods,
		ClusterState: &ClusterState{Nodes: []fwk.NodeInfo{existingNode}},
		Offerings:    offerings,
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	if len(result.Bindings) == 0 {
		t.Fatal("expected some bindings to the free existing node")
	}
	if len(result.NodeClaims) == 0 {
		t.Fatal("expected some NodeClaims for the overflow pods")
	}

	placed := len(result.Bindings)
	for _, nc := range result.NodeClaims {
		placed += len(nc.Pods)
	}
	if placed != 5 {
		t.Fatalf("expected 5 pods placed, got %d (errors: %d)", placed, len(result.Errors))
	}
	t.Logf("PASS: %d bindings + %d NodeClaims from one call", len(result.Bindings), len(result.NodeClaims))
}

// TestRemovalEvaluation (consolidation): node B is removed, its 4 pods become the
// batch. With no offerings, all 4 must rebind to the slack on retained nodes A and C.
func TestRemovalEvaluation(t *testing.T) {
	nodeA := makeConcreteNodeInfoWithPods("node-a", 8, 32,
		map[string]string{v1.LabelTopologyZone: "us-west-2a"},
		[]*v1.Pod{makePod("existing-a1", 2, 2048)})
	nodeC := makeConcreteNodeInfoWithPods("node-c", 8, 32,
		map[string]string{v1.LabelTopologyZone: "us-west-2b"},
		[]*v1.Pod{makePod("existing-c1", 2, 2048)})

	displacedPods := []*v1.Pod{
		makePod("displaced-1", 2, 2048),
		makePod("displaced-2", 2, 2048),
		makePod("displaced-3", 2, 2048),
		makePod("displaced-4", 2, 2048),
	}

	input := Input{
		Pods:         displacedPods,
		ClusterState: &ClusterState{Nodes: []fwk.NodeInfo{nodeA, nodeC}},
		Offerings:    nil, // no new capacity — test pure rebinding
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	if len(result.Bindings) != 4 {
		t.Fatalf("expected 4 bindings, got %d (NodeClaims: %d, Errors: %d)",
			len(result.Bindings), len(result.NodeClaims), len(result.Errors))
	}
	t.Logf("PASS: all %d displaced pods rebound to retained nodes", len(result.Bindings))
}

// --- Concrete NodeInfo test double ---

func makeConcreteNodeInfo(name string, cpuCores, memGi int64, labels map[string]string) fwk.NodeInfo {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(cpuCores, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", memGi)),
				v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
			},
		},
	}
	return &testNodeInfo{node: node}
}

func makeConcreteNodeInfoWithPods(name string, cpuCores, memGi int64, labels map[string]string, pods []*v1.Pod) fwk.NodeInfo {
	ni := makeConcreteNodeInfo(name, cpuCores, memGi, labels).(*testNodeInfo)
	for _, p := range pods {
		ni.pods = append(ni.pods, newTestPodInfo(p))
	}
	ni.recalculate()
	return ni
}

type testNodeInfo struct {
	node            *v1.Node
	pods            []fwk.PodInfo
	requestedCPU    int64
	requestedMemory int64
}

func (n *testNodeInfo) Node() *v1.Node                                    { return n.node }
func (n *testNodeInfo) GetPods() []fwk.PodInfo                            { return n.pods }
func (n *testNodeInfo) GetPodsWithAffinity() []fwk.PodInfo                { return nil }
func (n *testNodeInfo) GetPodsWithRequiredAntiAffinity() []fwk.PodInfo    { return nil }
func (n *testNodeInfo) GetUsedPorts() fwk.HostPortInfo                    { return make(fwk.HostPortInfo) }
func (n *testNodeInfo) GetImageStates() map[string]*fwk.ImageStateSummary { return nil }
func (n *testNodeInfo) GetPVCRefCounts() map[string]int                   { return nil }
func (n *testNodeInfo) GetGeneration() int64                              { return 0 }
func (n *testNodeInfo) GetNodeDeclaredFeatures() ndf.FeatureSet           { return ndf.FeatureSet{} }
func (n *testNodeInfo) String() string                                    { return n.node.Name }
func (n *testNodeInfo) Snapshot() fwk.NodeInfo                            { return n }
func (n *testNodeInfo) SetNode(node *v1.Node)                             { n.node = node }
func (n *testNodeInfo) RemovePod(_ klog.Logger, _ *v1.Pod) error          { return nil }

func (n *testNodeInfo) GetNodeAllocatableDRAClaimState() map[types.NamespacedName]*fwk.NodeAllocatableDRAClaimState {
	return nil
}

func (n *testNodeInfo) GetAllocatable() fwk.Resource {
	return &testResource{
		milliCPU: n.node.Status.Allocatable.Cpu().MilliValue(),
		memory:   n.node.Status.Allocatable.Memory().Value(),
	}
}

func (n *testNodeInfo) GetRequested() fwk.Resource {
	return &testResource{milliCPU: n.requestedCPU, memory: n.requestedMemory}
}

func (n *testNodeInfo) GetNonZeroRequested() fwk.Resource {
	return &testResource{milliCPU: n.requestedCPU, memory: n.requestedMemory}
}

func (n *testNodeInfo) AddPodInfo(pi fwk.PodInfo) {
	n.pods = append(n.pods, pi)
	for _, c := range pi.GetPod().Spec.Containers {
		if cpu, ok := c.Resources.Requests[v1.ResourceCPU]; ok {
			n.requestedCPU += cpu.MilliValue()
		}
		if mem, ok := c.Resources.Requests[v1.ResourceMemory]; ok {
			n.requestedMemory += mem.Value()
		}
	}
}

func (n *testNodeInfo) recalculate() {
	n.requestedCPU = 0
	n.requestedMemory = 0
	for _, pi := range n.pods {
		for _, c := range pi.GetPod().Spec.Containers {
			if cpu, ok := c.Resources.Requests[v1.ResourceCPU]; ok {
				n.requestedCPU += cpu.MilliValue()
			}
			if mem, ok := c.Resources.Requests[v1.ResourceMemory]; ok {
				n.requestedMemory += mem.Value()
			}
		}
	}
}

type testPodInfo struct {
	pod *v1.Pod
}

func newTestPodInfo(pod *v1.Pod) fwk.PodInfo { return &testPodInfo{pod: pod} }

func (p *testPodInfo) GetPod() *v1.Pod                                           { return p.pod }
func (p *testPodInfo) GetRequiredAffinityTerms() []fwk.AffinityTerm              { return nil }
func (p *testPodInfo) GetRequiredAntiAffinityTerms() []fwk.AffinityTerm          { return nil }
func (p *testPodInfo) GetPreferredAffinityTerms() []fwk.WeightedAffinityTerm     { return nil }
func (p *testPodInfo) GetPreferredAntiAffinityTerms() []fwk.WeightedAffinityTerm { return nil }
func (p *testPodInfo) CalculateResource() fwk.PodResource                        { return fwk.PodResource{} }

type testResource struct {
	milliCPU int64
	memory   int64
}

func (r *testResource) GetMilliCPU() int64                            { return r.milliCPU }
func (r *testResource) GetMemory() int64                              { return r.memory }
func (r *testResource) GetEphemeralStorage() int64                    { return 0 }
func (r *testResource) GetAllowedPodNumber() int                      { return 110 }
func (r *testResource) GetScalarResources() map[v1.ResourceName]int64 { return nil }
func (r *testResource) SetMaxResource(rl v1.ResourceList)             {}

// --- Benchmarks (drive Schedule() directly) ---
//
// Benchmarks run with PackingWeight=1.0 — the realistic production config. With
// the packing lookahead OFF (Options{}), the finely-grained synthetic catalog
// packs loosely (~375 in-flight nodes for 1000 pods) and each pod scans all of
// them, giving super-linear O(pods²) behavior (1000 pods ≈ 50s). Dense packing
// keeps the in-flight set small, so the scan stays cheap and runtime is ~linear
// in pod count. Measuring weight=0 would report the cost of a bug we already fix,
// not the scheduler's real performance.

func BenchmarkSchedule_100Pods_500InstanceTypes(b *testing.B) {
	offerings := generateOfferings(500, []string{"us-west-2a", "us-west-2b", "us-west-2c"})
	pods := generatePods(100)
	existingNodes := generateExistingNodes(10)

	input := Input{
		Pods:         pods,
		ClusterState: &ClusterState{Nodes: existingNodes},
		Offerings:    offerings,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{PackingWeight: 1.0}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSchedule_1000Pods_100InstanceTypes(b *testing.B) {
	offerings := generateOfferings(100, []string{"us-west-2a", "us-west-2b", "us-west-2c"})
	pods := generatePods(1000)

	input := Input{
		Pods:         pods,
		ClusterState: &ClusterState{Nodes: nil},
		Offerings:    offerings,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{PackingWeight: 1.0}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSchedule_50Pods_1000InstanceTypes(b *testing.B) {
	offerings := generateOfferings(1000, []string{"us-west-2a", "us-west-2b", "us-west-2c"})
	pods := generatePods(50)

	input := Input{
		Pods:         pods,
		ClusterState: &ClusterState{Nodes: nil},
		Offerings:    offerings,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{PackingWeight: 1.0}); err != nil {
			b.Fatal(err)
		}
	}
}

func generateOfferings(count int, zones []string) []*capacity.InstanceType {
	types := make([]*capacity.InstanceType, 0, count)
	cpuSizes := []int64{2, 4, 8, 16, 32, 64, 96, 128}
	memRatios := []int64{2, 4, 8}

	i := 0
	for _, cpus := range cpuSizes {
		for _, memRatio := range memRatios {
			if i >= count {
				return types
			}
			mem := cpus * memRatio
			name := fmt.Sprintf("m%d.%dcpu", memRatio, cpus)
			price := float64(cpus) * 0.012 * float64(memRatio) / 4.0

			reqs := capacity.NewRequirements()
			reqs[v1.LabelInstanceTypeStable] = capacity.NewRequirement(
				v1.LabelInstanceTypeStable, v1.NodeSelectorOpIn, name)
			reqs[v1.LabelTopologyZone] = capacity.NewRequirement(
				v1.LabelTopologyZone, v1.NodeSelectorOpIn, zones...)

			var offerings []*capacity.Offering
			for _, zone := range zones {
				for _, ct := range []string{"on-demand", "spot"} {
					offerPrice := price
					if ct == "spot" {
						offerPrice = price * 0.35
					}
					r := capacity.NewRequirements()
					r[v1.LabelTopologyZone] = capacity.NewRequirement(
						v1.LabelTopologyZone, v1.NodeSelectorOpIn, zone)
					r["karpenter.sh/capacity-type"] = capacity.NewRequirement(
						"karpenter.sh/capacity-type", v1.NodeSelectorOpIn, ct)
					offerings = append(offerings, &capacity.Offering{
						Requirements: r,
						Price:        offerPrice,
						Available:    true,
					})
				}
			}

			types = append(types, &capacity.InstanceType{
				Name:         name,
				Requirements: reqs,
				Offerings:    offerings,
				Capacity: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewQuantity(cpus, resource.DecimalSI),
					v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", mem)),
					v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
				},
			})
			i++
		}
	}

	for i < count {
		cpus := int64((i%8 + 1) * 2)
		mem := cpus * 4
		name := fmt.Sprintf("gen%d.%dcpu", i, cpus)
		price := float64(cpus) * 0.012

		reqs := capacity.NewRequirements()
		reqs[v1.LabelInstanceTypeStable] = capacity.NewRequirement(
			v1.LabelInstanceTypeStable, v1.NodeSelectorOpIn, name)
		reqs[v1.LabelTopologyZone] = capacity.NewRequirement(
			v1.LabelTopologyZone, v1.NodeSelectorOpIn, zones...)

		var offerings []*capacity.Offering
		for _, zone := range zones {
			r := capacity.NewRequirements()
			r[v1.LabelTopologyZone] = capacity.NewRequirement(
				v1.LabelTopologyZone, v1.NodeSelectorOpIn, zone)
			r["karpenter.sh/capacity-type"] = capacity.NewRequirement(
				"karpenter.sh/capacity-type", v1.NodeSelectorOpIn, "on-demand")
			offerings = append(offerings, &capacity.Offering{
				Requirements: r,
				Price:        price,
				Available:    true,
			})
		}

		types = append(types, &capacity.InstanceType{
			Name:         name,
			Requirements: reqs,
			Offerings:    offerings,
			Capacity: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(cpus, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", mem)),
				v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
			},
		})
		i++
	}

	return types
}

func generatePods(count int) []*v1.Pod {
	pods := make([]*v1.Pod, count)
	cpuSizes := []int64{500, 1000, 2000, 4000, 8000}
	memSizes := []int64{512, 1024, 2048, 4096, 8192}

	for i := 0; i < count; i++ {
		cpu := cpuSizes[i%len(cpuSizes)]
		mem := memSizes[i%len(memSizes)]
		pods[i] = &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("bench-pod-%d", i),
				Namespace: "default",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{{
					Name: "main",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewMilliQuantity(cpu, resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dMi", mem)),
						},
					},
				}},
			},
		}
	}
	return pods
}

func generateExistingNodes(count int) []fwk.NodeInfo {
	nodes := make([]fwk.NodeInfo, count)
	zones := []string{"us-west-2a", "us-west-2b", "us-west-2c"}
	for i := 0; i < count; i++ {
		nodes[i] = makeConcreteNodeInfo(
			fmt.Sprintf("existing-%d", i),
			16, 64,
			map[string]string{
				v1.LabelTopologyZone:       zones[i%len(zones)],
				v1.LabelInstanceTypeStable: "m5.4xlarge",
			},
		)
	}
	return nodes
}
