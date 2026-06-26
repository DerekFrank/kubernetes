package schedule

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
	"k8s.io/kubernetes/pkg/scheduler/unified/plugins"
)

// marginalCostFramework is a minimal framework.Framework-shaped stub that runs
// only our unified Filter plugins. It lets us drive the real Schedule() function
// (not a test-helper reimplementation) while keeping PreFilter/Score as no-ops —
// in this increment Schedule() selects purely on marginal cost.
type marginalCostFramework struct {
	framework.Framework
	filters []filterPlugin
}

func newMarginalCostFramework() *marginalCostFramework {
	return &marginalCostFramework{
		filters: []filterPlugin{
			&plugins.NodeResourcesFitUnified{},
			&plugins.NodeAffinityUnified{},
			&plugins.TaintTolerationUnified{},
		},
	}
}

func (f *marginalCostFramework) RunPreFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod) (*fwk.PreFilterResult, *fwk.Status, sets.Set[string]) {
	return nil, nil, nil
}

func (f *marginalCostFramework) RunFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	for _, p := range f.filters {
		if s := p.Filter(ctx, state, pod, nodeInfo); s != nil && !s.IsSuccess() {
			return s
		}
	}
	return nil
}

// TestSchedule_BindBeatsProvisionOnCost proves the core economic correction:
// a feasible existing node (marginal cost 0) always beats provisioning a new
// node (marginal cost > 0), even when the new node's *total* price is lower.
// This is the inverse of the old TestSamePathEvaluation, which was economically
// backwards (it compared total prices and let cheaper spot "beat" a free bind).
func TestSchedule_BindBeatsProvisionOnCost(t *testing.T) {
	existingNode := makeConcreteNodeInfo("existing", 8, 32, map[string]string{
		v1.LabelTopologyZone:       "us-west-2a",
		v1.LabelInstanceTypeStable: "m5.2xlarge",
	})

	// A cheaper-per-hour spot offering exists. Under the old model this "won."
	// Under marginal cost, binding to the already-paid existing node is free,
	// so it must win.
	cheapSpot := makeInstanceType("m5.2xlarge", 8, 32, "us-west-2a", "spot", 0.035)

	input := Input{
		Pods:         []*v1.Pod{makePod("workload", 2, 4096)},
		ClusterState: &ClusterState{Nodes: []fwk.NodeInfo{existingNode}},
		Offerings:    []*capacity.InstanceType{cheapSpot},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	if len(result.Bindings) != 1 {
		t.Fatalf("expected 1 binding to the free existing node, got %d bindings, %d NodeClaims",
			len(result.Bindings), len(result.NodeClaims))
	}
	if len(result.NodeClaims) != 0 {
		t.Fatalf("expected no provisioning (existing node is free), got %d NodeClaims", len(result.NodeClaims))
	}
	t.Logf("PASS: bound to free existing node instead of provisioning cheaper spot (marginal cost wins)")
}

// TestSchedule_ProvisionWhenNoFeasibleNode proves the dummy tier opens a new node
// when no existing node can take the pod.
func TestSchedule_ProvisionWhenNoFeasibleNode(t *testing.T) {
	// Existing node is full (0 CPU free): 8 CPU node already holding an 8 CPU pod.
	fullNode := makeConcreteNodeInfoWithPods("full", 8, 32,
		map[string]string{v1.LabelTopologyZone: "us-west-2a"},
		[]*v1.Pod{makePod("hog", 8, 1024)})

	offering := makeInstanceType("m5.2xlarge", 8, 32, "us-west-2a", "on-demand", 0.20)

	input := Input{
		Pods:         []*v1.Pod{makePod("needs-room", 4, 4096)},
		ClusterState: &ClusterState{Nodes: []fwk.NodeInfo{fullNode}},
		Offerings:    []*capacity.InstanceType{offering},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	if len(result.NodeClaims) != 1 {
		t.Fatalf("expected 1 NodeClaim (no feasible existing node), got %d NodeClaims, %d bindings (errors: %d)",
			len(result.NodeClaims), len(result.Bindings), len(result.Errors))
	}
	t.Logf("PASS: provisioned a new node when the only existing node was full")
}

// TestSchedule_PackOntoInFlightBeforeNewNode proves the in-flight tier: once a
// pod has opened a new node, a second pod prefers packing onto it (marginal cost
// ~0 if it fits the slack) over opening a third node.
func TestSchedule_PackOntoInFlightBeforeNewNode(t *testing.T) {
	// No existing nodes. One offering big enough for both pods together.
	offering := makeMultiZoneInstanceType("m5.4xlarge", 16, 64, []string{"us-west-2a"}, "on-demand", 0.32)

	input := Input{
		Pods: []*v1.Pod{
			makePod("p1", 4, 4096),
			makePod("p2", 4, 4096),
		},
		ClusterState: &ClusterState{Nodes: nil},
		Offerings:    []*capacity.InstanceType{offering},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	if len(result.NodeClaims) != 1 {
		t.Fatalf("expected 2 pods packed into 1 NodeClaim, got %d NodeClaims", len(result.NodeClaims))
	}
	if len(result.NodeClaims[0].Pods) != 2 {
		t.Fatalf("expected 2 pods on the single NodeClaim, got %d", len(result.NodeClaims[0].Pods))
	}
	t.Logf("PASS: second pod packed onto the in-flight node instead of opening a new one")
}
