package schedule

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
	"k8s.io/kubernetes/pkg/scheduler/provisioning/capacity"
)

// marginalCostFramework is a minimal framework.Framework-shaped stub that runs the
// STOCK kube-scheduler Filter plugins against concrete nodes. After the bind/
// provision split, PotentialNodes never reach the framework — solve() narrows them
// directly via PotentialNode.NarrowForPod — so the framework only ever sees
// concrete nodes on the bind path. That means we can drive the real Schedule()
// with unmodified upstream plugins (nodeaffinity, tainttoleration), proving the
// binding path needs no plugin fork. NodeResourcesFit is intentionally omitted:
// solve() does its resource-fit against the copy-on-write view (resourceFitsConcrete),
// which sees tentative in-batch placements the stock plugin's snapshot would not.
type marginalCostFramework struct {
	framework.Framework
	affinity fwk.FilterPlugin
	taints   fwk.FilterPlugin
}

func newMarginalCostFramework() *marginalCostFramework {
	aff, err := nodeaffinity.New(context.Background(), &config.NodeAffinityArgs{}, nil, feature.Features{})
	if err != nil {
		panic(fmt.Sprintf("nodeaffinity.New: %v", err))
	}
	taint, err := tainttoleration.New(context.Background(), nil, nil, feature.Features{})
	if err != nil {
		panic(fmt.Sprintf("tainttoleration.New: %v", err))
	}
	return &marginalCostFramework{
		affinity: aff.(fwk.FilterPlugin),
		taints:   taint.(fwk.FilterPlugin),
	}
}

func (f *marginalCostFramework) RunPreFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod) (*fwk.PreFilterResult, *fwk.Status, sets.Set[string]) {
	return nil, nil, nil
}

func (f *marginalCostFramework) RunFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if s := f.affinity.Filter(ctx, state, pod, nodeInfo); s != nil && !s.IsSuccess() {
		return s
	}
	if s := f.taints.Filter(ctx, state, pod, nodeInfo); s != nil && !s.IsSuccess() {
		return s
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

// TestSchedule_BindFirstBeatsCreditedProvisioning locks in D17: bind-first is a
// SELECTION rule, not just a tie-break. A feasible bind must win even when a
// provisioning candidate's effective cost has been driven *negative* by the
// packing lookahead credit (score − credit < 0). Under the old
// argmin-over-all-tiers, a large-enough credit could make an in-flight/dummy
// node "cheaper than free" and beat the bind; bind-first forecloses that by never
// scoring provisioning when any existing node is feasible.
func TestSchedule_BindFirstBeatsCreditedProvisioning(t *testing.T) {
	// One free existing node that fits the first pod.
	existing := makeConcreteNodeInfo("existing", 8, 32, map[string]string{
		v1.LabelTopologyZone: "us-west-2a",
	})
	// A big cheap-per-unit offering; with a high PackingWeight and lots of
	// remaining batch demand, the credit on a fresh/in-flight node is large and
	// positive, so its effective cost goes negative.
	big := makeMultiZoneInstanceType("m5.16xlarge", 64, 256, []string{"us-west-2a"}, "on-demand", 0.10)

	// Many small pods: pod 0 fits the free node; the rest create heavy remaining
	// demand (fueling the lookahead credit) and force provisioning.
	pods := []*v1.Pod{makePod("fits-existing", 2, 2048)}
	for i := 0; i < 20; i++ {
		pods = append(pods, makePod(fmt.Sprintf("filler-%d", i), 2, 2048))
	}

	input := Input{
		Pods:         pods,
		ClusterState: &ClusterState{Nodes: []fwk.NodeInfo{existing}},
		Offerings:    []*capacity.InstanceType{big},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{PackingWeight: 1.0})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	// The free existing node must absorb pods (>=1 binding), never be passed over
	// in favor of a credit-discounted new node.
	if len(result.Bindings) == 0 {
		t.Fatalf("bind-first violated: free existing node was passed over for credited provisioning (0 bindings, %d NodeClaims)",
			len(result.NodeClaims))
	}
	t.Logf("PASS: free bind won over credit-negative provisioning (%d bindings, %d NodeClaims)",
		len(result.Bindings), len(result.NodeClaims))
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
