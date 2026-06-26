package schedule

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

func nodeInfos(nodes ...fwk.NodeInfo) []fwk.NodeInfo { return nodes }

func spreadPod(name string, cpu, memMi int64, app string) *v1.Pod {
	p := makePod(name, cpu, memMi)
	p.Labels = map[string]string{"app": app}
	p.Spec.TopologySpreadConstraints = []v1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       v1.LabelTopologyZone,
		WhenUnsatisfiable: v1.DoNotSchedule,
		LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
	}}
	return p
}

// TestSchedule_TopologySpreadAcrossNewNodes: 6 spread pods, offering available in
// 3 zones. They should distribute ~evenly (2 per zone) across new NodeClaims, each
// pinned to a distinct zone — the snapshot's derived domain counts driving the
// least-loaded-domain choice as placements accumulate within the batch.
func TestSchedule_TopologySpreadAcrossNewNodes(t *testing.T) {
	offering := makeMultiZoneInstanceType("m5.xlarge", 4, 16,
		[]string{"us-west-2a", "us-west-2b", "us-west-2c"}, "on-demand", 0.10)

	var pods []*v1.Pod
	for i := 0; i < 6; i++ {
		pods = append(pods, spreadPod(fmt.Sprintf("p%d", i), 2, 2048, "web"))
	}

	input := Input{
		Pods:         pods,
		ClusterState: &ClusterState{},
		Offerings:    []*capacity.InstanceType{offering},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	placed := 0
	perZone := map[string]int{}
	for _, nc := range result.NodeClaims {
		placed += len(nc.Pods)
		zr := nc.Requirements.Get(v1.LabelTopologyZone)
		if zr == nil || zr.Len() != 1 {
			t.Fatalf("NodeClaim %s zone not pinned to a single value: %v", nc.Name, zr)
		}
		perZone[zr.Any()] += len(nc.Pods)
	}
	if placed != 6 {
		t.Fatalf("expected 6 pods placed, got %d (errors %d)", placed, len(result.Errors))
	}
	// maxSkew=1 across 3 zones with 6 pods → exactly 2 per zone.
	for z, n := range perZone {
		if n != 2 {
			t.Fatalf("expected even spread 2/zone, zone %s has %d (all: %v)", z, n, perZone)
		}
	}
	t.Logf("PASS: 6 pods spread 2/2/2 across zones %v", perZone)
}

// TestSchedule_TopologyRespectsExistingPods: an existing node in zone-a already
// holds 2 matching pods. New spread pods must avoid zone-a (it's already the
// heaviest) and go to zone-b/c — proving the snapshot seeds domain counts from
// pre-existing placements, not just batch placements.
func TestSchedule_TopologyRespectsExistingPods(t *testing.T) {
	existingPods := []*v1.Pod{spreadPod("old1", 2, 2048, "web"), spreadPod("old2", 2, 2048, "web")}
	nodeA := makeConcreteNodeInfoWithPods("node-a", 16, 64,
		map[string]string{v1.LabelTopologyZone: "us-west-2a"}, existingPods)

	offering := makeMultiZoneInstanceType("m5.xlarge", 4, 16,
		[]string{"us-west-2a", "us-west-2b", "us-west-2c"}, "on-demand", 0.10)

	// 2 new pods; with zone-a already at 2 and b/c at 0, maxSkew=1 forces both
	// new pods into b and c (one each), never a.
	pods := []*v1.Pod{spreadPod("new1", 2, 2048, "web"), spreadPod("new2", 2, 2048, "web")}

	input := Input{
		Pods:         pods,
		ClusterState: &ClusterState{Nodes: nodeInfos(nodeA)},
		Offerings:    []*capacity.InstanceType{offering},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	for _, nc := range result.NodeClaims {
		zr := nc.Requirements.Get(v1.LabelTopologyZone)
		if zr != nil && zr.Has("us-west-2a") {
			t.Fatalf("new pod placed in zone-a despite it already being heaviest: %v", zr.Values.UnsortedList())
		}
	}
	if len(result.Errors) != 0 {
		t.Fatalf("expected all new pods placed, got %d errors", len(result.Errors))
	}
	t.Logf("PASS: new spread pods avoided the already-loaded zone-a")
}

// TestDeschedule_TopologyIndexConsistent is the key pressure test for state
// management: descheduling a node must drop its pods from the topology index, so
// the rescheduled pods don't spread against phantom counts. If WithoutNodes
// merely hid the node on read (lazy mask) while leaving its pods in the domain
// counts, the displaced pods would see zone-a as "still full" and misplace.
func TestDeschedule_TopologyIndexConsistent(t *testing.T) {
	// Three nodes, one per zone, each holding one matching pod.
	nodeA := makeConcreteNodeInfoWithPods("node-a", 16, 64,
		map[string]string{v1.LabelTopologyZone: "us-west-2a"}, []*v1.Pod{spreadPod("a1", 2, 2048, "web")})
	nodeB := makeConcreteNodeInfoWithPods("node-b", 16, 64,
		map[string]string{v1.LabelTopologyZone: "us-west-2b"}, []*v1.Pod{spreadPod("b1", 2, 2048, "web")})
	nodeC := makeConcreteNodeInfoWithPods("node-c", 16, 64,
		map[string]string{v1.LabelTopologyZone: "us-west-2c"}, []*v1.Pod{spreadPod("c1", 2, 2048, "web")})

	input := Input{
		ClusterState: &ClusterState{Nodes: nodeInfos(nodeA, nodeB, nodeC)},
		Offerings:    nil, // pure rebinding — no new capacity
	}

	// Deschedule node-b. Its pod b1 (zone-b) must reschedule. After removal the
	// domains are a=1, c=1, b=0; b1 spreads with maxSkew=1, so it can only land
	// where the count stays within skew. nodeB is gone, so b1 must bind to A or C —
	// and only if the index correctly shows b's own pod removed will the skew math
	// permit it (a=1,c=1 → adding to either makes 2 vs min 1 = skew 1, allowed).
	result, err := Deschedule(context.Background(), newMarginalCostFramework(), input, []string{"node-b"}, Options{})
	if err != nil {
		t.Fatalf("Deschedule failed: %v", err)
	}

	if len(result.Errors) != 0 {
		t.Fatalf("expected b1 to reschedule, got %d errors: %v", len(result.Errors), result.Errors)
	}
	if len(result.Bindings) != 1 {
		t.Fatalf("expected 1 binding for the displaced pod, got %d (NodeClaims %d)",
			len(result.Bindings), len(result.NodeClaims))
	}
	if result.Bindings[0].NodeName == "node-b" {
		t.Fatalf("displaced pod rebound to the removed node-b")
	}
	t.Logf("PASS: descheduled node-b; pod b1 rebound to %s with a consistent topology index",
		result.Bindings[0].NodeName)
}
