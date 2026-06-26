package schedule

import (
	"context"
	"sync"
	"testing"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

// TestSnapshot_BaseImmutableAcrossSolves: one base snapshot, reused across several
// solves (including a Deschedule). The base's node set and placement count must be
// identical before and after — solves mutate only their per-call view overlay,
// never the shared base.
func TestSnapshot_BaseImmutableAcrossSolves(t *testing.T) {
	nodeA := makeConcreteNodeInfoWithPods("node-a", 16, 64,
		map[string]string{v1.LabelTopologyZone: "us-west-2a"}, []*v1.Pod{makePod("a1", 2, 2048)})
	nodeB := makeConcreteNodeInfoWithPods("node-b", 16, 64,
		map[string]string{v1.LabelTopologyZone: "us-west-2b"}, []*v1.Pod{makePod("b1", 2, 2048)})
	base := NewSnapshot([]fwk.NodeInfo{nodeA, nodeB})

	nodesBefore := len(base.nodes)
	placementsBefore := len(base.placements)

	offering := makeMultiZoneInstanceType("m5.xlarge", 4, 16,
		[]string{"us-west-2a", "us-west-2b"}, "on-demand", 0.10)

	// Solve 1: schedule new pods against the shared base.
	in := Input{
		Pods:         []*v1.Pod{makePod("new1", 2, 2048), makePod("new2", 2, 2048)},
		ClusterState: &ClusterState{snap: base},
		Offerings:    []*capacity.InstanceType{offering},
	}
	if _, err := Schedule(context.Background(), newMarginalCostFramework(), in, Options{}); err != nil {
		t.Fatalf("solve 1 failed: %v", err)
	}

	// Solve 2: deschedule node-b against the same shared base.
	descIn := Input{ClusterState: &ClusterState{snap: base}, Offerings: []*capacity.InstanceType{offering}}
	if _, err := Deschedule(context.Background(), newMarginalCostFramework(), descIn, []string{"node-b"}, Options{}); err != nil {
		t.Fatalf("solve 2 (deschedule) failed: %v", err)
	}

	if len(base.nodes) != nodesBefore || len(base.placements) != placementsBefore {
		t.Fatalf("base mutated by solves: nodes %d→%d, placements %d→%d",
			nodesBefore, len(base.nodes), placementsBefore, len(base.placements))
	}
	t.Logf("PASS: base unchanged after 2 solves (%d nodes, %d placements)", nodesBefore, placementsBefore)
}

// TestSnapshot_ConcurrentSolvesShareBase: many solves over one shared base run
// concurrently without data races (run with -race). Proves the base is safe to
// share by pointer across goroutines — the precondition for the atomic-pointer
// RCU model and for a consolidation sim running alongside live provisioning.
func TestSnapshot_ConcurrentSolvesShareBase(t *testing.T) {
	var nodes []fwk.NodeInfo
	for _, z := range []string{"us-west-2a", "us-west-2b", "us-west-2c"} {
		nodes = append(nodes, makeConcreteNodeInfoWithPods("node-"+z, 32, 128,
			map[string]string{v1.LabelTopologyZone: z}, []*v1.Pod{makePod("seed-"+z, 2, 2048)}))
	}
	base := NewSnapshot(nodes)
	offering := makeMultiZoneInstanceType("m5.2xlarge", 8, 32,
		[]string{"us-west-2a", "us-west-2b", "us-west-2c"}, "on-demand", 0.20)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pods := generatePods(20)
			in := Input{Pods: pods, ClusterState: &ClusterState{snap: base}, Offerings: []*capacity.InstanceType{offering}}
			if _, err := Schedule(context.Background(), newMarginalCostFramework(), in, Options{PackingWeight: 1.0}); err != nil {
				t.Errorf("concurrent solve %d failed: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	t.Logf("PASS: 16 concurrent solves shared one base with no race (run with -race to verify)")
}
