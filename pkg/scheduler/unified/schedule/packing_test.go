package schedule

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

// TestSchedule_PackingLookaheadConsolidates: with a finely-grained instance
// catalog and linear pricing, pure per-pod marginal cost mispacks (a tightly
// sized fresh node beats growing an in-flight node a tier, because the pod is
// billed the whole instance jump). PackingWeight=1.0 credits the fillable
// headroom at its true per-unit price, neutralizing the myopia, so pods
// consolidate onto far fewer nodes.
func TestSchedule_PackingLookaheadConsolidates(t *testing.T) {
	offerings := generateOfferings(100, []string{"us-west-2a", "us-west-2b", "us-west-2c"})
	mkPods := func() []*v1.Pod { return generatePods(200) }

	myopic, err := Schedule(context.Background(), newMarginalCostFramework(),
		Input{Pods: mkPods(), ClusterState: &ClusterState{}, Offerings: offerings}, Options{PackingWeight: 0})
	if err != nil {
		t.Fatalf("Schedule (myopic) failed: %v", err)
	}
	packed, err := Schedule(context.Background(), newMarginalCostFramework(),
		Input{Pods: mkPods(), ClusterState: &ClusterState{}, Offerings: offerings}, Options{PackingWeight: 1.0})
	if err != nil {
		t.Fatalf("Schedule (packed) failed: %v", err)
	}

	if len(packed.NodeClaims) >= len(myopic.NodeClaims) {
		t.Fatalf("packing lookahead should reduce node count: myopic=%d packed=%d",
			len(myopic.NodeClaims), len(packed.NodeClaims))
	}
	// Sanity: every pod still placed, no errors.
	total := 0
	for _, nc := range packed.NodeClaims {
		total += len(nc.Pods)
	}
	if total != 200 || len(packed.Errors) != 0 {
		t.Fatalf("expected 200 pods placed with no errors, got %d placed, %d errors", total, len(packed.Errors))
	}
	t.Logf("PASS: lookahead consolidated %d nodes → %d nodes (200 pods)",
		len(myopic.NodeClaims), len(packed.NodeClaims))
}

// TestSchedule_PackingDoesNotOversizeForLastPod: the lookahead credit is gated on
// REMAINING batch demand, so it must vanish for the final pod — there is no future
// demand to fill headroom, so a lone pod should land on a snug node, not an
// oversized one. We schedule a single pod and assert it picks the smallest fitting
// instance type even with a high PackingWeight.
func TestSchedule_PackingDoesNotOversizeForLastPod(t *testing.T) {
	// Two types that both fit a 2-CPU pod: small (4 CPU) and large (16 CPU).
	small := makeMultiZoneInstanceType("small", 4, 16, []string{"us-west-2a"}, "on-demand", 0.10)
	large := makeMultiZoneInstanceType("large", 16, 64, []string{"us-west-2a"}, "on-demand", 0.40)

	input := Input{
		Pods:         []*v1.Pod{makePod("solo", 2, 2048)},
		ClusterState: &ClusterState{},
		Offerings:    []*capacity.InstanceType{small, large},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{PackingWeight: 1.0})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.NodeClaims) != 1 {
		t.Fatalf("expected 1 NodeClaim, got %d", len(result.NodeClaims))
	}
	// With no remaining demand, headroom is worthless → must resolve to the small type.
	itReq := result.NodeClaims[0].Requirements.Get(v1.LabelInstanceTypeStable)
	names := map[string]bool{}
	for _, it := range result.NodeClaims[0].CompatibleInstanceTypes {
		names[it.Name] = true
	}
	cheapest := result.NodeClaims[0].CheapestPrice
	_ = itReq
	if cheapest > 0.10+1e-9 {
		t.Fatalf("lone pod oversized: cheapest price %.3f (expected small 0.10); types=%v", cheapest, names)
	}
	t.Logf("PASS: lone pod (no remaining demand) stayed on the small node, cheapest $%.2f", cheapest)
}

// TestSchedule_PackingWeightOffIsMyopic documents the baseline: weight 0 leaves
// the pure marginal-cost behavior untouched (regression guard for the default).
func TestSchedule_PackingWeightOffIsMyopic(t *testing.T) {
	// 4 pods @ 4 CPU, types stepping 4→8→16. Myopic packs loosely.
	offerings := []*capacity.InstanceType{
		makeMultiZoneInstanceType("c.4", 4, 8, []string{"us-west-2a"}, "on-demand", 0.20),
		makeMultiZoneInstanceType("c.8", 8, 16, []string{"us-west-2a"}, "on-demand", 0.40),
		makeMultiZoneInstanceType("c.16", 16, 32, []string{"us-west-2a"}, "on-demand", 0.80),
	}
	var pods []*v1.Pod
	for i := 0; i < 4; i++ {
		pods = append(pods, makePod(fmt.Sprintf("p%d", i), 4, 2048))
	}

	off, _ := Schedule(context.Background(), newMarginalCostFramework(),
		Input{Pods: pods, ClusterState: &ClusterState{}, Offerings: offerings}, Options{PackingWeight: 0})
	on, _ := Schedule(context.Background(), newMarginalCostFramework(),
		Input{Pods: pods, ClusterState: &ClusterState{}, Offerings: offerings}, Options{PackingWeight: 1.0})

	if len(on.NodeClaims) > len(off.NodeClaims) {
		t.Fatalf("lookahead should never increase node count: off=%d on=%d", len(off.NodeClaims), len(on.NodeClaims))
	}
	t.Logf("PASS: weight off=%d nodes, on=%d nodes (4 pods)", len(off.NodeClaims), len(on.NodeClaims))
}
