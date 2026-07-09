package schedule

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

// Performance-value ("newer gen is 20% better") is OFFERING data, not a workload
// preference — it discounts the effective price the solver compares. These tests
// pin that: a better-but-equally-priced offering wins the cost axis with no scoring
// term, and it does so by moving effective price, nothing else.

// TestOffering_EffectivePriceDiscount checks the arithmetic in isolation.
func TestOffering_EffectivePriceDiscount(t *testing.T) {
	o := &capacity.Offering{Price: 1.00, PerformanceValue: 0.20, Available: true}
	if got := o.EffectivePrice(); got != 0.80 {
		t.Fatalf("expected effective price 0.80 (1.00 × (1-0.20)), got %v", got)
	}
	// Clamps: value >1 floors effective price at 0, value <0 is treated as 0.
	if got := (&capacity.Offering{Price: 1, PerformanceValue: 2}).EffectivePrice(); got != 0 {
		t.Fatalf("value >1 should clamp effective price to 0, got %v", got)
	}
	if got := (&capacity.Offering{Price: 1, PerformanceValue: -1}).EffectivePrice(); got != 1 {
		t.Fatalf("negative value should be treated as 0 (no discount), got %v", got)
	}
}

// TestSchedule_PerformanceValueLowersEffectivePrice: two equal-priced offerings,
// one rated 25% better. The emitted claim keeps BOTH ("any of these will do" — the
// offering axis doesn't collapse), but its cheapest EFFECTIVE price reflects the
// discount (0.15, the better offering), not the sticker (0.20). Performance-value
// moves the cost axis Select compares on, with no separate score term — while
// flexibility (retaining both offerings) is preserved.
func TestSchedule_PerformanceValueLowersEffectivePrice(t *testing.T) {
	oldGen := makeInstanceType("m5.2xlarge", 8, 32, "us-west-2a", "on-demand", 0.20)
	newGen := makeInstanceType("m7i.2xlarge", 8, 32, "us-west-2a", "on-demand", 0.20)
	// newGen is intrinsically 25% better per dollar → effective 0.15 vs 0.20.
	for _, o := range newGen.Offerings {
		o.PerformanceValue = 0.25
	}

	input := Input{
		Pods:         []*v1.Pod{makePod("workload", 2, 4096)},
		ClusterState: &ClusterState{Nodes: nil}, // no existing node → provisioning forced
		Offerings:    []*capacity.InstanceType{oldGen, newGen},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.NodeClaims) != 1 {
		t.Fatalf("expected 1 NodeClaim, got %d", len(result.NodeClaims))
	}
	// The claim keeps both types (flexibility preserved — offering axis stays a set).
	itReq := result.NodeClaims[0].Requirements.Get(v1.LabelInstanceTypeStable)
	if itReq == nil || !itReq.Has("m7i.2xlarge") || !itReq.Has("m5.2xlarge") {
		t.Fatalf("expected the claim to retain BOTH offerings (any-of-these), got %v",
			func() any {
				if itReq == nil {
					return nil
				}
				return itReq.Values.UnsortedList()
			}())
	}
	// But the cheapest effective price reflects the higher-value offering's discount.
	if got := result.NodeClaims[0].CheapestPrice; got < 0.15-1e-9 || got > 0.15+1e-9 {
		t.Fatalf("expected cheapest EFFECTIVE price 0.15 (m7i discounted), got %v", got)
	}
	t.Logf("PASS: equal sticker, performance-value lowered effective price to 0.15; claim kept both offerings (flexibility intact)")
}

// TestSchedule_PerformanceValueDoesNotTipProvisioning: performance-value is a
// discount on a NEW node's price; it can't beat a free existing bind (a discount on
// $0 is $0). Same D5 property as soft prefs — value adjustments rank capacity we're
// buying, they never justify buying.
func TestSchedule_PerformanceValueDoesNotTipProvisioning(t *testing.T) {
	existing := makeConcreteNodeInfo("existing", 8, 32, map[string]string{
		v1.LabelTopologyZone: "us-west-2a",
	})
	// A hugely-valuable cheap offering — under any cost-only view it looks great.
	fancy := makeInstanceType("m7i.2xlarge", 8, 32, "us-west-2a", "on-demand", 0.05)
	for _, o := range fancy.Offerings {
		o.PerformanceValue = 0.9
	}

	input := Input{
		Pods:         []*v1.Pod{makePod("workload", 2, 4096)},
		ClusterState: &ClusterState{Nodes: []fwk.NodeInfo{existing}},
		Offerings:    []*capacity.InstanceType{fancy},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.Bindings) != 1 || len(result.NodeClaims) != 0 {
		t.Fatalf("performance-value must not tip provisioning over a free bind; got %d bindings, %d NodeClaims",
			len(result.Bindings), len(result.NodeClaims))
	}
	t.Logf("PASS: high-value cheap offering still lost to the free existing bind (discount on $0 is $0)")
}
