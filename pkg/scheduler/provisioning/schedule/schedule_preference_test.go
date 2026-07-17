package schedule

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler/provisioning/capacity"
)

const archKey = "kubernetes.io/arch"

// podPreferringArch builds a pod with a soft (preferred) node-affinity for an arch.
func podPreferringArch(name string, cpu, memMi int64, arch string, weight int32) *v1.Pod {
	pod := makePod(name, cpu, memMi)
	pod.Spec.Affinity = &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{{
				Weight: weight,
				Preference: v1.NodeSelectorTerm{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      archKey,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{arch},
					}},
				},
			}},
		},
	}
	return pod
}

// archInstanceType is makeInstanceType plus an arch label on the type's requirements.
func archInstanceType(name string, cpu, memGi int64, zone, capacityType, arch string, price float64) *capacity.InstanceType {
	it := makeInstanceType(name, cpu, memGi, zone, capacityType, price)
	it.Requirements[archKey] = capacity.NewRequirement(archKey, v1.NodeSelectorOpIn, arch)
	return it
}

// D5: soft preferences are a MULTIPLICATIVE discount on cost. They rank
// purchasable offerings (price-for-performance) but cannot tip provisioning over
// a free bind (a discount on $0 is $0). "If you need it, make it a hard
// constraint." These tests pin that behavior.

// TestSchedule_SoftPreferenceDoesNotTipProvisioning: a free existing x86 node can
// fit the pod; the pod softly prefers arm, and an arm offering exists. Under the
// multiplicative model the soft preference CANNOT justify launching a node — the
// free bind wins. (This is the deliberate inverse of the old, disowned additive
// model, where a large PreferenceValue tipped provisioning.)
func TestSchedule_SoftPreferenceDoesNotTipProvisioning(t *testing.T) {
	x86Node := makeConcreteNodeInfo("x86-existing", 8, 32, map[string]string{
		archKey:              "amd64",
		v1.LabelTopologyZone: "us-west-2a",
	})
	armOffering := archInstanceType("m6g.2xlarge", 8, 32, "us-west-2a", "on-demand", "arm64", 0.30)

	pod := podPreferringArch("wants-arm", 2, 4096, "arm64", 1)
	input := Input{
		Pods:         []*v1.Pod{pod},
		ClusterState: &ClusterState{Nodes: []fwk.NodeInfo{x86Node}},
		Offerings:    []*capacity.InstanceType{armOffering},
	}

	// Even with a large discount, a discount on a $0 free bind is still $0.
	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{PreferenceDiscount: 0.5})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.Bindings) != 1 || len(result.NodeClaims) != 0 {
		t.Fatalf("soft preference must not launch a node over a free bind: got %d bindings, %d NodeClaims",
			len(result.Bindings), len(result.NodeClaims))
	}
	t.Logf("PASS: soft arm preference did NOT tip provisioning; bound free x86 (a discount on $0 is $0)")
}

// TestSchedule_PreferenceRanksOfferingsWhenProvisioning: no existing node, so a
// node WILL be provisioned regardless. Two equal-priced offerings exist — arm
// (preferred) and x86. The preference discounts arm's effective cost below x86's,
// so the new node resolves to arm and the NodeClaim is arm-pinned. This is
// price-for-performance: ranking among offerings we're buying anyway.
func TestSchedule_PreferenceRanksOfferingsWhenProvisioning(t *testing.T) {
	armOffering := archInstanceType("m6g.2xlarge", 8, 32, "us-west-2a", "on-demand", "arm64", 0.20)
	x86Offering := archInstanceType("m5.2xlarge", 8, 32, "us-west-2a", "on-demand", "amd64", 0.20)

	pod := podPreferringArch("wants-arm", 2, 4096, "arm64", 1)
	input := Input{
		Pods:         []*v1.Pod{pod},
		ClusterState: &ClusterState{Nodes: nil},
		Offerings:    []*capacity.InstanceType{armOffering, x86Offering},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{PreferenceDiscount: 0.2})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.NodeClaims) != 1 {
		t.Fatalf("expected 1 NodeClaim (provisioning forced — no existing node), got %d bindings, %d NodeClaims",
			len(result.Bindings), len(result.NodeClaims))
	}
	archReq := result.NodeClaims[0].Requirements.Get(archKey)
	if archReq == nil || archReq.Len() != 1 || !archReq.Has("arm64") {
		t.Fatalf("preferred arm should win the offering ranking and pin the claim to arm64, got %v",
			func() any {
				if archReq == nil {
					return nil
				}
				return archReq.Values().UnsortedList()
			}())
	}
	t.Logf("PASS: with provisioning forced, the arm preference discounted arm below x86 → arm-pinned NodeClaim")
}

// TestSchedule_PreferenceDiscountTooSmallToReRank: same forced-provisioning setup,
// but arm is pricier and the discount is too small to overcome the gap, so the
// cheaper x86 offering wins. The preference loses on price — in one pass, no retry.
func TestSchedule_PreferenceDiscountTooSmallToReRank(t *testing.T) {
	armOffering := archInstanceType("m6g.2xlarge", 8, 32, "us-west-2a", "on-demand", "arm64", 0.40)
	x86Offering := archInstanceType("m5.2xlarge", 8, 32, "us-west-2a", "on-demand", "amd64", 0.20)

	pod := podPreferringArch("wants-arm", 2, 4096, "arm64", 1)
	input := Input{
		Pods:         []*v1.Pod{pod},
		ClusterState: &ClusterState{Nodes: nil},
		Offerings:    []*capacity.InstanceType{armOffering, x86Offering},
	}

	// 10% discount on arm: 0.40×0.9 = 0.36, still > x86's 0.20 → x86 wins.
	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{PreferenceDiscount: 0.1})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.NodeClaims) != 1 {
		t.Fatalf("expected 1 NodeClaim, got %d", len(result.NodeClaims))
	}
	archReq := result.NodeClaims[0].Requirements.Get(archKey)
	if archReq != nil && archReq.Has("arm64") && !archReq.Has("amd64") {
		t.Fatalf("discount too small to overcome arm's price gap; expected x86 to win, got arm-pinned %v",
			archReq.Values().UnsortedList())
	}
	t.Logf("PASS: arm discount (10%%) didn't cover the price gap; cheaper x86 offering won, one pass")
}

// TestSchedule_UnsatisfiablePreferenceIsHarmless: pod prefers arm but only x86 is
// provisionable, and a free x86 node exists. The preference guarantees nothing
// (no arm offering), so it applies no discount and binds free x86 — no relaxation,
// no error, regardless of discount magnitude.
func TestSchedule_UnsatisfiablePreferenceIsHarmless(t *testing.T) {
	x86Node := makeConcreteNodeInfo("x86-existing", 8, 32, map[string]string{
		archKey:              "amd64",
		v1.LabelTopologyZone: "us-west-2a",
	})
	x86Offering := archInstanceType("m5.2xlarge", 8, 32, "us-west-2a", "on-demand", "amd64", 0.20)

	pod := podPreferringArch("wants-impossible-arm", 2, 4096, "arm64", 10)
	input := Input{
		Pods:         []*v1.Pod{pod},
		ClusterState: &ClusterState{Nodes: []fwk.NodeInfo{x86Node}},
		Offerings:    []*capacity.InstanceType{x86Offering},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{PreferenceDiscount: 0.9})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.Bindings) != 1 || len(result.NodeClaims) != 0 {
		t.Fatalf("unsatisfiable preference should bind free x86, got %d bindings, %d NodeClaims, %d errors",
			len(result.Bindings), len(result.NodeClaims), len(result.Errors))
	}
	t.Logf("PASS: unsatisfiable arm preference applied no discount; bound free x86, no relaxation loop")
}
