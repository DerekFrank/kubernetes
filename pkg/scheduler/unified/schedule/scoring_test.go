package schedule

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

// TestSchedule_MultiTypePreferencePin: several arm instance types satisfy the
// preference. Honoring it must keep ALL arm types in the NodeClaim (pin the arch
// label, not collapse to one type), so the cloud provider keeps fallback choice.
func TestSchedule_MultiTypePreferencePin(t *testing.T) {
	// Three arm types + one x86 type, all in zone-a.
	arm1 := archInstanceType("m6g.large", 2, 8, "us-west-2a", "on-demand", "arm64", 0.10)
	arm2 := archInstanceType("m6g.xlarge", 4, 16, "us-west-2a", "on-demand", "arm64", 0.18)
	arm3 := archInstanceType("c6g.large", 2, 8, "us-west-2a", "on-demand", "arm64", 0.09)
	x86 := archInstanceType("m5.large", 2, 8, "us-west-2a", "on-demand", "amd64", 0.11)

	pod := podPreferringArch("wants-arm", 2, 4096, "arm64", 1)
	input := Input{
		Pods:         []*v1.Pod{pod},
		ClusterState: &ClusterState{Nodes: nil},
		Offerings:    []*capacity.InstanceType{arm1, arm2, arm3, x86},
	}

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{PreferenceDiscount: 0.2})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.NodeClaims) != 1 {
		t.Fatalf("expected 1 NodeClaim, got %d", len(result.NodeClaims))
	}
	nc := result.NodeClaims[0]

	// Arch must be pinned to arm64...
	archReq := nc.Requirements.Get(archKey)
	if archReq == nil || !archReq.Has("arm64") || archReq.Has("amd64") {
		t.Fatalf("expected arch pinned to arm64, got %v", archReq)
	}
	// ...but all THREE arm types must remain (m6g.large that fits the 2-CPU pod,
	// m6g.xlarge, c6g.large), and the x86 type must be gone.
	names := map[string]bool{}
	for _, it := range nc.CompatibleInstanceTypes {
		names[it.Name] = true
	}
	if names["m5.large"] {
		t.Fatalf("x86 type should have been filtered out, but remains: %v", names)
	}
	armKept := 0
	for _, n := range []string{"m6g.large", "m6g.xlarge", "c6g.large"} {
		if names[n] {
			armKept++
		}
	}
	if armKept < 3 {
		t.Fatalf("expected all 3 arm types retained (multi-type pin), kept %d: %v", armKept, names)
	}
	t.Logf("PASS: arch pinned arm64, all %d arm types retained, x86 dropped — multi-type pin works", armKept)
}

// TestSchedule_FlexibilityPenalizesOverConstraint: with a strong flexibility
// value and a marginal preference, honoring a preference that collapses the node
// to a single pool should LOSE to staying flexible. Proves the flexibility scorer
// competes in the same argmin as price and preference.
func TestSchedule_FlexibilityPenalizesOverConstraint(t *testing.T) {
	// A multi-zone, multi-capacity-type offering: lots of pools when unconstrained.
	// One arm offering exists in exactly one pool (single zone, on-demand).
	flexible := makeMultiZoneInstanceType("m5.xlarge", 4, 16,
		[]string{"us-west-2a", "us-west-2b", "us-west-2c"}, "spot", 0.10)
	// Give the flexible type both spot and on-demand offerings across zones by
	// adding on-demand offerings too.
	for _, zone := range []string{"us-west-2a", "us-west-2b", "us-west-2c"} {
		r := capacity.NewRequirements()
		r[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, zone)
		r["karpenter.sh/capacity-type"] = capacity.NewRequirement("karpenter.sh/capacity-type", v1.NodeSelectorOpIn, "on-demand")
		flexible.Offerings = append(flexible.Offerings, &capacity.Offering{Requirements: r, Price: 0.12, Available: true})
	}

	// Pod softly prefers zone-c (weight 1). Honoring it pins to one zone → far
	// fewer pools. With a high FlexibilityValue and tiny PreferenceDiscount, the
	// flexibility lost should outweigh the preference gained.
	pod := makePod("flex-pod", 2, 4096)
	pod.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{{
			Weight: 1,
			Preference: v1.NodeSelectorTerm{MatchExpressions: []v1.NodeSelectorRequirement{{
				Key:      v1.LabelTopologyZone,
				Operator: v1.NodeSelectorOpIn,
				Values:   []string{"us-west-2c"},
			}}},
		}},
	}}

	input := Input{
		Pods:         []*v1.Pod{pod},
		ClusterState: &ClusterState{Nodes: nil},
		Offerings:    []*capacity.InstanceType{flexible},
	}

	// Tiny preference discount, large flexibility value: the small price discount
	// from pinning zone-c cannot pay for the pools it collapses.
	result, err := Schedule(context.Background(), newMarginalCostFramework(), input,
		Options{PreferenceDiscount: 0.01, FlexibilityValue: 1.0})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.NodeClaims) != 1 {
		t.Fatalf("expected 1 NodeClaim, got %d", len(result.NodeClaims))
	}
	zoneReq := result.NodeClaims[0].Requirements.Get(v1.LabelTopologyZone)
	if zoneReq != nil && zoneReq.Len() == 1 {
		t.Fatalf("flexibility should have prevented pinning to a single zone, but zone=%v", zoneReq.Values.UnsortedList())
	}
	t.Logf("PASS: kept multi-zone flexibility (zones=%v) instead of honoring a cheap preference that collapsed pools",
		func() any {
			if zoneReq == nil {
				return "unconstrained"
			}
			return zoneReq.Values.UnsortedList()
		}())
}

// TestSchedule_FlexibilityYieldsToStrongPreference is the mirror: same setup but a
// large PreferenceDiscount. Now the preference is worth more than the flexibility, so
// the node IS pinned to zone-c. Confirms the tradeoff is a real argmin, not a veto.
func TestSchedule_FlexibilityYieldsToStrongPreference(t *testing.T) {
	flexible := makeMultiZoneInstanceType("m5.xlarge", 4, 16,
		[]string{"us-west-2a", "us-west-2b", "us-west-2c"}, "spot", 0.10)
	for _, zone := range []string{"us-west-2a", "us-west-2b", "us-west-2c"} {
		r := capacity.NewRequirements()
		r[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, zone)
		r["karpenter.sh/capacity-type"] = capacity.NewRequirement("karpenter.sh/capacity-type", v1.NodeSelectorOpIn, "on-demand")
		flexible.Offerings = append(flexible.Offerings, &capacity.Offering{Requirements: r, Price: 0.12, Available: true})
	}

	pod := makePod("flex-pod", 2, 4096)
	pod.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{{
			Weight: 1,
			Preference: v1.NodeSelectorTerm{MatchExpressions: []v1.NodeSelectorRequirement{{
				Key:      v1.LabelTopologyZone,
				Operator: v1.NodeSelectorOpIn,
				Values:   []string{"us-west-2c"},
			}}},
		}},
	}}

	input := Input{
		Pods:         []*v1.Pod{pod},
		ClusterState: &ClusterState{Nodes: nil},
		Offerings:    []*capacity.InstanceType{flexible},
	}

	// Large preference discount, modest flexibility value: now the price discount
	// from pinning zone-c outweighs the pools it costs, so the node pins to zone-c.
	// (Same setup as the previous test; only the discount↑ / flexibility↓ knobs
	// differ — proving the tradeoff is a real argmin, not a veto either way.)
	result, err := Schedule(context.Background(), newMarginalCostFramework(), input,
		Options{PreferenceDiscount: 0.9, FlexibilityValue: 0.02})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	zoneReq := result.NodeClaims[0].Requirements.Get(v1.LabelTopologyZone)
	if zoneReq == nil || zoneReq.Len() != 1 || !zoneReq.Has("us-west-2c") {
		t.Fatalf("strong preference should pin zone=us-west-2c, got %v", zoneReq)
	}
	t.Logf("PASS: strong preference discount (0.9) beat modest flexibility (0.02) → pinned zone=us-west-2c")
}
