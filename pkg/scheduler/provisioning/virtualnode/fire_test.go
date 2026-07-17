package virtualnode

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/pkg/scheduler/provisioning/capacity"
)

// fireTestType builds an instance type with a zone + arch label domain, one offering
// per zone, and the given cpu/mem capacity.
func fireTestType(name, arch string, cpu, memGi int64, zones ...string) *capacity.InstanceType {
	reqs := capacity.NewRequirements()
	reqs[v1.LabelInstanceTypeStable] = capacity.NewRequirement(v1.LabelInstanceTypeStable, v1.NodeSelectorOpIn, name)
	reqs["kubernetes.io/arch"] = capacity.NewRequirement("kubernetes.io/arch", v1.NodeSelectorOpIn, arch)
	reqs[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, zones...)
	var offs []*capacity.Offering
	for _, z := range zones {
		r := capacity.NewRequirements()
		r[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, z)
		offs = append(offs, &capacity.Offering{Requirements: r, Price: 0.1, Available: true})
	}
	return &capacity.InstanceType{
		Name:         name,
		Requirements: reqs,
		Offerings:    offs,
		Capacity: v1.ResourceList{
			v1.ResourceCPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
			v1.ResourceMemory: resource.MustParse(memGiStr(memGi)),
			v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
		},
	}
}

func memGiStr(g int64) string { return resource.NewQuantity(g<<30, resource.BinarySI).String() }

func firePod(name, arch string, cpu int64) *v1.Pod {
	spec := v1.PodSpec{
		Containers: []v1.Container{{
			Name: "c",
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: *resource.NewMilliQuantity(cpu*1000, resource.DecimalSI),
			}},
		}},
	}
	if arch != "" {
		spec.NodeSelector = map[string]string{"kubernetes.io/arch": arch}
	}
	return &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name)}, Spec: spec}
}

func newTestClaim(types ...*capacity.InstanceType) *PotentialNode {
	return New(unionReqs(types), types, nil)
}

func unionReqs(types []*capacity.InstanceType) capacity.Requirements {
	u := capacity.NewRequirements()
	for _, it := range types {
		for k, r := range it.Requirements {
			if e, ok := u[k]; ok {
				u[k] = e.Union(r)
			} else {
				u[k] = r.Copy()
			}
		}
	}
	return u
}

// TestFire_OneWayGate: pre-fire the claim narrows; after Fire, Narrow/UnNarrow are
// rejected (the claim is an immutable commitment).
func TestFire_OneWayGate(t *testing.T) {
	amd := fireTestType("m5.large", "amd64", 8, 16, "z1", "z2")
	arm := fireTestType("m6g.large", "arm64", 8, 16, "z1", "z2")
	claim := newTestClaim(amd, arm)

	// Pre-fire: an amd64 pod narrows the superposition to the amd64 type.
	if status := claim.NarrowForPod(firePod("p1", "amd64", 2)); !status.IsSuccess() {
		t.Fatalf("pre-fire narrow failed: %v", status.Message())
	}
	if len(claim.InstanceTypes) != 1 || claim.InstanceTypes[0].Name != "m5.large" {
		t.Fatalf("expected narrow to m5.large, got %v", claim.InstanceTypes)
	}
	if claim.Fired() {
		t.Fatal("claim should not be fired yet")
	}

	// Fire the claim — one-way gate.
	claim.Fire()
	if !claim.Fired() {
		t.Fatal("claim should be fired")
	}

	// Post-fire: Narrow is rejected.
	if err := claim.Narrow(capacity.Requirements{}, nil); err == nil {
		t.Fatal("post-fire Narrow must be rejected")
	}
	// Post-fire: UnNarrow is rejected.
	if err := claim.UnNarrow(); err == nil {
		t.Fatal("post-fire UnNarrow must be rejected")
	}
	// NarrowForPod (which calls Narrow under the hood) also fails post-fire.
	if status := claim.NarrowForPod(firePod("p2", "amd64", 2)); status.IsSuccess() {
		t.Fatal("post-fire NarrowForPod must fail")
	}
}

// TestUnNarrow_RecomputesFromMembers: a pod that pinned an axis leaves; UnNarrow
// rebuilds the claim from surviving members, re-widening the freed axis but keeping
// axes a staying member still pins. Recompute-from-members, not per-pod subtraction.
func TestUnNarrow_RecomputesFromMembers(t *testing.T) {
	amd := fireTestType("m5.large", "amd64", 8, 16, "z1", "z2")
	arm := fireTestType("m6g.large", "arm64", 8, 16, "z1", "z2")
	claim := newTestClaim(amd, arm)

	// p1 requires arm64 (pins arch→arm64, collapsing to m6g.large); p2 has no arch
	// preference. Both are members.
	p1 := firePod("p1", "arm64", 2)
	p2 := firePod("p2", "", 2)
	if status := claim.NarrowForPod(p1); !status.IsSuccess() {
		t.Fatalf("narrow p1 failed: %v", status.Message())
	}
	claim.AddPod(p1)
	if status := claim.NarrowForPod(p2); !status.IsSuccess() {
		t.Fatalf("narrow p2 failed: %v", status.Message())
	}
	claim.AddPod(p2)
	if len(claim.InstanceTypes) != 1 || claim.InstanceTypes[0].Name != "m6g.large" {
		t.Fatalf("expected claim pinned to arm64 m6g.large, got %v", claim.InstanceTypes)
	}

	// p1 (the arm64 pinner) leaves. UnNarrow should re-widen arch — p2 pins nothing,
	// so both instance types return.
	if err := claim.RemovePod(klog.TODO(), p1); err != nil {
		t.Fatalf("remove p1: %v", err)
	}
	if err := claim.UnNarrow(); err != nil {
		t.Fatalf("unnarrow: %v", err)
	}
	if len(claim.InstanceTypes) != 2 {
		t.Fatalf("expected both types back after arm64 pinner left, got %v", typeNames(claim.InstanceTypes))
	}
	// p2 is still a member.
	if len(claim.GetPods()) != 1 || claim.GetPods()[0].GetPod().Name != "p2" {
		t.Fatalf("expected p2 to remain a member, got %v", claim.GetPods())
	}
}

// TestUnNarrow_StayingMemberKeepsItsAxis: when the LEAVING pod is the unconstrained
// one, the axis the staying member pins is NOT released — recompute-from-members is
// correct by construction (the survivor is exactly what the claim would be if built
// from it).
func TestUnNarrow_StayingMemberKeepsItsAxis(t *testing.T) {
	amd := fireTestType("m5.large", "amd64", 8, 16, "z1", "z2")
	arm := fireTestType("m6g.large", "arm64", 8, 16, "z1", "z2")
	claim := newTestClaim(amd, arm)

	p1 := firePod("p1", "arm64", 2) // pins arch→arm64
	p2 := firePod("p2", "", 2)      // pins nothing
	for _, p := range []*v1.Pod{p1, p2} {
		if status := claim.NarrowForPod(p); !status.IsSuccess() {
			t.Fatalf("narrow %s failed: %v", p.Name, status.Message())
		}
		claim.AddPod(p)
	}

	// The unconstrained p2 leaves; p1 (arm64) stays.
	if err := claim.RemovePod(klog.TODO(), p2); err != nil {
		t.Fatalf("remove p2: %v", err)
	}
	if err := claim.UnNarrow(); err != nil {
		t.Fatalf("unnarrow: %v", err)
	}
	if len(claim.InstanceTypes) != 1 || claim.InstanceTypes[0].Name != "m6g.large" {
		t.Fatalf("staying arm64 member must keep the claim pinned to m6g.large, got %v", typeNames(claim.InstanceTypes))
	}
}

func typeNames(types []*capacity.InstanceType) []string {
	out := make([]string, len(types))
	for i, it := range types {
		out[i] = it.Name
	}
	return out
}

// TestFireTimer_QuietWindow: fires T-quiet after the last membership change; a change
// resets the window.
func TestFireTimer_QuietWindow(t *testing.T) {
	var ft FireTimer
	base := time.Unix(1000, 0)
	quiet := 10 * time.Second
	max := 5 * time.Minute

	if ft.ShouldFire(base, quiet, max) {
		t.Fatal("empty timer must not fire")
	}
	ft.Touch(base)
	if ft.ShouldFire(base.Add(5*time.Second), quiet, max) {
		t.Fatal("must not fire before quiet window elapses")
	}
	// A change at t=8s resets the quiet window.
	ft.Touch(base.Add(8 * time.Second))
	if ft.ShouldFire(base.Add(15*time.Second), quiet, max) {
		t.Fatal("membership change should have reset the quiet window")
	}
	// 10s of quiet after the last change (t=8s) → fire at t=18s.
	if !ft.ShouldFire(base.Add(18*time.Second), quiet, max) {
		t.Fatal("must fire once quiet window elapses since last change")
	}
}

// TestFireTimer_MaxCeiling: a claim that keeps accreting pods still fires at T-max.
func TestFireTimer_MaxCeiling(t *testing.T) {
	var ft FireTimer
	base := time.Unix(2000, 0)
	quiet := 10 * time.Second
	max := 60 * time.Second

	ft.Touch(base)
	// Keep touching every 5s (never quiet for 10s) up to t=59s.
	for s := 5; s <= 59; s += 5 {
		ft.Touch(base.Add(time.Duration(s) * time.Second))
		if ft.ShouldFire(base.Add(time.Duration(s)*time.Second), quiet, max) {
			t.Fatalf("must not fire at t=%ds (still accreting, under ceiling)", s)
		}
	}
	// At t=60s the T-max ceiling forces a fire even though it never went quiet.
	if !ft.ShouldFire(base.Add(60*time.Second), quiet, max) {
		t.Fatal("must fire at T-max ceiling even while still accreting")
	}
}
