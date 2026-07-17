package capacity

import (
	"testing"

	v1 "k8s.io/api/core/v1"
)

// These pin the ported complement/bounds Requirement model (from Karpenter's
// scheduling.Requirement) — specifically that NotIn / Exists / DoesNotExist / Gt /
// Lt are represented and intersect correctly, which the earlier In-only model could
// not do (it silently dropped them, emitting infeasible narrowings).

func TestRequirement_NotInExcludes(t *testing.T) {
	r := NewRequirement("arch", v1.NodeSelectorOpNotIn, "arm64")
	if r.Has("arm64") {
		t.Fatal("NotIn[arm64] must not permit arm64")
	}
	if !r.Has("amd64") {
		t.Fatal("NotIn[arm64] must permit amd64")
	}
	if r.Operator() != v1.NodeSelectorOpNotIn {
		t.Fatalf("expected NotIn operator, got %v", r.Operator())
	}
}

// The load-bearing fix: In ∩ NotIn removes the excluded value from the In set,
// rather than the In-only model's "ignore the NotIn entirely."
func TestRequirement_InIntersectNotIn(t *testing.T) {
	in := NewRequirement("arch", v1.NodeSelectorOpIn, "amd64", "arm64")
	notIn := NewRequirement("arch", v1.NodeSelectorOpNotIn, "arm64")
	got := in.Intersect(notIn)
	if got.Has("arm64") {
		t.Fatal("In[amd64,arm64] ∩ NotIn[arm64] must exclude arm64")
	}
	if !got.Has("amd64") {
		t.Fatal("intersection must still permit amd64")
	}
	if got.Len() != 1 {
		t.Fatalf("expected exactly {amd64}, got Len=%d", got.Len())
	}
}

// Two exclusions intersect to a still-infinite exclusion of the union.
func TestRequirement_NotInIntersectNotIn(t *testing.T) {
	a := NewRequirement("arch", v1.NodeSelectorOpNotIn, "arm64")
	b := NewRequirement("arch", v1.NodeSelectorOpNotIn, "ppc64")
	got := a.Intersect(b)
	if got.Has("arm64") || got.Has("ppc64") {
		t.Fatal("intersection of NotIn[arm64] and NotIn[ppc64] must exclude both")
	}
	if !got.Has("amd64") {
		t.Fatal("a value excluded by neither must still be permitted")
	}
}

func TestRequirement_ExistsDoesNotExist(t *testing.T) {
	exists := NewRequirement("gpu", v1.NodeSelectorOpExists)
	if !exists.Has("anything") {
		t.Fatal("Exists must permit any value")
	}
	if exists.Operator() != v1.NodeSelectorOpExists {
		t.Fatalf("expected Exists, got %v", exists.Operator())
	}
	dne := NewRequirement("gpu", v1.NodeSelectorOpDoesNotExist)
	if dne.Has("anything") {
		t.Fatal("DoesNotExist must permit no value")
	}
}

func TestRequirement_GtLtBounds(t *testing.T) {
	gt := NewRequirement("cpu", v1.NodeSelectorOpGt, "4") // Gt 4 → >= 5
	if gt.Has("4") {
		t.Fatal("Gt 4 must not permit 4")
	}
	if !gt.Has("5") {
		t.Fatal("Gt 4 must permit 5")
	}
	// Gt 4 ∩ Lt 8 → [5,7]
	lt := NewRequirement("cpu", v1.NodeSelectorOpLt, "8")
	band := gt.Intersect(lt)
	if band.Has("4") || band.Has("8") {
		t.Fatal("band must exclude 4 and 8")
	}
	if !band.Has("5") || !band.Has("7") {
		t.Fatal("band must include 5 and 7")
	}
}

// Compatible must treat two exclusions on the same key as compatible (a value
// excluded by both can still exist), while In vs its own exclusion is incompatible.
func TestRequirements_CompatibleExclusions(t *testing.T) {
	notArm := Requirements{"arch": NewRequirement("arch", v1.NodeSelectorOpNotIn, "arm64")}
	notPpc := Requirements{"arch": NewRequirement("arch", v1.NodeSelectorOpNotIn, "ppc64")}
	if !notArm.Compatible(notPpc) {
		t.Fatal("two NotIn requirements on one key must be compatible")
	}
	onlyArm := Requirements{"arch": NewRequirement("arch", v1.NodeSelectorOpIn, "arm64")}
	if onlyArm.Compatible(notArm) {
		t.Fatal("In[arm64] and NotIn[arm64] must be incompatible")
	}
}
