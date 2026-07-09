package solver

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
	"k8s.io/kubernetes/pkg/scheduler/unified/virtualnode"
)

const zoneKey = "topology.kubernetes.io/zone"

// spreadPod builds a pod that hard-spreads across zoneKey with maxSkew=1 and a
// label the spread selector matches.
func spreadPod(name string) *v1.Pod {
	p := pod(name, 2)
	p.Labels = map[string]string{"app": "web"}
	p.Spec.TopologySpreadConstraints = []v1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       zoneKey,
		WhenUnsatisfiable: v1.DoNotSchedule,
		LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
	}}
	return p
}

// zonedOffering: one instance type per zone (so choosing a zone == choosing a type).
func zonedOffering(zone string) *capacity.InstanceType {
	reqs := capacity.NewRequirements()
	reqs[v1.LabelInstanceTypeStable] = capacity.NewRequirement(v1.LabelInstanceTypeStable, v1.NodeSelectorOpIn, "c-"+zone)
	reqs[zoneKey] = capacity.NewRequirement(zoneKey, v1.NodeSelectorOpIn, zone)
	off := capacity.NewRequirements()
	off[zoneKey] = capacity.NewRequirement(zoneKey, v1.NodeSelectorOpIn, zone)
	return &capacity.InstanceType{
		Name:         "c-" + zone,
		Requirements: reqs,
		Offerings:    []*capacity.Offering{{Requirements: off, Price: 0.10, Available: true}},
		Capacity: v1.ResourceList{
			v1.ResourceCPU:    *resource.NewQuantity(4, resource.DecimalSI),
			v1.ResourceMemory: resource.MustParse("8Gi"),
			v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
		},
	}
}

// TestTopology_SpreadsAcrossZones: 6 hard-spread pods over 3 zones must land 2/2/2,
// not piled into one zone. Proves the stateful TopologySpreadNarrower reads/updates
// Problem.Topology across the greedy loop.
func TestTopology_SpreadsAcrossZones(t *testing.T) {
	zones := []string{"z1", "z2", "z3"}
	offs := []*capacity.InstanceType{zonedOffering("z1"), zonedOffering("z2"), zonedOffering("z3")}

	var pods []*v1.Pod
	for i := 0; i < 6; i++ {
		pods = append(pods, spreadPod(fmt.Sprintf("web-%d", i)))
	}

	topo := virtualnode.NewTopology(map[string]sets.Set[string]{
		zoneKey: sets.New[string](zones...),
	})
	sol := Greedy{}.Solve(Problem{Pods: pods, Offerings: offs, Topology: topo})[0]

	if len(sol.Unplaced) != 0 {
		t.Fatalf("expected all 6 pods placed, %d unplaced", len(sol.Unplaced))
	}

	// Count pods per resolved zone across the emitted claims.
	perZone := map[string]int{}
	for _, nc := range sol.NodeClaims {
		zr := nc.Requirements.Get(zoneKey)
		if zr == nil || zr.Len() != 1 {
			t.Fatalf("claim %s not pinned to a single zone: %v", nc.Name, zr)
		}
		zone := zr.Values().UnsortedList()[0]
		perZone[zone] += len(claimPodCount(sol, nc.Name))
	}
	for _, z := range zones {
		if perZone[z] != 2 {
			t.Fatalf("expected 2/2/2 spread, got %v", perZone)
		}
	}
	t.Logf("PASS: 6 spread pods landed 2/2/2 across zones %v", perZone)
}

// claimPodCount returns the bindings targeting a given claim name.
func claimPodCount(sol Solution, name string) []PodBinding {
	var out []PodBinding
	for _, b := range sol.Bindings {
		if b.NodeClaimName == name {
			out = append(out, b)
		}
	}
	return out
}

// TestTopology_RespectsExistingSkew: seed z1 already loaded; new spread pods must
// avoid z1 until the others catch up.
func TestTopology_RespectsExistingSkew(t *testing.T) {
	zones := []string{"z1", "z2", "z3"}
	offs := []*capacity.InstanceType{zonedOffering("z1"), zonedOffering("z2"), zonedOffering("z3")}

	topo := virtualnode.NewTopology(map[string]sets.Set[string]{
		zoneKey: sets.New[string](zones...),
	})
	// Two matching pods already in z1.
	topo.Record(zoneKey, "z1")
	topo.Record(zoneKey, "z1")

	// Two new spread pods: with maxSkew=1 and z1 at 2 / z2,z3 at 0, both must go to
	// z2 and z3 (z1 would push skew to 3-0=3 > 1).
	pods := []*v1.Pod{spreadPod("web-a"), spreadPod("web-b")}
	sol := Greedy{}.Solve(Problem{Pods: pods, Offerings: offs, Topology: topo})[0]

	if len(sol.Unplaced) != 0 {
		t.Fatalf("expected both placed, %d unplaced", len(sol.Unplaced))
	}
	for _, nc := range sol.NodeClaims {
		zone := nc.Requirements.Get(zoneKey).Values().UnsortedList()[0]
		if zone == "z1" {
			t.Fatalf("new pod placed in already-heavy z1 (skew violation)")
		}
	}
	t.Logf("PASS: new spread pods avoided the already-loaded z1")
}
