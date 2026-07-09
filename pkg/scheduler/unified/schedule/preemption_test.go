package schedule

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

// These tests pin the waterfall: preempt-vs-provision is decided by fixed rung
// ORDER, not by a cost comparison. The default order is preempt-first (faithful to
// today, back-compat-safe); flipping to provision-first is the opt-in the design
// argues most workloads should choose. Within a rung, selection stays cardinal
// (cheapest capacity / least-disruptive victim) — but across rungs it is ordinal.

// makePodWithPriority is makePod plus a priority value.
func makePodWithPriority(name string, cpu, memMi int64, priority int32) *v1.Pod {
	pod := makePod(name, cpu, memMi)
	pod.Spec.Priority = &priority
	return pod
}

// fullNodeWithVictim builds an 8-CPU node fully occupied by one low-priority
// victim, plus a high-priority pod that needs the whole node. Both preemption
// (evict the victim) and provisioning (an offering exists) are feasible — the
// contested case the waterfall order decides.
func fullNodeWithVictim() (Input, *v1.Pod, *v1.Pod) {
	victim := makePodWithPriority("low-pri-victim", 8, 1024, 1)
	fullNode := makeConcreteNodeInfoWithPods("node-a", 8, 32,
		map[string]string{v1.LabelTopologyZone: "us-west-2a"},
		[]*v1.Pod{victim})
	offering := makeInstanceType("m5.2xlarge", 8, 32, "us-west-2a", "on-demand", 0.20)
	highPri := makePodWithPriority("high-pri", 8, 1024, 100)
	return Input{
		Pods:         []*v1.Pod{highPri},
		ClusterState: &ClusterState{Nodes: []fwk.NodeInfo{fullNode}},
		Offerings:    []*capacity.InstanceType{offering},
	}, highPri, victim
}

// TestWaterfall_DefaultIsPreemptFirst: with no Waterfall set, the contested case
// resolves to preemption — faithful to today's bind→preempt→provision behavior.
// This is the back-compat default the design concedes.
func TestWaterfall_DefaultIsPreemptFirst(t *testing.T) {
	input, _, victim := fullNodeWithVictim()

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.Preemptions) != 1 {
		t.Fatalf("default waterfall is preempt-first; expected 1 preemption, got %d preemptions, %d NodeClaims",
			len(result.Preemptions), len(result.NodeClaims))
	}
	if len(result.Preemptions[0].Victims) != 1 || result.Preemptions[0].Victims[0].Name != victim.Name {
		t.Fatalf("expected the low-pri victim to be preempted, got %v", result.Preemptions[0].Victims)
	}
	t.Logf("PASS: default order preempts before provisioning (back-compat)")
}

// TestWaterfall_ProvisionFirstFlip: flipping the waterfall to provision-first
// resolves the SAME contested case to provisioning — no eviction. This is the
// opt-in the design argues for: same node cost, no disruption, victim untouched.
func TestWaterfall_ProvisionFirstFlip(t *testing.T) {
	input, _, _ := fullNodeWithVictim()

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input,
		Options{Waterfall: []Rung{RungProvision, RungPreempt}})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.Preemptions) != 0 {
		t.Fatalf("provision-first must not preempt when provisioning is feasible; got %d preemptions", len(result.Preemptions))
	}
	if len(result.NodeClaims) != 1 {
		t.Fatalf("expected the high-pri pod to provision a new node, got %d NodeClaims", len(result.NodeClaims))
	}
	t.Logf("PASS: provision-first flip provisions instead of preempting (same node, zero disruption)")
}

// TestWaterfall_PreemptionFiresWhenProvisioningImpossible: even under
// provision-first, if provisioning is impossible (no offerings — quota/stockout),
// the waterfall falls through to the preempt rung. Provision-first never means
// "never preempt"; it means "prefer provisioning when it's available."
func TestWaterfall_PreemptionFiresWhenProvisioningImpossible(t *testing.T) {
	input, _, _ := fullNodeWithVictim()
	input.Offerings = nil // provisioning impossible

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input,
		Options{Waterfall: []Rung{RungProvision, RungPreempt}})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.Preemptions) != 1 {
		t.Fatalf("provision-first must still preempt when provisioning is impossible; got %d preemptions, %d errors",
			len(result.Preemptions), len(result.Errors))
	}
	t.Logf("PASS: provision-first falls through to preemption when provisioning is unavailable")
}

// TestWaterfall_ProvisionOnlyNeverPreempts: a waterfall with only the provision
// rung never evicts, even when preemption is the only way to use existing
// capacity — the pod provisions or (if it can't) errors. Proves rungs absent from
// the list can't fire.
func TestWaterfall_ProvisionOnlyNeverPreempts(t *testing.T) {
	input, _, _ := fullNodeWithVictim()

	result, err := Schedule(context.Background(), newMarginalCostFramework(), input,
		Options{Waterfall: []Rung{RungProvision}})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.Preemptions) != 0 {
		t.Fatalf("provision-only waterfall must never preempt; got %d", len(result.Preemptions))
	}
	if len(result.NodeClaims) != 1 {
		t.Fatalf("expected provisioning, got %d NodeClaims", len(result.NodeClaims))
	}
	t.Logf("PASS: a rung absent from the waterfall never fires (provision-only never preempts)")
}

// TestWaterfall_NoLowerPriorityVictimIsInert: preemption is priority-gated, so the
// preempt rung is inert when nothing on the node is lower priority than the pod —
// the pod provisions regardless of waterfall order. (Preemption is already opt-in
// via PriorityClass.)
func TestWaterfall_NoLowerPriorityVictimIsInert(t *testing.T) {
	// Node full of a SAME-priority incumbent — not a valid victim.
	incumbent := makePodWithPriority("incumbent", 8, 1024, 100)
	fullNode := makeConcreteNodeInfoWithPods("node-a", 8, 32,
		map[string]string{v1.LabelTopologyZone: "us-west-2a"},
		[]*v1.Pod{incumbent})
	offering := makeInstanceType("m5.2xlarge", 8, 32, "us-west-2a", "on-demand", 0.20)
	pod := makePodWithPriority("same-pri", 8, 1024, 100)

	input := Input{
		Pods:         []*v1.Pod{pod},
		ClusterState: &ClusterState{Nodes: []fwk.NodeInfo{fullNode}},
		Offerings:    []*capacity.InstanceType{offering},
	}

	// Default (preempt-first) order — but there's no eligible victim, so the
	// preempt rung is inert and provisioning fires.
	result, err := Schedule(context.Background(), newMarginalCostFramework(), input, Options{})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if len(result.Preemptions) != 0 {
		t.Fatalf("no lower-priority victim exists; preempt rung must be inert, got %d preemptions", len(result.Preemptions))
	}
	if len(result.NodeClaims) != 1 {
		t.Fatalf("expected provisioning (nothing to preempt), got %d NodeClaims", len(result.NodeClaims))
	}
	t.Logf("PASS: preempt rung inert without an eligible victim; provisioned instead")
}
