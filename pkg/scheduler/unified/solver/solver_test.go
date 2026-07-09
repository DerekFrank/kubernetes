package solver

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

func pod(name string, cpu int64) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1.PodSpec{Containers: []v1.Container{{
			Name: "main",
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(cpu*1000, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("512Mi"),
			}},
		}}},
	}
}

func offering(name string, cpu, memGi int64, price float64) *capacity.InstanceType {
	reqs := capacity.NewRequirements()
	reqs[v1.LabelInstanceTypeStable] = capacity.NewRequirement(v1.LabelInstanceTypeStable, v1.NodeSelectorOpIn, name)
	return &capacity.InstanceType{
		Name:         name,
		Requirements: reqs,
		Offerings:    []*capacity.Offering{{Requirements: capacity.NewRequirements(), Price: price, Available: true}},
		Capacity: v1.ResourceList{
			v1.ResourceCPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
			v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", memGi)),
			v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
		},
	}
}

func pods(n int, cpu int64) []*v1.Pod {
	out := make([]*v1.Pod, n)
	for i := range out {
		out[i] = pod(fmt.Sprintf("p%d", i), cpu)
	}
	return out
}

func placed(s Solution) int { return len(s.Bindings) }

// TestGreedy_PlacesAll: greedy places every pod when capacity exists.
func TestGreedy_PlacesAll(t *testing.T) {
	offs := []*capacity.InstanceType{offering("m5.4xlarge", 16, 32, 0.32)}
	sol := Greedy{}.Solve(Problem{Pods: pods(8, 4), Offerings: offs})[0]
	if len(sol.Unplaced) != 0 {
		t.Fatalf("greedy stranded %d pods", len(sol.Unplaced))
	}
	if placed(sol) != 8 {
		t.Fatalf("expected 8 bindings, got %d", placed(sol))
	}
	t.Logf("PASS: greedy placed 8 pods on %d node(s)", len(sol.NodeClaims))
}

// TestGreedy_PacksOntoInFlight: 4×4CPU pods fit one 16-CPU node — greedy packs them
// onto one claim, not four.
func TestGreedy_PacksOntoInFlight(t *testing.T) {
	offs := []*capacity.InstanceType{offering("m5.4xlarge", 16, 64, 0.32)}
	sol := Greedy{}.Solve(Problem{Pods: pods(4, 4), Offerings: offs})[0]
	if len(sol.NodeClaims) != 1 {
		t.Fatalf("expected 4 pods packed onto 1 node, got %d nodes", len(sol.NodeClaims))
	}
	t.Logf("PASS: greedy packed 4 pods onto 1 node")
}

// TestILP_MatchesOrBeatsGreedy: on a mispacking-prone instance (fine-grained sizes),
// the ILP's node count is <= greedy's. This is the portfolio thesis: fan out both,
// Select the better; ILP never loses to greedy on small splits.
func TestILP_MatchesOrBeatsGreedy(t *testing.T) {
	// Two sizes: a 4-CPU and a 6-CPU node. Pods: 3×2CPU + 1×6CPU.
	// Greedy largest-first: 6CPU pod → open 6-CPU node (full). Then 2+2+2 → needs
	// a second node. ILP can see 6 + (2+2+2=6) doesn't share, but may still find the
	// min-node packing. Assert ILP <= greedy regardless.
	offs := []*capacity.InstanceType{
		offering("small", 4, 8, 0.10),
		offering("med", 6, 12, 0.15),
		offering("big", 8, 16, 0.20),
	}
	batch := []*v1.Pod{pod("a", 2), pod("b", 2), pod("c", 2), pod("d", 6)}

	g := Greedy{}.Solve(Problem{Pods: batch, Offerings: offs})[0]
	i := ILP{}.Solve(Problem{Pods: batch, Offerings: offs})[0]

	if len(i.Unplaced) != 0 {
		t.Fatalf("ILP stranded %d pods", len(i.Unplaced))
	}
	if len(i.NodeClaims) > len(g.NodeClaims) {
		t.Fatalf("ILP (%d nodes) should never use more than greedy (%d nodes)",
			len(i.NodeClaims), len(g.NodeClaims))
	}
	t.Logf("PASS: greedy=%d nodes, ILP=%d nodes (ILP ≤ greedy)", len(g.NodeClaims), len(i.NodeClaims))
}

// TestPipeline_PortfolioSelectsBest: the pipeline fanning out greedy+ILP returns a
// solution no worse (fewer-or-equal nodes) than greedy alone.
func TestPipeline_PortfolioSelectsBest(t *testing.T) {
	offs := []*capacity.InstanceType{
		offering("small", 4, 8, 0.10),
		offering("big", 12, 24, 0.30),
	}
	batch := []*v1.Pod{pod("a", 3), pod("b", 3), pod("c", 3), pod("d", 3)}

	greedyOnly := (&Pipeline{Solvers: []Solver{Greedy{}}}).Run(batch, offs)
	portfolio := (&Pipeline{Solvers: []Solver{Greedy{}, ILP{}}}).Run(batch, offs)

	if len(portfolio.Unplaced) != 0 {
		t.Fatalf("portfolio stranded %d pods", len(portfolio.Unplaced))
	}
	if len(portfolio.NodeClaims) > len(greedyOnly.NodeClaims) {
		t.Fatalf("portfolio (%d) should be <= greedy-only (%d)",
			len(portfolio.NodeClaims), len(greedyOnly.NodeClaims))
	}
	t.Logf("PASS: greedy-only=%d nodes, portfolio=%d nodes", len(greedyOnly.NodeClaims), len(portfolio.NodeClaims))
}
