package solver

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
	"k8s.io/kubernetes/pkg/scheduler/unified/virtualnode"
)

// Benchmarks for the provisioning pipeline. These measure the pure solver
// (Problem -> []Solution), which is the design's provisioning core. See
// provisioning-pipeline-scratch.md.
//
// Honest scope:
//   - P1 (cluster-size independence) and the kube-scheduler comparison live in the
//     schedule package's comparison_bench_test.go, which benches the real framework;
//     here we measure the solver's OWN scaling (pods, catalog width) and the
//     packing-quality claims that are fully runnable in-package.
//   - Parallelism (fan-out over splits) is NOT benched: Split is a pass-through stub,
//     so there is nothing to fan out yet. That benchmark is gated on Split landing.
//   - Karpenter is not vendored, so there is no runnable Karpenter comparison here.

// benchPod builds a pod with the given CPU (cores) and 512Mi memory.
func benchPod(name string, cpu int64) *v1.Pod {
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

// benchPods builds n pods in a 3-way size mix (2/4/8 CPU), the mispacking-prone
// shape the packing lookahead targets.
func benchPods(n int) []*v1.Pod {
	sizes := []int64{2, 4, 8}
	out := make([]*v1.Pod, n)
	for i := range out {
		out[i] = benchPod(fmt.Sprintf("p%d", i), sizes[i%3])
	}
	return out
}

// benchOfferings builds a catalog with genuinely distinct shapes: sizes × arches ×
// zones. This produces real diversity (unlike name-only padding), so catalog-width
// scaling measures shape count, not duplicate count. Includes a large 128-CPU type
// so packing has room to consolidate.
func benchOfferings(sizes []int64, arches, zones []string) []*capacity.InstanceType {
	var out []*capacity.InstanceType
	for _, cpu := range sizes {
		for _, arch := range arches {
			for _, zone := range zones {
				name := fmt.Sprintf("c%d-%s-%s", cpu, arch, zone)
				reqs := capacity.NewRequirements()
				reqs[v1.LabelInstanceTypeStable] = capacity.NewRequirement(v1.LabelInstanceTypeStable, v1.NodeSelectorOpIn, name)
				reqs["kubernetes.io/arch"] = capacity.NewRequirement("kubernetes.io/arch", v1.NodeSelectorOpIn, arch)
				reqs[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, zone)
				off := capacity.NewRequirements()
				off[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, zone)
				out = append(out, &capacity.InstanceType{
					Name:         name,
					Requirements: reqs,
					Offerings:    []*capacity.Offering{{Requirements: off, Price: float64(cpu) * 0.02, Available: true}},
					Capacity: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
						v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", cpu*2)),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
					},
				})
			}
		}
	}
	return out
}

// defaultCatalog: 24 distinct shapes (6 sizes × 2 arches × 2 zones), incl. a 128-CPU
// type for consolidation headroom.
func defaultCatalog() []*capacity.InstanceType {
	return benchOfferings([]int64{4, 8, 16, 32, 64, 128}, []string{"amd64", "arm64"}, []string{"z1", "z2"})
}

// --- P2: greedy packing quality + speed vs naive per-pod-one-node ---

// naiveSolve is the honest "no packing" baseline: one NodeClaim per pod, never
// packing onto in-flight capacity. It is what per-pod provisioning produces (the
// 375-node pathology). Reuses NewNodeClaim + finalize, so it's the real solver
// primitives with the packing step removed.
func naiveSolve(p Problem) Solution {
	narrowers := p.narrowers()
	var claims []*virtualnode.PotentialNode
	var unplaced []*v1.Pod
	for _, pod := range p.Pods {
		c := NewNodeClaim(pod, p.Offerings)
		if c == nil || !tryAdd(c, pod, narrowers) {
			unplaced = append(unplaced, pod)
			continue
		}
		claims = append(claims, c)
	}
	return finalize(claims, unplaced)
}

// BenchmarkPackingQuality reports greedy vs naive node counts and latency.
// Run with -v via `go test -bench=BenchmarkPackingQuality -benchmem`.
func BenchmarkPackingQuality(b *testing.B) {
	for _, n := range []int{100, 500, 1000} {
		offs := defaultCatalog()
		b.Run(fmt.Sprintf("greedy/pods=%d", n), func(b *testing.B) {
			var nodes int
			for i := 0; i < b.N; i++ {
				sol := Greedy{}.Solve(Problem{Pods: benchPods(n), Offerings: offs})[0]
				nodes = len(sol.NodeClaims)
			}
			b.ReportMetric(float64(nodes), "nodes")
			b.ReportMetric(float64(n)/float64(nodes), "pods/node")
		})
		b.Run(fmt.Sprintf("naive/pods=%d", n), func(b *testing.B) {
			var nodes int
			for i := 0; i < b.N; i++ {
				sol := naiveSolve(Problem{Pods: benchPods(n), Offerings: offs})
				nodes = len(sol.NodeClaims)
			}
			b.ReportMetric(float64(nodes), "nodes")
		})
	}
}

// --- P3: portfolio (greedy + ILP) packing gain vs latency, kept <= ILP MaxPods ---

func BenchmarkPortfolio(b *testing.B) {
	// Mispacking-prone: fine-grained sizes + a catalog that tempts greedy to mispack.
	offs := benchOfferings([]int64{4, 6, 8}, []string{"amd64"}, []string{"z1"})
	for _, n := range []int{6, 8, 10, 12} {
		batch := benchPods(n)
		b.Run(fmt.Sprintf("greedy/pods=%d", n), func(b *testing.B) {
			var nodes int
			for i := 0; i < b.N; i++ {
				nodes = len((&Pipeline{Solvers: []Solver{Greedy{}}}).Run(batch, offs).NodeClaims)
			}
			b.ReportMetric(float64(nodes), "nodes")
		})
		b.Run(fmt.Sprintf("portfolio/pods=%d", n), func(b *testing.B) {
			var nodes int
			for i := 0; i < b.N; i++ {
				nodes = len((&Pipeline{Solvers: []Solver{Greedy{}, ILP{}}}).Run(batch, offs).NodeClaims)
			}
			b.ReportMetric(float64(nodes), "nodes")
		})
	}
}

// --- P4: catalog-width scaling of the solve (genuinely distinct shapes) ---

func BenchmarkCatalogWidth(b *testing.B) {
	// Widen distinct shapes via zone count: sizes(6) × arches(2) × zones(k).
	for _, zk := range []int{1, 4, 16, 32} {
		zones := make([]string, zk)
		for i := range zones {
			zones[i] = fmt.Sprintf("z%d", i)
		}
		offs := benchOfferings([]int64{4, 8, 16, 32, 64, 128}, []string{"amd64", "arm64"}, zones)
		b.Run(fmt.Sprintf("shapes=%d/pods=200", len(offs)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				Greedy{}.Solve(Problem{Pods: benchPods(200), Offerings: offs})
			}
		})
	}
}
