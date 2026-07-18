package schedule

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

// This benchmark compares four setups on the SAME pods and SAME existing nodes,
// to show the unified scheduler's hot path is competitive with the stock
// kube-scheduler's per-pod work:
//
//  1. kube-scheduler  — the real framework runtime running the real
//     NodeResourcesFit plugin: per pod PreFilter → Filter(all nodes) → PreScore →
//     Score → argmax. This is the scheduler's actual scheduling-cycle work
//     (schedule_one.go's findNodesThatFitPod + prioritizeNodes), minus the
//     queue/cache/binding/informer machinery, which is shared overhead neither
//     side should be charged for in a hot-path comparison.
//  2. unified (no offerings)   — Schedule with Offerings=nil: pure binding, the
//     directly-comparable case (no dummy, no provisioning).
//  3. unified (few offerings)  — a handful of instance types available.
//  4. unified (many offerings) — a large catalog (provisioning fully in play).
//
// Setups 2–4 schedule the same pods onto the same existing nodes; offerings only
// add provisioning candidates. The point is to see how the unified machinery
// (superposition + expand-then-score + three tiers) compares to plain binding,
// and how it grows with catalog size.

// --- shared fixtures (real *v1.Node + *v1.Pod, reused by both schedulers) ---

func benchNodes(n int) []*v1.Node {
	zones := []string{"us-west-2a", "us-west-2b", "us-west-2c"}
	nodes := make([]*v1.Node, n)
	for i := 0; i < n; i++ {
		nodes[i] = &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("node-%d", i),
				Labels: map[string]string{
					v1.LabelTopologyZone:       zones[i%len(zones)],
					v1.LabelInstanceTypeStable: "m5.4xlarge",
				},
			},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewQuantity(16, resource.DecimalSI),
					v1.ResourceMemory: resource.MustParse("64Gi"),
					v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
				},
			},
		}
	}
	return nodes
}

func benchPods(n int) []*v1.Pod {
	cpuSizes := []int64{500, 1000, 2000}
	memSizes := []int64{512, 1024, 2048}
	pods := make([]*v1.Pod, n)
	for i := 0; i < n; i++ {
		pods[i] = &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("bp-%d", i), Namespace: "default"},
			Spec: v1.PodSpec{
				Containers: []v1.Container{{
					Name: "main",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewMilliQuantity(cpuSizes[i%3], resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dMi", memSizes[i%3])),
						},
					},
				}},
			},
		}
	}
	return pods
}

// --- Setup 1: stock kube-scheduler hot path ---

func newStockFramework(b *testing.B, nodes []*v1.Node) framework.Framework {
	b.Helper()
	ctx := context.Background()
	snap := cache.NewSnapshot(nil, nodes)
	registry := frameworkruntime.Registry{
		noderesources.Name: func(ctx context.Context, plArgs runtime.Object, h fwk.Handle) (fwk.Plugin, error) {
			return noderesources.NewFit(ctx, plArgs, h, feature.Features{})
		},
		"PrioritySort": func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
			return &queuesort.PrioritySort{}, nil
		},
		"NoopBind": func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
			return noopBind{}, nil
		},
	}
	profile := &config.KubeSchedulerProfile{
		SchedulerName: "stock-bench",
		Plugins: &config.Plugins{
			QueueSort: config.PluginSet{Enabled: []config.Plugin{{Name: "PrioritySort"}}},
			PreFilter: config.PluginSet{Enabled: []config.Plugin{{Name: noderesources.Name}}},
			Filter:    config.PluginSet{Enabled: []config.Plugin{{Name: noderesources.Name}}},
			Score:     config.PluginSet{Enabled: []config.Plugin{{Name: noderesources.Name, Weight: 1}}},
			Bind:      config.PluginSet{Enabled: []config.Plugin{{Name: "NoopBind"}}},
		},
		PluginConfig: []config.PluginConfig{{
			Name: noderesources.Name,
			Args: &config.NodeResourcesFitArgs{ScoringStrategy: &config.ScoringStrategy{Type: config.LeastAllocated}},
		}},
	}
	f, err := frameworkruntime.NewFramework(ctx, registry, profile,
		frameworkruntime.WithSnapshotSharedLister(snap))
	if err != nil {
		b.Fatalf("NewFramework: %v", err)
	}
	return f
}

// scheduleStock replicates the per-pod scheduling-cycle work: for each pod,
// PreFilter, Filter over all nodes, Score the feasible set, pick the max. No
// binding (we don't mutate the snapshot), matching what our solve measures.
func scheduleStock(ctx context.Context, f framework.Framework, nodes []fwk.NodeInfo, pods []*v1.Pod) {
	for _, pod := range pods {
		state := framework.NewCycleState()
		_, s, _ := f.RunPreFilterPlugins(ctx, state, pod)
		if !s.IsSuccess() && !s.IsSkip() {
			continue
		}
		var feasible []fwk.NodeInfo
		for _, ni := range nodes {
			if f.RunFilterPlugins(ctx, state, pod, ni).IsSuccess() {
				feasible = append(feasible, ni)
			}
		}
		if len(feasible) == 0 {
			continue
		}
		f.RunPreScorePlugins(ctx, state, pod, feasible)
		scores, _ := f.RunScorePlugins(ctx, state, pod, feasible)
		best, bestScore := "", int64(-1)
		for _, ns := range scores {
			if ns.TotalScore > bestScore {
				best, bestScore = ns.Name, ns.TotalScore
			}
		}
		_ = best
	}
}

func nodeInfosFromNodes(nodes []*v1.Node) []fwk.NodeInfo {
	out := make([]fwk.NodeInfo, len(nodes))
	for i, n := range nodes {
		ni := framework.NewNodeInfo()
		ni.SetNode(n)
		out[i] = ni
	}
	return out
}

// --- the comparison ---

// benchSizes is the pluggable (pods, nodes) matrix the comparison sweeps over.
// Add or edit rows to test other scales.
var benchSizes = []struct{ pods, nodes int }{
	{100, 10},
	{500, 50},
	{1000, 100},
}

func BenchmarkComparison(b *testing.B) {
	zones := []string{"us-west-2a", "us-west-2b", "us-west-2c"}
	fewOfferings := generateOfferings(10, zones)
	manyOfferings := generateOfferings(500, zones)

	for _, sz := range benchSizes {
		pods := benchPods(sz.pods)
		nodes := benchNodes(sz.nodes)
		ourNodes := nodeInfosFromNodes(nodes)
		label := fmt.Sprintf("pods=%d/nodes=%d", sz.pods, sz.nodes)

		b.Run(label+"/1-kube-scheduler", func(b *testing.B) {
			f := newStockFramework(b, nodes)
			stockNodes := nodeInfosFromNodes(nodes)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scheduleStock(ctx, f, stockNodes, pods)
			}
		})

		b.Run(label+"/2-unified-no-offerings", func(b *testing.B) {
			in := Input{Pods: pods, ClusterState: &ClusterState{Nodes: ourNodes}, Offerings: nil}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Schedule(context.Background(), newMarginalCostFramework(), in, Options{PackingWeight: 1.0})
			}
		})

		b.Run(label+"/3-unified-few-offerings", func(b *testing.B) {
			in := Input{Pods: pods, ClusterState: &ClusterState{Nodes: ourNodes}, Offerings: fewOfferings}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Schedule(context.Background(), newMarginalCostFramework(), in, Options{PackingWeight: 1.0})
			}
		})

		b.Run(label+"/4-unified-many-offerings", func(b *testing.B) {
			in := Input{Pods: pods, ClusterState: &ClusterState{Nodes: ourNodes}, Offerings: manyOfferings}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Schedule(context.Background(), newMarginalCostFramework(), in, Options{PackingWeight: 1.0})
			}
		})
	}
}
