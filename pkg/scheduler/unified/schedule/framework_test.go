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
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
	"k8s.io/kubernetes/pkg/scheduler/unified/plugins"
	"k8s.io/kubernetes/pkg/scheduler/unified/virtualnode"
)

// TestRealFramework_FilterPotentialNode demonstrates the modified plugins running
// inside the real kube-scheduler framework against a PotentialNode.
func init() {
	metrics.Register()
}

func TestRealFramework_FilterPotentialNode(t *testing.T) {
	ctx := context.Background()

	f, err := frameworkruntime.NewFramework(ctx, makeTestRegistry(),
		makeTestProfile(plugins.NodeResourcesFitUnifiedName, plugins.NodeAffinityUnifiedName, plugins.TaintTolerationUnifiedName))
	if err != nil {
		t.Fatalf("NewFramework failed: %v", err)
	}

	// Create a PotentialNode with two instance types
	instanceTypes := []*capacity.InstanceType{
		makeCapacityInstanceType("m5.xlarge", 4, 16, []string{"us-west-2a", "us-west-2b"}, 0.096),
		makeCapacityInstanceType("m5.2xlarge", 8, 32, []string{"us-west-2a", "us-west-2b"}, 0.192),
	}

	baseReqs := capacity.NewRequirements()
	baseReqs[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, "us-west-2a", "us-west-2b")
	baseReqs[v1.LabelInstanceTypeStable] = capacity.NewRequirement(v1.LabelInstanceTypeStable, v1.NodeSelectorOpIn, "m5.xlarge", "m5.2xlarge")

	pn := virtualnode.New(baseReqs, instanceTypes, nil)

	// Pod requesting 6 CPU — only m5.2xlarge (8 CPU) can satisfy
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "big-pod", Namespace: "default"},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "main",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("6"),
						v1.ResourceMemory: resource.MustParse("8Gi"),
					},
				},
			}},
		},
	}

	// Run Filter through the real framework
	state := framework.NewCycleState()
	status := f.RunFilterPlugins(ctx, state, pod, pn)

	if !status.IsSuccess() {
		t.Fatalf("RunFilterPlugins failed: %v", status.Message())
	}

	// After Filter, PotentialNode should have narrowed: m5.xlarge (4 CPU) eliminated
	if len(pn.InstanceTypes) != 1 {
		t.Fatalf("expected 1 instance type after narrowing, got %d", len(pn.InstanceTypes))
	}
	if pn.InstanceTypes[0].Name != "m5.2xlarge" {
		t.Fatalf("expected m5.2xlarge to remain, got %s", pn.InstanceTypes[0].Name)
	}

	t.Logf("PASS: Real framework Filter narrowed PotentialNode: 2 types → 1 (%s)", pn.InstanceTypes[0].Name)
}

// TestRealFramework_FilterConcreteNode verifies concrete nodes still work through our plugins.
func TestRealFramework_FilterConcreteNode(t *testing.T) {
	ctx := context.Background()

	f, err := frameworkruntime.NewFramework(ctx, makeTestRegistry(),
		makeTestProfile(plugins.NodeResourcesFitUnifiedName, plugins.TaintTolerationUnifiedName))
	if err != nil {
		t.Fatalf("NewFramework failed: %v", err)
	}

	node := makeConcreteNodeInfo("concrete-1", 4, 16, map[string]string{
		v1.LabelTopologyZone: "us-west-2a",
	})

	// Pod that fits (2 CPU)
	podFits := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "small-pod", Namespace: "default"},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "main",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("2"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
				},
			}},
		},
	}

	state := framework.NewCycleState()
	status := f.RunFilterPlugins(ctx, state, podFits, node)
	if !status.IsSuccess() {
		t.Fatalf("expected small pod to fit, got: %v", status.Message())
	}

	// Pod too big (8 CPU on 4 CPU node)
	podBig := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "big-pod", Namespace: "default"},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "main",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("8"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
				},
			}},
		},
	}

	state2 := framework.NewCycleState()
	status2 := f.RunFilterPlugins(ctx, state2, podBig, node)
	if status2.IsSuccess() {
		t.Fatal("expected big pod to NOT fit (8 CPU > 4 CPU)")
	}

	t.Logf("PASS: Real framework Filter correctly passes/rejects concrete nodes")
}

// noopBind satisfies the required Bind extension point.
type noopBind struct{}

func (noopBind) Name() string { return "NoopBind" }
func (noopBind) Bind(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ string) *fwk.Status {
	return nil
}

// makeTestRegistry builds a registry with our unified plugins + required infrastructure.
func makeTestRegistry() frameworkruntime.Registry {
	return frameworkruntime.Registry{
		"PrioritySort": func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
			return &queuesort.PrioritySort{}, nil
		},
		"NoopBind": func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
			return &noopBind{}, nil
		},
		plugins.NodeResourcesFitUnifiedName: func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
			return &plugins.NodeResourcesFitUnified{}, nil
		},
		plugins.NodeAffinityUnifiedName: func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
			return &plugins.NodeAffinityUnified{}, nil
		},
		plugins.TaintTolerationUnifiedName: func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
			return &plugins.TaintTolerationUnified{}, nil
		},
	}
}

// makeTestProfile builds a profile with QueueSort + Bind + our Filter plugins.
func makeTestProfile(filterPlugins ...string) *config.KubeSchedulerProfile {
	filters := make([]config.Plugin, len(filterPlugins))
	for i, name := range filterPlugins {
		filters[i] = config.Plugin{Name: name}
	}
	return &config.KubeSchedulerProfile{
		SchedulerName: "unified-poc",
		Plugins: &config.Plugins{
			QueueSort: config.PluginSet{
				Enabled: []config.Plugin{{Name: "PrioritySort"}},
			},
			Filter: config.PluginSet{
				Enabled: filters,
			},
			Bind: config.PluginSet{
				Enabled: []config.Plugin{{Name: "NoopBind"}},
			},
		},
	}
}

// helper to create instance types for framework tests
func makeCapacityInstanceType(name string, cpu, memGi int64, zones []string, price float64) *capacity.InstanceType {
	reqs := capacity.NewRequirements()
	reqs[v1.LabelInstanceTypeStable] = capacity.NewRequirement(v1.LabelInstanceTypeStable, v1.NodeSelectorOpIn, name)
	reqs[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, zones...)

	var offerings []*capacity.Offering
	for _, zone := range zones {
		r := capacity.NewRequirements()
		r[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, zone)
		offerings = append(offerings, &capacity.Offering{Requirements: r, Price: price, Available: true})
	}

	return &capacity.InstanceType{
		Name:         name,
		Requirements: reqs,
		Offerings:    offerings,
		Capacity: v1.ResourceList{
			v1.ResourceCPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
			v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", memGi)),
			v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
		},
	}
}
