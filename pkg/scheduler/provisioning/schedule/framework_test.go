package schedule

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/provisioning/capacity"
	"k8s.io/kubernetes/pkg/scheduler/provisioning/virtualnode"
)

func init() {
	metrics.Register()
}

// noopBind satisfies the required Bind extension point for benchmarks that build a
// real framework (comparison_bench_test.go).
type noopBind struct{}

func (noopBind) Name() string { return "NoopBind" }
func (noopBind) Bind(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ string) *fwk.Status {
	return nil
}

// TestBindPath_UsesStockPlugins verifies the binding path runs UNMODIFIED
// upstream kube-scheduler Filter plugins against concrete nodes — no plugin fork.
// After the bind/provision split, PotentialNodes never reach the framework, so the
// framework only ever filters concrete nodes and can use stock plugins verbatim.
func TestBindPath_UsesStockPlugins(t *testing.T) {
	ctx := context.Background()
	f := newMarginalCostFramework() // wraps stock nodeaffinity + tainttoleration

	// A node labeled arch=amd64; a pod that REQUIRES arch=arm64 must be rejected
	// by the stock NodeAffinity plugin (not a reimplementation).
	node := makeConcreteNodeInfo("amd64-node", 8, 32, map[string]string{
		"kubernetes.io/arch": "amd64",
	})
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "needs-arm", Namespace: "default"},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/arch",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"arm64"},
						}},
					}},
				},
			}},
		},
	}

	if status := f.RunFilterPlugins(ctx, framework.NewCycleState(), pod, node); status.IsSuccess() {
		t.Fatal("stock NodeAffinity should reject arm64-required pod on an amd64 node")
	}

	// A pod that tolerates nothing must be rejected from a tainted node by the
	// stock TaintToleration plugin.
	tainted := makeConcreteNodeInfo("tainted", 8, 32, nil)
	tainted.(*testNodeInfo).node.Spec.Taints = []v1.Taint{{
		Key: "dedicated", Value: "gpu", Effect: v1.TaintEffectNoSchedule,
	}}
	plain := makePod("no-tolerations", 2, 2048)
	if status := f.RunFilterPlugins(ctx, framework.NewCycleState(), plain, tainted); status.IsSuccess() {
		t.Fatal("stock TaintToleration should reject an untolerating pod from a NoSchedule-tainted node")
	}

	t.Logf("PASS: bind path filters concrete nodes with unmodified upstream plugins (no fork)")
}

// TestProvisionPath_NarrowsSuperposition verifies the provisioning path narrows a
// PotentialNode directly via NarrowForPod — NOT through the framework. This is the
// piece that will move to a PostFilter provisioning plugin; it operates on the
// instance-type superposition, which a concrete Filter plugin cannot represent.
func TestProvisionPath_NarrowsSuperposition(t *testing.T) {
	instanceTypes := []*capacity.InstanceType{
		makeCapacityInstanceType("m5.xlarge", 4, 16, []string{"us-west-2a", "us-west-2b"}, 0.096),
		makeCapacityInstanceType("m5.2xlarge", 8, 32, []string{"us-west-2a", "us-west-2b"}, 0.192),
	}
	baseReqs := capacity.NewRequirements()
	baseReqs[v1.LabelTopologyZone] = capacity.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, "us-west-2a", "us-west-2b")
	baseReqs[v1.LabelInstanceTypeStable] = capacity.NewRequirement(v1.LabelInstanceTypeStable, v1.NodeSelectorOpIn, "m5.xlarge", "m5.2xlarge")
	pn := virtualnode.New(baseReqs, instanceTypes, nil)

	// Pod requesting 6 CPU — only m5.2xlarge (8 CPU) can satisfy.
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

	if status := pn.NarrowForPod(pod); !status.IsSuccess() {
		t.Fatalf("NarrowForPod failed: %v", status.Message())
	}
	if len(pn.InstanceTypes) != 1 || pn.InstanceTypes[0].Name != "m5.2xlarge" {
		t.Fatalf("expected superposition narrowed to m5.2xlarge, got %d types", len(pn.InstanceTypes))
	}

	// A taint the pod doesn't tolerate rejects the whole superposition.
	tainted := virtualnode.New(baseReqs, instanceTypes, []v1.Taint{{
		Key: "dedicated", Value: "gpu", Effect: v1.TaintEffectNoSchedule,
	}})
	if status := tainted.NarrowForPod(makePod("untolerating", 2, 2048)); status.IsSuccess() {
		t.Fatal("NarrowForPod should reject a pod that doesn't tolerate the NodePool taint")
	}

	t.Logf("PASS: provision path narrowed superposition (2 types → 1) and enforced taints, no framework")
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
