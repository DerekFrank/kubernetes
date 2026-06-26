package schedule

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

// This file is a *caller harness*, not part of Schedule(). It demonstrates the
// resolved batch/bind-commit model (see "Remaining Considerations" in the POC
// doc): Schedule() is a pure function of the batch it's handed; the caller owns
//
//   - batch composition (which pending pods go into this Schedule call),
//   - execution of the returned decisions (binding, here simulated),
//   - failure handling via per-pod backoff (a failed pod is excluded from the
//     next batch until its backoff expires) — NOT node marking, NOT solver state,
//   - folding already-emitted NodeClaims into the next batch as FIXED in-flight
//     capacity (a launched node is a real commit that can't be un-launched).
//
// The harness reuses no scheduler-queue code; it's the minimal driver that proves
// the layering works end to end.

// driver is a minimal provisioning/binding caller around Schedule().
type driver struct {
	framework framework.Framework
	offerings []*capacity.InstanceType
	opts      Options

	// pending pods awaiting placement, with a backoff counter per pod.
	pending map[string]*pendingPod
	// nodes that have been "launched" from committed NodeClaims — fixed in-flight
	// capacity the next batch must treat as existing, not reopen.
	launched []fwk.NodeInfo
	// bound records pod → node for committed binds.
	bound map[string]string

	// bindFails injects a bind failure for (podName) on the given attempt count.
	// Returns true to fail this attempt. Models a transient/persistent failure.
	bindFails func(podName string, attempt int) bool
	nodeSeq   int
}

type pendingPod struct {
	pod     *v1.Pod
	backoff int // ticks remaining before this pod is eligible for a batch
	attempt int // bind attempts so far
}

func newDriver(f framework.Framework, offerings []*capacity.InstanceType, opts Options) *driver {
	return &driver{
		framework: f,
		offerings: offerings,
		opts:      opts,
		pending:   map[string]*pendingPod{},
		bound:     map[string]string{},
	}
}

func (d *driver) submit(pods ...*v1.Pod) {
	for _, p := range pods {
		d.pending[p.Name] = &pendingPod{pod: p}
	}
}

// composeBatch selects the pods eligible this tick: pending, not backed off.
// Decrements backoff for the rest. This is the caller's batch-composition policy;
// backoff-driven exclusion of failed pods lives here, not in Schedule().
func (d *driver) composeBatch() []*v1.Pod {
	var batch []*v1.Pod
	for _, pp := range d.pending {
		if pp.backoff > 0 {
			pp.backoff--
			continue
		}
		batch = append(batch, pp.pod)
	}
	return batch
}

// tick runs one scheduling round: compose a batch, Schedule it against current
// real state (existing launched nodes as fixed in-flight capacity), then execute
// — committing binds (with injected failures) and "launching" NodeClaims.
func (d *driver) tick(ctx context.Context) {
	batch := d.composeBatch()
	if len(batch) == 0 {
		return
	}

	in := Input{
		Pods:         batch,
		ClusterState: &ClusterState{Nodes: d.launched}, // committed nodes are fixed capacity
		Offerings:    d.offerings,
	}
	result, err := Schedule(ctx, d.framework, in, d.opts)
	if err != nil {
		return
	}

	// Execute bindings. A failed bind backs the pod off and leaves it pending;
	// a successful bind commits and removes it from pending.
	for _, b := range result.Bindings {
		pp := d.pending[b.Pod.Name]
		if pp == nil {
			continue
		}
		pp.attempt++
		if d.bindFails != nil && d.bindFails(b.Pod.Name, pp.attempt) {
			// Failure → back off (exponential-ish), stay pending, exclude next ticks.
			pp.backoff = pp.attempt // grows with attempts
			continue
		}
		d.bound[b.Pod.Name] = b.NodeName
		delete(d.pending, b.Pod.Name)
	}

	// "Launch" NodeClaims: each becomes a fixed in-flight node, and its pods are
	// committed (removed from pending). This models the provisioning external
	// commit that re-batching must treat as fixed, not reopen.
	for _, nc := range result.NodeClaims {
		node := d.materialize(nc)
		d.launched = append(d.launched, node)
		for _, p := range nc.Pods {
			d.bound[p.Name] = node.Node().Name
			delete(d.pending, p.Name)
		}
	}
}

// materialize turns a NodeClaim into a concrete launched node (cheapest type).
func (d *driver) materialize(nc NodeClaimResult) fwk.NodeInfo {
	d.nodeSeq++
	name := fmt.Sprintf("launched-%d", d.nodeSeq)
	var alloc v1.ResourceList
	labels := map[string]string{}
	if len(nc.CompatibleInstanceTypes) > 0 {
		it := nc.CompatibleInstanceTypes[0]
		alloc = it.Allocatable()
		for k, req := range nc.Requirements {
			if req.Len() == 1 {
				labels[k] = req.Any()
			}
		}
	}
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status:     v1.NodeStatus{Allocatable: alloc},
	}
	ni := &testNodeInfo{node: node}
	return ni
}

// runUntilEmpty ticks until no pods remain pending or maxTicks is hit (livelock
// guard for the test). Returns ticks used.
func (d *driver) runUntilEmpty(ctx context.Context, maxTicks int) int {
	for t := 1; t <= maxTicks; t++ {
		if len(d.pending) == 0 {
			return t - 1
		}
		d.tick(ctx)
	}
	return maxTicks
}

// --- tests ---

// TestHarness_HappyPathSingleBatch: no failures, all pods placed in one tick.
func TestHarness_HappyPathSingleBatch(t *testing.T) {
	offering := makeMultiZoneInstanceType("m5.4xlarge", 16, 64, []string{"us-west-2a"}, "on-demand", 0.32)
	d := newDriver(newMarginalCostFramework(), []*capacity.InstanceType{offering}, Options{PackingWeight: 1.0})
	for i := 0; i < 8; i++ {
		d.submit(makePod(fmt.Sprintf("p%d", i), 4, 4096))
	}
	ticks := d.runUntilEmpty(context.Background(), 10)
	if len(d.pending) != 0 {
		t.Fatalf("expected all pods placed, %d still pending after %d ticks", len(d.pending), ticks)
	}
	t.Logf("PASS: 8 pods placed in %d tick(s), %d launched nodes", ticks, len(d.launched))
}

// TestHarness_TransientBindFailureRecovers: pod p3's first bind fails (transient),
// it backs off, and a later tick rebinds it. Proves failure memory = caller-side
// backoff, and re-batching naturally retries without solver state or node marking.
func TestHarness_TransientBindFailureRecovers(t *testing.T) {
	// Existing node big enough for everyone — pods bind, no provisioning.
	node := makeConcreteNodeInfo("existing", 64, 256, map[string]string{v1.LabelTopologyZone: "us-west-2a"})
	d := newDriver(newMarginalCostFramework(), nil, Options{})
	d.launched = []fwk.NodeInfo{node}
	for i := 0; i < 4; i++ {
		d.submit(makePod(fmt.Sprintf("p%d", i), 2, 2048))
	}
	// p3 fails its first bind attempt only (transient).
	d.bindFails = func(name string, attempt int) bool {
		return name == "p3" && attempt == 1
	}

	ticks := d.runUntilEmpty(context.Background(), 10)
	if len(d.pending) != 0 {
		t.Fatalf("expected all pods bound, %d pending after %d ticks", len(d.pending), ticks)
	}
	if d.bound["p3"] == "" {
		t.Fatal("p3 never bound")
	}
	// It must have taken >1 tick because p3 backed off after its failed attempt.
	if ticks < 2 {
		t.Fatalf("expected p3's failure to require a backoff+retry (>=2 ticks), got %d", ticks)
	}
	t.Logf("PASS: transient bind failure on p3 recovered via backoff+re-batch in %d ticks", ticks)
}

// TestHarness_PersistentBindFailureIsolated: pod p1 always fails to bind, but the
// other pods still all place. Proves a persistent failure backs off and stops
// poisoning batches while the rest of the workload proceeds — no whole-batch
// livelock, no node marking.
func TestHarness_PersistentBindFailureIsolated(t *testing.T) {
	node := makeConcreteNodeInfo("existing", 64, 256, map[string]string{v1.LabelTopologyZone: "us-west-2a"})
	d := newDriver(newMarginalCostFramework(), nil, Options{})
	d.launched = []fwk.NodeInfo{node}
	for i := 0; i < 4; i++ {
		d.submit(makePod(fmt.Sprintf("p%d", i), 2, 2048))
	}
	d.bindFails = func(name string, attempt int) bool { return name == "p1" } // never binds

	// Run a bounded number of ticks; p1 will keep failing and backing off.
	d.runUntilEmpty(context.Background(), 12)

	// p0, p2, p3 must be bound; only p1 remains pending (backed off).
	for _, n := range []string{"p0", "p2", "p3"} {
		if d.bound[n] == "" {
			t.Fatalf("%s should have bound despite p1's persistent failure", n)
		}
	}
	if _, stillPending := d.pending["p1"]; !stillPending {
		t.Fatal("p1 should still be pending (persistently failing)")
	}
	if len(d.pending) != 1 {
		t.Fatalf("only p1 should remain pending, got %d", len(d.pending))
	}
	t.Logf("PASS: persistent failure isolated to p1; p0/p2/p3 bound, p1 backed off (attempt=%d)", d.pending["p1"].attempt)
}

// TestHarness_EmittedNodeClaimBecomesFixedCapacity: a first batch provisions a
// node; a second batch's pod binds to that launched node instead of provisioning
// again — proving emitted NodeClaims fold into the next batch as fixed in-flight
// capacity rather than being reopened.
func TestHarness_EmittedNodeClaimBecomesFixedCapacity(t *testing.T) {
	offering := makeMultiZoneInstanceType("m5.4xlarge", 16, 64, []string{"us-west-2a"}, "on-demand", 0.32)
	d := newDriver(newMarginalCostFramework(), []*capacity.InstanceType{offering}, Options{PackingWeight: 1.0})

	// Tick 1: one pod, no existing nodes → provisions a node.
	d.submit(makePod("first", 4, 4096))
	d.tick(context.Background())
	if len(d.launched) != 1 {
		t.Fatalf("expected 1 launched node after tick 1, got %d", len(d.launched))
	}
	if d.bound["first"] == "" {
		t.Fatal("first pod not committed")
	}
	nodesAfterTick1 := len(d.launched)

	// Tick 2: a second small pod. It should bind to the launched node (which has
	// slack), NOT provision a second node.
	d.submit(makePod("second", 4, 4096))
	d.tick(context.Background())
	if d.bound["second"] == "" {
		t.Fatal("second pod not placed")
	}
	if len(d.launched) != nodesAfterTick1 {
		t.Fatalf("second pod should have bound to the fixed in-flight node, but a new node was launched (%d → %d)",
			nodesAfterTick1, len(d.launched))
	}
	if d.bound["second"] != d.bound["first"] {
		t.Fatalf("second pod should have landed on the same launched node as first (%s), got %s",
			d.bound["first"], d.bound["second"])
	}
	t.Logf("PASS: tick-2 pod bound to the tick-1 launched node (fixed in-flight capacity), no second launch")
}
