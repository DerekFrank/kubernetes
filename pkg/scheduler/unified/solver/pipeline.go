package solver

import (
	v1 "k8s.io/api/core/v1"

	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
	"k8s.io/kubernetes/pkg/scheduler/unified/virtualnode"
)

// Pipeline is the provisioning orchestrator: Batch → Split → Solve (fan-out) →
// PostSolve → Select. It owns concurrency and the fixed topology; the plugins
// (Solvers, Narrowers) stay pure. Split and PostSolve are pass-through for now
// (identity), so the live shape is: one split, fan out to all Solvers, Select the
// cheapest. Registering a second Solver turns on the portfolio with no API change.
type Pipeline struct {
	Solvers   []Solver              // fan-out set; one = singleton, Select is identity
	Narrowers []virtualnode.Narrower // constraint plugins every solver consults
}

// NewPipeline returns the default pipeline: greedy solver + built-in Narrowers.
func NewPipeline() *Pipeline {
	return &Pipeline{Solvers: []Solver{Greedy{}}}
}

// Run executes the pipeline over a batch of pods against the offerings, returning
// the winning Solution.
func (pl *Pipeline) Run(pods []*v1.Pod, offerings []*capacity.InstanceType) Solution {
	// Split: pass-through (one split = the whole batch). A real Split partitions on
	// hard incompatibility for parallelism; identity keeps the batch whole.
	splits := split(pods)

	var winners []Solution
	for _, splitPods := range splits {
		prob := Problem{Pods: splitPods, Offerings: offerings, Narrowers: pl.Narrowers}

		// Solve: fan out to every registered solver, collect all candidates.
		var candidates []Solution
		for _, s := range pl.Solvers {
			candidates = append(candidates, s.Solve(prob)...)
		}

		// PostSolve: pass-through (no refine/ensemble/prune yet).
		candidates = postSolve(prob, candidates)

		// Select: reduce to the best candidate (lowest derived cost).
		winners = append(winners, selectBest(candidates))
	}

	// Union the per-split winners into one Solution.
	return union(winners)
}

// split is the pass-through Split: the whole batch as a single sub-problem.
func split(pods []*v1.Pod) [][]*v1.Pod {
	if len(pods) == 0 {
		return nil
	}
	return [][]*v1.Pod{pods}
}

// postSolve is the pass-through PostSolve: candidates unchanged.
func postSolve(_ Problem, candidates []Solution) []Solution { return candidates }

// selectBest is the reduce: lowest Cost() wins (a solution that strands pods is
// penalized so it never beats one that places them). Returns an empty Solution if
// there are no candidates.
func selectBest(candidates []Solution) Solution {
	if len(candidates) == 0 {
		return Solution{}
	}
	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.Cost() < best.Cost() {
			best = c
		}
	}
	return best
}

// union merges per-split winning Solutions into one.
func union(sols []Solution) Solution {
	var out Solution
	for _, s := range sols {
		out.NodeClaims = append(out.NodeClaims, s.NodeClaims...)
		out.Bindings = append(out.Bindings, s.Bindings...)
		out.Unplaced = append(out.Unplaced, s.Unplaced...)
	}
	return out
}
