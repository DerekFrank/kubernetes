package virtualnode

import (
	"time"

	v1 "k8s.io/api/core/v1"

	"k8s.io/kubernetes/pkg/scheduler/provisioning/capacity"
)

// The fire boundary (preferred-architecture.md §"The fire boundary is a one-way
// gate"). A NodeClaim is a MUTABLE superposition until it fires — is handed to the
// cloud provider — and an IMMUTABLE commitment after. That physical fact ("you
// cannot tell the provider 'launch a 32xl' and then mid-request say 'actually a
// 16xl'") splits every membership-change question cleanly:
//
//   - Pre-fire: nothing is purchased, so the claim's domain is provisional. A pod
//     joining narrows it; a pod leaving UnNarrows it (recompute-from-members).
//     Re-widening is free and a re-solve is correct by construction.
//   - Post-fire: the claim is effectively a real node that hasn't registered yet.
//     Its domain is committed and frozen. A pod leaving just leaves slack on an
//     oversized node — the scheduler does NOT re-solve, shrink, or cancel
//     (that is consolidation's job, later, on its own loop). Narrow/UnNarrow are
//     rejected (see Narrow in virtualnode.go, and UnNarrow below).
//
// The dangerous case — a re-solve re-narrowing the claim to a different domain and
// stranding a staying pod — therefore exists ONLY pre-fire, where it is harmless,
// and is structurally impossible post-fire (no re-solve happens).

// Fired reports whether the claim has crossed the fire boundary.
func (pn *PotentialNode) Fired() bool { return pn.fired }

// Fire crosses the one-way gate: the claim is handed to the cloud provider and
// becomes an immutable commitment. After Fire, Narrow and UnNarrow are rejected;
// only occupancy (AddPod/RemovePod) still changes, and a departing pod leaves slack
// rather than triggering a re-solve. Idempotent.
func (pn *PotentialNode) Fire() {
	pn.fired = true
}

// UnNarrow recomputes the claim's requirements and surviving instance types from its
// remaining members — the one new primitive the preferred architecture requires
// (preferred-architecture.md §"UnNarrow()"). It is the INVERSE of "narrow a pod in,"
// computed by REBUILD rather than by subtraction: narrowing is a monotone
// intersection and records no per-pod attribution, so reversing one pod's
// contribution is not well-defined. Instead we rebuild from the birth ⊤ (the widest
// requirements + full catalog the claim was born with) and re-narrow each surviving
// member. The result is exactly what the claim would be if built fresh from its
// current members — correct by construction, so a re-solve that would strand a
// staying member simply doesn't release the axis that member pinned.
//
// It runs ONLY pre-fire (a fired claim is frozen — a departing pod leaves slack, and
// the purchase stands). Post-fire it is a no-op returning an error. If the member set
// is empty the claim dissolves to the birth ⊤ (the caller drops a member-less claim).
// O(members × constraints), claim-sized.
func (pn *PotentialNode) UnNarrow(narrowers ...Narrower) error {
	if pn.fired {
		return errFired(pn.hostname)
	}
	if len(narrowers) == 0 {
		narrowers = DefaultNarrowers()
	}

	// Rebuild from birth ⊤: widest requirements + full catalog, hostname preserved.
	reqs := capacity.NewRequirements()
	reqs.Add(pn.birthReqs)
	reqs[v1.LabelHostname] = capacity.NewRequirement(v1.LabelHostname, v1.NodeSelectorOpIn, pn.hostname)
	pn.Requirements = reqs
	pn.InstanceTypes = append([]*capacity.InstanceType(nil), pn.birthTypes...)

	// Re-narrow each surviving member back in. Requested is recomputed from scratch
	// so it tracks the (possibly reduced) member set exactly.
	pn.requested = v1.ResourceList{}
	survivors := pn.pods
	pn.pods = nil
	for _, pi := range survivors {
		pod := pi.GetPod()
		for _, n := range narrowers {
			if !n.Hard() {
				continue
			}
			if status := n.Narrow(pod, pn); !status.IsSuccess() {
				// A survivor no longer fits the recomputed claim. This should not
				// happen for a claim that legitimately held all these members, but if
				// it does we keep the pod recorded (it stays a member) rather than
				// silently dropping it — recompute-from-members must not lose members.
				continue
			}
		}
		pn.AddPodInfo(pi)
	}
	pn.generation++
	pn.representative = nil
	return nil
}

func errFired(hostname string) error {
	return &firedError{hostname: hostname}
}

type firedError struct{ hostname string }

func (e *firedError) Error() string {
	return "cannot unnarrow " + e.hostname + ": claim has fired (immutable post-fire)"
}

// FireTimer implements the demand-driven debounce fire policy
// (preferred-architecture.md §"When to fire"). A claim fires T-quiet seconds after
// its LAST membership change (add OR leave), bounded by a T-max ceiling so a claim
// that keeps accreting pods can't starve launch indefinitely. This makes the "batch"
// whatever accumulated on the claim before it went quiet — per-claim, no global
// barrier — which fits a per-pod cycle with no global batch step.
//
// It is deliberately pure: every method takes `now` explicitly rather than reading
// the wall clock, so the fire decision is testable and matches the engine's no-I/O
// ethos. A live caller passes time.Now(); tests pass synthetic times.
type FireTimer struct {
	firstChange time.Time // when the claim first accreted a member (T-max anchor)
	lastChange  time.Time // when the claim last changed membership (T-quiet anchor)
	started     bool
}

// Touch records a membership change (add or leave) at `now`, resetting the T-quiet
// window. The first Touch also anchors the T-max ceiling.
func (ft *FireTimer) Touch(now time.Time) {
	if !ft.started {
		ft.firstChange = now
		ft.started = true
	}
	ft.lastChange = now
}

// ShouldFire reports whether the claim should fire at `now`: either it has been quiet
// for `quiet` since the last membership change, OR it has been open for `max` since
// the first change (the ceiling), whichever comes first. Returns false before any
// membership change (an empty claim never fires).
func (ft *FireTimer) ShouldFire(now time.Time, quiet, max time.Duration) bool {
	if !ft.started {
		return false
	}
	if now.Sub(ft.lastChange) >= quiet {
		return true
	}
	if max > 0 && now.Sub(ft.firstChange) >= max {
		return true
	}
	return false
}
