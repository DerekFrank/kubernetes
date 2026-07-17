// Package capacity defines the types for representing potential capacity offerings.
// These mirror Karpenter's cloudprovider.InstanceType and Offering types — we define
// them locally to avoid vendoring the full Karpenter module into the kubernetes tree.
// In production these would be imported from sigs.k8s.io/karpenter/pkg/cloudprovider.
package capacity

import (
	"math"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
)

// InstanceType represents a purchasable instance type with its resource capacity,
// label requirements, and available offerings (zone × capacity-type combinations).
type InstanceType struct {
	Name         string
	Requirements Requirements
	Offerings    []*Offering
	Capacity     v1.ResourceList
}

// MemoryOverheadBytes is a FIXED per-node memory reservation (kubelet,
// system-reserved, OS, DaemonSets) that is NOT allocatable to pods, regardless of
// instance size.
//
// This is a deliberately crude model. In real instance types overhead is far more
// complicated — it has both fixed and size-scaling components, splits across CPU /
// memory / ephemeral-storage / pod-count, varies by instance family and by the
// kubelet/OS/CNI in use, and kube-reserved scales in tiers rather than smoothly.
//
// We model only a fixed absolute amount because the FIXED component is what makes
// bin-packing economically rational: a flat 1Gi tax is negligible on a 256Gi node
// but lethal on a 2Gi one, so consolidating many small nodes onto one large node
// reclaims the overhead each small node paid. (A percentage overhead, by contrast,
// has no economy of scale — the fraction lost is identical at every size — so it
// does not drive consolidation at all.)
const MemoryOverheadBytes = 1 << 30 // 1Gi

// Allocatable returns the instance type's resources available to pods, i.e.
// Capacity minus per-node overhead. Only a fixed memory reservation is modeled
// (see MemoryOverheadBytes); memory below the reservation floors at zero.
func (it *InstanceType) Allocatable() v1.ResourceList {
	alloc := make(v1.ResourceList, len(it.Capacity))
	for name, qty := range it.Capacity {
		if name == v1.ResourceMemory {
			usable := qty.Value() - MemoryOverheadBytes
			if usable < 0 {
				usable = 0
			}
			alloc[name] = *resource.NewQuantity(usable, qty.Format)
			continue
		}
		alloc[name] = qty
	}
	return alloc
}

// Offering represents a purchasable instance in a specific zone and capacity type.
type Offering struct {
	Requirements Requirements
	Price        float64
	Available    bool

	// PerformanceValue is the intrinsic worth of this offering's hardware relative
	// to its price — e.g. "this generation is 20% better" is PerformanceValue 0.20.
	// It is OFFERING data, not a workload preference: the value of a chip generation
	// is a property of the capacity, not of the pod that lands on it. It discounts
	// the price the solver compares (see EffectivePrice), so a better-but-equally-
	// priced offering wins on the cost axis without any separate scoring term or
	// workload-authored exchange rate. Zero means "no adjustment" (price as-is).
	PerformanceValue float64
}

// EffectivePrice is the price the solver and Select compare on: the sticker Price
// discounted by PerformanceValue (capped so it never goes negative). This is the
// single place performance-value enters the cost axis — there is no separate Cost
// plugin; cost is offering data.
func (o *Offering) EffectivePrice() float64 {
	v := o.PerformanceValue
	if v < 0 {
		v = 0
	}
	if v > 1 {
		v = 1
	}
	return o.Price * (1 - v)
}

// Requirements maps label keys to allowed values. This is a simplified version of
// Karpenter's scheduling.Requirements — enough for the POC to demonstrate constraint
// narrowing without importing the full Karpenter scheduling package.
type Requirements map[string]*Requirement

// NewRequirements creates an empty requirements set.
func NewRequirements() Requirements {
	return make(Requirements)
}

// Add intersects the given requirements into this set. For each key, if the key
// already exists, the values are intersected. If the key is new, it's added.
func (r Requirements) Add(others ...Requirements) {
	for _, other := range others {
		for key, req := range other {
			if existing, ok := r[key]; ok {
				r[key] = existing.Intersect(req)
			} else {
				r[key] = req.Copy()
			}
		}
	}
}

// Compatible checks whether these requirements are compatible with another set:
// for every shared key, the two requirements must have a non-empty intersection —
// EXCEPT that two "exclusion" requirements (NotIn/DoesNotExist on both sides) are
// always compatible, since a value excluded by both can still exist (Karpenter's
// Intersects asymmetry). Without this, e.g. `arch NotIn [arm64]` vs
// `arch NotIn [ppc64]` would be wrongly rejected.
func (r Requirements) Compatible(other Requirements) bool {
	for key, req := range r {
		otherReq, ok := other[key]
		if !ok {
			continue
		}
		if req.HasIntersection(otherReq) {
			continue
		}
		if isExclusion(req.Operator()) && isExclusion(otherReq.Operator()) {
			continue
		}
		return false
	}
	return true
}

func isExclusion(op v1.NodeSelectorOperator) bool {
	return op == v1.NodeSelectorOpNotIn || op == v1.NodeSelectorOpDoesNotExist
}

// Get returns the requirement for a key, or nil if not present.
func (r Requirements) Get(key string) *Requirement {
	return r[key]
}

// Requirement is an efficient representation of a v1.NodeSelectorRequirement,
// ported from Karpenter's scheduling.Requirement (karpenter/pkg/scheduling/
// requirement.go). It represents ALL node-selector operators, not just In:
//
//   - In:            complement=false, values={a,b}   → "one of {a,b}"
//   - NotIn:         complement=true,  values={a,b}   → "anything except {a,b}"
//   - Exists:        complement=true,  values={}      → "any value (key present)"
//   - DoesNotExist:  complement=false, values={}      → "no value (key absent)"
//   - Gt/Lt:         complement=true with gte/lte bounds (canonicalized to Gte/Lte)
//
// The `complement` flag is what lets NotIn/Exists represent an *infinite* permitted
// set (everything except the finite excluded `values`), so intersection over any
// mix of operators is well-defined. An In-only representation cannot express NotIn
// and silently drops it — which produced infeasible NodeClaims that then won the
// cost argmin. This port fixes that.
type Requirement struct {
	Key        string
	complement bool
	values     sets.Set[string]
	gte        *int // inclusive lower bound (Gt canonicalized to Gte)
	lte        *int // inclusive upper bound (Lt canonicalized to Lte)
}

// NewRequirement creates a requirement for the given key, operator, and values.
// Gt/Lt are canonicalized to inclusive Gte/Lte bounds.
func NewRequirement(key string, operator v1.NodeSelectorOperator, values ...string) *Requirement {
	// Common case: In — inline it.
	if operator == v1.NodeSelectorOpIn {
		return &Requirement{Key: key, values: sets.New[string](values...), complement: false}
	}
	r := &Requirement{Key: key, values: sets.New[string](), complement: true}
	if operator == v1.NodeSelectorOpDoesNotExist {
		r.complement = false
	}
	if operator == v1.NodeSelectorOpNotIn {
		r.values.Insert(values...)
	}
	switch operator {
	case v1.NodeSelectorOpGt:
		v, _ := strconv.Atoi(values[0])
		v++ // canonicalize Gt N to inclusive lower bound N+1
		r.gte = &v
	case v1.NodeSelectorOpLt:
		v, _ := strconv.Atoi(values[0])
		v-- // canonicalize Lt N to inclusive upper bound N-1
		r.lte = &v
	}
	return r
}

// Intersect returns a new Requirement constrained by both req and other. Handles
// all four complement×complement combinations plus numeric bounds. A nil operand
// is treated as "unconstrained" (returns a copy of the other).
func (req *Requirement) Intersect(other *Requirement) *Requirement {
	if req == nil {
		return other.Copy()
	}
	if other == nil {
		return req.Copy()
	}

	complement := req.complement && other.complement
	gte := maxIntPtr(req.gte, other.gte)
	lte := minIntPtr(req.lte, other.lte)
	if gte != nil && lte != nil && *gte > *lte {
		return NewRequirement(req.Key, v1.NodeSelectorOpDoesNotExist)
	}

	var values sets.Set[string]
	switch {
	case req.complement && other.complement:
		values = req.values.Union(other.values)
	case req.complement && !other.complement:
		values = other.values.Difference(req.values)
	case !req.complement && other.complement:
		values = req.values.Difference(other.values)
	default:
		values = req.values.Intersection(other.values)
	}
	for v := range values {
		if !withinBounds(v, gte, lte) {
			values.Delete(v)
		}
	}
	if !complement {
		gte, lte = nil, nil // bounds only meaningful for the infinite (complement) set
	}
	return &Requirement{Key: req.Key, values: values, complement: complement, gte: gte, lte: lte}
}

// Union returns a new Requirement permitting any value either req or other permits.
// Used to widen a claim's domain across sibling instance types (the union of what
// each type could satisfy). Handles complement sets: the union of two "everything
// except X" / "everything except Y" sets is "everything except (X ∩ Y)".
func (req *Requirement) Union(other *Requirement) *Requirement {
	if req == nil {
		return other.Copy()
	}
	if other == nil {
		return req.Copy()
	}
	switch {
	case req.complement && other.complement:
		// (¬A) ∪ (¬B) = ¬(A ∩ B): excluded only what BOTH exclude.
		return &Requirement{Key: req.Key, complement: true, values: req.values.Intersection(other.values)}
	case req.complement && !other.complement:
		// (¬A) ∪ B = ¬(A \ B): still infinite, exclude what A excludes but B doesn't add back.
		return &Requirement{Key: req.Key, complement: true, values: req.values.Difference(other.values)}
	case !req.complement && other.complement:
		return &Requirement{Key: req.Key, complement: true, values: other.values.Difference(req.values)}
	default:
		return &Requirement{Key: req.Key, complement: false, values: req.values.Union(other.values)}
	}
}

// HasIntersection reports whether req and other share any permitted value, without
// materializing the intersection set.
func (req *Requirement) HasIntersection(other *Requirement) bool {
	gte := maxIntPtr(req.gte, other.gte)
	lte := minIntPtr(req.lte, other.lte)
	if gte != nil && lte != nil && *gte > *lte {
		return false
	}
	switch {
	case req.complement && other.complement:
		return true // two infinite sets always overlap
	case req.complement && !other.complement:
		for v := range other.values {
			if !req.values.Has(v) && withinBounds(v, gte, lte) {
				return true
			}
		}
		return false
	case !req.complement && other.complement:
		for v := range req.values {
			if !other.values.Has(v) && withinBounds(v, gte, lte) {
				return true
			}
		}
		return false
	default:
		for v := range req.values {
			if other.values.Has(v) && withinBounds(v, gte, lte) {
				return true
			}
		}
		return false
	}
}

// Operator reconstructs the node-selector operator this requirement represents.
func (req *Requirement) Operator() v1.NodeSelectorOperator {
	if req.complement {
		if req.values.Len() > 0 {
			return v1.NodeSelectorOpNotIn
		}
		return v1.NodeSelectorOpExists
	}
	if req.values.Len() > 0 {
		return v1.NodeSelectorOpIn
	}
	return v1.NodeSelectorOpDoesNotExist
}

// Len returns the number of permitted values. For complement (infinite) sets this
// is effectively unbounded; we report a large sentinel so "narrowed to a single
// value" checks (Len()==1) behave correctly for In requirements.
func (req *Requirement) Len() int {
	if req == nil {
		return 0
	}
	if req.complement {
		return math.MaxInt32 - req.values.Len()
	}
	return req.values.Len()
}

// Copy returns a deep copy of the requirement.
func (req *Requirement) Copy() *Requirement {
	if req == nil {
		return nil
	}
	var gte, lte *int
	if req.gte != nil {
		v := *req.gte
		gte = &v
	}
	if req.lte != nil {
		v := *req.lte
		lte = &v
	}
	return &Requirement{
		Key:        req.Key,
		complement: req.complement,
		values:     sets.New[string](req.values.UnsortedList()...),
		gte:        gte,
		lte:        lte,
	}
}

// Has reports whether the requirement permits the given value.
func (req *Requirement) Has(value string) bool {
	if req == nil {
		return false
	}
	if req.complement {
		return !req.values.Has(value) && withinBounds(value, req.gte, req.lte)
	}
	return req.values.Has(value) && withinBounds(value, req.gte, req.lte)
}

// Values returns the concrete permitted value set for an In requirement (the common
// case used for enumerating domains/labels). For a complement requirement the
// permitted set is infinite, so this returns the *excluded* set; callers that
// enumerate domains operate on In requirements, where this is the permitted set.
func (req *Requirement) Values() sets.Set[string] {
	if req == nil {
		return nil
	}
	return req.values
}

// Any returns an arbitrary permitted value, or "" if none/infinite-without-members.
func (req *Requirement) Any() string {
	if req == nil || req.complement {
		return ""
	}
	for v := range req.values {
		return v
	}
	return ""
}

func withinBounds(valueAsString string, gte, lte *int) bool {
	if gte == nil && lte == nil {
		return true
	}
	val, err := strconv.Atoi(valueAsString)
	if err != nil {
		return false // non-integer value can't satisfy a numeric bound
	}
	if gte != nil && val < *gte {
		return false
	}
	if lte != nil && val > *lte {
		return false
	}
	return true
}

func minIntPtr(a, b *int) *int {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case *a < *b:
		return a
	default:
		return b
	}
}

func maxIntPtr(a, b *int) *int {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case *a > *b:
		return a
	default:
		return b
	}
}
