// Package capacity defines the types for representing potential capacity offerings.
// These mirror Karpenter's cloudprovider.InstanceType and Offering types — we define
// them locally to avoid vendoring the full Karpenter module into the kubernetes tree.
// In production these would be imported from sigs.k8s.io/karpenter/pkg/cloudprovider.
package capacity

import (
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

// Compatible checks whether these requirements are compatible with another set.
// Two sets are compatible if, for every shared key, their values intersect.
func (r Requirements) Compatible(other Requirements) bool {
	for key, req := range r {
		if otherReq, ok := other[key]; ok {
			if req.Intersect(otherReq).Len() == 0 {
				return false
			}
		}
	}
	return true
}

// Get returns the requirement for a key, or nil if not present.
func (r Requirements) Get(key string) *Requirement {
	return r[key]
}

// Requirement represents the allowed values for a single label key.
type Requirement struct {
	Key      string
	Operator v1.NodeSelectorOperator
	Values   sets.Set[string]
}

// NewRequirement creates a requirement with the given key, operator, and values.
func NewRequirement(key string, op v1.NodeSelectorOperator, values ...string) *Requirement {
	return &Requirement{
		Key:      key,
		Operator: op,
		Values:   sets.New[string](values...),
	}
}

// Intersect returns a new Requirement representing the intersection of values.
func (req *Requirement) Intersect(other *Requirement) *Requirement {
	if req == nil || other == nil {
		if req != nil {
			return req.Copy()
		}
		if other != nil {
			return other.Copy()
		}
		return nil
	}

	// Both are In operators — intersect value sets
	intersection := req.Values.Intersection(other.Values)
	return &Requirement{
		Key:      req.Key,
		Operator: v1.NodeSelectorOpIn,
		Values:   intersection,
	}
}

// Len returns the number of allowed values.
func (req *Requirement) Len() int {
	if req == nil {
		return 0
	}
	return req.Values.Len()
}

// Copy returns a deep copy of the requirement.
func (req *Requirement) Copy() *Requirement {
	if req == nil {
		return nil
	}
	return &Requirement{
		Key:      req.Key,
		Operator: req.Operator,
		Values:   sets.New[string](req.Values.UnsortedList()...),
	}
}

// Has returns whether the requirement contains the given value.
func (req *Requirement) Has(value string) bool {
	if req == nil {
		return false
	}
	return req.Values.Has(value)
}

// Any returns an arbitrary value from the requirement.
func (req *Requirement) Any() string {
	if req == nil || req.Values.Len() == 0 {
		return ""
	}
	for v := range req.Values {
		return v
	}
	return ""
}
