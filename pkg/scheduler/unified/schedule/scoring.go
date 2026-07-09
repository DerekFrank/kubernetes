package schedule

import (
	"math"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/unified/capacity"
)

// This file prototypes a generic "expand then score" model for evaluating a
// potential node (a superposition of offerings).
//
//   - An option is one concrete narrowing of the superposition, plus the
//     preference benefit accumulated by the narrowings that produced it.
//   - An expander turns each input option into one or more output options. A
//     soft-preference expander emits the option unchanged (don't honor the
//     preference) AND a narrowed variant (honor it, banking its benefit) when
//     the preference is satisfiable. Chaining one expander per preference
//     generates the full honor/don't-honor lattice as a cartesian product —
//     order-free, because expansion is a product, not a sequence.
//   - A scorer is pure: option -> dollars (lower is better). Price and
//     flexibility are scorers. They read an option's resulting offering set and
//     never mutate it, so they compose in any order.
//
// Selection over a candidate is: seed one option = the whole superposition,
// run every expander, score every leaf option, take the min. The winning option
// carries the constraints to pin at commit — "score then constrain" falls out
// because the constraint is baked into the option that won.

// option is one narrowing of a superposition. It carries no benefit: benefit is
// a pure function of the resulting set (preferenceScorer), so identical sets
// reached via different narrowings score identically.
type option struct {
	reqs  capacity.Requirements
	types []*capacity.InstanceType
}

// expander maps options to (usually more) options. An expander is a pure SET
// GENERATOR: it produces alternative narrowings of its inputs. It does not score
// or bank benefit — benefit is a property of the resulting set (see
// preferenceScorer), so it belongs to scoring, not generation.
type expander interface {
	expand(in []option) []option
}

// scorer is a pure option -> cost ($/hr). Lower is better. Benefits (preference,
// flexibility) are returned as negative costs. A scorer reads only the option's
// resulting offering set, so scorers are order-free and individually parallelizable.
type scorer interface {
	score(o option) float64
}

// softPrefExpander emits each input option both unchanged (don't honor the
// preference) and narrowed to honor it, when satisfiable. Chaining one per
// preferred term builds the honor/don't-honor lattice. It banks no benefit —
// the preferenceScorer credits whatever the resulting set guarantees, so two
// options that reach the same set (e.g. pin-arm vs pin-zone when arm⟺zone are
// correlated) score identically and dedupe.
type softPrefExpander struct {
	term preferredTerm
}

func (e softPrefExpander) expand(in []option) []option {
	out := make([]option, 0, len(in)*2)
	for _, o := range in {
		out = append(out, o) // branch: don't honor
		if narrowed, ok := applyPreference(o, e.term); ok {
			out = append(out, narrowed) // branch: honor (pinned)
		}
	}
	return out
}

// applyPreference pins the option to ALL the preferred values its surviving
// types satisfy (keeping every sibling type that matches — the multi-type pin),
// then filters incompatible types. Returns false if nothing survives.
func applyPreference(o option, p preferredTerm) (option, bool) {
	// Which preferred values are actually achievable in this option?
	achievable := make([]string, 0, len(p.values))
	for v := range p.values {
		for _, it := range o.types {
			if req := it.Requirements.Get(p.key); req != nil && req.Has(v) {
				achievable = append(achievable, v)
				break
			}
		}
	}
	if len(achievable) == 0 {
		return option{}, false
	}

	newReqs := capacity.NewRequirements()
	newReqs.Add(o.reqs)
	newReqs.Add(capacity.Requirements{
		p.key: capacity.NewRequirement(p.key, v1.NodeSelectorOpIn, achievable...),
	})

	var newTypes []*capacity.InstanceType
	for _, it := range o.types {
		if newReqs.Compatible(it.Requirements) {
			newTypes = append(newTypes, it)
		}
	}
	if len(newTypes) == 0 {
		return option{}, false
	}
	return option{reqs: newReqs, types: newTypes}, true
}

// priceScorer scores an option by the cheapest available offering compatible
// with its (possibly pinned) requirements, then applies any soft-preference
// discount the option's set GUARANTEES (D5: preferences are a multiplicative
// adjustment *on the cost axis*, not a separate additive benefit).
//
// Why multiplicative on price, not additive benefit: a discount on a price scales
// with the price, so it can rank purchasable offerings ("arm is 20% better →
// effective cost arm×0.8") but it can NEVER make a candidate cheaper than a free
// bind (0 × anything = 0). That is the desired property — a soft preference must
// not justify launching a whole new node (if it should, it isn't soft; make it a
// hard requirement). The guarantee logic (`o.guarantees`) is unchanged from the
// additive model; only the arithmetic differs.
type priceScorer struct {
	prefs    []preferredTerm
	discount float64 // discount fraction per unit of guaranteed preference weight; 0 disables
}

const maxPreferenceDiscount = 0.95 // never free/negative, so preference can't out-rank a true free bind

func (p priceScorer) score(o option) float64 {
	price, found := cheapestOfferingPrice(o.reqs, o.types)
	if !found {
		return math.Inf(1)
	}
	if p.discount > 0 && len(p.prefs) > 0 {
		var weight int32
		for _, term := range p.prefs {
			if o.guarantees(term) {
				weight += term.weight
			}
		}
		if weight > 0 {
			d := float64(weight) * p.discount
			if d > maxPreferenceDiscount {
				d = maxPreferenceDiscount
			}
			price *= 1 - d
		}
	}
	return price
}

// flexibilityScorer values fallback diversity: the more failure-independent
// pools (zone × capacity-type) an option retains, the lower (better) its score.
// Narrowing reduces pools, so over-constraining self-penalizes. Diminishing
// (log) so the costly step is collapsing toward a single point of failure.
type flexibilityScorer struct {
	value float64 // $/hr value of full flexibility; 0 disables
}

func (f flexibilityScorer) score(o option) float64 {
	if f.value == 0 {
		return 0
	}
	pools := distinctPools(o.reqs, o.types)
	// benefit (negative cost) that saturates as pools grow.
	return -f.value * math.Log2(1+float64(pools))
}

// guarantees reports whether the term is GUARANTEED for any node materialized
// from this option. A node's value for the key is gated by the intersection of
// the option's requirements and the chosen type's requirements; the term is
// guaranteed iff, for every surviving type, that gated value set is a non-empty
// subset of the term's preferred values. Pinning the option's requirement to the
// preferred values (e.g. zone In [c]) is what makes this hold — a wider gate
// (zone In [a,b,c]) could resolve to a non-preferred value, so it does not.
func (o option) guarantees(term preferredTerm) bool {
	optReq := o.reqs.Get(term.key)
	for _, it := range o.types {
		// Effective permitted values = option gate ∩ type requirement.
		typeReq := it.Requirements.Get(term.key)
		eff := effectiveValues(optReq, typeReq)
		if len(eff) == 0 {
			return false
		}
		for v := range eff {
			if _, ok := term.values[v]; !ok {
				return false // could resolve to a non-preferred value
			}
		}
	}
	return true
}

// effectiveValues returns the permitted values for a key given an option-level
// gate and a type-level requirement. A nil requirement means "unconstrained on
// this key" and does not narrow; if both are nil the key is unconstrained and we
// return nil (caller treats empty as "not guaranteed").
func effectiveValues(a, b *capacity.Requirement) sets.Set[string] {
	switch {
	case a == nil && b == nil:
		return nil
	case a == nil:
		return b.Values()
	case b == nil:
		return a.Values()
	default:
		return a.Values().Intersection(b.Values())
	}
}

// totalScore sums the pure scorers (lower is better).
func totalScore(o option, scorers []scorer) float64 {
	total := 0.0
	for _, s := range scorers {
		total += s.score(o)
	}
	return total
}

// bestOption expands the seed through all expanders, dedupes options that
// resolved to the same offering set (correlated preference axes collapse here),
// and returns the lowest-scoring option plus its score.
func bestOption(seed option, expanders []expander, scorers []scorer) (option, float64) {
	opts := []option{seed}
	for _, e := range expanders {
		opts = e.expand(opts)
	}

	seen := make(map[string]struct{}, len(opts))
	var best option
	bestScore := math.Inf(1)
	first := true
	for _, o := range opts {
		key := optionKey(o)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		if s := totalScore(o, scorers); first || s < bestScore {
			best, bestScore, first = o, s, false
		}
	}
	return best, bestScore
}

// optionKey identifies an option by the offering set it resolves to: the
// surviving instance types AND the requirement values that gate which offerings
// are usable. Two narrowings dedupe only if both match (e.g. pin-arm vs pin-zone
// when arm⟺zone-a yields the same types and the same gated offerings). Pinning a
// requirement that changes which offerings survive must NOT dedupe against the
// wider option, even when the type set is unchanged.
func optionKey(o option) string {
	names := make([]string, 0, len(o.types))
	for _, it := range o.types {
		names = append(names, it.Name)
	}
	sort.Strings(names)

	reqParts := make([]string, 0, len(o.reqs))
	for key, req := range o.reqs {
		if key == v1.LabelHostname {
			continue
		}
		vals := req.Values().UnsortedList()
		sort.Strings(vals)
		reqParts = append(reqParts, key+"="+strings.Join(vals, "|"))
	}
	sort.Strings(reqParts)

	return strings.Join(names, ",") + ";" + strings.Join(reqParts, ";")
}

// --- shared offering helpers ---

func cheapestOfferingPrice(reqs capacity.Requirements, types []*capacity.InstanceType) (float64, bool) {
	price := 0.0
	found := false
	for _, it := range types {
		for _, off := range it.Offerings {
			if !off.Available || !reqs.Compatible(off.Requirements) {
				continue
			}
			// EffectivePrice folds in performance-value (offering data), so a
			// better-but-equally-priced offering wins the cost axis with no scoring term.
			if ep := off.EffectivePrice(); !found || ep < price {
				price, found = ep, true
			}
		}
	}
	return price, found
}

// cheapestTypeAndPrice returns the instance type the option would resolve to (the
// one with the cheapest compatible available offering) and that price.
func cheapestTypeAndPrice(reqs capacity.Requirements, types []*capacity.InstanceType) (*capacity.InstanceType, float64, bool) {
	var best *capacity.InstanceType
	price := 0.0
	found := false
	for _, it := range types {
		for _, off := range it.Offerings {
			if !off.Available || !reqs.Compatible(off.Requirements) {
				continue
			}
			if ep := off.EffectivePrice(); !found || ep < price {
				best, price, found = it, ep, true
			}
		}
	}
	return best, price, found
}

// packingCredit returns the lookahead discount for choosing a node that resolves
// to `chosen` at `price`, given `usedCPU` already committed to it (this pod
// included) and `remainingCPU` of still-unplaced batch demand. It values the
// node's *fillable* headroom — capacity beyond what's used that remaining pods
// can occupy for free — at its per-unit price, scaled by PackingWeight.
//
// Intuition: a fresh tight node and a grown node cost the same per CPU; the fresh
// node only looks cheaper because one pod is billed the whole instance jump. This
// credit refunds the part of that jump that future pods will fill, so growing to
// absorb pending demand beats opening tight nodes. The credit shrinks to zero as
// the batch drains (no remaining pods → no headroom value → don't oversize).
func packingCredit(chosen *capacity.InstanceType, price, usedCPU, remainingCPU, weight float64) float64 {
	if weight == 0 || chosen == nil || price <= 0 {
		return 0
	}
	capCPU := float64(chosen.Capacity.Cpu().MilliValue())
	if capCPU <= 0 {
		return 0
	}
	headroom := capCPU - usedCPU
	if headroom <= 0 {
		return 0
	}
	// Only headroom that remaining demand can actually fill is valuable.
	fillable := headroom
	if remainingCPU < fillable {
		fillable = remainingCPU
	}
	if fillable <= 0 {
		return 0
	}
	perUnit := price / capCPU // $/milliCPU
	return weight * perUnit * fillable
}

// distinctPools counts distinct available (zone × capacity-type) combinations
// across an option's offerings — a proxy for failure-independent fallbacks.
func distinctPools(reqs capacity.Requirements, types []*capacity.InstanceType) int {
	seen := make(map[string]struct{})
	for _, it := range types {
		for _, off := range it.Offerings {
			if !off.Available || !reqs.Compatible(off.Requirements) {
				continue
			}
			zone, ct := "", ""
			if r := off.Requirements.Get(v1.LabelTopologyZone); r != nil {
				zone = r.Any()
			}
			if r := off.Requirements.Get("karpenter.sh/capacity-type"); r != nil {
				ct = r.Any()
			}
			seen[zone+"/"+ct] = struct{}{}
		}
	}
	return len(seen)
}
