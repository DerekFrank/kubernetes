/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workqueue

import (
	"math"
	"strconv"
	"testing"
	"time"
)

var (
	randMin = func(int64) int64 { return 0 }
	randMax = func(n int64) int64 { return n - 1 }
)

func TestItemExponentialFullJitterRateLimiter(t *testing.T) {
	limiter := NewTypedItemExponentialFullJitterRateLimiter[string](1*time.Millisecond, 10*time.Millisecond).(*TypedItemExponentialFullJitterRateLimiter[string])
	limiter.int64N = randMax

	for _, e := range []time.Duration{1, 2, 4, 8, 10, 10} {
		if e, a := e*time.Millisecond-1, limiter.When("one"); e != a {
			t.Errorf("expected %v, got %v", e, a)
		}
	}
	if e, a := 6, limiter.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	limiter.int64N = randMin
	if e, a := time.Duration(0), limiter.When("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 1, limiter.NumRequeues("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	limiter.Forget("one")
	if e, a := 0, limiter.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	limiter.int64N = randMax
	if e, a := 1*time.Millisecond-1, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
}

func TestItemExponentialFullJitterRateLimiterOverflow(t *testing.T) {
	limiter := NewTypedItemExponentialFullJitterRateLimiter[string](1*time.Millisecond, 1000*time.Second).(*TypedItemExponentialFullJitterRateLimiter[string])
	limiter.int64N = randMax
	for i := 0; i < 100; i++ {
		limiter.When("one")
	}
	if e, a := 1000*time.Second-1, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	limiter = NewTypedItemExponentialFullJitterRateLimiter[string](1*time.Minute, math.MaxInt64).(*TypedItemExponentialFullJitterRateLimiter[string])
	limiter.int64N = randMax
	for i := 0; i < 100; i++ {
		limiter.When("two")
	}
	if e, a := time.Duration(math.MaxInt64-1), limiter.When("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
}

func TestItemDecorrelatedJitterRateLimiter(t *testing.T) {
	limiter := NewTypedItemDecorrelatedJitterRateLimiter[string](1*time.Millisecond, 100*time.Millisecond).(*TypedItemDecorrelatedJitterRateLimiter[string])
	limiter.int64N = randMax

	// Each delay is just under 3x the previous one until it reaches maxDelay.
	prev := 1 * time.Millisecond
	for i := 0; i < 10; i++ {
		e := min(100*time.Millisecond, 3*prev-1)
		if a := limiter.When("one"); e != a {
			t.Errorf("attempt %d: expected %v, got %v", i, e, a)
		}
		prev = e
	}
	if e, a := 10, limiter.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	// The lowest possible delay is always baseDelay, even after growing.
	limiter.int64N = randMin
	if e, a := 1*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	// And the next ceiling is derived from that low value, not the failure count.
	limiter.int64N = randMax
	if e, a := 3*time.Millisecond-1, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	limiter.Forget("one")
	if e, a := 0, limiter.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 3*time.Millisecond-1, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
}

func TestItemDecorrelatedJitterRateLimiterOverflow(t *testing.T) {
	limiter := NewTypedItemDecorrelatedJitterRateLimiter[string](1*time.Minute, math.MaxInt64).(*TypedItemDecorrelatedJitterRateLimiter[string])
	limiter.int64N = randMax
	for i := 0; i < 100; i++ {
		if a := limiter.When("one"); a < time.Minute {
			t.Fatalf("attempt %d: delay %v below baseDelay, overflowed", i, a)
		}
	}
	if e, a := time.Duration(math.MaxInt64-1), limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
}

// TestJitterRateLimitersSpreadRetries checks that items failing at the same
// time get different delays, which is the point of jitter.
func TestJitterRateLimitersSpreadRetries(t *testing.T) {
	const items = 1000
	for name, limiter := range map[string]TypedRateLimiter[string]{
		"full":         NewTypedItemExponentialFullJitterRateLimiter[string](5*time.Millisecond, 1000*time.Second),
		"decorrelated": NewTypedItemDecorrelatedJitterRateLimiter[string](5*time.Millisecond, 1000*time.Second),
	} {
		t.Run(name, func(t *testing.T) {
			for attempt := 0; attempt < 10; attempt++ {
				distinct := map[time.Duration]struct{}{}
				for i := 0; i < items; i++ {
					distinct[limiter.When(strconv.Itoa(i))] = struct{}{}
				}
				// Collisions are possible but should be rare at nanosecond resolution.
				if len(distinct) < items*9/10 {
					t.Errorf("attempt %d: only %d distinct delays for %d items", attempt, len(distinct), items)
				}
			}
		})
	}
}

func BenchmarkItemRateLimiters(b *testing.B) {
	for name, newLimiter := range map[string]func() TypedRateLimiter[int]{
		"exponential": func() TypedRateLimiter[int] {
			return NewTypedItemExponentialFailureRateLimiter[int](5*time.Millisecond, 1000*time.Second)
		},
		"fullJitter": func() TypedRateLimiter[int] {
			return NewTypedItemExponentialFullJitterRateLimiter[int](5*time.Millisecond, 1000*time.Second)
		},
		"decorrelatedJitter": func() TypedRateLimiter[int] {
			return NewTypedItemDecorrelatedJitterRateLimiter[int](5*time.Millisecond, 1000*time.Second)
		},
	} {
		b.Run(name, func(b *testing.B) {
			limiter := newLimiter()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					item := i % 1024
					limiter.When(item)
					if i%16 == 0 {
						limiter.Forget(item)
					}
					i++
				}
			})
		})
	}
}
