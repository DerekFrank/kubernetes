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
	"math/rand/v2"
	"sync"
	"time"
)

// TypedItemExponentialFullJitterRateLimiter returns a random delay in
// [0, min(maxDelay, baseDelay*2^<num-failures>)). Unlike
// TypedItemExponentialFailureRateLimiter, items that fail at the same time
// are spread across the backoff window instead of being retried in lockstep.
//
// See "full jitter" in https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
type TypedItemExponentialFullJitterRateLimiter[T comparable] struct {
	failuresLock sync.Mutex
	failures     map[T]int

	baseDelay time.Duration
	maxDelay  time.Duration

	// int64N is swappable for tests.
	int64N func(int64) int64
}

var _ TypedRateLimiter[any] = &TypedItemExponentialFullJitterRateLimiter[any]{}

func NewTypedItemExponentialFullJitterRateLimiter[T comparable](baseDelay time.Duration, maxDelay time.Duration) TypedRateLimiter[T] {
	return &TypedItemExponentialFullJitterRateLimiter[T]{
		failures:  map[T]int{},
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		int64N:    rand.Int64N,
	}
}

func (r *TypedItemExponentialFullJitterRateLimiter[T]) When(item T) time.Duration {
	r.failuresLock.Lock()
	exp := r.failures[item]
	r.failures[item] = exp + 1
	r.failuresLock.Unlock()

	ceiling := exponentialDelay(r.baseDelay, r.maxDelay, exp)
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(r.int64N(int64(ceiling)))
}

func (r *TypedItemExponentialFullJitterRateLimiter[T]) NumRequeues(item T) int {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	return r.failures[item]
}

func (r *TypedItemExponentialFullJitterRateLimiter[T]) Forget(item T) {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	delete(r.failures, item)
}

// TypedItemDecorrelatedJitterRateLimiter returns
// min(maxDelay, random in [baseDelay, 3*<previous delay>)), starting from a
// previous delay of baseDelay. Each item's delay grows from its own previous
// random value rather than from its failure count. baseDelay must be positive.
//
// See "decorrelated jitter" in https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
type TypedItemDecorrelatedJitterRateLimiter[T comparable] struct {
	failuresLock sync.Mutex
	failures     map[T]decorrelatedJitterState

	baseDelay time.Duration
	maxDelay  time.Duration

	// int64N is swappable for tests.
	int64N func(int64) int64
}

type decorrelatedJitterState struct {
	failures  int
	prevDelay time.Duration
}

var _ TypedRateLimiter[any] = &TypedItemDecorrelatedJitterRateLimiter[any]{}

func NewTypedItemDecorrelatedJitterRateLimiter[T comparable](baseDelay time.Duration, maxDelay time.Duration) TypedRateLimiter[T] {
	return &TypedItemDecorrelatedJitterRateLimiter[T]{
		failures:  map[T]decorrelatedJitterState{},
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		int64N:    rand.Int64N,
	}
}

func (r *TypedItemDecorrelatedJitterRateLimiter[T]) When(item T) time.Duration {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	state, ok := r.failures[item]
	if !ok {
		state.prevDelay = r.baseDelay
	}

	upper := time.Duration(math.MaxInt64)
	if state.prevDelay <= math.MaxInt64/3 {
		upper = 3 * state.prevDelay
	}
	delay := r.baseDelay
	if upper > r.baseDelay {
		delay += time.Duration(r.int64N(int64(upper - r.baseDelay)))
	}
	delay = min(delay, r.maxDelay)

	r.failures[item] = decorrelatedJitterState{failures: state.failures + 1, prevDelay: delay}
	return delay
}

func (r *TypedItemDecorrelatedJitterRateLimiter[T]) NumRequeues(item T) int {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	return r.failures[item].failures
}

func (r *TypedItemDecorrelatedJitterRateLimiter[T]) Forget(item T) {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	delete(r.failures, item)
}

// exponentialDelay returns min(maxDelay, baseDelay*2^exp) using shifts so it
// neither overflows nor pays for math.Pow.
func exponentialDelay(baseDelay, maxDelay time.Duration, exp int) time.Duration {
	if exp < 63 && baseDelay <= maxDelay>>exp {
		return baseDelay << exp
	}
	return maxDelay
}
