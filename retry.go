package koinosmq

import "time"

const (
	defaultEBInitialTimeout = (time.Second * 1)
	defaultEBMaxTimeout     = (time.Second * 30)
	defaultEBExponent       = 2.0
	noRetryTimeout          = (time.Second * 1)
)

// CheckRetryResult represents describes whether a retry is requested, and how long to timeout first
type CheckRetryResult struct {
	DoRetry bool
	Timeout time.Duration
}

// RetryFactory represent a factory function for producing retry instances
type RetryFactory func() RetryPolicy

// RetryPolicy interface represents an implementation of an RPC call retry
type RetryPolicy interface {
	CheckRetry(*RPCCallResult) *CheckRetryResult
	PollTimeout() time.Duration
}

// ----------------------------------------------------------------------------
// RetryPolicy Implementations
// ----------------------------------------------------------------------------

// NoRetryPolicy will never retry
type NoRetryPolicy struct {
}

// CheckRetry for this policy will always return false
func (rp *NoRetryPolicy) CheckRetry(callResult *RPCCallResult) *CheckRetryResult {
	return &CheckRetryResult{DoRetry: false}
}

// PollTimeout for this policy always returns a default value
func (rp *NoRetryPolicy) PollTimeout() time.Duration {
	return noRetryTimeout
}

// ExponentialBackoffRetryPolicy will retry with exponentially increasing timeouts
type ExponentialBackoffRetryPolicy struct {
	MaxTimeout time.Duration
	Exponent   float32

	nextTimeout time.Duration
}

// PollTimeout for this policy simply returns min(nextTimeout, MaxTimeout)
func (rp *ExponentialBackoffRetryPolicy) PollTimeout() time.Duration {
	if rp.nextTimeout > rp.MaxTimeout {
		return rp.MaxTimeout
	}

	return rp.nextTimeout
}

// CheckRetry for this policy will return whether or not a retry is reuqested, and how long to timeout
func (rp *ExponentialBackoffRetryPolicy) CheckRetry(callResult *RPCCallResult) *CheckRetryResult {
	// Simply clamp to MaxTimeout if exceeded
	if rp.nextTimeout > rp.MaxTimeout {
		return &CheckRetryResult{DoRetry: true, Timeout: rp.MaxTimeout}
	}

	// If not return current value, then increase it
	res := &CheckRetryResult{DoRetry: true, Timeout: rp.nextTimeout}
	rp.nextTimeout *= time.Duration(rp.Exponent)
	return res
}

// NewExponentialBackoffRetryPolicy will create a new instance
func NewExponentialBackoffRetryPolicy(initialTimeout time.Duration, maxTimeout time.Duration, exponent float32) *ExponentialBackoffRetryPolicy {
	return &ExponentialBackoffRetryPolicy{MaxTimeout: maxTimeout, Exponent: exponent, nextTimeout: initialTimeout}
}

// NewDefaultExponentialBackoffRetryPolicy will create a new instance wsith default parameters
func NewDefaultExponentialBackoffRetryPolicy() *ExponentialBackoffRetryPolicy {
	return NewExponentialBackoffRetryPolicy(defaultEBInitialTimeout, defaultEBMaxTimeout, defaultEBExponent)
}
