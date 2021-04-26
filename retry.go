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

// RetryPolicy is an enum
type RetryPolicy int

const (
	// NoRetry does not retry
	NoRetry RetryPolicy = iota
	// ExponentialBackoff retires forever, with exponentially increading sleep
	ExponentialBackoff
)

type retryPolicyInterface interface {
	SetOptions(interface{})
	CheckRetry(*RPCCallResult) *CheckRetryResult
	PollTimeout() time.Duration
}

func getRetryPolicy(policy RetryPolicy, options ...interface{}) retryPolicyInterface {
	var rp retryPolicyInterface

	switch policy {
	case NoRetry:
		rp = &noRetryPolicy{}
	case ExponentialBackoff:
		rp = &exponentialBackoffRetryPolicy{
			options: ExponentialBackoffOptions{
				MaxTimeout:  defaultEBInitialTimeout,
				Exponent:    defaultEBExponent,
				NextTimeout: defaultEBInitialTimeout,
			},
		}
	default:
		panic("Requested non-existent retry policy")
	}

	if len(options) > 0 {
		rp.SetOptions(options[0])
	}

	return rp
}

// ----------------------------------------------------------------------------
// RetryPolicy Implementations
// ----------------------------------------------------------------------------

type noRetryPolicy struct {
}

func (rp *noRetryPolicy) SetOptions(interface{}) {}

func (rp *noRetryPolicy) CheckRetry(callResult *RPCCallResult) *CheckRetryResult {
	return &CheckRetryResult{DoRetry: false}
}

func (rp *noRetryPolicy) PollTimeout() time.Duration {
	return noRetryTimeout
}

// ExponentialBackoffOptions are the options for the exponential backoff policy
type ExponentialBackoffOptions struct {
	MaxTimeout  time.Duration
	Exponent    float32
	NextTimeout time.Duration
}

type exponentialBackoffRetryPolicy struct {
	options ExponentialBackoffOptions
}

func (rp *exponentialBackoffRetryPolicy) SetOptions(o interface{}) {
	if options, ok := o.(ExponentialBackoffOptions); ok {
		rp.options = options
	}
}

func (rp *exponentialBackoffRetryPolicy) PollTimeout() time.Duration {
	return rp.options.NextTimeout
}

func (rp *exponentialBackoffRetryPolicy) CheckRetry(callResult *RPCCallResult) *CheckRetryResult {
	rp.options.NextTimeout *= time.Duration(rp.options.Exponent)

	if rp.options.NextTimeout > rp.options.MaxTimeout {
		rp.options.NextTimeout = rp.options.MaxTimeout
	}

	return &CheckRetryResult{DoRetry: true, Timeout: rp.options.NextTimeout}
}
