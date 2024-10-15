// Package gobreaker implements the Circuit Breaker pattern.
// See https://msdn.microsoft.com/en-us/library/dn589784.aspx.
package gobreaker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// State is a type that represents a state of CircuitBreaker.
type State int

// These constants are states of CircuitBreaker.
const (
	StateClosed State = iota
	//StateHalfOpen
	StateOpen
)

var (
	// ErrTooManyRequests is returned when the CB state is half open and the requests count is over the cb maxRequests
	ErrTooManyRequests = errors.New("too many requests")
	// ErrOpenState is returned when the CB state is open
	ErrOpenState = errors.New("circuit breaker is open")
)

// String implements stringer interface.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	//case StateHalfOpen:
	//	return "half-open"
	case StateOpen:
		return "open"
	default:
		return fmt.Sprintf("unknown state: %d", s)
	}
}

// Counts holds the numbers of requests and their successes/failures.
// CircuitBreaker clears the internal Counts either
// on the change of the state or at the closed-state intervals.
// Counts ignores the results of the requests sent before clearing.
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

func (c *Counts) onRequest() {
	c.Requests++
}

func (c *Counts) onSuccess() {
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

func (c *Counts) onFailure() {
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

func (c *Counts) clear() {
	c.Requests = 0
	c.TotalSuccesses = 0
	c.TotalFailures = 0
	c.ConsecutiveSuccesses = 0
	c.ConsecutiveFailures = 0
}

// Settings configures CircuitBreaker:
//
// Name is the name of the CircuitBreaker.
//
// MaxRequests is the maximum number of requests allowed to pass through
// when the CircuitBreaker is half-open.
// If MaxRequests is 0, the CircuitBreaker allows only 1 request.
//
// Interval is the cyclic period of the closed state
// for the CircuitBreaker to clear the internal Counts.
// If Interval is less than or equal to 0, the CircuitBreaker doesn't clear internal Counts during the closed state.
//
// Timeout is the period of the open state,
// after which the state of the CircuitBreaker becomes half-open.
// If Timeout is less than or equal to 0, the timeout value of the CircuitBreaker is set to 60 seconds.
//
// ReadyToTrip is called with a copy of Counts whenever a request fails in the closed state.
// If ReadyToTrip returns true, the CircuitBreaker will be placed into the open state.
// If ReadyToTrip is nil, default ReadyToTrip is used.
// Default ReadyToTrip returns true when the number of consecutive failures is more than 5.
//
// OnStateChange is called whenever the state of the CircuitBreaker changes.
//
// IsSuccessful is called with the error returned from a request.
// If IsSuccessful returns true, the error is counted as a success.
// Otherwise the error is counted as a failure.
// If IsSuccessful is nil, default IsSuccessful is used, which returns false for all non-nil errors.
type Settings struct {
	Name          string
	MaxRequests   uint32
	Interval      time.Duration
	Timeout       time.Duration
	ReadyToTrip   func(counts Counts) bool
	OnStateChange func(name string, from State, to State)
	IsSuccessful  func(err error) bool
}

// CircuitBreaker is a state machine to prevent sending requests that are likely to fail.
type CircuitBreaker struct {
	name          string
	maxRequests   uint32
	interval      time.Duration
	timeout       time.Duration
	readyToTrip   func(counts Counts) bool
	isSuccessful  func(err error) bool
	onStateChange func(name string, from State, to State)

	mutex   sync.Mutex
	cbState circuitBreakerState

	redisClient *redis.Client
	redisKey    string
}

type circuitBreakerState struct {
	State      State     `json:"state"`
	Generation uint64    `json:"generation"`
	Counts     Counts    `json:"counts"`
	Expiry     time.Time `json:"expiry"` // Store as Unix timestamp
}

// TwoStepCircuitBreaker is like CircuitBreaker but instead of surrounding a function
// with the breaker functionality, it only checks whether a request can proceed and
// expects the caller to report the outcome in a separate step using a callback.
type TwoStepCircuitBreaker struct {
	cb *CircuitBreaker
}

// NewCircuitBreaker returns a new CircuitBreaker configured with the given Settings.
func NewCircuitBreaker(st Settings, key string, redisClient *redis.Client) *CircuitBreaker {
	cb := new(CircuitBreaker)

	cb.name = st.Name
	cb.onStateChange = st.OnStateChange

	if st.MaxRequests == 0 {
		cb.maxRequests = 1
	} else {
		cb.maxRequests = st.MaxRequests
	}

	if st.Interval <= 0 {
		cb.interval = defaultInterval
	} else {
		cb.interval = st.Interval
	}

	if st.Timeout <= 0 {
		cb.timeout = defaultTimeout
	} else {
		cb.timeout = st.Timeout
	}

	if st.ReadyToTrip == nil {
		cb.readyToTrip = defaultReadyToTrip
	} else {
		cb.readyToTrip = st.ReadyToTrip
	}

	if st.IsSuccessful == nil {
		cb.isSuccessful = defaultIsSuccessful
	} else {
		cb.isSuccessful = st.IsSuccessful
	}

	cb.redisClient = redisClient
	cb.redisKey = fmt.Sprintf("circuit_breaker_%s", key)

	// Try to load existing state from Redis
	if err := cb.loadState(); err != nil {
		fmt.Printf("Failed to load state from Redis: %v. Initializing new state.\n", err)
		cb.toNewGeneration(time.Now())
	}

	return cb
}

func (cb *CircuitBreaker) loadState() error {
	ctx := context.Background()
	stateJSON, err := cb.redisClient.Get(ctx, cb.redisKey).Result()
	if err == redis.Nil {
		// Key doesn't exist, use default state
		return nil
	} else if err != nil {
		return err
	}

	var state circuitBreakerState
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		fmt.Printf("Error unmarshaling state: %v\n", err.Error())
		return err
	}

	cb.cbState = circuitBreakerState{
		State:      state.State,
		Generation: state.Generation,
		Counts:     state.Counts,
		Expiry:     state.Expiry,
	}

	return nil
}

func (cb *CircuitBreaker) saveState() error {
	state := circuitBreakerState{
		State:      cb.cbState.State,
		Generation: cb.cbState.Generation,
		Counts:     cb.cbState.Counts,
		Expiry:     cb.cbState.Expiry,
	}

	stateJSON, err := json.Marshal(state)
	if err != nil {
		fmt.Printf("Error marshaling state: %v\n", err.Error())
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = cb.redisClient.Set(ctx, cb.redisKey, string(stateJSON), 0).Err()
	if err != nil {
		fmt.Printf("Error saving state to Redis: %v\n", err.Error())
		return err
	}

	return nil
}

func (cb *CircuitBreaker) setState(state State, now time.Time) {
	if cb.cbState.State == state {
		return
	}

	prev := cb.cbState.State
	cb.cbState.State = state

	cb.toNewGeneration(now)

	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, state)
	}

	// Save state to Redis
	if err := cb.saveState(); err != nil {
		fmt.Printf("Failed to save state to Redis: %v\n", err)
	}
}

// NewTwoStepCircuitBreaker returns a new TwoStepCircuitBreaker configured with the given Settings.
func NewTwoStepCircuitBreaker(st Settings, redisClient *redis.Client, key string) *TwoStepCircuitBreaker {
	return &TwoStepCircuitBreaker{
		cb: NewCircuitBreaker(st, key, redisClient),
	}
}

const defaultInterval = time.Duration(0) * time.Second
const defaultTimeout = time.Duration(60) * time.Second

func defaultReadyToTrip(counts Counts) bool {
	return counts.ConsecutiveFailures > 5
}

func defaultIsSuccessful(err error) bool {
	return err == nil
}

// Name returns the name of the CircuitBreaker.
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// State returns the current state of the CircuitBreaker.
func (cb *CircuitBreaker) State() State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, _ := cb.currentState(now)
	return state
}

// Counts returns internal counters
func (cb *CircuitBreaker) Counts() Counts {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	return cb.cbState.Counts
}

// Execute runs the given request if the CircuitBreaker accepts it.
// Execute returns an error instantly if the CircuitBreaker rejects the request.
// Otherwise, Execute returns the result of the request.
// If a panic occurs in the request, the CircuitBreaker handles it as an error
// and causes the same panic again.
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	// Load the latest state from Redis before executing
	if err := cb.loadState(); err != nil {
		fmt.Printf("Failed to load state from Redis: %v\n", err)
	}

	generation, err := cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	defer func() {
		e := recover()
		if e != nil {
			cb.afterRequest(generation, false)
			panic(e)
		}
	}()

	result, err := req()
	cb.afterRequest(generation, cb.isSuccessful(err))
	return result, err
}

// Name returns the name of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) Name() string {
	return tscb.cb.Name()
}

// State returns the current state of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) State() State {
	return tscb.cb.State()
}

// Counts returns internal counters
func (tscb *TwoStepCircuitBreaker) Counts() Counts {
	return tscb.cb.Counts()
}

// Allow checks if a new request can proceed. It returns a callback that should be used to
// register the success or failure in a separate step. If the circuit breaker doesn't allow
// requests, it returns an error.
func (tscb *TwoStepCircuitBreaker) Allow() (done func(success bool), err error) {
	generation, err := tscb.cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	return func(success bool) {
		tscb.cb.afterRequest(generation, success)
	}, nil
}

func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	if state == StateOpen {
		return generation, ErrOpenState
	} /*else if state == StateHalfOpen && cb.counts.Requests >= cb.maxRequests {
		return generation, ErrTooManyRequests
	}*/

	cb.cbState.Counts.onRequest()
	return generation, nil
}

func (cb *CircuitBreaker) afterRequest(before uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)
	if generation != before {
		return
	}

	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}

	// Save state to Redis
	if err := cb.saveState(); err != nil {
		fmt.Printf("Failed to save state to Redis: %v\n", err)
	}

}

func (cb *CircuitBreaker) onSuccess(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.cbState.Counts.onSuccess()
		/*case StateHalfOpen:
		cb.counts.onSuccess()
		if cb.counts.ConsecutiveSuccesses >= cb.maxRequests {
			cb.setState(StateClosed, now)
		}*/
	}
}

func (cb *CircuitBreaker) onFailure(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.cbState.Counts.onFailure()
		if cb.readyToTrip(cb.cbState.Counts) {
			cb.setState(StateOpen, now)
		}
		/*case StateHalfOpen:
		cb.setState(StateOpen, now)*/
	}
}

func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	switch cb.cbState.State {
	case StateClosed:
		if !cb.cbState.Expiry.IsZero() && cb.cbState.Expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateOpen:
		if cb.cbState.Expiry.Before(now) {
			// cb.setState(StateHalfOpen, now)
			cb.setState(StateClosed, now)
		}
	}

	return cb.cbState.State, cb.cbState.Generation
}

func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.cbState.Generation++
	cb.cbState.Counts.clear()

	var zero time.Time
	switch cb.cbState.State {
	case StateClosed:
		if cb.interval == 0 {
			cb.cbState.Expiry = zero
		} else {
			cb.cbState.Expiry = now.Add(cb.interval)
		}
	case StateOpen:
		cb.cbState.Expiry = now.Add(cb.timeout)
	default: // StateHalfOpen
		cb.cbState.Expiry = zero
	}
}
