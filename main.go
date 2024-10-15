package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	gobreaker "github.com/EdwinWD/gobreaker/lib"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
)

var (
	mcb         *MultiCircuitBreaker
	redisClient *redis.Client
)

type MultiCircuitBreaker struct {
	mu       sync.RWMutex
	breakers map[string]*gobreaker.CircuitBreaker
}

func NewMultiCircuitBreaker() *MultiCircuitBreaker {
	return &MultiCircuitBreaker{
		mu:       sync.RWMutex{},
		breakers: make(map[string]*gobreaker.CircuitBreaker),
	}
}

func initialize() {
	// Initialize Redis client
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Replace with your Redis server address
		Password: "",               // Set password if required
		DB:       0,                // Use default DB
	})

	// Test the Redis connection
	ctx := redisClient.Context()
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Redis: %v", err))
	}

	mcb = NewMultiCircuitBreaker()
}

func main() {
	initialize()

	r := mux.NewRouter()
	r.HandleFunc("/api/circuit-breaker/handle/{value}/{status}", handleRequest).Methods("GET")
	r.HandleFunc("/api/circuit-breaker/check-status/{value}", checkStatus).Methods("GET")

	fmt.Println("Server is running on :8080")
	http.ListenAndServe(":8080", r)
}

func (mcb *MultiCircuitBreaker) getOrCreateCircuitBreaker(key string, redisClient *redis.Client) *gobreaker.CircuitBreaker {
	mcb.mu.RLock()
	cb, exists := mcb.breakers[key]
	mcb.mu.RUnlock()

	if !exists {
		mcb.mu.Lock()
		defer mcb.mu.Unlock()

		// Check again in case another goroutine created it
		cb, exists = mcb.breakers[key]
		if !exists {
			circuitBreakerSettings := gobreaker.Settings{
				Name:        key,
				MaxRequests: 3,
				Interval:    time.Second * 15,
				Timeout:     time.Second * 15,
				ReadyToTrip: func(counts gobreaker.Counts) bool {
					failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
					return counts.Requests >= 3 && failureRatio >= 0.6
				},
				OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
					fmt.Printf("Circuit Breaker '%s' state changed from %s to %s\n", name, from, to)
				},
			}

			cb = gobreaker.NewCircuitBreaker(circuitBreakerSettings, key, redisClient)
			mcb.breakers[key] = cb
		}
	}

	return cb
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	value := vars["value"]
	status := vars["status"]

	cb := mcb.getOrCreateCircuitBreaker(value, redisClient)

	result, err := cb.Execute(func() (interface{}, error) {
		// Simulating an external service call
		time.Sleep(time.Millisecond * 100)
		if status == "error" {
			return "", fmt.Errorf("error condition triggered")
		}

		return fmt.Sprintf("Success with value: %s", value), nil
	})

	if err != nil {
		if err == gobreaker.ErrOpenState {
			http.Error(w, "Circuit breaker is open", http.StatusServiceUnavailable)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	response := map[string]string{"message": result.(string)}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func checkStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	value := vars["value"]

	cb := mcb.getOrCreateCircuitBreaker(value, redisClient)

	state := cb.State()
	counts := cb.Counts()

	response := map[string]interface{}{
		"status":                state.String(),
		"total_requests":        counts.Requests,
		"total_failures":        counts.TotalFailures,
		"consecutive_failures":  counts.ConsecutiveFailures,
		"total_successes":       counts.TotalSuccesses,
		"consecutive_successes": counts.ConsecutiveSuccesses,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
