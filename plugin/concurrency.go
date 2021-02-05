package plugin

import (
	"log"
	"sync"
)

// if no max concurrency is specified in the plugin, use this value
const defaultMaxConcurrency = 1000

// if no max call concurrency is specified for a hydrate function, use this value
const defaultMaxConcurrencyPerCall = 500

// ConcurrencyManager :: struct which ensures hydrate funcitons stay within concurrency limits
type ConcurrencyManager struct {
	mut sync.Mutex
	// the maximun number of all hydrate calls which can run concurrently
	maxConcurrency int
	// the maximum concurrency for a single hydrate call
	// (this may be overridden by the HydrateConfig for the call)
	defaultMaxConcurrencyPerCall int
	// total number of hydrate calls in progress
	callsInProgress int
	// map of the number of instances of each call in progress
	callMap map[string]int
}

func newConcurrencyManager(t *Table) *ConcurrencyManager {
	// if plugin does not define max concurrency, use default
	max := defaultMaxConcurrency
	// if hydrate calls do not define max concurrency, use default
	maxPerCall := defaultMaxConcurrencyPerCall
	if config := t.Plugin.DefaultHydrateConfig; config != nil {
		if config.MaxConcurrency != 0 {
			max = config.MaxConcurrency
		}
		if config.DefaultMaxConcurrencyPerCall != 0 {
			maxPerCall = config.DefaultMaxConcurrencyPerCall
		} else if max < maxPerCall {
			// if the default call concurrency is greater than the toal max concurrency, clamp to total
			maxPerCall = max
		}
	}
	return &ConcurrencyManager{
		maxConcurrency:               max,
		defaultMaxConcurrencyPerCall: maxPerCall,
		callMap:                      make(map[string]int),
	}
}

// StartIfAllowed :: check whether the named hydrate call is permitted to start
// based on the number of running instances of that call, and the total calls in progress
func (c *ConcurrencyManager) StartIfAllowed(name string, maxCallConcurrency int) (res bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	// is the total call limit exceeded?
	if c.callsInProgress == c.maxConcurrency {
		return false
	}

	// if there is no config or empty config, the maxCallConcurrency will be 0
	// - use defaultMaxConcurrencyPerCall set on the concurrencyManager
	if maxCallConcurrency == 0 {
		maxCallConcurrency = c.defaultMaxConcurrencyPerCall
	}

	// how many concurrent executions of this function ar in progress right now?
	currentExecutions := c.callMap[name]

	// if we at the call limit return
	if currentExecutions == maxCallConcurrency {
		return false
	}

	// to get here we are allowed to execute - increment the call counters
	c.callMap[name] = currentExecutions + 1
	c.callsInProgress++
	return true
}

// Finished :: decrement the counter for the named function
func (c *ConcurrencyManager) Finished(name string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] %v", r)
		}
	}()
	c.mut.Lock()
	defer c.mut.Unlock()
	c.callMap[name]--
	c.callsInProgress--
}
