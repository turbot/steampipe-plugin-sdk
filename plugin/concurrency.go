package plugin

import (
	"log"
	"sync"
)

// DefaultConcurrencyConfig contains plugin level config to define default hydrate concurrency
// - this is used if no HydrateConfig is specified for a specific call
type DefaultConcurrencyConfig struct {
	// max number of ALL hydrate calls in progress
	TotalMaxConcurrency   int
	DefaultMaxConcurrency int
}

// concurrencyManager struct ensures that hydrate functions stay within concurrency limits
type concurrencyManager struct {
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
	// instrumentaton properties
	maxCallsInProgress int
	maxCallMap         map[string]int
}

func newConcurrencyManager(t *Table) *concurrencyManager {
	// if plugin does not define max concurrency, use default
	var totalMax int
	// if hydrate calls do not define max concurrency, use default
	var maxPerCall int
	if config := t.Plugin.DefaultConcurrency; config != nil {
		if config.TotalMaxConcurrency != 0 {
			totalMax = config.TotalMaxConcurrency
		}
		if config.DefaultMaxConcurrency != 0 {
			maxPerCall = config.DefaultMaxConcurrency
		} else if totalMax < maxPerCall {
			// if the default call concurrency is greater than the toal max concurrency, clamp to total
			maxPerCall = totalMax
		}
	}
	return &concurrencyManager{
		maxConcurrency:               totalMax,
		defaultMaxConcurrencyPerCall: maxPerCall,
		callMap:                      make(map[string]int),
		maxCallMap:                   make(map[string]int),
	}
}

// StartIfAllowed checks whether the named hydrate call is permitted to start
// based on the number of running instances of that call, and the total calls in progress
func (c *concurrencyManager) StartIfAllowed(name string, maxCallConcurrency int) (res bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	// is the total call limit exceeded?
	if c.maxConcurrency > 0 && c.callsInProgress == c.maxConcurrency {
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
	if maxCallConcurrency > 0 && currentExecutions == maxCallConcurrency {
		return false
	}

	// to get here we are allowed to execute - increment the call counters
	c.callMap[name] = currentExecutions + 1
	c.callsInProgress++

	// update instrumentation
	if c.callMap[name] > c.maxCallMap[name] {
		c.maxCallMap[name] = c.callMap[name]
	}
	if c.callsInProgress > c.maxCallsInProgress {
		c.maxCallsInProgress = c.callsInProgress
	}
	return true
}

// Finished decrements the counter for the named function
func (c *concurrencyManager) Finished(name string) {
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

// Close executs when the query is complete and dumps out the concurrency stats
func (c *concurrencyManager) Close() {
	c.DisplayConcurrencyStats()
}

// DisplayConcurrencyStats displays the summary of all the concurrent hydrate calls
func (c *concurrencyManager) DisplayConcurrencyStats() {
	if len(c.maxCallMap) == 0 {
		return
	}
	log.Printf("[TRACE] ------------------------------------")
	log.Printf("[TRACE] Concurrency Summary")
	log.Printf("[TRACE] ------------------------------------")
	for call, concurrency := range c.maxCallMap {
		log.Printf("[TRACE] %-30s: %d", call, concurrency)
	}
	log.Printf("[TRACE] ------------------------------------")
	log.Printf("[TRACE] %-30s: %d", "Total", c.maxCallsInProgress)

	log.Printf("[TRACE] ------------------------------------")
}
