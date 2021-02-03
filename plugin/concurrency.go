package plugin

import (
	"log"
	"sync"
)

const defaultMaxConcurrency = 1000
const defaultMaxConcurrencyPerCall = 500

type ConcurrencyManager struct {
	mut sync.Mutex
	// the maximun number of hydrate calls which can run concurrently
	maxConcurrency int
	// the maximum concurrency for a single hydrate call
	// (this may be overridden by the Hydrate config for the call)
	defaultMaxConcurrencyPerCall int
	// total number of hydrate calls in progress
	callsInProgress int
	// map of the number of instances of each call in progress
	callMap map[string]int
}

func newConcurrencyManager(t *Table) *ConcurrencyManager {
	defer func() {
		if r := recover(); r != nil {
			// TODO handle panic higher?
			log.Printf("[WARN] %v", r)
		}
	}()
	max := defaultMaxConcurrency
	maxPerCall := defaultMaxConcurrencyPerCall
	if config := t.Plugin.DefaultHydrateConfig; config != nil {
		if config.MaxConcurrency != 0 {
			max = config.MaxConcurrency
		}
		if config.DefaultMaxConcurrencyPerCall != 0 {
			maxPerCall = config.DefaultMaxConcurrencyPerCall
		} else if max < maxPerCall {
			maxPerCall = max
		}
	}
	return &ConcurrencyManager{
		maxConcurrency:               max,
		defaultMaxConcurrencyPerCall: maxPerCall,
		callMap:                      make(map[string]int),
	}
}

func (c *ConcurrencyManager) StartIfAllowed(name string, maxCallConcurrency int) (res bool) {
	defer func() {
		if r := recover(); r != nil {
			// TODO handle panic higher?
			log.Printf("[WARN] %v", r)
		}
	}()

	c.mut.Lock()
	defer c.mut.Unlock()

	// is the total call limit exceeded?
	if c.callsInProgress == c.maxConcurrency {
		return false
	}

	// if there is no config or empty config, the maxCallConcurrency will be 0
	// use defaultMaxConcurrencyPerCall set on the concurrencyManager
	if maxCallConcurrency == 0 {
		maxCallConcurrency = c.defaultMaxConcurrencyPerCall
	}

	// how many concurrent executions of this function right now?
	currentExecutions := c.callMap[name]

	// are we at the call limit?
	if currentExecutions == maxCallConcurrency {
		return false
	}

	// to get here we are allowed to execute - increment the call counters
	c.callMap[name] = currentExecutions + 1
	c.callsInProgress++
	return true
}

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
