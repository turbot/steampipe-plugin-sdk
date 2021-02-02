package plugin

import (
	"log"
	"sync"
)

const defaultMaxCalls = 50
const defaultMaxPerCall = 10

type ConcurrencyManager struct {
	mut        sync.Mutex
	totalCalls int
	// map of max calls allowed for each func
	callMaxMap map[string]int
	// map of current calls for each func
	callMap map[string]int
}

func newConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{
		callMaxMap: make(map[string]int),
		callMap:    make(map[string]int),
	}
}

func (c *ConcurrencyManager) StartIfAllowed(name string, maxCalls int) (res bool) {
	defer func() {
		if r := recover(); r != nil {
			// TODO handle panic higher?
			log.Printf("[WARN] %v", r)
		}
	}()

	c.mut.Lock()
	defer c.mut.Unlock()

	// if there is no config or empty config, the max concurrency will be 0. Use default max calls specified.
	if maxCalls == 0 {
		maxCalls = defaultMaxCalls
	}

	// is the total call limit exceeded?
	if c.totalCalls == maxCalls {
		return false
	}
	// how many concurrent executions of this function right now?
	currentExecutions := c.callMap[name]
	// is there a limit defined for this particular call -
	limit, ok := c.callMaxMap[name]
	if !ok {
		// otherwise use the default
		limit = defaultMaxPerCall
	}
	// are we at the call limit?
	if currentExecutions == limit {
		return false
	}
	// to get here we are allowed to execute
	c.callMap[name] = currentExecutions + 1
	c.totalCalls++

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
	c.totalCalls--
}
