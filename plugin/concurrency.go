package plugin

import (
	"log"
	"sync"
)

const maxCalls = 50
const defaultMaxPerCall = 5

type ConcurrencyManager struct {
	mut        sync.Mutex
	TotalCalls int
	// map of max calls allowed for each func
	CallMaxMap map[string]int
	// map of current calls for each func
	CallMap map[string]int
}

func (c *ConcurrencyManager) StartIfAllowed(name string) bool {
	log.Printf("[WARN] START IF ALLOWED BEFORE LOCK %s", name)
	c.mut.Lock()
	log.Printf("[WARN] START IF ALLOWED AFTER LOCK %s", name)
	c.CallMap[name] = 10
	c.mut.Unlock()
	log.Printf("[WARN] START IF ALLOWED AFTER UNLOCK %s", name)
	// // is the total call limit exceeded?
	// if c.TotalCalls == maxCalls {
	// 	return false
	// }
	// // how many concurrent executions of this function right now?
	// currentExecutions := c.CallMap[name]
	// // is there a limit defined for this particular call -
	// limit, ok := c.CallMaxMap[name]
	// if !ok {
	// 	// otherwise use the default
	// 	limit = defaultMaxPerCall
	// }
	// // are we at the call limit?
	// if currentExecutions == limit {
	// 	return false
	// }
	// // to get here we are allowed to execute
	// c.CallMap[name] = currentExecutions + 1
	// c.TotalCalls++
	return true
}

func (c *ConcurrencyManager) Finished(name string) {
	c.mut.Lock()
	defer c.mut.Unlock()
	// c.CallMap[name]--
	c.TotalCalls--
}
