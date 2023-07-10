package rate_limiter

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"golang.org/x/time/rate"
	"sync"
)

// LimiterMap is a struct encapsulatring a map of rate limiters
// map key is built from the limiter tag values,
// e.g.
// tags: {"connection": "aws1", "region": "us-east-1"}
// key: hash("{\"connection\": \"aws1\", \"region\": \"us-east-1\"})
type LimiterMap struct {
	limiters map[string]*rate.Limiter
	mut      sync.RWMutex
}

func NewLimiterMap() *LimiterMap {
	return &LimiterMap{
		limiters: make(map[string]*rate.Limiter),
	}
}

// GetOrCreate checks the map for a limiter with the specified key values - if none exists it creates it
func (m *LimiterMap) GetOrCreate(tags map[string]string, config *Config) (*rate.Limiter, error) {
	key, err := m.buildKey(tags)
	if err != nil {
		return nil, err
	}

	m.mut.RLock()
	limiter, ok := m.limiters[key]
	m.mut.RUnlock()

	if ok {
		return limiter, nil
	}

	// get a write lock
	m.mut.Lock()
	// ensure release lock
	defer m.mut.Unlock()

	// try to read again
	limiter, ok = m.limiters[key]
	if ok {
		// someone beat us to creation
		return limiter, nil
	}

	limiter = rate.NewLimiter(config.Limit, config.BurstSize)

	m.limiters[key] = limiter
	return limiter, nil
}

// map key is the hash of the tag values as json
func (*LimiterMap) buildKey(tags map[string]string) (string, error) {
	jsonString, err := json.Marshal(tags)
	if err != nil {
		return "", err
	}

	// return hash of JSON representaiton
	hash := md5.Sum([]byte(jsonString))
	return hex.EncodeToString(hash[:]), nil

}
