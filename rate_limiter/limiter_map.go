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
	limiters map[string]*Limiter
	mut      sync.RWMutex
}

func NewLimiterMap() *LimiterMap {
	return &LimiterMap{
		limiters: make(map[string]*Limiter),
	}
}

// GetOrCreate checks the map for a limiter with the specified key values - if none exists it creates it
func (m *LimiterMap) GetOrCreate(l *definition, tagValues map[string]string) (*Limiter, error) {
	// build the key from the tag values
	key, err := buildLimiterKey(tagValues)
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

	// ok we need to create one
	limiter = &Limiter{
		Limiter:   rate.NewLimiter(l.Limit, l.BurstSize),
		tagValues: tagValues,
	}
	// put it in the map
	m.limiters[key] = limiter
	return limiter, nil
}

func buildLimiterKey(values map[string]string) (string, error) {
	// build the key for this rate limiter

	// map key is the hash of the tag values as json

	// json marsjall sorts the array so the same keys in different order will produce the same key
	jsonString, err := json.Marshal(values)
	if err != nil {
		return "", err
	}

	// return hash of JSON representaiton
	hash := md5.Sum(jsonString)
	key := hex.EncodeToString(hash[:])

	return key, nil
}
