package connection

// deprecated
// use ConnectionCache
type Manager struct {
	Cache *Cache
}

func NewManager(connectionCache *ConnectionCache) *Manager {
	return &Manager{Cache: NewCache(connectionCache)}
}
