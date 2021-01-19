package connection

type Manager struct {
	Cache *Cache
}

func NewManager() *Manager {
	return &Manager{Cache: NewCache(nil)}
}
