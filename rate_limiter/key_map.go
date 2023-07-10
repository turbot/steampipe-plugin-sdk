package rate_limiter

type KeyMap map[string]string

func (m KeyMap) satisfies(keys KeyMap) bool {
	for testKey, testVal := range keys {
		// if we do not have this key, ignore
		// only validate keyvals for keys which we have
		if keyVal, ok := m[testKey]; ok && keyVal != testVal {
			return false
		}
	}
	return true
}
