package rate_limiter

//type ApiLimiter struct {
//	*rate.Limiter
//
//	Keys KeyMap
//}
//
//func NewApiLimiter(config *Config) *ApiLimiter {
//	return &ApiLimiter{
//		Limiter: rate.NewLimiter(config.Limit, config.BurstSize),
//		Keys:    config.Keys,
//	}
//}

//
//type MultiLimiter struct {
//	limiters []*ApiLimiter
//}
//
//func (m *MultiLimiter) Add(r rate.Limit, burstSize int, keys KeyMap) {
//	if keys == nil {
//		keys = KeyMap{}
//	}
//	m.limiters = append(m.limiters, NewApiLimiter(r, burstSize, keys))
//}
//
//func (m *MultiLimiter) limitersForKeys(keys KeyMap) []*ApiLimiter {
//	var res []*ApiLimiter
//
//	for _, l := range m.limiters {
//		if l.Keys.satisfies(keys) {
//			res = append(res, l)
//		}
//	}
//	return res
//}
//
//func (m *MultiLimiter) Wait(ctx context.Context, keys KeyMap) {
//	// wait for the max time required by all rate limiters
//	// NOTE: if one rate limiter has zero delay, and another has a long delay, this may mess up the rate limiting
//	// of the first limiter as the api call will not happen til later than anticipated
//
//	limiters := m.limitersForKeys(keys)
//	var maxDelay time.Duration
//	var reservations []*rate.Reservation
//
//	// find the max delay from all the limiters
//	for _, l := range limiters {
//		r := l.Reserve()
//		reservations = append(reservations, r)
//		if d := r.Delay(); d > maxDelay {
//			maxDelay = d
//		}
//	}
//	if maxDelay == 0 {
//		return
//	}
//
//	var keyStr strings.Builder
//	keyNames := maps.Keys(keys)
//	sort.Strings(keyNames)
//	for _, k := range keyNames {
//		keyStr.WriteString(fmt.Sprintf("%s=%s, ", k, keys[k]))
//	}
//	log.Printf("[INFO] rate limiter waiting %dms: %s", maxDelay.Milliseconds(), keyStr.String())
//	// wait for the max delay time
//	t := time.NewTimer(maxDelay)
//	defer t.Stop()
//	select {
//	case <-t.C:
//		// We can proceed.
//	case <-ctx.Done():
//		// Context was canceled before we could proceed.  Cancel the
//		// reservations, which may permit other events to proceed sooner.
//		for _, r := range reservations {
//			r.Cancel()
//		}
//	}
//}
