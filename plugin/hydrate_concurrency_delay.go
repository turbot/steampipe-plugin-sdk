package plugin

import "time"

type hydrateConcurrencyDelay struct {
	// the time when we _could_ start the call, if concurrency limits allowed
	potentialStartTime time.Time
	delay              time.Duration
}

func newHydrateConcurrencyDelay() *hydrateConcurrencyDelay {
	return &hydrateConcurrencyDelay{
		potentialStartTime: time.Now(),
	}
}

func (d *hydrateConcurrencyDelay) setCanStart() {
	d.delay = time.Since(d.potentialStartTime)
}
