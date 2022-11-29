package plugin

import (
	"context"
	"log"
	"sync"

	"github.com/turbot/go-kit/helpers"
)

var cacheableHydrateFunctionsPending = make(map[string]*sync.WaitGroup)
var cacheableHydrateLock sync.Mutex

// WithCache is a chainable function which wraps a hydrate call with caching and checks for
// pending execution of the same function
func (hydrate HydrateFunc) WithCache(opts ...WithCacheOption) HydrateFunc {
	var config = &WithCacheConfiguration{hydrate: hydrate}
	for _, o := range opts {
		o(config)
	}
	// build a function to return the cache key
	buildCacheKey := config.getCacheKeyFunction()

	return func(ctx context.Context, d *QueryData, h *HydrateData) (interface{}, error) {
		show := helpers.GetFunctionName(hydrate) == "getCommonColumns"

		// build key
		k, err := buildCacheKey(ctx, d, h)
		if err != nil {
			return nil, err
		}
		cacheKey := k.(string)

		if show {
			log.Printf("[WARN] getCommonColumns WithCache %s %s", d.KeyColumnQualString("region"), d.Connection.Name)
		}
		// wait until there is no instance of the hydrate function running
		// get the global lock
		cacheableHydrateLock.Lock()
		functionLock, ok := cacheableHydrateFunctionsPending[cacheKey+d.Connection.Name]
		if ok {
			log.Printf("[WARN] q %s %s got function lock (%s)", d.KeyColumnQualString("region"), d.Connection.Name, cacheKey+d.Connection.Name)
			// a hydrate function is running - or it has completed
			// unlock the global lock and try to lock the functionLock
			cacheableHydrateLock.Unlock()
			functionLock.Wait()

			// we have the function lock
			// so at this point, there is no hydrate function running - we hope the data is in the cache
			// (but it may not be - if there was an error)
			// look in the cache to see if the data is there
			cachedData, ok := d.ConnectionManager.Cache.Get(cacheKey)
			if ok {
				log.Printf("[WARN] getCommonColumns WithCache %s %s GOT CACHED DATA", d.KeyColumnQualString("region"), d.Connection.Name)
				// we got the data
				return cachedData, nil
			}
			log.Printf("[WARN] getCommonColumns WithCache %s %s NO CACHED DATA :(", d.KeyColumnQualString("region"), d.Connection.Name)

			// so there is no cached data - call the hydrate function and cache the result
			return callAndCacheHydrate(ctx, d, h, hydrate, cacheKey)

		} else {
			log.Printf("[WARN] getCommonColumns WithCache %s %s NO FUNCTION LOCK :( (%s)", d.KeyColumnQualString("region"), d.Connection.Name, cacheKey+d.Connection.Name)
			log.Printf("[TRACE] WithCache no function lock key %s", cacheKey)
			// there is no lock for this function, which means it has not been run yet
			// create a lock
			functionLock = new(sync.WaitGroup)
			// lock it
			functionLock.Add(1)
			// ensure we unlock before return
			defer functionLock.Done()
			// add to map
			cacheableHydrateFunctionsPending[cacheKey+d.Connection.Name] = functionLock
			// unlock the global lock
			cacheableHydrateLock.Unlock()

			log.Printf("[TRACE] WithCache added lock to map key %s", cacheKey)
			// now call the hydrate function and cache the result
			return callAndCacheHydrate(ctx, d, h, hydrate, cacheKey)
		}
	}
}

func callAndCacheHydrate(ctx context.Context, d *QueryData, h *HydrateData, hydrate HydrateFunc, cacheKey string) (interface{}, error) {
	// now call the hydrate function
	hydrateData, err := hydrate(ctx, d, h)
	if err != nil {
		// there was an error
		return nil, err
	}

	// so we have a hydrate result - add to the cache
	d.ConnectionManager.Cache.Set(cacheKey, hydrateData)
	// return the hydrate data
	return hydrateData, nil
}
