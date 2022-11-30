package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/turbot/go-kit/helpers"
)

var cacheableHydrateFunctionsPending = make(map[string]*sync.WaitGroup)
var cacheableHydrateLock sync.Mutex

// WithCache is a chainable function which wraps a hydrate call with caching and checks for
// pending execution of the same function
func (hydrate HydrateFunc) WithCache(args ...HydrateFunc) HydrateFunc {
	// build a function to return the cache key
	getCacheKey := hydrate.getCacheKeyFunction(args)

	return func(ctx context.Context, d *QueryData, h *HydrateData) (interface{}, error) {
		// build key
		k, err := getCacheKey(ctx, d, h)
		if err != nil {
			return nil, err
		}
		cacheKey := k.(string)
		// build a key to access the cacheableHydrateFunctionsPending map, which includes the connection
		// NOTE: when caching the hydrate data, the connection name will also be added but this happens lower down
		executeLockKey := fmt.Sprintf("%s-%s", cacheKey, d.Connection.Name)

		// wait until there is no instance of the hydrate function running
		// get the global lock
		cacheableHydrateLock.Lock()
		functionLock, ok := cacheableHydrateFunctionsPending[executeLockKey]
		if ok {
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
				// we got the data
				return cachedData, nil
			}

			// so there is no cached data - call the hydrate function and cache the result
			return callAndCacheHydrate(ctx, d, h, hydrate, cacheKey)

		} else {
			log.Printf("[TRACE] WithCache no function lock key %s", cacheKey)
			// there is no lock for this function, which means it has not been run yet
			// create a lock
			functionLock = new(sync.WaitGroup)
			// lock it
			functionLock.Add(1)
			// ensure we unlock before return
			defer functionLock.Done()
			// add to map
			cacheableHydrateFunctionsPending[executeLockKey] = functionLock
			// unlock the global lock
			cacheableHydrateLock.Unlock()

			log.Printf("[TRACE] WithCache added lock to map key %s", cacheKey)
			// no call the hydrate function and cache the result
			return callAndCacheHydrate(ctx, d, h, hydrate, cacheKey)
		}
	}
}

func (hydrate HydrateFunc) getCacheKeyFunction(args []HydrateFunc) HydrateFunc {
	var getCacheKey HydrateFunc
	switch len(args) {
	case 0:
		// no argument was supplied - infer cache key from the hydrate function
		getCacheKey = func(context.Context, *QueryData, *HydrateData) (interface{}, error) {
			return helpers.GetFunctionName(hydrate), nil
		}
	case 1:
		getCacheKey = args[0]
	default:
		panic("WithCache accepts 0 or 1 argument")
	}
	return getCacheKey
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
