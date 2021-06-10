package plugin

import (
	"context"
	"log"
	"sync"
)

var cacheableHydrateFunctionsPending = make(map[string]*sync.Mutex)
var cacheableHydrateLock sync.Mutex

func CacheableHydrate(hydrate, getCacheKey HydrateFunc) HydrateFunc {
	return func(ctx context.Context, d *QueryData, h *HydrateData) (interface{}, error) {
		// build key
		k, err := getCacheKey(ctx, d, h)
		if err != nil {
			return nil, err
		}
		cacheKey := k.(string)

		// wait until there is no instance of the hydrate function running
		// get the global lock
		cacheableHydrateLock.Lock()
		functionLock, ok := cacheableHydrateFunctionsPending[cacheKey]
		if ok {
			// a hydrate function is running - or it has completed
			// unlock the global lock and try to lock the functionLock
			cacheableHydrateLock.Unlock()
			(*functionLock).Lock()
			// ensure we unlock the function lock before return
			defer (*functionLock).Unlock()

			// we have the function lock
			// so at this point, there is no hydrate function running - we hope the data is in the cache
			// (but it may not be - if there was an error)
			// look in the cache to see if the data is there
			cachedData, ok := d.ConnectionManager.Cache.Get(cacheKey)
			if ok {
				log.Printf("[TRACE] CacheableHydrate CACHE HIT key %s", cacheKey)
				// we got the data
				return cachedData, nil
			}
			log.Printf("[TRACE] CacheableHydrate CACHE MISS key %s", cacheKey)

			// so there is no cached data - call the hydrate function and cache the result
			return callAndCacheHydrate(ctx, d, h, hydrate, cacheKey)

		} else {
			log.Printf("[TRACE] CacheableHydrate no function lock key %s", cacheKey)
			// there is no lock for this function, which means it has not been run yet
			// create a lock
			functionLock = &sync.Mutex{}
			// lock it
			(*functionLock).Lock()
			// ensure we unlock before return
			defer (*functionLock).Unlock()
			// add to map
			cacheableHydrateFunctionsPending[cacheKey] = functionLock
			// unlock the global lock
			cacheableHydrateLock.Unlock()

			log.Printf("[TRACE] CacheableHydrate added lock to map key %s", cacheKey)
			// no call the hydrate function and cache the result
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

func ConstantCacheKey(key string) HydrateFunc {
	return func(context.Context, *QueryData, *HydrateData) (interface{}, error) {
		return key, nil
	}
}

func ExecuteCacheableHydrate(ctx context.Context, d *QueryData, h *HydrateData, hydrate HydrateFunc, getCacheKey HydrateFunc) (interface{}, error) {
	return CacheableHydrate(hydrate, getCacheKey)(ctx, d, h)
}
