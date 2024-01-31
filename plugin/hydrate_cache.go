package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
)

//// map of memoized functions to the original function name
//var memoizedNameMap = make(map[string]string)
//var memoizedNameMapLock sync.RWMutex

/*
HydrateFunc is a function that gathers data to build table rows.
Typically this would make an API call and return the raw API output.

List and Get are special hydrate functions.

  - List returns data for all rows. Almost all tables will have a List function.

  - Get returns data for a single row. In order to filter as cheaply as possible a Get function should be implemented if 		the API supports fetching single items by key.

A column may require data not returned by the List or Get calls and an additional API
call will be required. A HydrateFunc that wraps this API call can be specified in the [Column] definition.

You could do this the hard way by looping through the List API results and enriching each item
by making an additional API call. However the SDK does all this for you.
*/
type HydrateFunc func(context.Context, *QueryData, *HydrateData) (interface{}, error)

/*
WithCache deprecated: use Memoize
*/
func (f HydrateFunc) WithCache(args ...HydrateFunc) HydrateFunc {
	// build a function to return the cache key
	getCacheKey := f.getCacheKeyFunction(args)

	return f.Memoize(func(o *MemoizeConfiguration) {
		o.GetCacheKeyFunc = getCacheKey
	})
}

/*
Memoize ensures the [HydrateFunc] results are saved in the [connection.ConnectionCache].

Use it to reduce the number of API calls if the HydrateFunc is used by multiple tables.

# Usage

	{
		Name:        "account",
		Type:        proto.ColumnType_STRING,
		Hydrate:     plugin.HydrateFunc(getCommonColumns).Memoize(),
		Description: "The Snowflake account ID.",
		Transform:   transform.FromCamel(),
	}
*/
func (f HydrateFunc) Memoize(opts ...MemoizeOption) HydrateFunc {

	// if this hydrate func is already in the map, it means it has already been memoized
	//if name, alreadyMemoized := f.getOriginalFuncName(); alreadyMemoized {
	//	log.Printf("[WARN] Memoize - %s has already been memoized", name)
	//	// just return the original function (which is already memoized
	//	return f
	//}

	log.Printf("[INFO] Memoize %p %s", f, helpers.GetFunctionName(f))

	config := newMemoizeConfiguration(f)
	for _, o := range opts {
		o(config)
	}
	// build a function to return the cache key
	buildCacheKey := config.GetCacheKeyFunc
	ttl := config.Ttl

	memoizedFunc := func(ctx context.Context, d *QueryData, h *HydrateData) (interface{}, error) {
		// build key
		k, err := buildCacheKey(ctx, d, h)
		if err != nil {
			return nil, err
		}
		cacheKey := k.(string)
		// build a key to access the cacheableHydrateFunctionsPending map, which includes the connection
		// NOTE: when caching the actual hydrate data, the connection name will also be added to the cache key
		// but this happens lower down
		// here, we need to add it
		executeLockKey := fmt.Sprintf("%s-%s", cacheKey, d.Connection.Name)

		// wait until there is no instance of the hydrate function running

		// acquire a Read lock on the pending call map
		memoizedHydrateLock.RLock()
		functionLock, ok := memoizedHydrateFunctionsPending[executeLockKey]
		memoizedHydrateLock.RUnlock()
		if ok {
			// a hydrate function is running - or it has completed
			// wait for the function lock
			return f.waitForHydrate(ctx, d, h, functionLock, cacheKey, ttl)
		}

		// so there was no function lock - no pending hydrate so we must execute

		// acquire a Write lock
		memoizedHydrateLock.Lock()

		// check again for pending call (in case another thread got the Write lock first)
		functionLock, ok = memoizedHydrateFunctionsPending[executeLockKey]
		if ok {
			//  release Write lock
			memoizedHydrateLock.Unlock()

			// a hydrate function is running - or it has completed
			return f.waitForHydrate(ctx, d, h, functionLock, cacheKey, ttl)
		}

		// there is no lock for this function, which means it has not been run yet

		// create a lock
		functionLock = new(sync.WaitGroup)
		// lock it
		functionLock.Add(1)
		// ensure we unlock before return
		defer functionLock.Done()
		// add to map
		memoizedHydrateFunctionsPending[executeLockKey] = functionLock
		// and release Write lock
		memoizedHydrateLock.Unlock()

		log.Printf("[TRACE] Memoize (connection %s, cache key %s) - no pending call found so calling and caching hydrate", d.Connection.Name, cacheKey)
		// no call the hydrate function and cache the result
		return callAndCacheHydrate(ctx, d, h, f, cacheKey, ttl)

	}

	log.Printf("[INFO] Memoize %p %s", f, helpers.GetFunctionName(f))
	// store the memoized func in the name map
	//f.setMemoizedFuncName(memoizedFunc)

	return memoizedFunc
}

func (f HydrateFunc) waitForHydrate(ctx context.Context, d *QueryData, h *HydrateData, functionLock *sync.WaitGroup, cacheKey string, ttl time.Duration) (interface{}, error) {
	functionLock.Wait()

	// we have the function lock
	// so at this point, there is no hydrate function running - we hope the data is in the cache
	// (but it may not be - if there was an error)
	// look in the cache to see if the data is there
	cachedData, ok := d.ConnectionCache.Get(ctx, cacheKey)
	if ok {
		// we got the data
		return cachedData, nil
	}

	// so there is no cached data - call the hydrate function and cache the result
	return callAndCacheHydrate(ctx, d, h, f, cacheKey, ttl)
}

// deprecated
func (f HydrateFunc) getCacheKeyFunction(args []HydrateFunc) HydrateFunc {
	var getCacheKey HydrateFunc
	switch len(args) {
	case 0:
		// no argument was supplied - infer cache key from the hydrate function
		getCacheKey = func(context.Context, *QueryData, *HydrateData) (interface{}, error) {
			return helpers.GetFunctionName(f), nil
		}
	case 1:
		getCacheKey = args[0]
	default:
		panic("WithCache accepts 0 or 1 argument")
	}
	return getCacheKey
}

func callAndCacheHydrate(ctx context.Context, d *QueryData, h *HydrateData, hydrate HydrateFunc, cacheKey string, ttl time.Duration) (interface{}, error) {
	log.Printf("[TRACE] callAndCacheHydrate (connection %s, cache key %s) ", d.Connection.Name, cacheKey)

	// now call the hydrate function
	hydrateData, err := hydrate(ctx, d, h)
	if err != nil {
		// there was an error
		return nil, err
	}

	// so we have a hydrate result - add to the cache
	d.ConnectionCache.SetWithTTL(ctx, cacheKey, hydrateData, ttl)

	// return the hydrate data
	return hydrateData, nil
}
