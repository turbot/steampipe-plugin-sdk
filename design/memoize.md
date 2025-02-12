# Memoizing hydrate funcs

When a HydrateFunc is memoized, the result of the hydrate func is cached in memory for the duration of the plugin run. This is useful for hydrate funcs which are expensive to run and are called multiple times during a single plugin run.

When memoizing a func, a new function is returned which wraps the underlying func in cache get/set logic. The original func is not modified.

A map of original function names is maintained, keyed by the pointer to the wrapped function. THis allows retrieval of the underlying function name from the wrapped function. 76 

*** This does not work - all anon function have the same pointer so the map wil only have a single entry *** 


Who uses memoized name map/NamedHydrateFunc

## IsMemoized 
used for rate limiters - still works as all memoized functions will appear in the map 

## newNamedHydrateFunc
Who creates named hydrate call? This is crucial as we MUST NOT call newNamedHydrateFunc with a memoized hydrate func

### hydrate call depends
```
func newHydrateCall(config *HydrateConfig, d *QueryData) (*hydrateCall, error) {
	res := &hydrateCall{
		Config:    config,
		queryData: d,
		// default to empty limiter
		rateLimiter: rate_limiter.EmptyMultiLimiter(),
	}
	res.NamedHydrateFunc = *config.NamedHydrate

	for _, f := range config.Depends {
		res.Depends = append(res.Depends, newNamedHydrateFunc(f))
	}

	return res, nil
}
```

### RetryHydrate
Doesn't seem to be used anywhere
```
func RetryHydrate(ctx context.Context, d *QueryData, hydrateData *HydrateData, hydrate HydrateFunc, retryConfig *RetryConfig) (hydrateResult interface{}, err error) {
	return retryNamedHydrate(ctx, d, hydrateData, newNamedHydrateFunc(hydrate), retryConfig)
}
```

## Name

Who references the name field

### HydrateConfig tags
GetConfig/ListConfig/HydrateConfig has a map ot tags which is auto populated with the hydrate name


### hydrateCall canStart
```

// check whether all hydrate functions we depend on have saved their results
for _, dep := range h.Depends {
    if !slices.Contains(rowData.getHydrateKeys(), dep.Name) {
        return false
    }
}
```

### hydrateCall start

```
// retrieve the concurrencyDelay for the call
concurrencyDelay := r.getHydrateConcurrencyDelay(h.Name)
```

### ListConfig Validate
```
// ensure that if there is an explicit hydrate config for the list hydrate, it does not declare dependencies
listHydrateName := table.List.namedHydrate.Name
for _, h := range table.HydrateConfig {
    if h.namedHydrate.Name == listHydrateName {
    ...
}
```

### Plugin buildHydrateConfigMap
```
for i := range p.HydrateConfig {
		h := &p.HydrateConfig[i]
		h.initialise(nil)
		funcName := h.namedHydrate.Name
		p.hydrateConfigMap[funcName] = h
```
