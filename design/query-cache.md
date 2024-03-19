# Query Cache

The query cache is defined in the `query_cache` package. It is used to store the results of query executions.

## Cache Size and TTL
The size of the cache backing store is determine by the steampipe argument `--max-cache-size` or `database` option property `cache_max_size_mb`. This is the maximum size of the cache in bytes.

The default maximum TTL of the cache is 24 hours. It can be set using the Steampipe argument is determine by the steampipe argument `--cache-max-ttl` or `database` option property `cache_max_ttl`. This is the longest TTL the cache can provivde.
NOTE: a shorter TTL may be specified by a Steampipe client using client cache options.

## Cache Keying

### Index Buckets
Index buckets (of type `IndexBucket`) contain index items for all cache results for a given `table` and `connection`

Index buckets are also stored in the cache. The index bucket key is built from the `table` and `connection`

Index buckets contain a list of index items (type `IndexItem`). Each index item contains the `quals`, `columns` and `limit` for a given cache result.

## Subscribing to cache results
Consider the scenario where execution `q1` requests data from a table `t1`. 
Before the query is complete another execution `q2` requests the same data from `t1`.

We know that we _will_ have the data available in the cache when `q1` completes. To avoid re-executing the same query  - 
and to avoid the overhead of waiting for the query to complete - we can subscribe to the cache result.

When calling `Get` on the cache, a `streamCachedRowFunc` parameter is provided. 
This is a function which streams the data out of the plugin/ (It is a wrapper function around `queryData.streamRow`). 

If a cache `Get` results in a cache hit, the cached data will be streamed to the `streamCachedRowFunc` function. 
There are several permutations to consider:
#### 1) All the data is in the cache
In this case, the cache will retrieve the data and stream it to the `streamCachedRowFunc` function

#### 2) Some of the data is in the cache, and some is not
In this case, the cache will retrieves any data from the cache and streams it. The, as more data is _written_ to the cache,
it will be streamed to the `streamCachedRowFunc` function for any `subscribers` to that cache operation 

#### 3) None of the data is in the cache
Similar to the previous case, as data is written to the cache, it will be streamed to the `streamCachedRowFunc` function
for any `subscribers` to that cache operation

### Implementation of subscription

#### subscribing to cache data

The cache `Get` function calls `getCachedQueryResult`. If all data is in the cache, a `cacheResultSubscriber` is returned.
This is a struct which manages the reading of data which is _already in the cache_.
It provides the method `waitUntilDone` which is used to wait until all cached data has been streamed.

If not all data is in the cache, then the `Get` function determines whether any in-progress caching operations would satisfy the request.
(This is the function `findAndSubscribeToPendingRequest`) If successful, this function returns a `setRequestSubscriber`.
This is a struct which manages the reading of data which is _currently being writtento the cache_.
It also provides the method `waitUntilDone`, to allow the Get call to wait for all data to be streamed.

If either of these functions are successful, the `Get` call waits for all data to be streamed by calling `resultSubscriber.waitUntilDone`

Otherwise the result is a cache MISS.

#### setRequest

The `setRequest` struct is central to the pub/sub model of the cache.

When a query is executed, the cache method `startSet` is called. This returns a `setRequest` struct.

The `setRequest` embeds the cache request struct, which contains the `resultKeyRoot` - this is used to generate a cache key for each page of cache results. 
The `setRequest` also contains a page buffer to store data before we have enough to write to the cache, as well as a bufferIndex
to store how many pages have been written.

When data is received from a query execution, the data is passed to the cache function `iterateSet`.
The `setRequest` for that query execution is found (the queryCache stores a map of `setRequest` indexed by `callId`). 
The data is added to the `setRequest` by calling `addRow`, which writes the row to the page buffer. 
If the page buffer is full, a page of data is written to the cache, using a cache key constructed from the `resultKeyRoot` and the `bufferIndex`.

### setRequestSubscriber


### Finding pending requests

`findAndSubscribeToPendingRequest` first looks for existing pending requests for the same data, by calling `getPendingResultItem`. 
If successful, this returns returns a `pendingIndexItem`, which contains the `indexItem` for the pending request,
as well as a `setRequest` which is the operation currently providing the data (i.e. the initial query execution) 

If a pending request is found, a new subscriber is added to that request by calling `subscribeToPendingRequest`. 
This returns a `setRequestSubscriber`

If no pending request is found, it is assumed that the calling code will execute the query and stream data into the cache 
- so a new pending request is created by calling `addPendingResult`.

## Checking for cache hit

- first find an existing index bucket
- if the index bucket exists, then check for an IndexItem that covers the required quals, columns and limit
  (the requirement is that the cached data must have quals/columns/limit which provide a _superset_ of the requested data )
- if an index item is found, then this is a cache hit! The `Get` function returns a 
`cacheResultSubscriber` which receives data from the cache result (see [Subscribing to cache results])

## Storing cache results


