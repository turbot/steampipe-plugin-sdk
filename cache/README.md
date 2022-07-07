#Caching

##Overview
The plugin query cache maintains a bidirectional GRPC stream connection to the cache server (hosted by the plugin manager process)

Cache data is streamed row by row, in both directions.

###Cache Set
- call `CacheCommand_SET_RESULT_START` command
- for each row:
  - stream to FDW
  - stream to cache with `CacheCommand_SET_RESULT_ITERATE` command
- mark completion with `CacheCommand_SET_RESULT_END`
- fetch index bucket for this table (if it exists): `CacheCommand_GET_INDEX`
- update index bucket and write back `CacheCommand_SET_INDEX`

###Cache Get
- fetch index bucket for this table (if it exists): `CacheCommand_GET_INDEX`
- if this is a cache hit (i.e. an index item satisfying columns and quals exists), call `CacheCommand_GET_RESULT`
- cache will stream results to us a row at a time - for each row 
  - stream to fdw

##Concurrency
The plugin may make more than one request to the cache at a time (for example, if a query consists of multiple scans 
with the same connetion, i.e. multiple concurrent plugin Execution calls). 

Therefore results from the cache may be interleaved, e.g. the results from 2 different cache gets may be streaming 
at the same time    

To handle this, each cache command is made using a CallId (unique time-based identifier). When making a cache command, 
the query cache registers a callback which is stored in a map keyed by call id.

When the cache stream receiver processes a response from the cache, it uses the response callId to find the appropriate 
callback function and invokes it.   

##TTL

The TTL is controlled by the _client_, as multiple clients might be using the same connections and each may have a different TTL.

This is achieved as follows. The cache saves data with a _default TTL_ of 5 minutes (this is the TTL passed to the underlying cache).
When fetching data, the age of the retrieved data is determined using the _insertion time_.
If the age of the data is older than the _client TTL_, it is considered a cache miss

**NOTE:** If the _client TTL_ is greater than the _default TTL_ of the cache, the _default TTL_ is increased as 
appropriate to ensure the data remains in the cache at lkeast as long as is needed for the _client TTL_  
