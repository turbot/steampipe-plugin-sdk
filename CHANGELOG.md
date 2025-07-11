## v5.12.0 [2025-06-20]
_What's new?_
- Add `UnmarshalJSON` transform. ([#867](https://github.com/turbot/steampipe-plugin-sdk/issues/867))

## v5.11.7 [2025-06-04]
_Bug fixes_
- Fix issue where rate limiters were not working for column hydrate functions. ([#838](https://github.com/turbot/steampipe-plugin-sdk/issues/838))

## v5.11.6 [2025-05-22]
_Bug fixes_
- Fixed issue where errors from plugins during data streaming were not affecting the Export CLI's exit code. Now, any errors encountered during streaming will properly set a non-zero exit code. ([#851](https://github.com/turbot/steampipe-plugin-sdk/issues/851))

_Enhancements_
- Show panic stack trace in error logs. ([#850](https://github.com/turbot/steampipe-plugin-sdk/pull/850))

## v5.11.5 [2025-03-31]
_Bug fixes_
- Fix issue where `EqualsQuals` was not evaluating bool columns correctly. ([#847](https://github.com/turbot/steampipe-plugin-sdk/issues/847))

_Dependencies_
- Upgrade `golang.org/x/net` package to remediate moderate vulnerabilities.

## v5.11.4 [2025-03-12]
_Dependencies_
- Remove dependency on `pipe-fittings`.

## v5.11.3 [2025-02-11]
_Dependencies_
- Upgrade `pipe-fittings` to v2.1.1.

## v5.11.2 [2025-02-05]
_Dependencies_
- Upgrade `pipe-fittings` to v1.6.8.

## v5.11.1 [2025-01-20]
_Dependencies_
- Upgrade `crypto` and `net` packages to remediate critical and high vulnerabilities.

## v5.11.0 [2024-10-22]
_What's new?_
* Add support for pushing down sort order. ([#596](https://github.com/turbot/steampipe-plugin-sdk/issues/596))
* Added RateLimiterFromProto and RateLimiterAsProto functions. ([#826](https://github.com/turbot/steampipe-plugin-sdk/issues/826))

## v5.10.4 [2024-08-29]
_What's new?_
* Updated `hashicorp/go-getter` dependency to v1.7.5.

## v5.10.3 [2024-08-13]
_What's new?_
* Compiled with Go 1.22. ([#811](https://github.com/turbot/steampipe-plugin-sdk/issues/811))

## v5.10.2 [2024-08-02]
_Bug fixes_
* Remove unnecessary INFO log statements in Memoize func. This fixes logging in the plugin export tools. ([#813](https://github.com/turbot/steampipe-plugin-sdk/issues/813))

## v5.10.1 [2024-05-09]
_Bug fixes_
* Ensure QueryData passed to ConnectionKeyColumns value callback is populated with ConnectionManager. ([#797](https://github.com/turbot/steampipe-plugin-sdk/issues/797)) 

## v5.10.0 [2024-04-10]
  _What's new?_
* Add support for connection key columns. ([#768](https://github.com/turbot/steampipe-plugin-sdk/issues/768))
* Add `sp_ctx` and `sp_connection_name` columns to all tables. ([#769](https://github.com/turbot/steampipe-plugin-sdk/issues/769))

## v5.9.0 [2024-02-26]
_What's new?_
* Remove support for Memoized functions to be directly assigned as column hydrate functions. Instead require a wrapper hydrate function.  ([#738](https://github.com/turbot/steampipe-plugin-sdk/issues/738))

_Bug fixes_
* If cache is disabled for the server, but enabled for the client, the query execution code tries to stream to the cache even though there is not active set operation. ([#740](https://github.com/turbot/steampipe-plugin-sdk/issues/740))

## v5.8.0 [2023-12-11]
_What's new?_
* Encapsulate plugin server so it is possible to use it in-process as well as via GRPC. ([#719](https://github.com/turbot/steampipe-plugin-sdk/issues/719))
* Add `steampipe` field to `_ctx` column, containing sdk version. ([#712](https://github.com/turbot/steampipe-plugin-sdk/issues/712))

_Bug fixes_
* Remove `plugin has no connections` error when deleting and then re-adding a connection. ([#725](https://github.com/turbot/steampipe-plugin-sdk/issues/725))
* Fix potential divide by zero bug when setting cache size

## v5.7.0 [2023-11-28]
_What's new?_
* Update `proto.ColumnDefinition` to include column `default` and `hydrate` function name. ([#711](https://github.com/turbot/steampipe-plugin-sdk/issues/711))
* Show connection name along with error message. ([#684](https://github.com/turbot/steampipe-plugin-sdk/issues/684))
* Dynamically allocate a TCP port when STEAMPIPE_PPROF environment variable is set.

_Bug fixes_
* Fix issue where `NullIfEmptySliceValue` panics when input is not a slice. ([#707](https://github.com/turbot/steampipe-plugin-sdk/issues/707))

## v5.6.3 [2023-11-06]
_Bug fixes_
* Fix expired credentials sometimes being left in connection cache - update connection cache to use a backing store per connection, rather than a shared backing store. ([#699](https://github.com/turbot/steampipe-plugin-sdk/issues/699))

## v5.6.2 [2023-10-03]
_Bug fixes_
* The `initialise` function is now being called for implicit hydrate configs (i.e. hydrate functions without explicit config), thereby preventing nil pointer reference errors when the hydrate function returns an error. ([#683](https://github.com/turbot/steampipe-plugin-sdk/issues/683))

## v5.6.1 [2023-09-29]
_What's new?_
* `SetConnectionCacheOptions`, a new GRPC endpoint to clear connection cache. ([#678](https://github.com/turbot/steampipe-plugin-sdk/issues/678))

## v5.6.0 [2023-09-27]
_What's new?_
* Define [rate and concurrency limits](https://steampipe.io/docs/guides/limiter#concurrency--rate-limiting) for plugin execution. ([#623](https://github.com/turbot/steampipe-plugin-sdk/issues/623))
* [Diagnostics](https://steampipe.io/docs/guides/limiter?#exploring--troubleshooting-with-diagnostic-mode) property added to `_ctx` column, containing information on hydrate calls and rate limiting (enabled by setting env var `STEAMPIPE_DIAGNOSTIC_LEVEL=all`)
* Support for JSONB operators in `List` hydrate functions. ([#594](https://github.com/turbot/steampipe-plugin-sdk/issues/594)).
* `Type` property added to `ConnectionConfig` protobuf definition and use to determine if a connection is an aggregator. ([#590](https://github.com/turbot/steampipe-plugin-sdk/issues/590))
* When plugin startup fails, write specially formatted string to stdout so plugin manager can parse the output and display a useful message.  ([#619](https://github.com/turbot/steampipe-plugin-sdk/issues/619))
* Support for multi-line log entries. ([#612](https://github.com/turbot/steampipe-plugin-sdk/issues/612))
* Added `Equals` function for `QualValue`. ([#646](https://github.com/turbot/steampipe-plugin-sdk/issues/646))
 
_Bug fixes_
* Fix cache deadlock caused when the same table is scanned multiple times, and Postgres does not iterate the first scan. 
  Update the query cache to make all scans a subscriber of the cache request, and decouple the reading ands writing of cached data . ([#586](https://github.com/turbot/steampipe-plugin-sdk/issues/586))

## v5.5.2 [2023-09-29]
_What's new?_
* Improve logging for connection config updates and connection cache clearing ([#677](https://github.com/turbot/steampipe-plugin-sdk/issues/677))
 
## v5.5.1 [2023-07-26]
_What's new?_
* Upgrade opentelemetry SDK to v1.16.0. ([#570](https://github.com/turbot/steampipe-plugin-sdk/issues/570))

## v5.5.0 [2023-06-16]
_What's new?_
* Update cache pending item implementation. ([#564](https://github.com/turbot/steampipe-plugin-sdk/issues/564),[#566](https://github.com/turbot/steampipe-plugin-sdk/issues/566)) 
  * Cache requests subscribe to pending items and stream them as the data is received rather than waiting for the item to complete. 
  * When making cache request, update request to include ALL columns which will be returned, not just columns requested. This will allow pending item code to match in cases where a different column in the same hydrate function is requested.

_Bug fixes_
* Optimise `GetSourceFiles` to avoid recursing into child folders when not necessary (update go-kit version). ([#557](https://github.com/turbot/steampipe-plugin-sdk/issues/557))
* Fix hang in hydrate retry code - fix invalid default `retryInterval` in `getBackoff`. ([#558](https://github.com/turbot/steampipe-plugin-sdk/issues/558))
* Update GetSourceFiles to use go-getter GetAny method for https URLs. ([#560](https://github.com/turbot/steampipe-plugin-sdk/issues/560))
* Do not print call stacks in logs. ([#553](https://github.com/turbot/steampipe-plugin-sdk/issues/553))

## v5.4.1 [2023-05-05]
_Bug fixes_
* Avoid loading schema repeatedly when initializing plugin with multiple connections. ([#547](https://github.com/turbot/steampipe-plugin-sdk/issues/547))

## v5.4.0 [2023-04-27]
_What's new?_
* Add SetCacheOptions to allow control of cache at server level. ([#546](https://github.com/turbot/steampipe-plugin-sdk/issues/546))

## v5.3.0 [2023-03-16]
_What's new?_
* Add env var support for limiting the folders from which source files are retrieved. ([#540](https://github.com/turbot/steampipe-plugin-sdk/issues/540))
* Add go-getter support to `TableMapFunc` - add `GetSourceFiles` method to `ConnectionMapData`. ([#542](https://github.com/turbot/steampipe-plugin-sdk/issues/542))

## v5.2.0 [2023-03-02]
_What's new?_
* Add support for update of dynamic plugin schema based on file watching events. ([#457](https://github.com/turbot/steampipe-plugin-sdk/issues/457))
* Update SetAllConnectionConfigs to return map of failed connections.  ([#458](https://github.com/turbot/steampipe-plugin-sdk/issues/458))
* Add support/handling for aggregator connections using dynamic plugins. ([#453](https://github.com/turbot/steampipe-plugin-sdk/issues/453))
* Add new Hydrate function wrapper Memoize to replace WithCache. ([#499](https://github.com/turbot/steampipe-plugin-sdk/issues/499)) 
* Replace Mutexes with RWMutexes and update locking to use RLock where possible. ([#498](https://github.com/turbot/steampipe-plugin-sdk/issues/498))
* If an empty Matrix is returned from MatrixItemFunc, fetch function should not be called.([#496](https://github.com/turbot/steampipe-plugin-sdk/issues/496))
* Remove need for connection config schema - support hcl tags on connection config struct. ([#482](https://github.com/turbot/steampipe-plugin-sdk/issues/482))
* Add support for including child `struct` properties in connection config. ([#168](https://github.com/turbot/steampipe-plugin-sdk/issues/168))
* Instead of renaming `_ctx` column to dedupe it, implement a set of reserved columns which may not be used. ([#536](https://github.com/turbot/steampipe-plugin-sdk/issues/536))
* Add support for like, not like, ilike and not ilike operators in List hydrate functions. ([#592](https://github.com/turbot/steampipe-plugin-sdk/issues/592))

_Bug fixes_
* Fix query cache pending item mechanism. ([#511](https://github.com/turbot/steampipe-plugin-sdk/issues/511),[#512](https://github.com/turbot/steampipe-plugin-sdk/issues/512),[#517](https://github.com/turbot/steampipe-plugin-sdk/issues/517))
* Fix dynamic plugins exhibiting very high CPU usage due to message stream function. ([#537](https://github.com/turbot/steampipe-plugin-sdk/issues/537))

**BREAKING CHANGE**
* Fix typo in `TableMapData` - rename `ConectionCache` to `ConnectionCache`. ([#535](https://github.com/turbot/steampipe-plugin-sdk/issues/535))

## v5.1.2 [2023-01-24]
_Bug fixes_
* Fix cache timeout when doing `select *` -  when adding all columns to the cache request, include the `_ctx` column. ([#490](https://github.com/turbot/steampipe-plugin-sdk/issues/490))

## v5.1.1 [2023-01-24]
_Bug fixes_
* When caching query results, cache all columns, not just those that were requested. ([#487](https://github.com/turbot/steampipe-plugin-sdk/issues/487))

## v5.1.0 [2023-01-11]
_What's new?_
* Add DiagsToWarnings function. ([#474](https://github.com/turbot/steampipe-plugin-sdk/issues/474))
* Add NullIfEmptySlice transform. ([#476](https://github.com/turbot/steampipe-plugin-sdk/issues/476))

## v5.0.2 [2023-01-04]
_Bug fixes_
* When adding the _ctx column to table schema, if that column already exists, keep prepending '_' (e.g. __ctx) until a unique name is found. ([#466](https://github.com/turbot/steampipe-plugin-sdk/issues/466))
* Fix validation for dynamic plugins. This fixes optional key columns which were sometimes not working for dynamic plugins as the key column operators were not being defaulted. ([#469](https://github.com/turbot/steampipe-plugin-sdk/issues/469))

## v5.0.1 [2022-11-30]
_Bug fixes_
* Fix hydrate function caching using `WithCache` for aggregator connections. ([#460](https://github.com/turbot/steampipe-plugin-sdk/issues/460))

## v5.0.0 [2022-11-16]
_What's new?_
* Add `QueryData.GetSourceFiles` which fetches files using [go-getter](https://github.com/hashicorp/go-getter). ([#434](https://github.com/turbot/steampipe-plugin-sdk/issues/434))
* Add support for watching files specified by connection config properties with the tag `watch`. ([#451](https://github.com/turbot/steampipe-plugin-sdk/issues/451))
* Update comments to improve GoDoc documentation and remove unnecessary exports.  ([#432](https://github.com/turbot/steampipe-plugin-sdk/issues/432))
* Update signature of TableMapFunc to accept TableMapData, which included ConnectionCache.  ([#436](https://github.com/turbot/steampipe-plugin-sdk/issues/436))
* Add support to customize the RetryHydrate function. ([#349](https://github.com/turbot/steampipe-plugin-sdk/issues/349))
* Remove explicit setting of rlimit, now that Go 1.19 does it automatically. ([#444](https://github.com/turbot/steampipe-plugin-sdk/issues/444))
* Fix query cache TTL maxing out at 5 mins - set hard limit to 24 hours.  ([#446](https://github.com/turbot/steampipe-plugin-sdk/issues/446))

_Bug fixes_
* Fix nil pointer reference error when qual value is nil. ([#442](https://github.com/turbot/steampipe-plugin-sdk/issues/442))

_Breaking changes_
* The signature or `TableMapFunc` has changed to
  ```func(ctx context.Context, d *TableMapData) (map[string]*Table, error)```
  where `TableMapData` is:
```
type TableMapData struct {
	Connection     *Connection
	ConectionCache *connection.ConnectionCache
}
```
* `QueryData.QueryStatus.RowsRemaining` has been moved to `QueryData.RowsRemaining`. (`QueryData.QueryStatus` is no longer exported)
* `QueryData.KeyColumnQuals` has been renamed to `EqualsQuals` 
* `QueryData.KeyColumnQualsString` has been renamed to  `EqualsQualString`
* `ConcurrencyManager` and `HydrateCall` and no longer exported

## v4.1.11 [2023-01-24]
_Bug fixes_
* Fix cache timeout when doing `select *` -  when adding all columns to the cache request, include the `_ctx` column. ([#490](https://github.com/turbot/steampipe-plugin-sdk/issues/490))

## v4.1.10 [2023-01-24]
_Bug fixes_
* When caching query results, cache all columns, not just those that were requested. ([#487](https://github.com/turbot/steampipe-plugin-sdk/issues/487))

## v4.1.9 [2022-11-30]
_Bug fixes_
* Fix hydrate function caching using `WithCache` for aggregator connections. ([#460](https://github.com/turbot/steampipe-plugin-sdk/issues/460))

## v4.1.8 [2022-09-08]
_Bug fixes_
* Remove explicit setting of the open-files limit, which was using a default which was too small. Instead, make use of the fact that Go 1.19 now sets the soft file limit to the hard limit automatically. ([#444](https://github.com/turbot/steampipe-plugin-sdk/issues/444))

## v4.1.7 [2022-11-08]
 _Bug fixes_
* Move `IsCancelled` back to `plugin` package.

## v4.1.6 [2022-09-02]
_Bug fixes_
* Fix issue where multi-region queries with a `region` where-clause do not have the region in the cache key, causing incorrect cache hits. ([#402](https://github.com/turbot/steampipe-plugin-sdk/issues/402))

## v4.1.5 [2022-08-31]
_Bug fixes_
* Fix `ConnectionCache.SetWithTTL`. ([#399](https://github.com/turbot/steampipe-plugin-sdk/issues/399))
* When connection config changes, store updated connection config _before_ calling `ConnectionConfigChangedFunc`. ([#401](https://github.com/turbot/steampipe-plugin-sdk/issues/401))

## v4.1.4 [2022-08-26]
Remove warning logs

## v4.1.3 [2022-08-26]
_Bug fixes_
* Fix timeout waiting for pending cached item - if pending item has error, all queries waiting for that pending item now return error rather than rerunning the query. ([#396](https://github.com/turbot/steampipe-plugin-sdk/issues/396))

## v4.1.2 [2022-08-25]
_Bug fixes_
* Fix queries sometimes hanging when multiple scans are accessing the cache. ([#391](https://github.com/turbot/steampipe-plugin-sdk/issues/391))

## v4.1.1 [2022-08-24]
_Bug fixes_
* Fix concurrent map access crash for Plugin.connectionCacheMap. ([#389](https://github.com/turbot/steampipe-plugin-sdk/issues/389))

## v4.1.0 [2022-08-24]
_What's new?_
* Add `Plugin` property`ConnectionConfigChangedFunc`. This is a callback function invoked when the connection config changes. The default implementation clears the connection cache and query cache for the changed connection. ([#387](https://github.com/turbot/steampipe-plugin-sdk/issues/387))

## v4.0.2 [2022-08-22]
_Bug fixes_
* Fix `Get` calls stalling due to an attempt to write to an unbuffered channel. ([#382](https://github.com/turbot/steampipe-plugin-sdk/issues/382))

## v4.0.1 [2022-08-11]
_Bug fixes_
* Fix UpdateConnectionConfigs not setting the connection config. ([#375](https://github.com/turbot/steampipe-plugin-sdk/issues/375))
* Fix query results with zero rows not being cached, leading to timeouts and failure to load pending cache results. ([#372](https://github.com/turbot/steampipe-plugin-sdk/issues/372))
* Fix duplicate results returned from cache. ([#371](https://github.com/turbot/steampipe-plugin-sdk/issues/371))
* Remove max concurrent row semaphore. Max concurrent row limiting is disabled by default, and under certain circumstances has been seen to cause a NRE. Removed for now until more detailed benchmarking can be done to justify re-adding. ([#369](https://github.com/turbot/steampipe-plugin-sdk/issues/369))
* Fix parent-child listing, which was broken in v4.0.x  ([#378](https://github.com/turbot/steampipe-plugin-sdk/issues/378))

## v4.0.0 [2022-08-04]
_What's new?_
* A single plugin instance now supports multiple connections, as opposed to an instance being created per connection. ([#365](https://github.com/turbot/steampipe-plugin-sdk/issues/365))
* Memory usage has been substantially reduced, particularly when streaming high row counts. ([#366](https://github.com/turbot/steampipe-plugin-sdk/issues/366))
* Allow control of maximum cache memory usage. ([#302](https://github.com/turbot/steampipe-plugin-sdk/issues/302))
* `QueryData` functions `StreamListItem` and `StreamLeafListItem` are now variadic, and accept multiple items to passed in a single call, simplifying the streaming of a page of data. ([#341](https://github.com/turbot/steampipe-plugin-sdk/issues/341))

_Breaking changes_
* Go version updated to 1.19
* `Plugin` property `TableMapFunc` has changed signature. This is the function which is called for plugins with dynamic schema to return their table schema. Note that the parameter `connection` has been added.
  This may be used in place of the removed `Plugin.Connection` property.

The new signature is:
```
func(ctx context.Context, connection *Connection) (map[string]*Table, error)
```


* `Plugin` properties `Connection`, and `Schema` have been removed, and new property `ConnectionMap` added.  
  This is a map of `ConnectionData` objects, keyed by connection. This is needed as each plugin instance may support multiple connections.
  `ConnectionData` looks as follows
``` // ConnectionData is the data stored by the plugin which is connection dependent
type ConnectionData struct {
	// TableMap is a map of all the tables in the plugin, keyed by the table name
	TableMap map[string]*Table
	// connection this plugin is instantiated for
	Connection *Connection
	// schema - this may be connection specific for dynamic schemas
	Schema map[string]*proto.TableSchema
}
```
* `ConnectionManager` has been renamed to `ConnectionCache`. As the plugin can support multiple connections,
  each connection has its own `ConnectionCache`,  which is a wrapper round an single underlying connection data cache.

NOTE: the property `QueryData.ConnectionManager` has been retained for comptibility reasons - this will be deprecated in a future version

## v3.3.2  [2022-07-11]
_What's new?_
* Add `MaxConcurrency` to `GetConfig` - for use when using the `Get` hydrate as a column hydrate function. ([#353](https://github.com/turbot/steampipe-plugin-sdk/issues/353))
* Validate table Name property matches key in plugin's TableMap. ([#355](https://github.com/turbot/steampipe-plugin-sdk/issues/355))

## v3.3.1  [2022-6-30]
_Bug fixes_
* Deprecated `ShouldIgnoreError` property is not being respected if defined in `plugin.DefaultGetConfig`. ([#347](https://github.com/turbot/steampipe-plugin-sdk/issues/347))
* If cached item has limit, quals must match exactly to be considered a cache hit. ([#345](https://github.com/turbot/steampipe-plugin-sdk/issues/345))

## v3.3.0  [2022-06-22]
_What's new?_
* Add support for Open Telemetry. ([#337](https://github.com/turbot/steampipe-plugin-sdk/issues/337))
* Return query metadata with the scan result, such as the number of hydrate functions called and the cache status. ([#338](https://github.com/turbot/steampipe-plugin-sdk/issues/338))

_Bug fixes_
* Incomplete results should not be added to cache if the context is cancelled. ([#339](https://github.com/turbot/steampipe-plugin-sdk/issues/339))
* Avoid deadlock after panic during newQueryData. ([#332](https://github.com/turbot/steampipe-plugin-sdk/issues/332))

## v3.2.0  [2022-05-20]
_What's new?_
* Deprecate `ShouldIgnoreError` and `ShouldRetryError`. 

  Add instead `ShouldRetryErrorFunc` and a new `IgnoreConfig` containing `ShouldIgnoreErrorFunc`. 

  These functions receive as args the context, QueryData and HydrateData to allow access to connection config and other context data. ([#261](https://github.com/turbot/steampipe-plugin-sdk/issues/261))

_Bug fixes_
* Fix potential transform function casting errors caused by empty hydrate items. If no hydrate data is available, do not call transform functions.  ([#325](https://github.com/turbot/steampipe-plugin-sdk/issues/325))
* Fix the sdk not respecting the DefaultGetConfig when resolving the `ShouldIgnoreError` function.  ([#319](https://github.com/turbot/steampipe-plugin-sdk/issues/319))

## v3.1.0  [2022-03-30]
_What's new?_
* Add `CacheMatch` property to `KeyColumn`, to support key columns which require exact matching to be considered a cache hit. ([#298](https://github.com/turbot/steampipe-plugin-sdk/issues/298))
* Add table and plugin level defaults for `ShouldIgnoreError` and `RetryConfig`. ([#257](https://github.com/turbot/steampipe-plugin-sdk/issues/257))

## v3.0.1 [2022-03-10]
_Bug fixes_
* Fix issue when executing list calls with 'in' clauses, key column values passed in Quals map are incorrect. ([#294](https://github.com/turbot/steampipe-plugin-sdk/issues/294))

## v3.0.0 [2022-03-09]
_What's new?_
* Add support for `ltree` column type. ([#248](https://github.com/turbot/steampipe-plugin-sdk/issues/248))
* Add support for `inet` column type. ([#248](https://github.com/turbot/steampipe-plugin-sdk/issues/291))

## v2.2.0  [2022-03-30]
_What's new?_
* Add `CacheMatch` property to `KeyColumn`, to support key columns which require exact matching to be considered a cache hit. ([#298](https://github.com/turbot/steampipe-plugin-sdk/issues/298))
* Add table and plugin level defaults for `ShouldIgnoreError` and `RetryConfig`. ([#257](https://github.com/turbot/steampipe-plugin-sdk/issues/257))

## v2.1.1  [2022-03-10]
_Bug fixes_
* Fix issue when executing list calls with 'in' clauses, key column values passed in Quals map are incorrect. ([#294](https://github.com/turbot/steampipe-plugin-sdk/issues/294))

## v2.1.0  [2022-03-04]
_What's new?_
* Add support for `is null` and `is not null` quals. ([#286](https://github.com/turbot/steampipe-plugin-sdk/issues/286))

_Bug fixes_
* Fix list call not respecting `in` qual if list config has multiple key columns. ([#275](https://github.com/turbot/steampipe-plugin-sdk/issues/275))

## v2.0.3  [2022-02-14]
_What's new?_
* Update all references to use `github.com/turbot/steampipe-plugin-sdk/v2`. ([#272](https://github.com/turbot/steampipe-plugin-sdk/issues/272))

## v2.0.2  [2022-02-14]
_What's new?_
* Update package name to `github.com/turbot/steampipe-plugin-sdk/v2`. ([#272](https://github.com/turbot/steampipe-plugin-sdk/issues/272))

## v2.0.1  [2022-02-10]
_Bug fixes_
* Fix `requires hydrate data from xxxx but none is available` error  - avoid mutating Column objects, which may be shared between tables. ([#259](https://github.com/turbot/steampipe-plugin-sdk/issues/229))

## v2.0.0  [2022-02-04]
_Changed behaviour_
* A `_ctx` column is now added to all tables. This is a JSON field which specified the Steampipe connection name. ([#246](https://github.com/turbot/steampipe-plugin-sdk/issues/246))

_Bug fixes_
* Fix a query cache bug which under very specific circumstances would lead to an incomplete data set being returned. ([#254](https://github.com/turbot/steampipe-plugin-sdk/issues/254))

 ## v1.8.3  [2021-12-23]
_What's new?_
* Updated `missing required quals` error to include table name. ([#166](https://github.com/turbot/steampipe-plugin-sdk/issues/166))
* Move setting R Limit to OS specific code to allow compilation on Windows systems.
* Update makefile and GRPCServer to support protoc-gen-go-grpc 1.1.0_2.

_Bug fixes_
* Fix 'in' clause not being correctly evaluated when more than one key column qual is specified. ([#239](https://github.com/turbot/steampipe-plugin-sdk/issues/239))
* Fix invalid memory address error when joining on a column with null value. ([#233](https://github.com/turbot/steampipe-plugin-sdk/issues/233))
* Avoid adding duplicate quals to KeyColumnQualMap. Was causing invalid key column errors.  ([#236](https://github.com/turbot/steampipe-plugin-sdk/issues/236))

## v1.8.2  [2021-11-22]

_What's new?_
* Query cache TTL defaults to 5 minutes and is increased to match the TTL of incoming queries. ([#226](https://github.com/turbot/steampipe-plugin-sdk/issues/226))
* Set cache cost of items based on number of rows and columns inserted. ([#227](https://github.com/turbot/steampipe-plugin-sdk/issues/227))
* Add logging for query cache usage. ([#229](https://github.com/turbot/steampipe-plugin-sdk/issues/229))

## v1.8.1  [2021-11-22]
_What's new?_
* Query result caching now determines whether a cache request is a subset of an existing cached item, taking the quals into account.  ([#224](https://github.com/turbot/steampipe-plugin-sdk/issues/224))

_Bug fixes_
* Fix timeout waiting for pending cache transfer to complete. ([#218](https://github.com/turbot/steampipe-plugin-sdk/issues/218))
* Support cancellation while waiting for pending cache transfer. ([#219](https://github.com/turbot/steampipe-plugin-sdk/issues/219))

## v1.8.0  [2021-11-10]
_What's new?_
* Add support for query result caching with stampede prevention, and concurrent query execution.  ([#211](https://github.com/turbot/steampipe-plugin-sdk/issues/211))

## v1.7.3  [2021-11-08]
_Bug fixes_
* FromField transform should return nil property value if property is nil, rather than nil interface value. ([#212](https://github.com/turbot/steampipe-plugin-sdk/issues/212))

## v1.7.2  [2021-11-03]
_Bug_ _fixes_
* Fix KeyColumn `Require` and `Operators` properties not being set to default if a TableMapFunc is used. ([#206](https://github.com/turbot/steampipe-plugin-sdk/issues/206))
* Remove unnecessary TableMapFunc validation from plugin - this breaks dynamic plugins when plugin manager is used. ([#204](https://github.com/turbot/steampipe-plugin-sdk/issues/204))

## v1.7.1  [2021-11-01]
_Bug fixes_
* `FromValue` transform should fall back to next property path if a property value is nil. ([#197](https://github.com/turbot/steampipe-plugin-sdk/issues/197))
* Avoid nil data being passed to hydrate calls if plugin calls StreamListItem with a nil item. ([#198](https://github.com/turbot/steampipe-plugin-sdk/issues/198))

## v1.7.0  [2021-10-18]
_What's new?_
* Add dynamic schema support - add `SchemaMode` property to Plugin. If this is set to `dynamic`, Steampipe will check for plugin schema changes on startup. ([#195](https://github.com/turbot/steampipe-plugin-sdk/issues/195))

## v1.6.2  [2021-10-08]
_Bug fixes_
* Fix `in` clause not working when the table has `any_of` key columns. ([#189](https://github.com/turbot/steampipe-plugin-sdk/issues/189))
* Fix transform functions being called with null data when the `Get` call returns a null item but no error. ([#186](https://github.com/turbot/steampipe-plugin-sdk/issues/186))

## v1.6.1  [2021-09-21]
_Bug fixes_
* Pass context to table creation callback `TableMapFunc`. ([#183](https://github.com/turbot/steampipe-plugin-sdk/issues/183))

## v1.6.0  [2021-09-21]
_What's new?_
* Add `QueryStatus.RowsRemaining` function which performs context cancellation and limit checking to determine how much more data the plugin should provide. ([#177](https://github.com/turbot/steampipe-plugin-sdk/issues/177))
  * This function is used internally by StreamListItem to avoid calling hydrate functions once sufficient data has been returned. 
  * It may also be called directly by the plugin to avoid retrieving unneeded data from the external API
* Enable plugin table creation to use the connection config. New plugin property has been added: `TableMapFunc`. This can be set to a table creation function which, when invoked, has access to the parsed connection config. ([#180](https://github.com/turbot/steampipe-plugin-sdk/issues/180))

## v1.5.1  [2021-09-13]
_Bug fixes_
* Fix `get` call returning nothing if there is an `in` clause for the key column, and matrix parameters are used. ([#170](https://github.com/turbot/steampipe-plugin-sdk/issues/170))

## v1.5.0  [2021-08-06]
_What's new?_ 
* Add cache functions `SetWithTTL` and `Delete`. ([#163](https://github.com/turbot/steampipe-plugin-sdk/issues/163))

_Bug fixes_
* When listing missing quals, only report required quals. ([#159](https://github.com/turbot/steampipe-plugin-sdk/issues/159))
## v1.4.1  [2021-07-20]
_Bug fixes_
* Extraneous log output removed

## v1.4.0  [2021-07-20]
_What's new?_
* Return all columns provided by hydrate functions, not just requested columns. ([#156](https://github.com/turbot/steampipe-plugin-sdk/issues/156))

_Bug fixes_
* Fix matrix parameters not being added to the KeyColumns map passed to hydrate functions. ([#151](https://github.com/turbot/steampipe-plugin-sdk/issues/151))

## v1.3.1  [2021-07-15]
_Bug fixes_
* Fix crash caused by thread sync issue with multi-region union queries. ([#149](https://github.com/turbot/steampipe-plugin-sdk/issues/149))
* When checking if StreamListItem is called from a non-list function, do not through errors for anonymous functions. ([#147](https://github.com/turbot/steampipe-plugin-sdk/issues/147))

## v1.3.0  [2021-07-09]

_What's new?_
* When defining key columns it is now possible to specify supported operators for each column (defaulting to '='). ([#121](https://github.com/turbot/steampipe-plugin-sdk/issues/121))
* Add support for optional key columns. ([#112](https://github.com/turbot/steampipe-plugin-sdk/issues/112))
* Cancellation of GRPC stream is now reflected in the context passed to plugin operations, so plugins can easily handle cancellation by checking the context. ([#17](https://github.com/turbot/steampipe-plugin-sdk/issues/17))
* Add IsCancelled() function to simplify plugins checking for a cancelled context. ([#143](https://github.com/turbot/steampipe-plugin-sdk/issues/143))
* Add WithCache() function - if this is chained after a hydrate function definition, it enables plugin cache optimisation to avoid concurrent hydrate functions with same parameters. ([#116](https://github.com/turbot/steampipe-plugin-sdk/issues/116))

_Breaking changes_
* The property `QueryData.QueryContext.Quals` has been renamed to `QueryContext.UnsafeQuals`. This property contains all quals, not just key columns. These quals should not be used for filtering data as this may break the FDW row data caching, which is keyed based on key column quals. Instead, use the new property `QueryData.Quals`which contains only key column quals. ([#119](https://github.com/turbot/steampipe-plugin-sdk/issues/119))
* Plugins built with `v1.3` of the sdk will only be loaded by Steampipe `v0.6.2` onwards.

## v0.2.10 [2021-06-09]
_What's new?_
* Provide SDK transform to get a qual value. ([#77](https://github.com/turbot/steampipe-plugin-sdk/issues/77))
* Change plugin license to Apache 2.0 ([#488](https://github.com/turbot/steampipe/issues/488))
  
_Bug fixes_
* Fix Cache being recreated for every query. ([#106](https://github.com/turbot/steampipe-plugin-sdk/issues/106))
* Improve error messages when hydrate function fails and when StreamListItem is called from anywhere other than a list function. ([#70](https://github.com/turbot/steampipe-plugin-sdk/issues/70))
* Fix key column setting of AnyColumn only populating the first key column in the KeyColumnQuals map. ([#101](https://github.com/turbot/steampipe-plugin-sdk/issues/101))
* Fix EnsureStringArray transform not working for input type *string. closes #92 (#100)
* Fix Steampipe hanging after hydrate error. ([#103](https://github.com/turbot/steampipe-plugin-sdk/issues/103))
* Fix list call failing with error "get call requires an '=' qual". ([#103](https://github.com/turbot/steampipe-plugin-sdk/issues/103))

## v0.2.9 [2021-05-13]
_What's new?_
* Export GetQualValue function ([#98](https://github.com/turbot/steampipe-plugin-sdk/issues/98))

## v0.2.8 [2021-05-06]
_What's new?_
* Added support for retryable errors and ignorable errors inside `getConfig` and `hydrateConfig`. ([#15](https://github.com/turbot/steampipe-plugin-sdk/issues/15))
* Update `FromField` transform to accept multiple arguments, which are tried in order. ([#55](https://github.com/turbot/steampipe-plugin-sdk/issues/55))
* Add `ProtocolVersion` property to the plugin `GetSchema` response. ([#94](https://github.com/turbot/steampipe-plugin-sdk/issues/94))

## v0.2.7 [2021-03-31]
_Bug fixes_
* Multiregion queries should take region quals into account for 'get' calls. ([#78](https://github.com/turbot/steampipe-plugin-sdk/issues/78))

## v0.2.6 [2021-03-18]
_re-tagged after pushing missing commit_

## v0.2.5 [2021-03-18]
_What's new?_
* Improve the hcl diagnostic to error message conversion to improve parse failure messages. ([#72](https://github.com/turbot/steampipe-plugin-sdk/issues/72))

## v0.2.4 [2021-03-16]
_What's new?_
* Include key column information in GetSchema response to support dynamic path key generation. ([#57](https://github.com/turbot/steampipe-plugin-sdk/issues/57))
* Make get calls with 'in' clauses asynchronous. ([#30](https://github.com/turbot/steampipe-plugin-sdk/issues/30))
* Remove need to call StreamLeafListItem for parent-child list calls. The child list function can now just cal StreamListItem. ([#64](https://github.com/turbot/steampipe-plugin-sdk/issues/64))
* For parent-child list calls, store the parent list call results in RowData `ParentItem` property so they can be accessed by hydrate functions. ([#65](https://github.com/turbot/steampipe-plugin-sdk/issues/65))

_Bug fixes_
* Queries with 'in' clause now work for list calls with required key columns. ([#61](https://github.com/turbot/steampipe-plugin-sdk/issues/61))
* For Get or List calls with required key columns and 'in' clauses, incorrect quals are passed to hydrate calls. ([#69](https://github.com/turbot/steampipe-plugin-sdk/issues/69))

## v0.2.3 [2021-03-02]
_Bug fixes_
* Fix failure of Get calls which use `ItemFromKey` to provide a hydrate item. ([#53](https://github.com/turbot/steampipe-plugin-sdk/issues/53))

## v0.2.2 [2021-02-24]
_What's new?_
* Set the ulimit for plugin processes, respecting env var STEAMPIPE_ULIMIT.  ([#43](https://github.com/turbot/steampipe-plugin-sdk/issues/43))
* When displaying hcl errors, show the context if available.  ([#48](https://github.com/turbot/steampipe-plugin-sdk/issues/48))
* Only show concurrency summary if there is any summary data to show. ([#47](https://github.com/turbot/steampipe-plugin-sdk/issues/47))

_Bug fixes_
* Fix error message not displaying when a query does not provide required get or listquals. ([#42](https://github.com/turbot/steampipe-plugin-sdk/issues/42))

## v0.2.1 [2021-02-18]
_Bug fixes_
* Remove "rc" from version number in the release branch. ([#38](https://github.com/turbot/steampipe-plugin-sdk/issues/38))

## v0.2.0 [2021-02-17]
_What's new?_

* Add support for multi-region querying. ([#20](https://github.com/turbot/steampipe-plugin-sdk/issues/20))
* Add support for connection config. ([#21](https://github.com/turbot/steampipe-plugin-sdk/issues/21))
* Add mechanism to limit max hydrate function concurrency. ([#12](https://github.com/turbot/steampipe-plugin-sdk/issues/12))
* Update environment variables to use STEAMPIPE prefix. ([#32](https://github.com/turbot/steampipe-plugin-sdk/issues/32))
* Provide dependency mechanism to allow Steampipe to know if a plugin uses a newer sdk version. ([#25](https://github.com/turbot/steampipe-plugin-sdk/issues/25))

## v0.1.1 [2021-02-11]

_What's new?_
* Add transforms StringArrayToMap and EnsureStringArray. ([#3](https://github.com/turbot/steampipe-plugin-sdk/issues/3))

_Bug fixes_
* Fix ToLower and ToUpper transforms not working when input value is a with string pointers. ([#13](https://github.com/turbot/steampipe-plugin-sdk/issues/13))
* Fix failure to report errors returned from Get function. ([#23](https://github.com/turbot/steampipe-plugin-sdk/issues/23))

