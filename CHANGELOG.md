## v5.0.2 [2023-01-04]
_Bug fixes_
* When adding the `_ctx` column to table schema, if that column already exists, keep prepending '_' (e.g. `__ctx`) until a unique name is found. ([#466](https://github.com/turbot/steampipe-plugin-sdk/issues/466))
* Fix validation for dynamic plugins. This fixes optional key columns which were sometimes not working for dynamic plugins as the key column operators were not being defaulted. ([#469](https://github.com/turbot/steampipe-plugin-sdk/issues/469))

## v5.0.1 [2022-11-30]
_Bug fixes_
* Fix hydrate function caching using `WithCache` for aggregator connections. ([#460](https://github.com/turbot/steampipe-plugin-sdk/issues/460))

## v5.0.0 [2022-11-16]
_What's new_
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
_What's new_
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
_What's new_
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
_What's new_
* Add `MaxConcurrency` to `GetConfig` - for use when using the `Get` hydrate as a column hydrate function. ([#353](https://github.com/turbot/steampipe-plugin-sdk/issues/353))
* Validate table Name property matches key in plugin's TableMap. ([#355](https://github.com/turbot/steampipe-plugin-sdk/issues/355))

## v3.3.1  [2022-6-30]
_Bug fixes_
* Deprecated `ShouldIgnoreError` property is not being respected if defined in `plugin.DefaultGetConfig`. ([#347](https://github.com/turbot/steampipe-plugin-sdk/issues/347))
* If cached item has limit, quals must match exactly to be considered a cache hit. ([#345](https://github.com/turbot/steampipe-plugin-sdk/issues/345))

## v3.3.0  [2022-06-22]
_What's new_
* Add support for Open Telemetry. ([#337](https://github.com/turbot/steampipe-plugin-sdk/issues/337))
* Return query metadata with the scan result, such as the number of hydrate functions called and the cache status. ([#338](https://github.com/turbot/steampipe-plugin-sdk/issues/338))

_Bug fixes_
* Incomplete results should not be added to cache if the context is cancelled. ([#339](https://github.com/turbot/steampipe-plugin-sdk/issues/339))
* Avoid deadlock after panic during newQueryData. ([#332](https://github.com/turbot/steampipe-plugin-sdk/issues/332))

## v3.2.0  [2022-05-20]
_What's new_
* Deprecate `ShouldIgnoreError` and `ShouldRetryError`. 

  Add instead `ShouldRetryErrorFunc` and a new `IgnoreConfig` containing `ShouldIgnoreErrorFunc`. 

  These functions receive as args the context, QueryData and HydrateData to allow access to connection config and other context data. ([#261](https://github.com/turbot/steampipe-plugin-sdk/issues/261))

_Bug fixes_
* Fix potential transform function casting errors caused by empty hydrate items. If no hydrate data is available, do not call transform functions.  ([#325](https://github.com/turbot/steampipe-plugin-sdk/issues/325))
* Fix the sdk not respecting the DefaultGetConfig when resolving the `ShouldIgnoreError` function.  ([#319](https://github.com/turbot/steampipe-plugin-sdk/issues/319))

## v3.1.0  [2022-03-30]
_What's new_
* Add `CacheMatch` property to `KeyColumn`, to support key columns which require exact matching to be considered a cache hit. ([#298](https://github.com/turbot/steampipe-plugin-sdk/issues/298))
* Add table and plugin level defaults for `ShouldIgnoreError` and `RetryConfig`. ([#257](https://github.com/turbot/steampipe-plugin-sdk/issues/257))

## v3.0.1 [2022-03-10]
_Bug fixes_
* Fix issue when executing list calls with 'in' clauses, key column values passed in Quals map are incorrect. ([#294](https://github.com/turbot/steampipe-plugin-sdk/issues/294))

## v3.0.0 [2022-03-09]
_What's new_
* Add support for `ltree` column type. ([#248](https://github.com/turbot/steampipe-plugin-sdk/issues/248))
* Add support for `inet` column type. ([#248](https://github.com/turbot/steampipe-plugin-sdk/issues/291))

## v2.2.0  [2022-03-30]
_What's new_
* Add `CacheMatch` property to `KeyColumn`, to support key columns which require exact matching to be considered a cache hit. ([#298](https://github.com/turbot/steampipe-plugin-sdk/issues/298))
* Add table and plugin level defaults for `ShouldIgnoreError` and `RetryConfig`. ([#257](https://github.com/turbot/steampipe-plugin-sdk/issues/257))

## v2.1.1  [2022-03-10]
_Bug fixes_
* Fix issue when executing list calls with 'in' clauses, key column values passed in Quals map are incorrect. ([#294](https://github.com/turbot/steampipe-plugin-sdk/issues/294))

## v2.1.0  [2022-03-04]
_What's new_
* Add support for `is null` and `is not null` quals. ([#286](https://github.com/turbot/steampipe-plugin-sdk/issues/286))

_Bug fixes_
* Fix list call not respecting `in` qual if list config has multiple key columns. ([#275](https://github.com/turbot/steampipe-plugin-sdk/issues/275))

## v2.0.3  [2022-02-14]
_What's new_
* Update all references to use `github.com/turbot/steampipe-plugin-sdk/v2`. ([#272](https://github.com/turbot/steampipe-plugin-sdk/issues/272))

## v2.0.2  [2022-02-14]
_What's new_
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
_What's new_
* Updated `missing required quals` error to include table name. ([#166](https://github.com/turbot/steampipe-plugin-sdk/issues/166))
* Move setting R Limit to OS specific code to allow compilation on Windows systems.
* Update makefile and GRPCServer to support protoc-gen-go-grpc 1.1.0_2.

_Bug fixes_
* Fix 'in' clause not being correctly evaluated when more than one key column qual is specified. ([#239](https://github.com/turbot/steampipe-plugin-sdk/issues/239))
* Fix invalid memory address error when joining on a column with null value. ([#233](https://github.com/turbot/steampipe-plugin-sdk/issues/233))
* Avoid adding duplicate quals to KeyColumnQualMap. Was causing invalid key column errors.  ([#236](https://github.com/turbot/steampipe-plugin-sdk/issues/236))

## v1.8.2  [2021-11-22]

_What's new_
* Query cache TTL defaults to 5 minutes and is increased to match the TTL of incoming queries. ([#226](https://github.com/turbot/steampipe-plugin-sdk/issues/226))
* Set cache cost of items based on number of rows and columns inserted. ([#227](https://github.com/turbot/steampipe-plugin-sdk/issues/227))
* Add logging for query cache usage. ([#229](https://github.com/turbot/steampipe-plugin-sdk/issues/229))

## v1.8.1  [2021-11-22]
_What's new_
* Query result caching now determines whether a cache request is a subset of an existing cached item, taking the quals into account.  ([#224](https://github.com/turbot/steampipe-plugin-sdk/issues/224))

_Bug fixes_
* Fix timeout waiting for pending cache transfer to complete. ([#218](https://github.com/turbot/steampipe-plugin-sdk/issues/218))
* Support cancellation while waiting for pending cache transfer. ([#219](https://github.com/turbot/steampipe-plugin-sdk/issues/219))

## v1.8.0  [2021-11-10]
_What's new_
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
_What's new_
* Add dynamic schema support - add `SchemaMode` property to Plugin. If this is set to `dynamic`, Steampipe will check for plugin schema changes on startup. ([#195](https://github.com/turbot/steampipe-plugin-sdk/issues/195))

## v1.6.2  [2021-10-08]
_Bug fixes_
* Fix `in` clause not working when the table has `any_of` key columns. ([#189](https://github.com/turbot/steampipe-plugin-sdk/issues/189))
* Fix transform functions being called with null data when the `Get` call returns a null item but no error. ([#186](https://github.com/turbot/steampipe-plugin-sdk/issues/186))

## v1.6.1  [2021-09-21]
_Bug fixes_
* Pass context to table creation callback `TableMapFunc`. ([#183](https://github.com/turbot/steampipe-plugin-sdk/issues/183))

## v1.6.0  [2021-09-21]
_What's new_
* Add `QueryStatus.RowsRemaining` function which performs context cancellation and limit checking to determine how much more data the plugin should provide. ([#177](https://github.com/turbot/steampipe-plugin-sdk/issues/177))
  * This function is used internally by StreamListItem to avoid calling hydrate functions once sufficient data has been returned. 
  * It may also be called directly by the plugin to avoid retrieving unneeded data from the external API
* Enable plugin table creation to use the connection config. New plugin property has been added: `TableMapFunc`. This can be set to a table creation function which, when invoked, has access to the parsed connection config. ([#180](https://github.com/turbot/steampipe-plugin-sdk/issues/180))

## v1.5.1  [2021-09-13]
_Bug fixes_
* Fix `get` call returning nothing if there is an `in` clause for the key column, and matrix parameters are used. ([#170](https://github.com/turbot/steampipe-plugin-sdk/issues/170))

## v1.5.0  [2021-08-06]
_What's new_ 
* Add cache functions `SetWithTTL` and `Delete`. ([#163](https://github.com/turbot/steampipe-plugin-sdk/issues/163))

_Bug fixes_
* When listing missing quals, only report required quals. ([#159](https://github.com/turbot/steampipe-plugin-sdk/issues/159))
## v1.4.1  [2021-07-20]
_Bug fixes_
* Extraneous log output removed

## v1.4.0  [2021-07-20]
_What's new_
* Return all columns provided by hydrate functions, not just requested columns. ([#156](https://github.com/turbot/steampipe-plugin-sdk/issues/156))

_Bug fixes_
* Fix matrix parameters not being added to the KeyColumns map passed to hydrate functions. ([#151](https://github.com/turbot/steampipe-plugin-sdk/issues/151))

## v1.3.1  [2021-07-15]
_Bug fixes_
* Fix crash caused by thread sync issue with multi-region union queries. ([#149](https://github.com/turbot/steampipe-plugin-sdk/issues/149))
* When checking if StreamListItem is called from a non-list function, do not through errors for anonymous functions. ([#147](https://github.com/turbot/steampipe-plugin-sdk/issues/147))

## v1.3.0  [2021-07-09]

_What's new_
* When defining key columns it is now possible to specify supported operators for each column (defaulting to '='). ([#121](https://github.com/turbot/steampipe-plugin-sdk/issues/121))
* Add support for optional key columns. ([#112](https://github.com/turbot/steampipe-plugin-sdk/issues/112))
* Cancellation of GRPC stream is now reflected in the context passed to plugin operations, so plugins can easily handle cancellation by checking the context. ([#17](https://github.com/turbot/steampipe-plugin-sdk/issues/17))
* Add IsCancelled() function to simplify plugins checking for a cancelled context. ([#143](https://github.com/turbot/steampipe-plugin-sdk/issues/143))
* Add WithCache() function - if this is chained after a hydrate function definition, it enables plugin cache optimisation to avoid concurrent hydrate functions with same parameters. ([#116](https://github.com/turbot/steampipe-plugin-sdk/issues/116))

_Breaking changes_
* The property `QueryData.QueryContext.Quals` has been renamed to `QueryContext.UnsafeQuals`. This property contains all quals, not just key columns. These quals should not be used for filtering data as this may break the FDW row data caching, which is keyed based on key column quals. Instead, use the new property `QueryData.Quals`which contains only key column quals. ([#119](https://github.com/turbot/steampipe-plugin-sdk/issues/119))
* Plugins built with `v1.3` of the sdk will only be loaded by Steampipe `v0.6.2` onwards.

## v0.2.10 [2021-06-09]
_What's new_
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
_What's new_
* Export GetQualValue function ([#98](https://github.com/turbot/steampipe-plugin-sdk/issues/98))

## v0.2.8 [2021-05-06]
_What's new_
* Added support for retryable errors and ignorable errors inside `getConfig` and `hydrateConfig`. ([#15](https://github.com/turbot/steampipe-plugin-sdk/issues/15))
* Update `FromField` transform to accept multiple arguments, which are tried in order. ([#55](https://github.com/turbot/steampipe-plugin-sdk/issues/55))
* Add `ProtocolVersion` property to the plugin `GetSchema` response. ([#94](https://github.com/turbot/steampipe-plugin-sdk/issues/94))

## v0.2.7 [2021-03-31]
_Bug fixes_
* Multiregion queries should take region quals into account for 'get' calls. ([#78](https://github.com/turbot/steampipe-plugin-sdk/issues/78))

## v0.2.6 [2021-03-18]
_re-tagged after pushing missing commit_

## v0.2.5 [2021-03-18]
_What's new_
* Improve the hcl diagnostic to error message conversion to improve parse failure messages. ([#72](https://github.com/turbot/steampipe-plugin-sdk/issues/72))

## v0.2.4 [2021-03-16]
_What's new_
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

