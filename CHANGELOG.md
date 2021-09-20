## v1.6.0  [2021-09-20]
_What's new_
* Add `ShouldStreamListItem` function which performs context cancellation and limit checking to determine if more data is required. ([#177](https://github.com/turbot/steampipe-plugin-sdk/issues/177))
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

