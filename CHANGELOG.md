## v0.2.4 [2021-03-12]
_What's new_
* Include key column information in GetSchema response to support dynamic path key generation. [#57](https://github.com/turbot/steampipe-plugin-sdk/issues/57))
* Make get calls with 'in' clauses asynchronous. [#30](https://github.com/turbot/steampipe-plugin-sdk/issues/30))
* Remove need to call StreamLeafListItem for parent-child list calls. The child list function can now just cal StreamListItem. [#64](https://github.com/turbot/steampipe-plugin-sdk/issues/64))
* For parent-child list call, store parent list call results in hydrateResults so they can be accessed by hydrate functions. [#65](https://github.com/turbot/steampipe-plugin-sdk/issues/65))

_Bug fixes_
* Queries with 'in' clause now work for list calls with required key columns. [#61](https://github.com/turbot/steampipe-plugin-sdk/issues/61))

## v0.2.3 [2021-03-02]
_Bug fixes_
* Fix failure of Get calls which use `ItemFromKey` to provide a hydrate item. [#53](https://github.com/turbot/steampipe-plugin-sdk/issues/53))

## v0.2.2 [2021-02-24]
_What's new?_
* Set the ulimit for plugin processes, respecting env var STEAMPIPE_ULIMIT.  [#43](https://github.com/turbot/steampipe-plugin-sdk/issues/43))
* When displaying hcl errors, show the context if available.  [#48](https://github.com/turbot/steampipe-plugin-sdk/issues/48))
* Only show concurrency summary if there is any summary data to show. [#47](https://github.com/turbot/steampipe-plugin-sdk/issues/47))

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