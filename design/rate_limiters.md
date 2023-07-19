
# Rate Limiting

CHANGES
- flat scopes - only matrix quals allowed (warn is a static scope defined with same name as matrix key)
- only limiter defs at top level
- no `hydrate` scope
- hcl override
- (hcl defintion of plugin limiters - embedded and parsed?)
- rate limiter def introspection table
- rate limit metadata in _ctx


## Overview

Rate limiting can be applied to all `Get`, `List` and column `Hydrate` calls.

For each call, multiple rate limiters may apply. When more than one limiter applies to a call, 
the rate limiter with the largest required wait is respected.

The rate limiters which apply to a call are resolved using `scopes`. Each rate limiter definition specifies
scopes which apply to it, for example `region`, `connection`. Then for each call, the values of these scopes are 
determined and used to identify which limiters apply. 


## Defining Rate Limiters

Rate limiters may be defined in a number of places, in *decreasing* order of precedence:
* *in the plugin options block (COMING SOON???)*
* in the `HydrateConfig`/`GetConfig`/`ListConfig`
* in the Table definition
* in the Plugin Definition
* the SDK default rate limiter (controlled by environment variables *for now??*)

Defining rate limiters at the hydrate/table level allows targeting of the rate limiter without having to use filters: 
- Rate limiters defined in the `HydrateConfig`/`GetConfig`/`ListConfig` will apply **only to that hydrate call**
- Rate limiters defined at the table level will apply for **all hydrate calls in that table** 
(unless overridden by a hydrate config rate limiter) 

NOTE: there is no point defining limiter with a scope of the same level or lower than we are defining it at. 
e.g. If defining limiters in the table defintion, there is no point adding a `table` scope to the limiters
- they are already implicitly table-scoped.     

A set of rate limiters are defined using the `Definitions` struct:
```go
type Definitions struct {
	Limiters []*Definition
	// if set, do not look for further rate limiters for this call
	FinalLimiter bool
}
```
By default, all limiters defined at every precedence level are combined - resolution is additive by default.

(Note that when adding a lower precedence rate limiter, it is not added if a limiter with the same scopes has already been added. i.e. higher precedence limiters are not overwritten by lower precedence ones)

The `FinalLimiter` property *(NAME TBD)* controls whether to continue resolving lower precedence limiter definitions. 
When true, this provides a way of only using limiters defined so far, and excluding lower level limiters. 
For example, if a limiter was defined in *(AS YET NON-EXISTENT)* plugin config, if the `FinalLimiter` flag was set, no further limiters would be added

Each rate limiter is defined using a `Definition`: 

// TODO consider fluent syntax
```go
type Definition struct {
	// the actual limiter config
	Limit     rate.Limit
	BurstSize int

	// the scopes which identify this limiter instance
	// one limiter instance will be created for each combination of scopes which is encountered
	Scopes Scopes

	// this limiter only applies to these these scope values
	Filters []ScopeFilter
}
```
`Scopes` is list of all the scopes which apply to the rate limiter. 
For example, if you want a rate limiter that applies to a single account, region and service, you could use the scopes:
[`connection`, `region`,`service`].
(See below for details of predefined vs custom scope names)

`Filters` is a set of scope value filters which allows a rate limiter to be targeted to spcific set of scope values, 
for example to specify a rate limiter for a specific service only, the filter `"service"="s3` be used. 
```go
type ScopeFilter struct {
	StaticFilterValues map[string]string
	ColumnFilterValues map[string]string
}
```
- For a filter to be satisfied by a set of scope values, all values defined in the filter must match.
- When multiple filters are declared for a rate limiter defintion, **they are OR'ed together**  


### Defining Scopes
Scopes are defined using the `Scopes` struct:
```go
type Scopes struct {
	StaticScopes []string
	ColumnScopes []string
}
```

Scopes are defined in 3 ways:
- `implicit` scopes, with values auto-populated. (Stored in `StaticScopes`)
- `custom` scopes defined by the hydrate config. (Stored in `StaticScopes`)
- `column` scopes, which are populated from `equals quals`. (Stored in `ColumnScopes`)

#### Implicit Scopes
There are a set of predefined scopes which the SDK defines. 
When a query is executed, values for these scopes will be automatically populated:
- **plugin** (the plugin name)
- **table** (the table name)
- **connection** (the steampipe conneciton name)
- **hydrate** (the hydrate function name)

#### Custom scopes
A limiter definition may reference arbitrary custom scopes, e.g. `service`. 
Values for these scopes can be provided by the hydrate call rate limiter configuration:

```go
type HydrateRateLimiterConfig struct {
	// the hydrate config can define additional rate limiters which apply to this call
	Definitions *rate_limiter.Definitions

	// static scope values used to resolve the rate limiter for this hydrate call
	// for example:
	// "service": "s3"
	StaticScopeValues map[string]string

	// how expensive is this hydrate call
	// roughly - how many API calls does it hit
	Cost int
}
```

### Column scopes
Finally, scopes can be derived from column values, specifically Key-Columns. 
The scope values will be populated from the `Qual` values (specifically `equals` Quals).
This will include matrix values, as these are converted into equals quals.

## Resolving Rate Limiters

When executing a hydrate call the following steps are followed:
1) Build the set of rate limiter definitions which may apply to the hydrate call
2) Build the set of scope values which apply to the hydrate call
3) Determine which limiter defintions are satisfied by the scope values (looking at both required scopes and the scope filters)
4) Build a MultiLimiter from the resultant limiter defintions


## Paged List Calls

If the list call uses paging, the SDK provides a hook, `WaitForListRateLimit`, which can be called before paging to apply rate limiting to the list call:

```go
	// List call
	for paginator.HasMorePages() {
		
		// apply rate limiting
		d.WaitForListRateLimit(ctx)
		
		output, err := paginator.NextPage(ctx)
		if err != nil {
			plugin.Logger(ctx).Error("aws_codepipeline_pipeline.listCodepipelinePipelines", "api_error", err)
			return nil, err
		}
		for _, items := range output.Pipelines {
			d.StreamListItem(ctx, items)

			// Context can be cancelled due to manual cancellation or the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil, nil
			}
		}
	}
```

## Scenarios

### 1. Plugin does not define any rate limiters

In this case, the default rate limiter would apply. This is controlled by the following env vars:

```
STEAMPIPE_RATE_LIMIT_ENABLED        (default false)
STEAMPIPE_DEFAULT_HYDRATE_RATE      (default 50)
STEAMPIPE_DEFAULT_HYDRATE_BURST     (default 5
```

### 2. Plugin defines a single plugin scoped rate limiter
NOTE: This overrides the default rate limiter implicitly (by redefining a limiter with the same scope ads the default)

```go
func Plugin(_ context.Context) *plugin.Plugin {
	p := &plugin.Plugin{
		Name:     pluginName,
		TableMap: map[string]*plugin.Table{...},
		RateLimiters: &rate_limiter.Definitions{
			Limiters: []*rate_limiter.Definition{
				{
					Limit:     50,
					BurstSize: 10,
					// this will override the default unscoped limiter
				},
			},
		},
		...
	}

	return p
}
```

### 3. Plugin defines a rate limiter scoped by implicit scope "connection", custom scope "service" and column scope "region"  
NOTE: This overrides the plugin default rate limiter explicitly (by setting `FinalLimiter=true)`

#### Plugin definition
```go

func Plugin(_ context.Context) *plugin.Plugin {
	p := &plugin.Plugin{
		Name:     pluginName,
		TableMap: map[string]*plugin.Table{...},
		RateLimiters: &rate_limiter.Definitions{
			Limiters: []*rate_limiter.Definition{
				{
					Limit:     50,
					BurstSize: 10,
					Scopes: rate_limiter.Scopes{
						StaticScopes: []string{
							"connection",
							"service"
						},
						ColumnScopes: []string{
							"region",
						},
					},
				},
				// do not use the default rate limiter
				FinalLimiter: true,
			},
		},
		...
	}

	return p
}
```
NOTE: `region` must be defined as a key column in order to use the columnScope value, 
and `service` must be defined as a custom scope value for tables or hydrate calls which this limiter targets.    

#### 3a. Table definition which defines a "region" key column and sets the "service" scope value for all hydrate calls

```go
func tableAwsS3AccessPoint(_ context.Context) *plugin.Table {
	return &plugin.Table{
        Name: "aws_s3_access_point",
		List: &plugin.ListConfig{
			Hydrate:    listS3AccessPoints,
			KeyColumns: plugin.SingleColumn("region"),
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "region"}),
			Hydrate:    getS3AccessPoint,
		},
		// set "service" scope to "s3" for all hydrate calls
		RateLimit: &plugin.TableRateLimiterConfig{
			ScopeValues: map[string]string{
				"service": "s3",
			},
		},
		Columns: awsRegionalColumns([]*plugin.Column{...}),
	}
}

```
#### 3b. Hydrate call definition which specifies the "service" scope value


```go
func tableAwsS3AccountSettings(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name: "aws_s3_account_settings",
		List: &plugin.ListConfig{...},
		HydrateConfig: []plugin.HydrateConfig{
			{
				Func: getAccountBucketPublicAccessBlock,
				// Use RateLimit block to define scope values only
						// set the "service" scope value for this hydrate call
				RateLimit: &plugin.HydrateRateLimiterConfig{
					ScopeValues: map[string]string{
						"service": "s3",
					},
				},
			},
		},
		Columns: awsGlobalRegionColumns([]*plugin.Column{...}),
	}
}

```


### 4. Plugin defines rate limiters for "s3" and "ec2" services and one for all other services
NOTE: also scoped by "connection" and "region"
NOTE: This overrides the plugin default rate limiter explicitly (by setting `FinalLimiter=true`)


```go

// scopes used for all rate limiters
var rateLimiterScopes=rate_limiter.Scopes{
    StaticScopes:[]string{
		"connection",
		"service",
	},
    QualScopes:[]string{
        "region",
    }
}

func Plugin(_ context.Context) *plugin.Plugin {
	p := &plugin.Plugin{
		Name: pluginName,
		TableMap: map[string]*plugin.Table{ ...	},
		RateLimiters: &rate_limiter.Definitions{
			Limiters: []*rate_limiter.Definition{
				// rate limiter for s3 service
				{
					Limit:     20,
					BurstSize: 5,
					Scopes: rateLimiterScopes,
					Filters: []rate_limiter.ScopeFilter{
						{
							StaticFilterValues: map[string]string{"service": "s3"},
						},
					},
				},
				// rate limiter for ec2 service
				{
					Limit:     40,
					BurstSize: 5,
					Scopes: rateLimiterScopes,
					Filters: []rate_limiter.ScopeFilter{
						{
							StaticFilterValues: map[string]string{"service": "ec2"},
						},
					},
				},
				// rate limiter for all other services
				{
					Limit:     75,
					BurstSize: 10,
					Scopes: rateLimiterScopes,
				},
			},
		}
		...
	}

	return p
}
```

### 5. Table defines rate limiters for all child hydrate calls, scoped by "hydrate", "connection" and "region"

```go
func tableAwsS3AccountSettings(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_s3_account_settings",
		Description: "AWS S3 Account Block Public Access Settings",
		List:        &plugin.ListConfig{...},
		Columns: awsGlobalRegionColumns([]*plugin.Column{...}),
		RateLimit: &plugin.TableRateLimiterConfig{
			Definitions: &rate_limiter.Definitions{
				Limiters: []*rate_limiter.Definition{
					{
						Limit:     50,
						BurstSize: 10,
						Scopes: rate_limiter.Scopes{
							StaticScopes: map[string]string{
								"connection",
								"hydrate",
							},
							ColumnScopes: []string{
								"region",
							},
						},
					},
				},
			},
		},
	}
}
```
### 6. Hydrate call defines limiter, scoped by "connection" and "region"
```go
func tableAwsS3AccountSettings(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_s3_account_settings",
		Description: "AWS S3 Account Block Public Access Settings",
		List:        &plugin.ListConfig{...},
		Columns:     awsGlobalRegionColumns([]*plugin.Column{...}),
		HydrateConfig: []plugin.HydrateConfig{
			Func: getAccountBucketPublicAccessBlock,
			RateLimit: &plugin.HydrateRateLimiterConfig{
				Definitions: &rate_limiter.Definitions{
					Limiters: []*rate_limiter.Definition{
						{
							Limit:     50,
							BurstSize: 10,
							Scopes: rate_limiter.Scopes{
								StaticScopes: map[string]string{
									"connection",
								},
								ColumnScopes: []string{
									"region",
								},
							},
						},
					},
				},
			},
		},
	}
}
```

### 7. Plugin defines unscoped limiter and hydrate call limiter scoped by "connection" and "region"