/*
Package transform defines functions that modify [plugin.Column] values.

For a given column, data comes from a hydrate call either implicitly (from List or Get), or explicitly if you declared a hydrate function. 
In either case the result is a hydrate item that may need to be transformed in order to populate the column. 

The transform functions in this package handle common needs. You can also use [transform.From] to call your own custom transform function.

You can form chains of transform functions, but the transform chain must begin with a transform.FromXXX function:

{
	Name: "created_date",
	Type: proto.ColumnType_TIMESTAMP,
	Transform: transform.FromField("Domain.CreatedDate").Transform(whoisDateToTimestamp).NullIfZero(),
	Description: "Date when the domain was first registered."
},

# DefaultTransform

To transform all columns of a plugin in the same way:

func Plugin(ctx context.Context) *plugin.Plugin {
	p := &plugin.Plugin{
		Name: "steampipe-plugin-hackernews",
		DefaultTransform: transform.FromJSONTag().NullIfZero(),
	...
}

Examples:
	- [hackernews]

[hackernews]:  https://github.com/turbot/steampipe-plugin-hackernews/blob/bbfbb12751ad43a2ca0ab70901cde6a88e92cf44/hackernews/plugin.go#L10

# FromConstant

To populate a column with a constant value:

	// standard gcp columns
	{
		Name:        "location",
		Description: ColumnDescriptionLocation,
		Type:        proto.ColumnType_STRING,
		Transform:   transform.FromConstant("global"),
	},

Examples:
	- [gcp]

[gcp]: https://github.com/turbot/steampipe-plugin-gcp/blob/f8af4bfb9a5d368d9a75c57b7405dc12aed3a513/gcp/table_gcp_audit_policy.go#L44

# FromField

To extract a field from a property path in a Go struct:

	{
		Name: "id", 
		Type: proto.ColumnType_STRING, 
		Transform: transform.FromField("Comment.ID"), Description: "ID of the comment."},
	}		

Examples:
	- [reddit]

[reddit]: https://github.com/turbot/steampipe-plugin-reddit/blob/8e95ae6da8f8bb29013016513dfac3f8e443994d/reddit/table_reddit_my_comment.go#L23

# FromValue

To use the raw value of the hydrate item (e.g., a string), when it is the value intended for the column.

	{
		Name:        "cloud_environment",
		Type:        proto.ColumnType_STRING,
		Hydrate:     getCloudEnvironment,
		Description: ColumnDescriptionCloudEnvironment,
		Transform:   transform.FromValue(),
	},


Examples:
	- [azure] 

[azure]: https://github.com/turbot/steampipe-plugin-azure/blob/be913eacb88adbab93ca6749bb805d11731785d0/azure/common_columns.go#L14

# FromCamel

To convert a camel-cased Go field name to a snake-cased column name:

		{
			Name:        "account",
			Type:        proto.ColumnType_STRING,
			Hydrate:     plugin.HydrateFunc(getCommonColumns).WithCache(),
			Description: "The Snowflake account ID.",
			Transform:   transform.FromCamel(),
		},

Examples:
	- [snowflake] 

[snowflake]: https://github.com/turbot/steampipe-plugin-snowflake/blob/6e243aad63b5706ee1a9dd8979df88eb097e38a8/snowflake/common_columns.go#L25

# FromGo

To convert a camel-cased Go field name to a snake-cased column name, while preserving common acronyms such as ASCII or HTTP:

	{
		Name:        "user_arn",
		Description: "The Amazon Resource Name (ARN) of the user.",
		Type:        proto.ColumnType_STRING,
		Transform:   transform.FromGo(),
	},

Examples:
	- [aws]

[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/010ec0762c273b4549b4369fe05d61ec1ce24a9b/aws/table_aws_iam_credential_report.go#L57

# FromTag

To generate a value from a field of a Go struct with a tag 'tagName:value'

Examples:
	- none

# FromJSONTag

To generate a value from a field of a Go struct with a tag 'json:value'

	{
		Name: "adversary_ids", 
		Type: proto.ColumnType_JSON, 
		Transform: transform.FromJSONTag()
	},

Examples:	
	- [crowdstrike]

[crowdstrike]: https://github.com/turbot/steampipe-plugin-crowdstrike/blob/2c02025fd83b525c8b8ed9cf98fe7540ef1209b8/crowdstrike/table_crowdstrike_detection.go

# From

To generate a value by calling 'transformFunc'

Examples:
	- none

# FromP

To generate a value by calling 'transformFunc', passing param.

	{
		Name: "client_delete_prohibited", 
		Type: proto.ColumnType_BOOL, 
		Transform: transform.FromP(statusToBool, "clientdeleteprohibited"), 
		Description: "This status code tells your domain's registry to reject requests to delete the domain."
	},

Examples:
	- [whois]

[whois]: https://github.com/turbot/steampipe-plugin-whois/blob/f5e218cd11e04a7afa6855cf0af419ba1fd33842/whois/table_whois_domain.go#L43

# Chained function: Transform

To apply an arbitrary transform to the data (specified by 'transformFunc').

	{
		Name: "created_date", 
		Type: proto.ColumnType_TIMESTAMP, 
		Transform: transform.FromField("Domain.CreatedDate").Transform(whoisDateToTimestamp).NullIfZero(), 
		Description: "Date when the domain was first registered."
	},

Examples:
	- [whois]

[whois]: https://github.com/turbot/steampipe-plugin-whois/blob/f5e218cd11e04a7afa6855cf0af419ba1fd33842/whois/table_whois_domain.go#L26

# Chained function: NullIfEqual

To return nil if the input value equals the transform param

		{
			Name:        "access_key_1_last_rotated",
			Description: "The date and time when the user's access key was created or last changed.",
			Type:        proto.ColumnType_TIMESTAMP,
			Transform:   transform.FromGo().NullIfEqual("N/A"),
		},

Examples:
	- [aws]

[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/010ec0762c273b4549b4369fe05d61ec1ce24a9b/aws/table_aws_iam_credential_report.go#L81

# Chained function: NullIfZero

To return nil of the input value equals the zero value of its type.

	{
		Name: "domain_id", 
		Type: proto.ColumnType_STRING, 
		Transform: transform.FromField("Domain.ID").NullIfZero(), 
		Description: "Unique identifier for the domain."
	},
	
Examples:	
	- [whois] 

[whois]: https://github.com/turbot/steampipe-plugin-whois/blob/f5e218cd11e04a7afa6855cf0af419ba1fd33842/whois/table_whois_domain.go#L31

*/
package transform
