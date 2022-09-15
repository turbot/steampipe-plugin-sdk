package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
)

/* 
Column defines a column of a table. 

A column may be populated by a List or Get call. It may alternatively define its own [HydrateFunc] that makes an additional API call for each row. 

A column may transform the data it receives using one or more [transform functions].

[transform functions]: https://steampipe.io/docs/develop/writing-plugins#transform-functions

# Usage

A column populated by a List or Get call:

func itemCols() []*plugin.Column {
    return []*plugin.Column{
        {Name: "id", Type: proto.ColumnType_INT, Description: "The item's unique id."},
    }
}

A column populated by a HydrateFunc:

		Columns: awsColumns([]*plugin.Column{
			{
				Name:        "permissions_boundary_arn",
				Description: "The ARN of the policy used to set the permissions boundary for the user.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getAwsIamUserData,
			},
		}

Columns that transform the data.

		Columns: awsColumns([]*plugin.Column{
			{
				Name:        "mfa_enabled",
				Description: "The MFA status of the user.",
				Type:        proto.ColumnType_BOOL,
				Hydrate:     getAwsIamUserMfaDevices,
				Transform:   transform.From(handleEmptyUserMfaStatus),
			},
			{
				Name:        "login_profile",
				Description: "Contains the user name and password create date for a user.",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getAwsIamUserLoginProfile,
				Transform:   transform.FromValue(),
			},
			...
		}

Examples:

	- [hackernews]
	
[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/d14efdd3f2630f0146e575fe07666eda4e126721/hackernews/item.go#L14

	- [aws]
	
[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/7d17ee78e56da592e0a4e82b90b557225d1a6c11/aws/table_aws_iam_user.go#L22



*/
type Column struct {
	// column name
	Name string
	// column type
	Type proto.ColumnType
	// column description
	Description string
	// explicitly specify the function which populates this data
	// - this is only needed if any of the default hydrate functions wil NOT return this column
	Hydrate HydrateFunc
	// the default column value
	Default interface{}
	//  a list of transforms to generate the column value
	Transform *transform.ColumnTransforms
}

// QueryColumn is struct storing column name and resolved hydrate name
// this is used in the query data when the hydrate funciton has been resolved
type QueryColumn struct {
	*Column
	// the name of the hydrate function which will be used to populate this column
	// - this may be a default hydrate function
	hydrateName string
}

func NewQueryColumn(column *Column, hydrateName string) *QueryColumn {
	return &QueryColumn{column, hydrateName}
}
