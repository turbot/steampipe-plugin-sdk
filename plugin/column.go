package plugin

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"log"
	"net"
)

/*
Column defines a column of a table.

A column may be populated by a List or Get call. It may alternatively define its own [HydrateFunc] that makes an additional API call for each row.

A column may transform the data it receives using one or more [transform functions].

To define a column populated by a List or Get call:

	func itemCols() []*plugin.Column {
	    return []*plugin.Column{
	        {Name: "id", Type: proto.ColumnType_INT, Description: "The item's unique id."},
	    }
	}

To define a column populated by a HydrateFunc:

	Columns: awsColumns([]*plugin.Column{
		{
			Name:        "permissions_boundary_arn",
			Description: "The ARN of the policy used to set the permissions boundary for the user.",
			Type:        proto.ColumnType_STRING,
			Hydrate:     getAwsIamUserData,
		},
	}

To define columns that transform the data:

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
  - [aws]

[transform functions]: https://steampipe.io/docs/develop/writing-plugins#transform-functions
[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/d14efdd3f2630f0146e575fe07666eda4e126721/hackernews/item.go#L14
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
	// - this is only needed if any of the default hydrate functions will NOT return this column
	Hydrate      HydrateFunc
	NamedHydrate NamedHydrateFunc
	// the default column value
	Default interface{}
	//  a list of transforms to generate the column value
	Transform *transform.ColumnTransforms
}

func (c Column) initialise() {
	if c.Hydrate == nil && c.NamedHydrate.empty() {
		return
	}
	// populate the named hydrate funcs
	if c.NamedHydrate.empty() {
		// create a named hydrate func, assuming this function is not memoized
		c.NamedHydrate = newNamedHydrateFunc(c.Hydrate)
	} else {
		c.Hydrate = c.NamedHydrate.Func
		// named hydrate was explicitly specified - probably meaning the hydrate is memoized
		// call initialize to populate IsInitialised
		c.NamedHydrate.initialize()
	}

}

// ToColumnValue converts a value of unknown type to a valid protobuf column value.type
func (c Column) ToColumnValue(val any) (*proto.Column, error) {
	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Errorf("%s: %v", c.Name, r))
		}
	}()

	// if the value is a pointer, get its value and use that
	val = helpers.DereferencePointer(val)
	if val == nil {
		if c.Default != nil {
			val = c.Default
		} else {
			// return nil
			return &proto.Column{Value: &proto.Column_NullValue{}}, nil
		}
	}

	var columnValue *proto.Column

	switch c.Type {
	case proto.ColumnType_STRING:
		columnValue = &proto.Column{Value: &proto.Column_StringValue{StringValue: types.ToString(val)}}
		break
	case proto.ColumnType_BOOL:
		b, err := types.ToBool(val)
		if err != nil {
			return nil, fmt.Errorf("interfaceToColumnValue failed for column '%s': %v", c.Name, err)
		}
		columnValue = &proto.Column{Value: &proto.Column_BoolValue{BoolValue: b}}
		break
	case proto.ColumnType_INT:
		i, err := types.ToInt64(val)
		if err != nil {
			return nil, fmt.Errorf("interfaceToColumnValue failed for column '%s': %v", c.Name, err)
		}

		columnValue = &proto.Column{Value: &proto.Column_IntValue{IntValue: i}}
		break
	case proto.ColumnType_DOUBLE:
		d, err := types.ToFloat64(val)
		if err != nil {
			return nil, fmt.Errorf("interfaceToColumnValue failed for column '%s': %v", c.Name, err)
		}
		columnValue = &proto.Column{Value: &proto.Column_DoubleValue{DoubleValue: d}}
		break
	case proto.ColumnType_JSON:
		strValue, ok := val.(string)
		if ok {
			// NOTE: Strings are assumed to be raw JSON, so are passed through directly.
			// This is the most common case, but means it's currently impossible to
			// pass through a string and have it marshalled to be a JSON representation
			// of a string.
			columnValue = &proto.Column{Value: &proto.Column_JsonValue{JsonValue: []byte(strValue)}}
		} else {
			res, err := json.Marshal(val)
			if err != nil {
				log.Printf("[ERROR] failed to marshal value to json: %v\n", err)
				return nil, fmt.Errorf("%s: %v ", c.Name, err)
			}
			columnValue = &proto.Column{Value: &proto.Column_JsonValue{JsonValue: res}}
		}
	case proto.ColumnType_DATETIME, proto.ColumnType_TIMESTAMP:
		// cast val to time
		var timeVal, err = types.ToTime(val)
		if err != nil {
			return nil, fmt.Errorf("interfaceToColumnValue failed for column '%s': %v", c.Name, err)
		}
		// now convert time to protobuf timestamp
		timestamp, err := ptypes.TimestampProto(timeVal)
		if err != nil {
			return nil, fmt.Errorf("interfaceToColumnValue failed for column '%s': %v", c.Name, err)
		}
		columnValue = &proto.Column{Value: &proto.Column_TimestampValue{TimestampValue: timestamp}}
		break
	case proto.ColumnType_IPADDR:
		ipString := types.SafeString(val)
		// treat an empty string as a null ip address
		if ipString == "" {
			columnValue = &proto.Column{Value: &proto.Column_NullValue{}}
		} else {
			if ip := net.ParseIP(ipString); ip == nil {
				return nil, fmt.Errorf("%s: invalid ip address %s", c.Name, ipString)
			}
			columnValue = &proto.Column{Value: &proto.Column_IpAddrValue{IpAddrValue: ipString}}
		}
		break
	case proto.ColumnType_CIDR:
		cidrRangeString := types.SafeString(val)
		// treat an empty string as a null ip address
		if cidrRangeString == "" {
			columnValue = &proto.Column{Value: &proto.Column_NullValue{}}
		} else {
			if _, _, err := net.ParseCIDR(cidrRangeString); err != nil {
				return nil, fmt.Errorf("%s: invalid ip address %s", c.Name, cidrRangeString)
			}
			columnValue = &proto.Column{Value: &proto.Column_CidrRangeValue{CidrRangeValue: cidrRangeString}}
		}
		break
	case proto.ColumnType_INET:
		inetString := types.SafeString(val)
		// treat an empty string as a null ip address
		if inetString == "" {
			columnValue = &proto.Column{Value: &proto.Column_NullValue{}}
		} else {
			if ip := net.ParseIP(inetString); ip == nil {
				if _, _, err := net.ParseCIDR(inetString); err != nil {
					return nil, fmt.Errorf("%s: invalid ip address %s", c.Name, inetString)
				}
			}
			columnValue = &proto.Column{Value: &proto.Column_CidrRangeValue{CidrRangeValue: inetString}}
		}
	case proto.ColumnType_LTREE:
		columnValue = &proto.Column{Value: &proto.Column_LtreeValue{LtreeValue: types.ToString(val)}}
		break

	default:
		return nil, fmt.Errorf("unrecognised columnValue type '%s'", c.Type)
	}

	return columnValue, nil
}

// TODO REMOVE NOW THAT WE HAV ENAMED HYDRATE
// QueryColumn is struct storing column name and resolved hydrate name
// this is used in the query data when the hydrate function has been resolved
type QueryColumn struct {
	*Column
	// the name of the hydrate function which will be used to populate this column
	// - this may be a default hydrate function
	hydrateName string
}

func NewQueryColumn(column *Column, hydrateName string) *QueryColumn {
	return &QueryColumn{column, hydrateName}
}
