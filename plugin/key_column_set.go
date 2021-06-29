package plugin

import (
	"fmt"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

// KeyColumnSet is a set of columns which form the key of a table
// may specify:
// - a Single column
// - a set of columns which together All form the key
// - a set of columns Any of which which form the key
type KeyColumnSet struct {
	Single *KeyColumn
	All    KeyColumnSlice
	Any    KeyColumnSlice
}

func (k *KeyColumnSet) String() string {
	if k.Single != nil {
		return k.Single.String()
	}
	if k.All != nil {
		return fmt.Sprintf("ALL of: \n%s", k.All)
	}
	if k.Any != nil {
		return fmt.Sprintf("ANY of: \n%s", k.Any)
	}
	return ""
}

func (k *KeyColumnSet) ToKeyColumnSlice() KeyColumnSlice {

	if k.Single != nil {
		return []*KeyColumn{k.Single}
	}
	if k.All != nil {
		return k.All
	}
	if k.Any != nil {
		return k.Any
	}

	return nil
}

func (k *KeyColumnSet) ToProtobuf() *proto.KeyColumnsSet {
	res := &proto.KeyColumnsSet{}
	if k.Single != nil {
		res.Single = k.Single.Column
		res.SingleKeyColumn = k.Single.ToProtobuf()
	}
	if k.All != nil {
		res.All = k.All.StringSlice()
		res.AllKeyColumns = k.All.ToProtobuf()
	}
	if k.Any != nil {
		res.Any = k.Any.StringSlice()
		res.AnyKeyColumns = k.Any.ToProtobuf()
	}

	return res
}

// AllEquals returns whether all child KeyColumns only use equals operators
func (k *KeyColumnSet) AllEquals() bool {
	if k.Single != nil {
		return k.Single.SingleEqualsQual()
	}
	if k.All != nil {
		return k.All.AllEquals()
	}
	if k.Any != nil {
		return k.Any.AllEquals()
	}

	return true
}

func (k *KeyColumnSet) Validate() []string {
	if k.Single != nil {
		if k.All != nil || k.Any != nil {
			return []string{"only 1 of 'Single', 'Any' and 'All' may be set'"}
		}
		return k.Single.Validate()
	}

	if k.All != nil {
		if k.Any != nil {
			return []string{"only 1 of 'Single', 'Any' and 'All' may be set'"}
		}
		var res []string
		// a column may only appear once in an 'All' slice
		columnMap := make(map[string]bool)
		for _, col := range k.All {
			if columnMap[col.Column] {
				res = append(res, fmt.Sprintf("a column may only appear once in an 'All' clause. column %s is repeated", col.Column))
				break
			}
			columnMap[col.Column] = true
		}
		return k.All.Validate()
	}
	if k.Any != nil {
		return k.All.Validate()
	}

	return nil

}
