package plugin

import (
	"fmt"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/plugin/option"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

// KeyColumnSet is used by plugin to specify a set of columns which form the key of a table
// may specify:
// - a Single column
// - a set of columns which together All form the key
// - a set of columns Any of which which form the key
type KeyColumnSet struct {
	Columns KeyColumnSlice
	Minimum int
}

func (k *KeyColumnSet) String() string {
	var strs = make([]string, len(k.Columns))

	for i, keyColumn := range k.Columns {
		strs[i] = keyColumn.String()
	}

	if k.Minimum != 0 {
		strs = append([]string{fmt.Sprintf("at least %d of: ", k.Minimum)}, strs...)
	}
	return strings.Join(strs, "\n")
}

// SingleEqualsQual returns whether this key column set has a single qual with a single = operator
func (k *KeyColumnSet) SingleEqualsQual() *KeyColumn {
	if len(k.Columns) == 1 && k.Columns[0].SingleEqualsQual() {
		return k.Columns[0]
	}
	return nil
}

func (k *KeyColumnSet) ToProtobuf() *proto.KeyColumnsSet {
	res := &proto.KeyColumnsSet{}
	//if k.Single != nil {
	//	res.Single = k.Single.Column
	//	res.SingleKeyColumn = k.Single.ToProtobuf()
	//}
	//if k.All != nil {
	//	res.All = k.All.StringSlice()
	//	res.AllKeyColumns = k.All.ToProtobuf()
	//}
	//if k.Any != nil {
	//	res.Any = k.Any.StringSlice()
	//	res.AnyKeyColumns = k.Any.ToProtobuf()
	//}

	// TODO
	return res
}

// AllEquals returns whether all child KeyColumns only use equals operators
func (k *KeyColumnSet) AllEquals() bool {
	return k.Columns.AllEquals()

}

func (k *KeyColumnSet) Validate() []string {
	var res = k.Columns.Validate()

	// TODO verify valid combinations of ALL etc

	return res

}

func (k *KeyColumnSet) SetOption(option option.KeyColumnSetOptions) {
	if option.MinimumQuals != 0 {
		k.Minimum = option.MinimumQuals
	}
}
