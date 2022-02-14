package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v2/plugin/quals"

	"github.com/turbot/go-kit/helpers"
)

// KeyColumnQuals is a struct representing all quals for a specific column
type KeyColumnQuals struct {
	Name  string
	Quals quals.QualSlice
}

func (k KeyColumnQuals) SatisfiesKeyColumn(keyColumn *KeyColumn) bool {
	if keyColumn.Name != k.Name {
		return false
	}
	for _, q := range k.Quals {
		if helpers.StringSliceContains(keyColumn.Operators, q.Operator) {
			return true
		}
	}
	return false
}

func (k KeyColumnQuals) SingleEqualsQual() bool {
	return k.Quals.SingleEqualsQual()
}
