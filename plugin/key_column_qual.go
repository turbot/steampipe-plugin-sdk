package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/plugin/quals"

	"github.com/turbot/go-kit/helpers"
)

// KeyColumnQuals is a struct representing all quals for a specific column
type KeyColumnQuals struct {
	Column string
	Quals  []*quals.Qual
}

func (k KeyColumnQuals) SatisfiesKeyColumn(keyColumn *KeyColumn) bool {
	if keyColumn.Column != k.Column {
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
	return len(k.Quals) == 1 && k.Quals[0].Operator == "="
}
