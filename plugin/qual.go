package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

// Qual is a struct which represents a database qual in a more easily digestible form that proto.Qual
type Qual struct {
	Column   string
	Operator string
	Value    *proto.QualValue
}

func NewQual(q *proto.Qual) *Qual {
	return &Qual{
		Column:   q.FieldName,
		Operator: q.GetStringValue(),
		Value:    q.Value,
	}
}
