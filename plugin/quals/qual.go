package quals

import (
	"github.com/turbot/steampipe-plugin-sdk/v2/grpc/proto"
)

const QualOperatorIsNull = "is null"
const QualOperatorIsNotNull = "is not null"

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

func (q *Qual) Equals(other *Qual) bool {
	return q.Column == other.Column && q.Operator == other.Operator && q.Value.String() == other.Value.String()
}

type QualSlice []*Qual

func (s QualSlice) SingleEqualsQual() bool {
	return len(s) == 1 && s[0].Operator == "="
}

func (s QualSlice) Contains(other *Qual) bool {
	alreadyExists := false
	for _, existingQual := range s {
		if existingQual.Equals(other) {
			alreadyExists = true
			break
		}
	}

	return alreadyExists
}
