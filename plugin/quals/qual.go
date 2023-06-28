// Package quals is the SDK representation of a SQL query qualifier, i.e. a value used in a where clause
package quals

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

const (
	QualOperatorEqual          = "="
	QualOperatorNotEqual       = "<>"
	QualOperatorLess           = "<"
	QualOperatorLessOrEqual    = "<="
	QualOperatorGreater        = ">"
	QualOperatorGreaterOrEqual = ">="
	QualOperatorLike           = "~~"
	QualOperatorNotLike        = "!~~"
	QualOperatorILike          = "~~*"
	QualOperatorNotILike       = "!~~*"
	QualOperatorRegex          = "~"
	QualOperatorNotRegex       = "!~"
	QualOperatorIRegex         = "~*"
	QualOperatorNotIRegex      = "!~*"
	QualOperatorIsNull         = "is null"
	QualOperatorIsNotNull      = "is not null"

	QualOperatorJsonbContainsLeftRight = "@>"
	QualOperatorJsonbContainsRightLeft = "<@"
	QualOperatorJsonbExistsOne         = "?"
	QualOperatorJsonbExistsAny         = "?|"
	QualOperatorJsonbExistsAll         = "?&"
	QualOperatorJsonbPathExists        = "@?"
	QualOperatorJsonbPathPredicate     = "@@" // FIXME: I'm a little confused about this one
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

func (q *Qual) Equals(other *Qual) bool {
	return q.Column == other.Column && q.Operator == other.Operator && q.Value.String() == other.Value.String()
}
func (q *Qual) ToProto() *proto.Qual {
	return &proto.Qual{
		FieldName: q.Column,
		Operator: &proto.Qual_StringValue{
			StringValue: q.Operator,
		},
		Value: q.Value,
	}
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

func (s QualSlice) ToProto() *proto.Quals {
	res := &proto.Quals{
		Quals: make([]*proto.Qual, len(s)),
	}
	for i, q := range s {
		res.Quals[i] = q.ToProto()
	}
	return res
}
