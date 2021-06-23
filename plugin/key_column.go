package plugin

type Operator int

const (
	OperatorEq = iota
	OperatorNe
	OperatorLt
	OperatorLe
	OperatorGt
	OperatorGe
)

type KeyColumn struct {
	Name      string
	Operators []Operator
}
