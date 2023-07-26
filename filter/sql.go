package filter

import (
	"fmt"
	"strings"
)

func appendIdentifier(identifiers []string, identifier string) []string {
	for _, i := range identifiers {
		if i == identifier {
			return identifiers
		}
	}
	return append(identifiers, identifier)
}

// Record the requested identifiers so that we can compare to the ones supported by the API requesting this
func ComparisonToSQL(node ComparisonNode, identifiers []string) (string, []string, error) {
	switch node.Type {
	case "and", "or":
		return LogicToSQL(node, identifiers)
	case "compare", "is", "like":
		return CompareToSQL(node, identifiers)
	case "in":
		sql, err := InToSQL(node)
		return sql, identifiers, err
	case "not":
		return NotToSQL(node, identifiers)
	case "identifier":
		sql, err := IdentifierToSQL(node)
		return sql, identifiers, err
	}
	return "", identifiers, nil
}

func CodeToSQL(node CodeNode) (string, error) {
	s := node.Value
	switch node.Type {
	case "quoted_identifier", "unquoted_identifier":
		s = fmt.Sprintf(`"%s"`, strings.ReplaceAll(node.Value, `"`, `""`))
		for _, i := range node.JsonbSelector {
			sql, _ := CodeToSQL(i)
			s += fmt.Sprintf(" %s", sql)
		}
	case "string":
		s = fmt.Sprintf(`'%s'`, strings.ReplaceAll(node.Value, `'`, `''`))
	}
	return s, nil
}

func OperatorSQL(node CodeNode) (string, error) {
	return node.Value, nil
}

func LogicToSQL(node ComparisonNode, identifiers []string) (string, []string, error) {
	newIdentifiers := identifiers
	parts := []string{}
	for _, v := range toIfaceSlice(node.Values) {
		s, i, _ := ComparisonToSQL(v.(ComparisonNode), newIdentifiers)
		newIdentifiers = i
		parts = append(parts, s)
	}
	return fmt.Sprintf("( %s )", strings.Join(parts, fmt.Sprintf(" %s ", node.Type))), newIdentifiers, nil
}

func IdentifierToSQL(node ComparisonNode) (string, error) {
	values := node.Values.([]CodeNode)
	return CodeToSQL(values[0])
}

func NotToSQL(node ComparisonNode, identifiers []string) (string, []string, error) {
	values := node.Values.([]ComparisonNode)
	rightSQL, newIdentifiers, _ := ComparisonToSQL(values[0], identifiers)
	return fmt.Sprintf(`( not %s )`, rightSQL), newIdentifiers, nil
}

func CompareToSQL(node ComparisonNode, identifiers []string) (string, []string, error) {
	values := node.Values.([]CodeNode)
	leftCodeNode := values[0]
	newIdentifiers := appendIdentifier(identifiers, leftCodeNode.Value)
	rightCodeNode := values[1]
	leftSQL, _ := CodeToSQL(leftCodeNode)
	opSQL, _ := OperatorSQL(node.Operator)
	rightSQL, _ := CodeToSQL(rightCodeNode)
	return fmt.Sprintf("( %s %s %s )", leftSQL, opSQL, rightSQL), newIdentifiers, nil
}

func InToSQL(node ComparisonNode) (string, error) {
	values := node.Values.([]CodeNode)
	leftSQL, _ := CodeToSQL(values[0])
	opSQL, _ := OperatorSQL(node.Operator)
	inValues := []string{}
	for _, v := range values[1:] {
		s, _ := CodeToSQL(v)
		inValues = append(inValues, s)
	}
	return fmt.Sprintf("( %s %s ( %s ) )", leftSQL, opSQL, strings.Join(inValues, ", ")), nil
}
