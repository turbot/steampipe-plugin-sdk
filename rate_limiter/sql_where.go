package rate_limiter

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/filter"
)

func parseWhere(w string) (*filter.ComparisonNode, error) {
	got, err := filter.Parse("", []byte(w))
	if err != nil {
		return nil, err
	}
	c := got.(filter.ComparisonNode)
	return &c, nil

	//sql, _, err := ComparisonToSQL(got.(ComparisonNode), []string{})
	//filter.Parse("",w)
	//// convert where clause to valid SQL statement
	//sql := fmt.Sprintf("select * from a where %s", w)
	//stmt, err := sqlparser.Parse(sql)
	//if err != nil {
	//	return nil, err
	//
	//}
	//return stmt.(*sqlparser.Select).Where.Expr, nil
}

func whereSatisfied(c filter.ComparisonNode, values map[string]string) bool {
	switch c.Type {
	case "identifier":
	case "is":
	case "like":
		nodes, ok := c.Values.([]any)
		if !ok {
			return false
		}
		for _, n := range nodes {
			c, ok := n.(filter.ComparisonNode)
			if !ok {
				return false
			}

			if !whereSatisfied(c, values) {
				return false
			}
		}
		return true
	case "compare":
		codeNodes, ok := c.Values.([]filter.CodeNode)
		if !ok {
			return false
		}
		if len(codeNodes) != 2 {
			return false
		}

		// dereference the value from the map
		lval := values[codeNodes[0].Value]
		rval := codeNodes[1].Value
		switch c.Operator.Value {
		case "=":
			return lval == rval
		case "!=":
			return lval != rval
		}
	case "in":
		codeNodes, ok := c.Values.([]filter.CodeNode)
		if !ok {
			return false
		}
		if len(codeNodes) < 2 {
			return false
		}

		key := codeNodes[0].Value
		// build look up of possible values
		rvals := make(map[string]struct{}, len(codeNodes)-1)
		for _, c := range codeNodes[1:] {
			rvals[c.Value] = struct{}{}
		}

		lval := values[key]
		// does this value exist in rvals?
		_, rvalsContainValue := rvals[lval]

		// operator determines expected result
		switch c.Operator.Value {
		case "in":
			return rvalsContainValue
		case "not in":
			return !rvalsContainValue
		}
	case "not":
	case "or":
		nodes, ok := c.Values.([]any)
		if !ok {
			return false
		}
		for _, n := range nodes {
			c, ok := n.(filter.ComparisonNode)
			if !ok {
				return false
			}

			if whereSatisfied(c, values) {
				return true
			}
		}
		return false
	case "and":
	}
	return true
}
