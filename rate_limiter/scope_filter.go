package rate_limiter

import (
	"fmt"
	"github.com/danwakefield/fnmatch"
	"github.com/turbot/steampipe-plugin-sdk/v5/filter"
	"strings"
)

type scopeFilter struct {
	filter filter.ComparisonNode
	raw    string
}

func newScopeFilter(raw string) (*scopeFilter, error) {
	parsed, err := filter.Parse("", []byte(raw))
	if err != nil {
		return nil, err
	}

	res := &scopeFilter{
		filter: parsed.(filter.ComparisonNode),
		raw:    raw,
	}

	// do a test run of the filter to ensure all operators are supported
	if _, err := scopeFilterSatisfied(res.filter, map[string]string{}); err != nil {
		return nil, err
	}

	return res, nil

}

func (f *scopeFilter) satisfied(values map[string]string) bool {
	res, _ := scopeFilterSatisfied(f.filter, values)
	return res
}

func scopeFilterSatisfied(c filter.ComparisonNode, values map[string]string) (bool, error) {
	switch c.Type {
	case "identifier":
		// not sure when wthis would be used
		return false, invalidScopeOperatorError(c.Operator.Value)
	case "is":
		// is is not (currently) supported
		return false, invalidScopeOperatorError(c.Operator.Value)
	case "like": // (also ilike?)
		codeNodes, ok := c.Values.([]filter.CodeNode)
		if !ok {
			return false, fmt.Errorf("failed to parse filter")
		}
		if len(codeNodes) != 2 {
			return false, fmt.Errorf("failed to parse filter")
		}

		// dereference the value from the map
		lval := values[codeNodes[0].Value]
		pattern := codeNodes[1].Value

		switch c.Operator.Value {
		case "like":
			res := evaluateLike(lval, pattern, 0)
			return res, nil
		case "not like":
			res := !evaluateLike(lval, pattern, 0)
			return res, nil
		case "ilike":
			res := evaluateLike(lval, pattern, fnmatch.FNM_IGNORECASE)
			return res, nil
		case "not ilike":
			res := !evaluateLike(lval, pattern, fnmatch.FNM_IGNORECASE)
			return res, nil
		default:
			return false, invalidScopeOperatorError(c.Operator.Value)
		}
	case "compare":
		codeNodes, ok := c.Values.([]filter.CodeNode)
		if !ok {
			return false, fmt.Errorf("failed to parse filter")
		}
		if len(codeNodes) != 2 {
			return false, fmt.Errorf("failed to parse filter")
		}

		// dereference the value from the map
		lval := values[codeNodes[0].Value]
		rval := codeNodes[1].Value

		switch c.Operator.Value {
		case "=":
			return lval == rval, nil
		case "!=", "<>":
			return lval != rval, nil
		// as we (currently) only suport string scopes, < and > are not supported
		case "<=", ">=", "<", ">":
			return false, invalidScopeOperatorError(c.Operator.Value)
		}
	case "in":
		codeNodes, ok := c.Values.([]filter.CodeNode)
		if !ok {
			return false, fmt.Errorf("failed to parse filter")
		}
		if len(codeNodes) < 2 {
			return false, fmt.Errorf("failed to parse filter")
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
			return rvalsContainValue, nil
		case "not in":
			return !rvalsContainValue, nil
		}
	case "not":
		// TODO have not identified  queries which give a top level 'not'
		return false, fmt.Errorf("unsupported location for 'not' operator")
	case "or":
		nodes, ok := c.Values.([]any)
		if !ok {
			return false, fmt.Errorf("failed to parse filter")
		}
		for _, n := range nodes {
			c, ok := n.(filter.ComparisonNode)
			if !ok {
				return false, fmt.Errorf("failed to parse filter")
			}
			// if any child nodes are satisfied, return true
			childSatisfied, err := scopeFilterSatisfied(c, values)
			if err != nil {
				return false, err
			}
			if childSatisfied {
				return true, nil
			}
		}
		// nothing is satisfied - return false
		return false, nil
	case "and":
		nodes, ok := c.Values.([]any)
		if !ok {
			return false, fmt.Errorf("failed to parse filter")
		}
		for _, n := range nodes {
			c, ok := n.(filter.ComparisonNode)
			if !ok {
				return false, fmt.Errorf("failed to parse filter")
			}
			// if any child nodes are unsatidsfied, return false
			childSatisfied, err := scopeFilterSatisfied(c, values)
			if err != nil {
				return false, err
			}
			if !childSatisfied {
				return false, nil
			}
		}
		// everything is satisfied - return true
		return true, nil
	}

	return false, fmt.Errorf("failed to parse filter")
}

func evaluateLike(val, pattern string, flag int) bool {
	pattern = strings.ReplaceAll(pattern, "_", "?")
	pattern = strings.ReplaceAll(pattern, "%", "*")
	return fnmatch.Match(pattern, val, flag)

}

func invalidScopeOperatorError(operator string) error {
	return fmt.Errorf("invalid scope filter operator '%s'", operator)
}
