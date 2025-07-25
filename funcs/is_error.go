package funcs

import (
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
)

// is_error: Given a reference to a step, is_error returns a boolean true
// if there are 1 or more errors, or false it there are no errors.
var IsErrorFunc = function.New(&function.Spec{
	Description: `Given a reference to a step, is_error returns a boolean true if there are 1 or more errors, or false it there are no errors.`,
	Params: []function.Parameter{
		{
			Name:             "step",
			Type:             cty.DynamicPseudoType,
			AllowUnknown:     true,
			AllowDynamicType: true,
			AllowNull:        true,
		},
	},
	Type: function.StaticReturnType(cty.Bool),
	Impl: func(args []cty.Value, retType cty.Type) (cty.Value, error) {
		if len(args) == 0 {
			return cty.False, nil
		}

		val := args[0]

		if val.IsNull() {
			return cty.False, nil
		}

		if !val.Type().IsMapType() && !val.Type().IsObjectType() {
			return cty.False, nil
		}

		valueMap := val.AsValueMap()
		if valueMap == nil {
			return cty.False, nil
		}

		if valueMap["errors"].IsNull() {
			return cty.False, nil
		}

		if valueMap["errors"].Type().IsListType() && valueMap["errors"].Type().IsSetType() {
			return cty.False, nil
		}

		errorSlice := valueMap["errors"].AsValueSlice()
		if errorSlice == nil {
			return cty.False, nil
		}

		if len(errorSlice) == 0 {
			return cty.False, nil
		}

		return cty.True, nil
	},
})
