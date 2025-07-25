package funcs

import (
	"fmt"
	"os"

	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
)

// is_error: Given a reference to a step, is_error returns a boolean true
// if there are 1 or more errors, or false it there are no errors.
var EnvFunc = function.New(&function.Spec{
	Description: `Get environment variable`,
	Params: []function.Parameter{
		{
			Name: "key",
			Type: cty.String,
		},
	},
	Type: function.StaticReturnType(cty.String),
	Impl: func(args []cty.Value, retType cty.Type) (cty.Value, error) {
		key := args[0].AsString()

		// ADD LOGGING TO DETECT IF PLUGIN-SIDE ENV() IS EVER CALLED
		fmt.Printf("🚨 PLUGIN-SIDE env() called with key: %s\n", key)

		value, exists := os.LookupEnv(key)

		if !exists {
			return cty.StringVal(""), nil
		}

		return cty.StringVal(value), nil
	},
})
