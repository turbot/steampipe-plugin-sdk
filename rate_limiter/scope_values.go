package rate_limiter

import (
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"strings"
)

func ScopeValuesString(sv map[string]string) string {
	keys := helpers.SortedMapKeys(sv)
	var strs = make([]string, len(keys))
	for i, k := range keys {
		strs[i] = fmt.Sprintf("%s=%s", k, sv[k])
	}
	return strings.Join(strs, ",")
}

// MergeScopeValues combines a set of scope values in order of precedence
// NOT: it adds the given values to the resulting map WITHOUT OVERWRITING existing values
// i.e. follow the precedence order
func MergeScopeValues(values []map[string]string) map[string]string {
	res := map[string]string{}

	for _, valueMap := range values {
		for k, v := range valueMap {
			// only set tag if not already set - earlier tag values have precedence
			if _, gotValue := res[k]; !gotValue {
				res[k] = v
			}
		}
	}
	return res
}
