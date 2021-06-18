package proto

import "github.com/turbot/go-kit/helpers"

// ToSlice returns a slice of all columns referenced by the KeyColumnSet
func (k *KeyColumnsSet) ToSlice() []string {
	var res []string
	if k.Single != "" {
		res = append(res, k.Single)
	}
	for _, c := range k.All {
		if !helpers.StringSliceContains(res, c) {
			res = append(res, c)
		}
	}
	for _, c := range k.Any {
		if !helpers.StringSliceContains(res, c) {
			res = append(res, c)
		}
	}
	return res
}
