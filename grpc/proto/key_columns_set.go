package proto

// ToSlice returns a slice of all columns referenced by the protobuf KeyColumnSet
func (x *KeyColumnsSet) ToSlice() []string {
	var res = make([]string, len(x.KeyColumns))
	for i, col := range x.KeyColumns {
		res[i] = col.Name
	}
	return res
}
