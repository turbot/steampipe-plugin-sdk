package proto

func (x *KeyColumn) Equals(other *KeyColumn) bool {
	if len(x.Operators) != len(other.Operators) {
		return false
	}
	for i, op := range x.Operators {
		if other.Operators[i] != op {
			return false
		}
	}
	return x.Name == other.Name &&
		x.Require == other.Require &&
		x.CacheMatch == other.CacheMatch
}
