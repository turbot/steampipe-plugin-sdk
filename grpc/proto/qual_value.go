package proto

func (x *QualValue) Equals(other *QualValue) bool {
	return x.String() == other.String()
}
