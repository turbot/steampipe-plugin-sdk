package proto

func (x *ColumnDefinition) Equals(other *ColumnDefinition) bool {
	return x.Name == other.Name &&
		x.Type == other.Type &&
		x.Description == other.Description
}
