package proto

func (x *SortColumn) Equals(other *SortColumn) bool {
	return x.Column == other.Column && x.Order == other.Order
}
