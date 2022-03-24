package proto

func (x *TableSchema) GetColumnMap() map[string]*ColumnDefinition {
	if x != nil {
		res := make(map[string]*ColumnDefinition, len(x.Columns))
		for _, c := range x.Columns {
			res[c.Name] = c
		}
		return res
	}
	return nil
}
