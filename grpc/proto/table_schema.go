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

func (x *TableSchema) Equals(other *TableSchema) bool {
	if len(x.Columns) != len(other.Columns) {
		return false
	}
	columnMap := x.GetColumnMap()
	otherColumnMap := other.GetColumnMap()

	for k, column := range columnMap {
		otherColumn, ok := otherColumnMap[k]
		if !ok {
			return false
		}
		if !column.Equals(otherColumn) {
			return false
		}
	}
	if len(x.GetCallKeyColumnList) != len(other.GetCallKeyColumnList) {
		return false
	}
	for i, getKeyColumn := range x.GetCallKeyColumnList {
		if !other.GetCallKeyColumnList[i].Equals(getKeyColumn) {
			return false
		}
	}
	if len(x.ListCallKeyColumnList) != len(other.ListCallKeyColumnList) {
		return false
	}
	for i, listKeyColumn := range x.ListCallKeyColumnList {
		if !other.ListCallKeyColumnList[i].Equals(listKeyColumn) {
			return false
		}
	}
	return true

}
