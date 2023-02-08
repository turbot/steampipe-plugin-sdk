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

func (x *TableSchema) Diff(other *TableSchema) *TableSchemaDiff {
	var res = &TableSchemaDiff{}

	columnMap := x.GetColumnMap()
	otherColumnMap := other.GetColumnMap()

	for k, column := range columnMap {
		otherColumn, ok := otherColumnMap[k]
		if !ok || !column.Equals(otherColumn) {
			res.MismatchingColumns = append(res.MismatchingColumns, k)
		}
	}
	for k, otherColumn := range otherColumnMap {
		column, ok := columnMap[k]
		if !ok || !column.Equals(otherColumn) {
			res.MismatchingColumns = append(res.MismatchingColumns, k)
		}
	}

	if len(x.GetCallKeyColumnList) != len(other.GetCallKeyColumnList) {
		res.KeyColumnsEqual = false
	} else {
		for i, getKeyColumn := range x.GetCallKeyColumnList {
			if !other.GetCallKeyColumnList[i].Equals(getKeyColumn) {
				res.KeyColumnsEqual = false

			}
		}
	}
	if len(x.ListCallKeyColumnList) != len(other.ListCallKeyColumnList) {
		res.KeyColumnsEqual = false
	} else {
		for i, listKeyColumn := range x.ListCallKeyColumnList {
			if !other.ListCallKeyColumnList[i].Equals(listKeyColumn) {
				res.KeyColumnsEqual = false
			}
		}
	}
	return res
}

type TableSchemaDiff struct {
	MismatchingColumns []string
	KeyColumnsEqual    bool
}
