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

func (x *TableSchema) GetKeyColumnMap() map[string]*KeyColumn {
	var res = make(map[string]*KeyColumn)
	for _, k := range x.GetCallKeyColumnList {
		res[k.Name] = k
	}
	return res
}

func (x *TableSchema) ListKeyColumnMap() map[string]*KeyColumn {
	var res = make(map[string]*KeyColumn)
	for _, k := range x.ListCallKeyColumnList {
		res[k.Name] = k
	}
	return res
}

func (x *TableSchema) Equals(other *TableSchema) bool {
	return !x.Diff(other).HasDiffs()
}

func (x *TableSchema) Diff(other *TableSchema) *TableSchemaDiff {
	var res = NewTableSchemaDiff()

	columnMap := x.GetColumnMap()
	otherColumnMap := other.GetColumnMap()

	for columnName, column := range columnMap {
		otherColumn, ok := otherColumnMap[columnName]
		if !ok {
			res.MissingColumns[columnName] = struct{}{}
		} else if column.Type != otherColumn.Type {
			res.TypeMismatchColumns[columnName] = struct{}{}
		}
	}
	for columnName := range otherColumnMap {
		_, ok := columnMap[columnName]
		if !ok {
			res.MissingColumns[columnName] = struct{}{}
		}
		// we've already checked type of matching columns
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
	MissingColumns      map[string]struct{}
	TypeMismatchColumns map[string]struct{}
	KeyColumnsEqual     bool
}

func NewTableSchemaDiff() *TableSchemaDiff {
	return &TableSchemaDiff{
		MissingColumns:      make(map[string]struct{}),
		TypeMismatchColumns: make(map[string]struct{}),
		KeyColumnsEqual:     true,
	}
}

func (d *TableSchemaDiff) HasDiffs() bool {
	return len(d.MissingColumns)+len(d.TypeMismatchColumns) > 0 ||
		d.KeyColumnsEqual
}

func (d *TableSchemaDiff) Merge(other *TableSchemaDiff) {
	for missingColumn := range other.MissingColumns {
		d.MissingColumns[missingColumn] = struct{}{}
	}
	for typeMismatchColumn := range other.TypeMismatchColumns {
		d.TypeMismatchColumns[typeMismatchColumn] = struct{}{}
	}
	if !other.KeyColumnsEqual {
		d.KeyColumnsEqual = false
	}
}
