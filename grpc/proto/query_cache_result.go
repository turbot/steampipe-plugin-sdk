package proto

func (q *QueryResult) Append(row *Row) {
	q.Rows = append(q.Rows, row)
}
