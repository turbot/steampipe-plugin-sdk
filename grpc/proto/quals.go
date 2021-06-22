package proto

func (x *Quals) Append(q *Qual) {
	x.Quals = append(x.Quals, q)
}
