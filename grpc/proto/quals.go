package proto

func (x *BoolQual) Append(q *DbQual) {
	x.Quals.Quals = append(x.Quals.Quals, q)
}

func (x *DbQuals) Append(q *DbQual) {
	x.Quals = append(x.Quals, q)
}
func (x *Quals) Append(q *Qual) {
	x.Quals = append(x.Quals, q)
}

// LegacyQualMap converts a map map[string]*DbQuals to map[string]*Quals as required by older plugins
// ignore bool quals and only add simple quals
func LegacyQualMap(qualMap map[string]*DbQuals) map[string]*Quals {
	var res = make(map[string]*Quals)
	for k, v := range qualMap {
		res[k] = &Quals{}
		for _, dbQual := range v.Quals {
			if q := dbQual.GetQual(); q != nil {
				res[k].Append(q)
			}
		}
	}
	return res
}
