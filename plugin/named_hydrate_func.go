package plugin

type namedHydrateFunc struct {
	Func HydrateFunc
	Name string
}

func newNamedHydrateFunc(f HydrateFunc) namedHydrateFunc {
	res := namedHydrateFunc{
		Func: f,
		Name: getOriginalFuncName(f),
	}

	return res
}

func (h namedHydrateFunc) clone() namedHydrateFunc {
	return namedHydrateFunc{
		Func: h.Func,
		Name: h.Name,
	}
}
