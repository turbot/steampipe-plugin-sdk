package plugin

type namedHydrateFunc struct {
	Func       HydrateFunc
	Name       string
	IsMemoized bool
}

func newNamedHydrateFunc(f HydrateFunc) namedHydrateFunc {
	res := namedHydrateFunc{
		Func: f,
	}
	res.Name, res.IsMemoized = f.getOriginalFuncName()
	return res
}

func (h namedHydrateFunc) clone() namedHydrateFunc {
	return namedHydrateFunc{
		Func: h.Func,
		Name: h.Name,
	}
}
