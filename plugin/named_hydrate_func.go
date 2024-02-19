package plugin

import "github.com/turbot/go-kit/helpers"

type namedHydrateFunc struct {
	Func HydrateFunc
	Name string
}

func newNamedHydrateFunc(f HydrateFunc) namedHydrateFunc {
	res := namedHydrateFunc{
		Func: f,
		Name: helpers.GetFunctionName(f),
	}

	return res
}

func (h namedHydrateFunc) clone() namedHydrateFunc {
	return namedHydrateFunc{
		Func: h.Func,
		Name: h.Name,
	}
}

func (h namedHydrateFunc) empty() bool {
	return h.Func == nil
}
