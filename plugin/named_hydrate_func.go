package plugin

import "github.com/turbot/go-kit/helpers"

type NamedHydrateFunc struct {
	Func       HydrateFunc
	Name       string
	IsMemoized bool
}

func newNamedHydrateFunc(f HydrateFunc) NamedHydrateFunc {
	res := NamedHydrateFunc{
		Func: f,
		Name: helpers.GetFunctionName(f),
	}

	return res
}

func (h NamedHydrateFunc) clone() NamedHydrateFunc {
	return NamedHydrateFunc{
		Func: h.Func,
		Name: h.Name,
	}
}

// determine whether we are memoized
func (h NamedHydrateFunc) initialize() {
	h.IsMemoized = h.Name == helpers.GetFunctionName(h.Func)
}

func (h NamedHydrateFunc) empty() bool {
	return h.Func == nil
}
