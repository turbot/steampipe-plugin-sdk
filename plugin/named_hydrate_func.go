package plugin

import (
	"github.com/turbot/go-kit/helpers"
	"log"
)

type namedHydrateFunc struct {
	Func HydrateFunc
	Name string
}

func newNamedHydrateFunc(f HydrateFunc) namedHydrateFunc {
	res := namedHydrateFunc{Func: f}

	originalName, isMemoized := getMemoizedFuncName(f)
	if isMemoized {
		log.Printf("[TRACE] newNamedHydrateFunc  - function %s is memoized", originalName)
		res.Name = originalName
	} else {
		res.Name = helpers.GetFunctionName(f)
		log.Printf("[TRACE] newNamedHydrateFunc  - function %s is NOT memoized", res.Name)
	}
	return res
}

func (h namedHydrateFunc) clone() namedHydrateFunc {
	return namedHydrateFunc{
		Func: h.Func,
		Name: h.Name,
	}
}
