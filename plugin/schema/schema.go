package schema

import (
	"fmt"

	"github.com/hashicorp/hcl/v2/hcldec"
	"github.com/zclconf/go-cty/cty"
)

type Attribute struct {
	// Type is the type of the value and must be one of the ValueType values.
	//   TypeBool - bool
	//   TypeInt - int
	//   TypeFloat - float64
	//   TypeString - string
	//   TypeList - []interface{}
	Type ValueType

	// Elem represents the element type. THis may only be set for only set for TypeList.
	Elem *Attribute

	// is this atribute required
	Required bool
}

func SchemaToObjectSpec(schema map[string]*Attribute) hcldec.ObjectSpec {
	spec := hcldec.ObjectSpec{}

	for name, attr := range schema {
		spec[name] = attributeToSpec(name, attr)
	}
	return spec
}

func attributeToSpec(name string, attr *Attribute) hcldec.Spec {
	return &hcldec.AttrSpec{
		Name: name,
		Type: attributeTypeToCty(attr),
		// we have validated that Required = !Optional so here we can safely only consider Required
		Required: attr.Required,
	}
}

func attributeTypeToCty(attr *Attribute) cty.Type {
	switch attr.Type {
	case TypeString:
		return cty.String
	case TypeBool:
		return cty.Bool
	case TypeFloat, TypeInt:
		return cty.Number
	case TypeList:
		return cty.List(attributeTypeToCty(attr.Elem))
	default:
		panic(fmt.Sprintf("invalid attribute type %v", attr.Type))
	}
}
