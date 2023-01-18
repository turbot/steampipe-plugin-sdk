package schema

import (
	"fmt"

	"github.com/hashicorp/hcl/v2/hcldec"
	"github.com/zclconf/go-cty/cty"
)

type Attribute struct {
	// name - this will be inferred by the schema map key
	Name string
	// Type is the type of the value and must be one of the ValueType values.
	//   TypeBool - bool
	//   TypeInt - int
	//   TypeFloat - float64
	//   TypeString - string
	//   TypeList - []interface{}
	Type ValueType

	// Elem represents the element type. This may only be set for only set for TypeList.
	Elem *Attribute

	// Elem represents the element types for a map. This may only be set for only set for TypeMap.
	AttrTypes map[string]*Attribute

	// is this attribute required
	Required bool
}

func SchemaToObjectSpec(schema map[string]*Attribute) hcldec.ObjectSpec {
	spec := hcldec.ObjectSpec{}

	for name, attr := range schema {
		attr.Name = name
		spec[name] = attributeToSpec(attr)
	}
	return spec
}

func attributeToSpec(attr *Attribute) hcldec.Spec {
	return &hcldec.AttrSpec{
		Name: attr.Name,
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
		if attr.Elem == nil {
			panic(fmt.Sprintf("attribute %s is TypeList but 'Elem' is not set", attr.Name))
		}
		return cty.List(attributeTypeToCty(attr.Elem))
	case TypeMap:
		if attr.AttrTypes == nil {
			panic(fmt.Sprintf("attribute %s is TypeMap but 'AttrTypes; is not set", attr.Name))
		}
		return cty.Object(attributeTypeMapToCty(attr.AttrTypes))
	default:
		panic(fmt.Sprintf("invalid attribute type %v", attr.Type))
	}
}

func attributeTypeMapToCty(attrTypes map[string]*Attribute) map[string]cty.Type {
	res := make(map[string]cty.Type, len(attrTypes))
	for k, v := range attrTypes {
		res[k] = attributeTypeToCty(v)
	}
	return res
}
