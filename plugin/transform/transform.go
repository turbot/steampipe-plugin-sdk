package transform

// Transform definition functions - function used to build a list of transforms

// FROM functions
// The transform chain must be started with a From... function

// FromConstant returns a constant value (specified by 'param')
func FromConstant(value interface{}) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: ConstantValue, Param: value}}}
}

// FromMethod invokes a function on the hydrate item
func FromMethod(methodName string) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: MethodValue, Param: methodName}}}
}

// FromField generates a value by retrieving a field or a set of fields from the source item
func FromField(fieldNames ...string) *ColumnTransforms {
	var fieldNameArray []string
	for _, fieldName := range fieldNames {
		fieldNameArray = append(fieldNameArray, fieldName)
	}
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: FieldValue, Param: fieldNameArray}}}
}

// FromQual takes the specific column and generates it's values from key column quals
func FromQual(qual string) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: QualValue, Param: qual}}}
}

// FromValue generates a value by returning the raw hydrate item
func FromValue() *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: RawValue}}}
}

// FromCamel generates a value by converting the given field name to camel case and retrieving from the source item
func FromCamel() *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: FieldValueCamelCase}}}
}

// FromGo generates a value by converting the given field name to camel case and retrieving from the source item
func FromGo() *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: FieldValueGo}}}
}

// FromMatrixItem takes key from transform data and generates the value from Matrix Items
func FromMatrixItem(key string) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: MatrixItemValue, Param: key}}}
}

// From generate a value by calling 'transformFunc'
func From(transformFunc TransformFunc) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: transformFunc}}}
}

// FromJSONTag generates a value by finding a struct property with the json tag matching the column name
func FromJSONTag() *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: FieldValueTag, Param: "json"}}}
}

// FromTag generates a value by finding a struct property with the tag 'tagName' matching the column name
func FromTag(tagName string) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: FieldValueTag, Param: tagName}}}
}

// FromP generates a value by calling 'transformFunc' passing param
func FromP(transformFunc TransformFunc, param interface{}) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: transformFunc, Param: param}}}
}

//  TRANSFORM functions
// these can be chained after a From function to transform the data

// Transform function applies an arbitrary transform to the data (specified by 'transformFunc')
func (t *ColumnTransforms) Transform(transformFunc TransformFunc) *ColumnTransforms {
	t.Transforms = append(t.Transforms, &TransformCall{Transform: transformFunc})
	return t

}

// TransformP function applies an arbitrary transform to the data, passing a parameter
func (t *ColumnTransforms) TransformP(transformFunc TransformFunc, param interface{}) *ColumnTransforms {
	t.Transforms = append(t.Transforms, &TransformCall{Transform: transformFunc, Param: param})
	return t
}

// NullIfEqual returns nil if the input Value equals the transform param
func (t *ColumnTransforms) NullIfEqual(nullValue interface{}) *ColumnTransforms {
	t.Transforms = append(t.Transforms, &TransformCall{Transform: NullIfEqualParam, Param: nullValue})
	return t
}

// NullIfZero returns nil if the input value equals the zero value of its type
func (t *ColumnTransforms) NullIfZero() *ColumnTransforms {
	t.Transforms = append(t.Transforms, &TransformCall{Transform: NullIfZeroValue})
	return t
}
