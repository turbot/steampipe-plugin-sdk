package transform

// Transform definition functions - function used to build a list of transforms

// FROM functions
// The transform chain must be started with a From... function

// Return a constant value (specified by 'param')
func FromConstant(value interface{}) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: ConstantValue, Param: value}}}
}

// FromMethod invokes a function on the hydrate item
func FromMethod(methodName string) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: MethodValue, Param: methodName}}}
}

// Generate a value by retrieving a field from the source item
func FromField(fieldNames ...string) *ColumnTransforms {
	var fieldNameArray []string
	for _, fieldName := range fieldNames {
		fieldNameArray = append(fieldNameArray, fieldName)
	}
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: FieldValue, Param: fieldNameArray}}}
}

func FromQual(qual string) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: QualValue, Param: qual}}}
}

// Generate a value by returning the raw hydrate item
func FromValue() *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: RawValue}}}
}

// Generate a value by converting the given field name to camel case and retrieving from the source item
func FromCamel() *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: FieldValueCamelCase}}}
}

// Generate a value by converting the given field name to camel case and retrieving from the source item
func FromGo() *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: FieldValueGo}}}
}

func FromMatrixItem(key string) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: MatrixItemValue, Param: key}}}
}

// Generate a value by calling 'transformFunc'
func From(transformFunc TransformFunc) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: transformFunc}}}
}

// Generate a value by finding a struct property with the json tag matching the column name
func FromJSONTag() *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: FieldValueTag, Param: "json"}}}
}

// Generate a value by finding a struct property with the tag 'tagName' matching the column name
func FromTag(tagName string) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: FieldValueTag, Param: tagName}}}
}

// Generate a value by calling 'transformFunc' passing param
func FromP(transformFunc TransformFunc, param interface{}) *ColumnTransforms {
	return &ColumnTransforms{Transforms: []*TransformCall{{Transform: transformFunc, Param: param}}}
}

//  TRANSFORM functions
// these can be chained after a From function to transform the data

// Apply an arbitrary transform to the data (specified by 'transformFunc')
func (t *ColumnTransforms) Transform(transformFunc TransformFunc) *ColumnTransforms {
	t.Transforms = append(t.Transforms, &TransformCall{Transform: transformFunc})
	return t

}

// Apply an arbitrary transform to the data, passing a parameter
func (t *ColumnTransforms) TransformP(transformFunc TransformFunc, param interface{}) *ColumnTransforms {
	t.Transforms = append(t.Transforms, &TransformCall{Transform: transformFunc, Param: param})
	return t
}

// NullValue :: if the input Value equals the transform param, return nil
func (t *ColumnTransforms) NullIfEqual(nullValue interface{}) *ColumnTransforms {
	t.Transforms = append(t.Transforms, &TransformCall{Transform: NullIfEqualParam, Param: nullValue})
	return t
}

// NullIfZero :: if the input value equals the zero value of its type, return nil
func (t *ColumnTransforms) NullIfZero() *ColumnTransforms {
	t.Transforms = append(t.Transforms, &TransformCall{Transform: NullIfZeroValue})
	return t
}
