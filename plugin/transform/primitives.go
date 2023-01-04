// Transform package provides the ability to transform data from APIs. It contains transform functions which can be chained together to get the desired values
package transform

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"

	"github.com/ghodss/yaml"
	"github.com/iancoleman/strcase"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/go-kit/types"
)

///////////////////////
// Transform primitives
// predefined transform functions that may be chained together

// FieldValue function is intended for the start of a transform chain.
// This returns a field value of either the hydrate call result (if present)  or the root item if not
// the 'Param' is a list of field names
// - Each field is tried in turn. If the property does not exist or is nil, the next field is tried
func FieldValue(_ context.Context, d *TransformData) (interface{}, error) {
	var item = d.HydrateItem
	var fieldNames []string

	switch p := d.Param.(type) {
	case []string:
		fieldNames = p
	case string:
		fieldNames = []string{p}
	default:
		return nil, fmt.Errorf("'FieldValue' requires one or more string parameters containing property path but received %v", d.Param)
	}
	// try each element of field names in turn -
	// if the property does not exist, or has a nil value, continue to the next property
	// NOTE: if a property exists but has a nil value,
	// we will fall back to returning this if no non nil value is found
	// - this is so that any casting code in the plugin still works
	var fieldValue interface{} = nil
	for _, propertyPath := range fieldNames {
		fieldValue, _ = helpers.GetNestedFieldValueFromInterface(item, propertyPath)
		if !helpers.IsNil(fieldValue) {
			break
		}
	}
	// return whatever value of fieldValue we have found, even if it is nil
	return fieldValue, nil
}

// FieldValueCamelCase is intended for the start of a transform chain
// This converts the column name to camel case and call FieldValue
func FieldValueCamelCase(ctx context.Context, d *TransformData) (interface{}, error) {
	propertyPath := strcase.ToCamel(d.ColumnName)
	if propertyPath == "" {
		return nil, fmt.Errorf("'FieldValue' requires a string parameter containing property path but received %v", d.Param)
	}

	d.Param = propertyPath
	return FieldValue(ctx, d)
}

// FieldValueGo is intended for the start of a transform chain
// This converts the column name to camel case, with common initialisms upper case, and call FieldValue
func FieldValueGo(ctx context.Context, d *TransformData) (interface{}, error) {
	// call lintName to make common initialisms upper case
	propertyPath := helpers.LintName(strcase.ToCamel(d.ColumnName))
	if propertyPath == "" {
		return nil, fmt.Errorf("'FieldValue' requires a string parameter containing property path but received %v", d.Param)
	}
	d.Param = propertyPath
	return FieldValue(ctx, d)
}

// MatrixItemValue is intended for the start of a transform chain
// This retrieves a value from the matrix item, using the param from transform data as a key
func MatrixItemValue(ctx context.Context, d *TransformData) (interface{}, error) {
	metadataKey, ok := d.Param.(string)
	if !ok {
		return nil, fmt.Errorf("'MatrixItemValue' requires a string parameter containing metadata keybut received %v", d.Param)
	}
	return d.MatrixItem[metadataKey], nil
}

// FieldValueTag is intended for the start of a transform chain
// This finds the data value with the tag matching the column name
func FieldValueTag(ctx context.Context, d *TransformData) (interface{}, error) {
	tagName, ok := d.Param.(string)
	if !ok {
		return nil, fmt.Errorf("'FieldValueTag' requires a string parameter containing tag name but received %v", d.Param)
	}
	// TypeOf returns the reflection Type that represents the dynamic type of variable.
	// If variable is a nil interface value, TypeOf returns nil.
	item := helpers.DereferencePointer(d.HydrateItem)
	t := reflect.TypeOf(item)

	// Iterate over all available fields and read the tag value
	for i := 0; i < t.NumField(); i++ {
		// Get the field, returns https://golang.org/pkg/reflect/#StructField
		field := t.Field(i)
		// Get the field tag value
		tag := field.Tag.Get(tagName)
		if tag == "" {
			continue
		}
		// get the first segment of the tag
		tagField := strings.Split(tag, ",")[0]
		if tagField == d.ColumnName {
			// mutate transform data to set the param to the field name and call FieldValue
			d.Param = field.Name
			return FieldValue(ctx, d)
		}
	}
	return nil, fmt.Errorf("'FieldValueTag' - no property found with tag matching column %s", d.ColumnName)

}

// ConstantValue is intended for the start of a transform chain
// This returns the value passed as d.Param
func ConstantValue(_ context.Context, d *TransformData) (interface{}, error) {
	return d.Param, nil
}

// MethodValue function takes the transform data and invokes specified method on the hydrate item
func MethodValue(_ context.Context, d *TransformData) (interface{}, error) {
	param := d.Param.(string)
	if res, err := helpers.ExecuteMethod(d.HydrateItem, param); err != nil {
		return nil, err
	} else {
		if res == nil {
			return nil, nil
		}
		return res[0], nil
	}
}

// RawValue is intended for the start of a transform chain
// This returns the whole hydrate item as it is
func RawValue(_ context.Context, d *TransformData) (interface{}, error) {
	return d.HydrateItem, nil
}

// ToUpper converts the (string or *string) value to upper case,
// returns unaltered value if value from the transform data is not a string
func ToUpper(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	valStr, ok := types.CastString(d.Value)
	if !ok {
		return d.Value, nil
	}
	return strings.ToUpper(valStr), nil
}

// ToLower converts the (string or *string) value to lower case
// returns unaltered value if value is not a string
func ToLower(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	valStr, ok := types.CastString(d.Value)
	if !ok {
		return d.Value, nil
	}
	return strings.ToLower(valStr), nil
}

// ToBool converts the (string) value to a bool
// This returns nil if value is not a string
func ToBool(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	return types.ToBool(d.Value)
}

// NullIfEqualParam returns nil if the input Value equals the transform param
func NullIfEqualParam(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	if helpers.DereferencePointer(d.Value) == d.Param {
		return nil, nil
	}
	return d.Value, nil
}

// NullIfZeroValue takes the transform data and returns nil if the input value equals the zero value of its type
func NullIfZeroValue(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	v := helpers.DereferencePointer(d.Value)
	// Booleans are skipped by null if zero, since false would always be null
	_, ok := v.(bool)
	if ok {
		return d.Value, nil
	}
	if helpers.IsZero(v) {
		return nil, nil
	}
	return d.Value, nil
}

// NullIfEmptySliceValue returns nil if the input Value is an empty slice/array
func NullIfEmptySliceValue(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	v := helpers.DereferencePointer(d.Value)
	b, l := reflect.TypeOf(v).Kind() == reflect.Slice, reflect.ValueOf(v).Len()
	if b && l == 0 {
		return nil, nil
	}
	return d.Value, nil
}

// UnmarshalYAML parse the yaml-encoded data and return the result
func UnmarshalYAML(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	inputStr := types.SafeString(d.Value)
	var result interface{}
	if inputStr != "" {
		decoded, err := url.QueryUnescape(inputStr)
		if err != nil {
			return nil, err
		}

		err = yaml.Unmarshal([]byte(decoded), &result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// ToString convert the value from transform data to a string
func ToString(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	return types.ToString(d.Value), nil
}

// ToInt convert the value from transform data to an int64
func ToInt(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	return types.ToInt64(d.Value)
}

// ToDouble convert the value from transform data to float64
func ToDouble(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	return types.ToFloat64(d.Value)
}

// UnixToTimestamp convert unix time format to go time object
// (which will later be converted to RFC3339 format by the FDW)
func UnixToTimestamp(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value != nil {
		epochTime, err := types.ToInt64(d.Value)
		if err != nil {
			return nil, err
		}
		if epochTime == 0 {
			return nil, nil
		}
		t := time.Unix(epochTime, 0)
		return t, nil
	}
	return nil, nil
}

// UnixMsToTimestamp convert unix time in milliseconds to go time object
// (which will later be converted to RFC3339 format by the FDW)
func UnixMsToTimestamp(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value != nil {
		epochTime, err := types.ToInt64(d.Value)
		if err != nil {
			return nil, err
		}
		if epochTime == 0 {
			return nil, nil
		}
		nanoSeconds := epochTime * 1000000
		t := time.Unix(0, nanoSeconds)
		return t, nil
	}
	return nil, nil
}

// EnsureStringArray convert the input value from transform data into a string array
func EnsureStringArray(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value != nil {
		switch v := d.Value.(type) {
		case []string:
			return v, nil
		case string:
			return []string{v}, nil
		case *string:
			return []string{*v}, nil
		default:
			str := fmt.Sprintf("%v", d.Value)
			return []string{str}, nil
		}

	}
	return nil, nil
}

// StringArrayToMap converts a string array to a map where the keys are the array elements
func StringArrayToMap(_ context.Context, d *TransformData) (interface{}, error) {
	result := map[string]bool{}
	switch labels := d.Value.(type) {
	case []string:
		if labels == nil {
			return result, nil
		}
		for _, i := range labels {
			result[i] = true
		}
		return result, nil
	default:
		t := reflect.TypeOf(d.Value).Name()
		return nil,
			fmt.Errorf("StringArrayToMap transform requires the input to be []string, got %s", t)
	}

}

// QualValue takes the column name from the transform data param and retrieves any quals for it
// If the quals is a single equals quals it returns it
// If there are any other quals and error is returned
func QualValue(ctx context.Context, d *TransformData) (interface{}, error) {
	columnQuals := d.KeyColumnQuals[d.Param.(string)]
	if len(columnQuals) == 0 {
		return nil, nil
	}
	if !columnQuals.SingleEqualsQual() {
		return nil, fmt.Errorf("FromQual transform can only be called if there is a singe equals qual for the given column")
	}
	qualValue := grpc.GetQualValue(columnQuals[0].Value)
	return qualValue, nil
}
