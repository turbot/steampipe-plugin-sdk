// Transform package provides the ability to transform data from APIs. It contains transform functions which can be chained together to get the desired values
package transform

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/url"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/ghodss/yaml"
	"github.com/iancoleman/strcase"
	"github.com/turbot/go-kit/types"

	pluralize "github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/helpers"
)

///////////////////////
// Transform primitives
// predefined transform functions that may be chained together

// FieldValue function is intended for the start of a transform chain.
// This returns a field value of either the hydrate call result (if present)  or the root item if not
// the field name is in the 'Param'
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

	for _, propertyPath := range fieldNames {
		fieldValue, ok := helpers.GetNestedFieldValueFromInterface(item, propertyPath)
		if ok {
			return fieldValue, nil

		}

	}
	pluralize := pluralize.NewClient()
	log.Printf("[TRACE] failed to retrieve value for property %s %s\n", pluralize.Pluralize("path", len(fieldNames), false), fmt.Sprintf(strings.Join(fieldNames[:], " or ")))

	return nil, nil
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
	propertyPath := lintName(strcase.ToCamel(d.ColumnName))
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
	log.Printf("[DEBUG] MatrixItemValue key %s metadata %v", metadataKey, d.MatrixItem)

	return d.MatrixItem[metadataKey], nil
}

// modify the name to make common intialialisms upper case
func lintName(name string) (should string) {
	// Fast path for simple cases: "_" and all lowercase.
	if name == "_" {
		return name
	}
	allLower := true
	for _, r := range name {
		if !unicode.IsLower(r) {
			allLower = false
			break
		}
	}
	if allLower {
		return name
	}

	// Split camelCase at any lower->upper transition, and split on underscores.
	// Check each word for common initialisms.
	runes := []rune(name)
	w, i := 0, 0 // index of start of word, scan
	for i+1 <= len(runes) {
		eow := false // whether we hit the end of a word
		if i+1 == len(runes) {
			eow = true
		} else if runes[i+1] == '_' {
			// underscore; shift the remainder forward over any run of underscores
			eow = true
			n := 1
			for i+n+1 < len(runes) && runes[i+n+1] == '_' {
				n++
			}

			// Leave at most one underscore if the underscore is between two digits
			if i+n+1 < len(runes) && unicode.IsDigit(runes[i]) && unicode.IsDigit(runes[i+n+1]) {
				n--
			}

			copy(runes[i+1:], runes[i+n+1:])
			runes = runes[:len(runes)-n]
		} else if unicode.IsLower(runes[i]) && !unicode.IsLower(runes[i+1]) {
			// lower->non-lower
			eow = true
		}
		i++
		if !eow {
			continue
		}

		// [w,i) is a word.
		word := string(runes[w:i])
		if u := strings.ToUpper(word); commonInitialisms[u] {
			// Keep consistent case, which is lowercase only at the start.
			if w == 0 && unicode.IsLower(runes[w]) {
				u = strings.ToLower(u)
			}
			// All the common initialisms are ASCII,
			// so we can replace the bytes exactly.
			copy(runes[w:], []rune(u))
		} else if w > 0 && strings.ToLower(word) == word {
			// already all lowercase, and not the first word, so uppercase the first character.
			runes[w] = unicode.ToUpper(runes[w])
		}
		w = i
	}
	return string(runes)
}

// commonInitialisms is a set of common initialisms.
// Only add entries that are highly unlikely to be non-initialisms.
// For instance, "ID" is fine (Freudian code is rare), but "AND" is not.
var commonInitialisms = map[string]bool{
	"ACL":   true,
	"API":   true,
	"ASCII": true,
	"CPU":   true,
	"CSS":   true,
	"DNS":   true,
	"EOF":   true,
	"GUID":  true,
	"HTML":  true,
	"HTTP":  true,
	"HTTPS": true,
	"ID":    true,
	"IP":    true,
	"JSON":  true,
	"LHS":   true,
	"QPS":   true,
	"RAM":   true,
	"RHS":   true,
	"RPC":   true,
	"SLA":   true,
	"SMTP":  true,
	"SQL":   true,
	"SSH":   true,
	"TCP":   true,
	"TLS":   true,
	"TTL":   true,
	"UDP":   true,
	"UI":    true,
	"UID":   true,
	"UUID":  true,
	"URI":   true,
	"URL":   true,
	"UTF8":  true,
	"VM":    true,
	"XML":   true,
	"XMPP":  true,
	"XSRF":  true,
	"XSS":   true,
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
			log.Printf("[TRACE] FieldValueTag for column %s, found matching '%s' tag on field %s", d.ColumnName, d.Param, field.Name)
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
	log.Printf("[TRACE] NullIfEqualParam value %v, param %v equal %v", d.Value, d.Param, d.Value == d.Param)
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
		log.Printf("[TRACE] NullIfZeroValue column %s is zero\n", d.ColumnName)
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

// UnixToTimestamp convert unix time format to RFC3339 format
func UnixToTimestamp(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value != nil {
		epochTime, err := types.ToFloat64(d.Value)
		if err != nil {
			return nil, err
		}
		if epochTime == 0 {
			return nil, nil
		}
		sec, dec := math.Modf(epochTime)
		timestamp := time.Unix(int64(sec), int64(dec*(1e9)))
		timestampRFC3339Format := timestamp.Format(time.RFC3339)
		return timestampRFC3339Format, nil
	}
	return nil, nil
}

// UnixMsToTimestamp convert unix time in milliseconds to RFC3339 format
func UnixMsToTimestamp(_ context.Context, d *TransformData) (interface{}, error) {
	if d.Value != nil {
		epochTime, err := types.ToInt64(d.Value)
		if err != nil {
			return nil, err
		}
		if epochTime == 0 {
			return nil, nil
		}
		timeIn := time.Unix(0, epochTime*int64(time.Millisecond))
		timestampRFC3339Format := timeIn.Format(time.RFC3339)
		return timestampRFC3339Format, nil
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

// QualValue takes the column name from the transform data param and returns the value of the column
func QualValue(ctx context.Context, d *TransformData) (interface{}, error) {
	value := d.KeyColumnQuals[d.Param.(string)]
	return value, nil
}
