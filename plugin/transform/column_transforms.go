package transform

import (
	"context"
	"log"

	"github.com/turbot/steampipe-plugin-sdk/v2/plugin/quals"
)

// TransformData is the input to a transform function.
type TransformData struct {
	// an optional parameter
	Param interface{}
	// the value to be transformed
	Value interface{}
	// a data object containing the source data for this column
	HydrateItem interface{}
	// all hydrate results
	HydrateResults map[string]interface{}
	// the column this transform is generating
	ColumnName string
	// the 'matrix item' associated with this row
	MatrixItem map[string]interface{}
	// KeyColumnQuals will be populated with the quals as a map of column name to an array of quals for that column
	KeyColumnQuals map[string]quals.QualSlice
}

// TransformFunc is a function to transform a data value from the api value to a column value
// parameters are: value, parent json object, param
// returns the transformed HydrateItem
type TransformFunc func(context.Context, *TransformData) (interface{}, error)
type GetSourceFieldFunc func(interface{}) string

// ColumnTransforms struct defines the data transforms required to map from a JSON value to a column value
type ColumnTransforms struct {
	// a list of transforms to apply to the data
	Transforms []*TransformCall
	// should this transform chain start with the default transform for the column
	ApplyDefaultTransform bool
}

func (t *ColumnTransforms) Execute(ctx context.Context, transformData *TransformData, defaultTransform *ColumnTransforms) (interface{}, error) {
	var value interface{}
	var err error
	if t.ApplyDefaultTransform {
		log.Printf("[TRACE] ColumnTransforms.Execute - running default transforms first\n")
		if value, err = callTransforms(ctx, value, transformData, defaultTransform.Transforms); err != nil {
			return nil, err
		}
	}
	return callTransforms(ctx, value, transformData, t.Transforms)
}

func callTransforms(ctx context.Context, value interface{}, transformData *TransformData, transforms []*TransformCall) (interface{}, error) {
	for _, tr := range transforms {
		var err error
		value, err = tr.Execute(ctx, value, transformData)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}
