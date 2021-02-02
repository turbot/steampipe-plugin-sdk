package transform

import (
	"context"
	"log"

	"github.com/turbot/go-kit/helpers"
)

// TransformData ::  the input to a transform function
type TransformData struct {
	// an optional parameter
	Param interface{}
	// the value to be transformed
	Value interface{}
	// a data object containing the the source data for this column
	HydrateItem interface{}
	// all hydrate results
	HydrateResults map[string]interface{}
	// the column this transform is generating
	ColumnName string
	// the 'fetch metadata' associated with this row
	// (this is plugin dependent)
	FetchMetadata map[string]interface{}
}

// TransformFunc :: function to transform a data value from the api value to a column value
// parameters are: value, parent json object, param
// returns the transformed HydrateItem
type TransformFunc func(context.Context, *TransformData) (interface{}, error)
type GetSourceFieldFunc func(interface{}) string

// ColumnTransforms :: a struct defining the data transforms required to map from a JSON value to a column value
type ColumnTransforms struct {
	// a list of transforms to apply to the data
	Transforms []*TransformCall
	// should this transform chain start with the default transform for the column
	ApplyDefaultTransform bool
}

func (t *ColumnTransforms) Execute(ctx context.Context, hydrateItem interface{}, hydrateResults map[string]interface{}, defaultTransform *ColumnTransforms, columnName string) (interface{}, error) {
	var value interface{}
	var err error
	if t.ApplyDefaultTransform {
		log.Printf("[TRACE] ColumnTransforms.Execute - running default transforms first\n")
		if value, err = callTransforms(ctx, value, hydrateItem, hydrateResults, defaultTransform.Transforms, columnName); err != nil {
			return nil, err
		}
	}
	return callTransforms(ctx, value, hydrateItem, hydrateResults, t.Transforms, columnName)
}

func callTransforms(ctx context.Context, value interface{}, hydrateItem interface{}, hydrateResults map[string]interface{}, transforms []*TransformCall, columnName string) (interface{}, error) {
	for _, tr := range transforms {
		log.Printf("[TRACE] executeTransform %s\n", helpers.GetFunctionName(tr.Transform))
		var err error
		value, err = tr.Execute(ctx, value, hydrateItem, hydrateResults, columnName)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}
